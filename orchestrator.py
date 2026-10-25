#!/usr/bin/env python3
"""GreenOps autonomous orchestrator (v3.2): risk-averse DP, generalised for unseen scenarios.

Runs unattended from start to finish. Nothing is specific to one workload pack:
* Catalog loaded at start from the portal's public /api/catalog (fallback: --catalog file).
* Time base = the run's start (READY virtual time), persisted for restarts. Horizon and
  electricity areas are derived from the catalog. No workload IDs, zones or dates in code.
* Credentials from --config (exo CLI JSON) and/or env vars (EXOSCALE_API_KEY/SECRET,
  GREENOPS_CLOUD_ENDPOINT, GREENOPS_GRID_SECRET/ENDPOINT). Without grid credentials it
  degrades to carbon-agnostic planning (efficiency x PUE).
* Self-healing: reconciles against the live VM list every few seconds (adopts its own
  VMs, deletes VMs of delivered workloads), catches per-iteration errors, and restarts
  itself after a crash (recovering state from the server).
* State/logs go to $GREENOPS_STATE_DIR (default /tmp/greenops-<run>) so it runs in a
  read-only container with only /tmp writable.

Algorithm (per workload DP, coordinated by quota prices; see CLAUDE.md / knowledge/):
* DP over (hour, progress, created?) choosing workers per hour, cost = risk-adjusted
  carbon CVaR_a(CI_t) = mu_t + c_a*sigma(lead) (alpha 0.8) + Lagrangian quota holding prices.
* Hard deadline with planned slack max(2h, 10% window) + buffer from measured status
  staleness (CVaR 95%). Must-run override when slack is exhausted.
* Sole-option protection: a GPU workload whose legal configs use a single GPU family is
  "protected". Other workloads may use that family only if they finish before its release,
  and its quota (vCPU/VM/cards) is reserved from shortly before its release.
* Infeasible fallback waits for a deadline-feasible size while time remains; only at
  the last moment does it take the fastest that fits (partial credit).
"""
import argparse
import base64
import hashlib
import hmac
import json
import math
import os
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from urllib.parse import urlparse

import numpy as np
import requests
from exoscale.api.v2 import Client

ALPHA = 0.8
C_ALPHA = NormalDist().pdf(NormalDist().inv_cdf(ALPHA)) / (1 - ALPHA)
CPU_FAMILY = "cpu"
WORKER_OPTIONS = (1, 2, 4)
TOP_AREAS = 3
REPLAN_WALL_S = 0.5          # periodic fallback re-plan (~6 sim-min @720x); events re-plan immediately
FULL_SEARCH_PER_PLAN = 2     # uncreated workloads whose full config search is refreshed per re-plan (rotation)
RECONCILE_WALL_S = 4.0
DWELL_H = 0.75
STATUS_TIMEOUT = 3
INF = float("inf")
VM_PREFIX = "dp-"
# Capacity protection: "urgency" = generic deadline-urgency reservation for ANY pending
# workload; "family" = original sole-GPU-family rule (+ others finish before its release); "none".
PROTECT_MODE = os.environ.get("GREENOPS_PROTECT", "urgency")
# v4 behaviour switches (off = v3 baseline behaviour)
OUTPUT_DWELL_EXEMPT = False   # start VMs for the output phase immediately (no hold time)
STALE_STOP_FREEZE_H = None    # do not stop VMs while status is older than this (sim-h)
CREATE_GUARD = False          # fallback never creates earlier than release - setup - 1h
BLOCK_LOG = False             # log blocked admissions (throttled)
RESERVE_SMALLEST_FEASIBLE = False  # urgency reservation: least-quota feasible config (deadlock fix)
SLACK_MIN_H, SLACK_FRAC = 2.0, 0.10   # planned deadline slack
BEYOND_SIGMA = 0.20          # relative uncertainty beyond the 24 h forecast
EFFICIENT_TIEBREAK = False    # exp: near-ties (within EFF_TOL) go to the lowest energy per unit of work, not the fastest
EFF_TOL = 0.02
SETUP_PAUSE = False           # exp: setup (boot+input) may be paused and scheduled into clean hours
GPU_JOINT = False             # exp: workloads competing for a GPU card family are planned jointly on a card timetable
USE_FC6 = True                # fetch the 6 h forecast too; where it differs from the 24 h one it wins for hours 1-6
SCORE_WEIGHTING = False       # v3.3: value carbon in leaderboard points (marginal points per kg, per workload)
SCORE_ORDER = False           # v3.3: contested capacity goes to urgent, then highest-points workloads
DECISION_PERIOD_H = None      # v3.2: start/stop only at fixed sim-time decision points (e.g. 0.25 = 15 sim-min)
STATUS_POLL_S = 1.0           # wall seconds between status polls


def log(*a):
    print(f"[{time.strftime('%H:%M:%S')}]", *a, flush=True)


def parse_ts(ts):
    """ISO timestamp -> aware datetime (handles 1-5 digit fractions on Python 3.10)."""
    s = ts.replace("Z", "+00:00")
    base, sep, rest = s.partition(".")
    if sep:
        n = 0
        while n < len(rest) and rest[n].isdigit():
            n += 1
        s = f"{base}.{(rest[:n] + '000000')[:6]}{rest[n:]}"
    return datetime.fromisoformat(s)


# ------------------------------------------------------------------ setup ----
CLOUD_TIMEOUT = 20


def with_timeout(c, timeout=CLOUD_TIMEOUT):
    """The Exoscale client sets no HTTP timeout; a slow portal can hang a call forever."""
    sess = c.http_client
    orig = sess.request

    def request(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return orig(method, url, **kw)
    sess.request = request
    return c


def load_account(path):
    if path and Path(path).exists():
        cfg = json.loads(Path(path).read_text())
        a = next(x for x in cfg["accounts"] if x["name"] == cfg["defaultaccount"])
        return {"key": a["key"], "secret": a["secret"], "endpoint": a["endpoint"]}
    return {"key": os.environ["EXOSCALE_API_KEY"], "secret": os.environ["EXOSCALE_API_SECRET"],
            "endpoint": os.environ["GREENOPS_CLOUD_ENDPOINT"]}


def check_endpoint(url, root=None):
    u = urlparse(url)
    parts = u.path.strip("/").split("/")
    if u.scheme not in ("http", "https") or len(parts) not in (4, 5) or parts[0] != "runs" \
            or parts[2] != "cloud" or parts[-1] != "v2" or (u.hostname or "").endswith("exoscale.com"):
        raise ValueError("not a challenge run cloud endpoint")
    if root:
        r = urlparse(root)
        if (u.scheme, u.netloc) != (r.scheme, r.netloc) or parts[1] != r.path.strip("/").split("/")[1]:
            raise ValueError("endpoint escaped the configured run")
    return url


def load_catalog(account, fallback):
    u = urlparse(account["endpoint"])
    try:
        r = requests.get(f"{u.scheme}://{u.netloc}/api/catalog", timeout=10)
        r.raise_for_status()
        cat = r.json()
        if cat.get("workloads") and cat.get("profiles") and cat.get("zones"):
            return cat, "portal /api/catalog"
    except Exception as e:
        log("catalog API unavailable:", type(e).__name__)
    return json.loads(Path(fallback).read_text()), str(fallback)


def load_rules(account, fallback):
    """Public scoring rules (reference energy per workload); bundled copy as fallback."""
    u = urlparse(account["endpoint"])
    try:
        r = requests.get(f"{u.scheme}://{u.netloc}/api/rules", timeout=10)
        r.raise_for_status()
        ref = r.json()["rules"]["reference_energy_kwh"]
        return {k: float(v) for k, v in ref.items()}, "portal /api/rules"
    except Exception as e:
        log("rules API unavailable:", type(e).__name__)
    try:
        ref = json.loads(Path(fallback).read_text())["rules"]["reference_energy_kwh"]
        return {k: float(v) for k, v in ref.items()}, str(fallback)
    except Exception:
        return {}, "none"


def score_weights(ref_kwh, carbon_kg, pending):
    """Relative marginal leaderboard points per kg for each pending workload, from the fitted score
    5000 + 20000*sum_k w_k[3/4 R_k/(R_k+C_k) + 1/4 R/(R+C)], w_k = R_k/R, R_k = ref_kWh*0.2 kg."""
    if not ref_kwh or not pending:
        return {}
    R = {k: v * 0.2 for k, v in ref_kwh.items()}
    SR = sum(R.values())
    C = sum(carbon_kg.get(k, 0.0) for k in R)
    m = {}
    for k in pending:
        if k not in R:
            continue
        Rk, Ck = R[k], carbon_kg.get(k, 0.0)
        m[k] = 0.75 * (Rk / SR) * Rk / (Rk + Ck) ** 2 + 0.25 * SR / (SR + C) ** 2
    mean = sum(m.values()) / len(m) if m else 1.0
    return {k: v / mean for k, v in m.items()}


# ---------------------------------------------------------------- catalog ----
class Catalog:
    def __init__(self, d, t0):
        self.t0 = t0
        self.zones = {z["slug"]: z for z in d["zones"]}
        self.profiles = {p["id"]: p for p in d["profiles"]}
        self.workloads = {w["id"]: w for w in d["workloads"]}
        self.caps = {"vcpu": d["max_vcpus"], "vm": d["max_vms"],
                     **{f"card:{k}": v for k, v in d.get("gpu_family_quotas", {}).items()}}
        self.hmax = int(math.ceil(max(self.h(w["deadline_at"]) for w in d["workloads"]))) + 36
        self.areas = sorted({z["electricity_area"] for z in d["zones"]})
        self._legal = {}
        # sole-option GPU workloads -> protected family
        self.protected = {}
        for wid in self.workloads:
            fams = {p["family"] for p in self.profiles.values()
                    for zs in p["zones"] if self.legal(wid, zs, p)}
            if len(fams) == 1 and next(iter(fams)) != CPU_FAMILY:
                self.protected[wid] = next(iter(fams))

    def h(self, ts):
        return (parse_ts(ts) - self.t0).total_seconds() / 3600

    def legal(self, wid, zone_slug, p):
        key = (wid, zone_slug, p["id"])
        if key in self._legal:
            return self._legal[key]
        w, z = self.workloads[wid], self.zones.get(zone_slug)
        ok = z is not None and zone_slug in p["zones"]
        ok = ok and z["country"] not in w.get("forbidden_countries", [])
        ok = ok and (not w.get("allowed_countries") or z["country"] in w["allowed_countries"])
        ok = ok and not (w.get("residency") == "EU" and not z["eu_member"])
        ok = ok and not (w.get("residency") == "CH" and z["country"] != "CH")
        if ok and w.get("allowed_profile_ids"):
            ok = p["id"] in w["allowed_profile_ids"]
        elif ok:
            ok = p["family"] in w.get("allowed_families", [p["family"]]) and \
                p.get("gpu_memory_gib", 0) >= w.get("minimum_gpu_memory_gib", 0)
        self._legal[key] = ok
        return ok

    def configs(self, wid):
        w = self.workloads[wid]
        best = {}
        for p in self.profiles.values():
            for zs in p["zones"]:
                if not self.legal(wid, zs, p):
                    continue
                key = (self.zones[zs]["electricity_area"], p["id"])
                if key not in best or float(self.zones[zs]["pue"]) < float(self.zones[best[key]]["pue"]):
                    best[key] = zs
        out = [Config(self, wid, zs, pid, n) for (area, pid), zs in best.items()
               for n in WORKER_OPTIONS if n <= w.get("max_workers", 1)]
        out = [c for c in out if all(v <= self.caps.get(r, 10 ** 9) for r, v in c.use.items())]  # e.g. 8 cards > quota 4
        # drop CPU profiles strictly dominated (less efficient AND slower than another in same area)
        keep = []
        for c in out:
            dominated = any(o.area == c.area and o.n == c.n and o.family == c.family == CPU_FAMILY
                            and o.eff < c.eff and o.R >= c.R for o in out)
            if not dominated:
                keep.append(c)
        return keep


class Config:
    def __init__(self, cat, wid, zone, pid, n):
        w, p, z = cat.workloads[wid], cat.profiles[pid], cat.zones[zone]
        self.wid, self.zone, self.pid, self.n = wid, zone, pid, n
        self.area, self.pue = z["electricity_area"], float(z["pue"])
        self.family, self.size = p["family"], p["size"]
        self.vcpu, self.cards, self.watts = p["cpu_cores"], p.get("gpu_count", 0), float(p["active_watts"])
        unit = 1 if str(w["work_unit"]).endswith("hour") else 3600      # tflop: TFLOP/s (measured)
        self.R = float(p["throughput"]) * unit
        self.req = float(w["required_work"])
        self.sync = float(w.get("sync_overhead", 0))
        self.rel_h, self.dl_h = cat.h(w["release_at"]), cat.h(w["deadline_at"])
        for pid_, fam in (cat.protected.items() if PROTECT_MODE == "family" else ()):  # family rule only
            if pid_ != wid and fam == self.family:
                prel = cat.h(cat.workloads[pid_]["release_at"])
                if self.rel_h < prel:
                    self.dl_h = min(self.dl_h, prel)
        self.S = (p.get("boot_seconds", 0) + w.get("input_seconds", 0)) / 3600
        self.s_h = max(1, math.ceil(self.S - 1e-9))
        self.O = w.get("output_seconds", 0) / 3600
        self.o_h = max(1, math.ceil(self.O - 1e-9))
        self.eff = self.watts * self.pue / self.R
        self.use = {"vcpu": n * self.vcpu, "vm": n}
        if self.cards:
            self.use[f"card:{self.family}"] = n * self.cards

    def rate(self, k):
        return 0.0 if k == 0 else k * self.R / (1 + self.sync * (k - 1))

    def speed(self):
        return self.rate(self.n)

    def energy_per_unit(self):
        """kWh-equivalent per unit of work incl. PUE and worker coordination overhead."""
        return self.n * self.watts * self.pue / self.rate(self.n)

    def label(self):
        return f"{self.zone}/{self.family}.{self.size}x{self.n}"

    def slack(self):
        return max(SLACK_MIN_H, SLACK_FRAC * (self.dl_h - self.rel_h))

    def g_per_unit(self, ci):
        return self.n * self.watts * self.pue / 1000 * ci / self.rate(self.n)


def pick_green_fast(cands, ci, tol=0.03):
    """Lowest carbon per unit of work; among options within `tol` of the best (e.g. the
    RTX small/medium/large sizes, which have equal energy per TFLOP and equal vCPU-hours),
    take the fastest so scarce accelerators are fully used and deadlines are safe."""
    if not cands:
        return None
    g = {id(c): c.g_per_unit(ci[c.area]) for c in cands}
    best = min(g.values())
    near = [c for c in cands if g[id(c)] <= best * (1 + tol)]
    if EFFICIENT_TIEBREAK:
        return min(near, key=lambda c: (c.energy_per_unit(), g[id(c)]))
    return max(near, key=lambda c: (c.speed(), -g[id(c)]))


# ------------------------------------------------------------ electricity ----
class Grid:
    def __init__(self, cat, endpoint, secret):
        self.cat = cat
        self.ep = endpoint.rstrip("/") if endpoint else None
        self.hdr = {"auth-token": secret} if secret else None
        self.obs = {a: {} for a in cat.areas}
        self.fc = {a: {} for a in cat.areas}
        self.fc6 = {a: {} for a in cat.areas}
        self.fc6_differs = False

    def _merge6(self, a, f6):
        """Keep the 6 h forecast; note (once) when it differs from the 24 h forecast's first hours."""
        self.fc6[a] = f6
        diff = [h for h, v in f6.items() if h in self.fc[a] and abs(self.fc[a][h] - v) > 1e-6]
        if diff and not self.fc6_differs:
            self.fc6_differs = True
            log(f"NOTE: 6 h forecast differs from the 24 h forecast ({a}, {len(diff)} h) - using it for hours 1-6")

    def _get(self, path, area, **params):
        for _ in range(3):
            try:
                r = requests.get(self.ep + path, params={"zone": area, **params}, headers=self.hdr,
                                 timeout=8, allow_redirects=False)
                if r.status_code == 429:
                    time.sleep(1)
                    continue
                if r.status_code in (404, 422):
                    return None
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                time.sleep(0.5)
        return None

    def refresh(self, pool):
        if not self.ep:
            return

        def one(a):
            return a, self._get("/carbon-intensity/forecast", a, horizonHours=24), \
                self._get("/carbon-intensity/history", a), \
                (self._get("/carbon-intensity/forecast", a, horizonHours=6) if USE_FC6 else None)
        for a, f, hist, f6 in pool.map(one, self.cat.areas):
            if hist:
                for pt in hist.get("history", []):
                    self.obs[a][round(self.cat.h(pt["datetime"]))] = float(pt["carbonIntensity"])
            if f and f.get("forecast"):
                self.fc[a] = {round(self.cat.h(pt["datetime"])): float(pt["carbonIntensity"]) for pt in f["forecast"]}
            if f6 and f6.get("forecast"):
                self._merge6(a, {round(self.cat.h(pt["datetime"])): float(pt["carbonIntensity"]) for pt in f6["forecast"]})

    def series(self, now_h):
        H, out = self.cat.hmax, {}
        for a in self.cat.areas:
            mu, rel = np.zeros(H), np.zeros(H)
            known = dict(self.obs[a])
            known.update(self.fc[a])
            if USE_FC6 and self.fc6_differs:
                known.update(self.fc6.get(a, {}))          # fresher short-range values win for hours 1-6
            fallback = float(np.mean(list(known.values()))) if known else 300.0
            for h in range(H):
                if h in known:
                    mu[h] = known[h]
                    rel[h] = (0.02 + 0.006 * max(0, h - now_h)) if h in self.fc[a] else 0.0
                else:
                    mu[h] = mu[h - 24] if h >= 24 and mu[h - 24] > 0 else fallback
                    rel[h] = BEYOND_SIGMA
            out[a] = (mu, mu * (1 + C_ALPHA * rel))
        return out


# --------------------------------------------------------------------- DP ----
def solve(cfg, now_h, p_frac, created, ready_h, ci_risk, prices, buffer, H, hold_ok=None, wscale=1.0):
    start_h = int(math.floor(now_h))
    first_frac = max(0.05, 1.0 - (now_h - start_h))
    t_fin_max = int(math.floor(cfg.dl_h - buffer - cfg.slack() - cfg.O - 1))
    if t_fin_max < start_h or t_fin_max + cfg.o_h >= H:
        return INF, None
    G = max(20, min(4000, int(round(cfg.req / cfg.R * 20))))
    p0 = min(G - 1, int(math.floor(p_frac * G)))
    e = cfg.watts * cfg.pue / 1000.0 * ci_risk * wscale
    hold = np.zeros(H)
    for r, u in cfg.use.items():
        if r in prices:
            hold += prices[r] * u
    if hold_ok is not None:                       # ledger: cannot hold the VM(s) where capacity is booked
        hold = np.where(hold_ok[:H], hold, INF)
    rel_int = math.ceil(cfg.rel_h - 1e-9)

    def out_cost(t):
        c = 0.0
        for i, u in enumerate(range(t + 1, t + 1 + cfg.o_h)):
            frac = 1.0 if i < cfg.o_h - 1 else cfg.O - (cfg.o_h - 1)
            c += cfg.n * e[u] * frac + hold[u]
        return c

    idx = np.arange(G)
    V = {t_fin_max + 1: np.full(G, INF)}
    pol, stp = {}, {}
    for t in range(t_fin_max, start_h - 1, -1):
        frac = first_frac if t == start_h else 1.0
        nxt, oc = V[t + 1], out_cost(t)
        best, arg = np.full(G, INF), np.zeros(G, dtype=int)
        exact = [cfg.rate(k) * frac / cfg.req * G for k in range(cfg.n + 1)]
        stp[t] = [int(math.floor(x + 1e-9)) for x in exact]
        for k in range(cfg.n + 1):
            if k > 0 and t < rel_int:
                break
            if k > 0 and stp[t][k] == 0:
                continue
            # charge energy only for the progress actually counted: flooring the steps of a partial
            # (current) hour otherwise makes "now" look worse than any future hour -> procrastination/flapping
            used = (stp[t][k] / exact[k]) if k > 0 else 1.0
            c = (k * e[t] * used + hold[t]) * frac
            newp = idx + stp[t][k]
            # finishing hour: only the fraction of the hour actually needed is paid (VMs are
            # deleted at delivery); charging the full hour made small progress now look worthless
            need = np.minimum(1.0, (G - idx) / max(stp[t][k], 1)) if k > 0 else 1.0
            c_fin = (k * e[t] * used * need + hold[t]) * frac
            val = np.where(newp >= G, c_fin + oc, c + nxt[np.minimum(newp, G - 1)])
            better = val < best
            best, arg = np.where(better, val, best), np.where(better, k, arg)
        V[t], pol[t] = best, arg

    def extract(t, p):
        run = {}
        while t <= t_fin_max:
            k = int(pol[t][p])
            run[t] = k
            p += stp[t][k]
            if p >= G:
                return run, t
            t += 1
        return run, None

    if created:
        r0 = max(start_h, math.ceil(ready_h - 1e-9))
        if r0 > t_fin_max:
            return INF, None
        val = sum(cfg.n * e[u] + hold[u] for u in range(start_h, r0)) + V[r0][p0]
        if not math.isfinite(val):
            return INF, None
        run, fin = extract(r0, p0)
        return val, {"create_h": None, "run": run, "finish_h": fin}
    best_val, best_c = INF, None
    for tc in range(start_h, t_fin_max + 1):
        r0 = tc + cfg.s_h
        if r0 > t_fin_max:
            break
        stage = sum(cfg.n * e[u] * (1.0 if u < r0 - 1 else cfg.S - (cfg.s_h - 1)) + hold[u] for u in range(tc, r0))
        val = stage + V[r0][0]
        if val < best_val:
            best_val, best_c = val, tc
    if best_c is None or not math.isfinite(best_val):
        return INF, None
    run, fin = extract(best_c + cfg.s_h, 0)
    return best_val, {"create_h": best_c, "run": run, "finish_h": fin}


def solve_ps(cfg, now_h, p_frac, created, setup_left_h, ci_risk, prices, buffer, H, hold_ok=None, wscale=1.0):
    """DP with PAUSABLE setup: progress = [setup steps | compute steps]. During setup all n VMs stage
    together (k in {0, n}, no release constraint, full power); paused VMs hold quota. Compute as solve().
    Returns (value, plan) with plan["run"][t] = VMs running in hour t (setup or compute)."""
    start_h = int(math.floor(now_h))
    first_frac = max(0.05, 1.0 - (now_h - start_h))
    t_fin_max = int(math.floor(cfg.dl_h - buffer - cfg.slack() - cfg.O - 1))
    if t_fin_max < start_h or t_fin_max + cfg.o_h >= H:
        return INF, None
    rem = max(1e-9, (1.0 - p_frac) * cfg.req)
    Gc = max(20, min(4000, int(round(rem / cfg.R * 20))))
    setup_h = cfg.S if not created else max(0.0, setup_left_h)
    Gs = int(math.ceil(setup_h * 20 - 1e-9))
    G = Gs + Gc
    e = cfg.watts * cfg.pue / 1000.0 * ci_risk * wscale
    hold = np.zeros(H)
    for r, u in cfg.use.items():
        if r in prices:
            hold += prices[r] * u
    if hold_ok is not None:
        hold = np.where(hold_ok[:H], hold, INF)
    rel_int = math.ceil(cfg.rel_h - 1e-9)

    def out_cost(t):
        c = 0.0
        for i, u in enumerate(range(t + 1, t + 1 + cfg.o_h)):
            frac = 1.0 if i < cfg.o_h - 1 else cfg.O - (cfg.o_h - 1)
            c += cfg.n * e[u] * frac + hold[u]
        return c

    idx = np.arange(G)
    in_setup = idx < Gs
    q = np.maximum(0, idx - Gs)
    V = {t_fin_max + 1: np.full(G, INF)}
    pol, stp, sstp = {}, {}, {}
    for t in range(t_fin_max, start_h - 1, -1):
        frac = first_frac if t == start_h else 1.0
        nxt, oc = V[t + 1], out_cost(t)
        best = hold[t] * frac + nxt                    # k = 0: paused (or not yet started)
        arg = np.zeros(G, dtype=int)
        # setup with all n VMs (20 steps per hour)
        s_ex = 20.0 * frac
        s_st = int(math.floor(s_ex + 1e-9))
        sstp[t] = s_st
        if s_st > 0 and Gs > 0:
            need_s = np.minimum(1.0, np.maximum(Gs - idx, 0) / s_st)     # last setup hour: only the part needed
            c_s = (cfg.n * e[t] * (s_st / s_ex) * need_s + hold[t]) * frac
            newp = np.minimum(idx + s_st, Gs)
            val = np.where(in_setup, c_s + nxt[np.minimum(newp, G - 1)], INF)
            better = val < best
            best, arg = np.where(better, val, best), np.where(better, cfg.n, arg)
        # compute
        exact = [cfg.rate(k) * frac / rem * Gc for k in range(cfg.n + 1)]
        stp[t] = [int(math.floor(x + 1e-9)) for x in exact]
        if t >= rel_int:
            for k in range(1, cfg.n + 1):
                if stp[t][k] == 0:
                    continue
                used = stp[t][k] / exact[k]
                c = (k * e[t] * used + hold[t]) * frac
                newq = q + stp[t][k]
                need = np.minimum(1.0, (Gc - q) / stp[t][k])
                c_fin = (k * e[t] * used * need + hold[t]) * frac
                val = np.where(newq >= Gc, c_fin + oc, c + nxt[np.minimum(Gs + newq, G - 1)])
                val = np.where(in_setup, INF, val)
                better = val < best
                # in setup k=n means 'stage', in compute k means workers: encode compute as k
                best, arg = np.where(better, val, best), np.where(better, k, arg)
        V[t], pol[t] = best, arg

    def extract(t, pp):
        run = {}
        while t <= t_fin_max:
            k = int(pol[t][pp])
            run[t] = k
            if pp < Gs:
                pp = min(Gs, pp + (sstp[t] if k else 0))
            else:
                pp += stp[t][k]
                if pp >= G:
                    return run, t
            t += 1
        return run, None

    if created:
        val = V[start_h][0]
        if not math.isfinite(val):
            return INF, None
        run, fin = extract(start_h, 0)
        return val, {"create_h": None, "run": run, "finish_h": fin}
    best_val, best_c = INF, None
    for tc in range(start_h, t_fin_max + 1):        # create at tc (holding from tc), V[tc][0]
        val = V[tc][0]
        if val <= best_val + 1e-9:                   # ties: create as late as possible (less quota held)
            best_val, best_c = val, tc
    if best_c is None or not math.isfinite(best_val):
        return INF, None
    run, fin = extract(best_c, 0)
    return best_val, {"create_h": best_c, "run": run, "finish_h": fin}


def plan_solve(cfg, now_h, p_frac, created, ready_or_setup, ci_risk, prices, buffer, H, hold_ok=None, wscale=1.0):
    """Dispatch: pausable-setup DP when SETUP_PAUSE, else the original DP (ready_or_setup = ready_h)."""
    if SETUP_PAUSE:
        return solve_ps(cfg, now_h, p_frac, created, ready_or_setup, ci_risk, prices, buffer, H, hold_ok, wscale)
    return solve(cfg, now_h, p_frac, created, ready_or_setup, ci_risk, prices, buffer, H, hold_ok, wscale)


# ---------------------------------------------------------- status poller ----
class StatusPoller(threading.Thread):
    def __init__(self, account):
        super().__init__(daemon=True)
        self.account = account
        self.url = account["endpoint"].removesuffix("/cloud/v2") + "/work/v1/status"
        self.snap, self.wall, self.lock = None, 0.0, threading.Lock()
        self.timeouts, self.stop_flag = 0, False

    def fetch(self, timeout=STATUS_TIMEOUT):
        expires = str(int(time.time()) + 60)
        path = urlparse(self.url).path
        sig = base64.b64encode(hmac.new(self.account["secret"].encode(),
                                        "\n".join(["GET " + path, "", "", "", expires]).encode(),
                                        hashlib.sha256).digest()).decode()
        r = requests.get(self.url, timeout=timeout, allow_redirects=False, headers={
            "Authorization": f'EXO2-HMAC-SHA256 credential={self.account["key"]},expires={expires},signature={sig}'})
        if r.status_code == 429:
            time.sleep(1)
            return None
        r.raise_for_status()
        return r.json()

    def blocking(self):
        for _ in range(60):
            try:
                st = self.fetch(timeout=10)
                if st:
                    with self.lock:
                        self.snap, self.wall = st, time.monotonic()
                    return st
            except requests.RequestException:
                time.sleep(1)
        raise RuntimeError("work status unavailable")

    def run(self):
        while not self.stop_flag:
            t0 = time.monotonic()
            try:
                st = self.fetch()
                if st:
                    with self.lock:
                        self.snap, self.wall = st, time.monotonic()
            except requests.RequestException:
                self.timeouts += 1
            time.sleep(max(0.0, STATUS_POLL_S - (time.monotonic() - t0)))

    def latest(self):
        with self.lock:
            return self.snap, self.wall


# ------------------------------------------------------------- controller ----
class Orchestrator:
    def __init__(self, args):
        self.args = args
        self.account = load_account(args.config)
        check_endpoint(self.account["endpoint"])
        self.poller = StatusPoller(self.account)
        st = self.poller.blocking()
        self.run_id = st["run_id"]
        self.state_dir = Path(os.environ.get("GREENOPS_STATE_DIR", f"/tmp/greenops-{self.run_id[:10]}"))
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.t0 = self._time_base(st)
        raw, src = load_catalog(self.account, args.catalog)
        self.cat = Catalog(raw, self.t0)
        self.cat.ref_kwh, rsrc = load_rules(self.account, Path(args.catalog).with_name("rules.json"))
        log(f"rules from {rsrc}: {len(self.cat.ref_kwh)} reference energies")
        log(f"run {self.run_id} state={st['state']} speed={st['speed_multiplier']} t0={self.t0.isoformat()} "
            f"catalog={src} workloads={len(self.cat.workloads)} protected={self.cat.protected}")
        self.grid = Grid(self.cat, os.environ.get("GREENOPS_GRID_ENDPOINT"), os.environ.get("GREENOPS_GRID_SECRET"))
        if not self.grid.ep:
            log("WARNING: no grid credentials - carbon-agnostic planning (efficiency x PUE only)")
        self.pool = ThreadPoolExecutor(16)
        self.zone_ep = {}
        self.clients, self.templates = {}, {}
        self.reg, self.inflight, self.creating = {}, {}, set()
        self.pending, self.last_toggle = {}, {}
        self.plans, self.series = {}, None
        self.prices = {r: np.zeros(self.cat.hmax) for r in self.cat.caps}
        self.staleness, self.carbon, self.last_carbon_t = [], {}, None
        self.fc_hour, self.fc_thread = -1, None

    def _time_base(self, st):
        f = self.state_dir / "t0.txt"
        if st["state"] == "READY":
            t0 = parse_ts(st["virtual_time"])
            f.write_text(t0.isoformat())
            return t0
        if f.exists():
            return datetime.fromisoformat(f.read_text().strip())
        # restart without state: window starts at UTC midnight of the earliest release
        u = urlparse(self.account["endpoint"])
        try:
            cat = requests.get(f"{u.scheme}://{u.netloc}/api/catalog", timeout=10).json()
        except Exception:
            cat = json.loads(Path(self.args.catalog).read_text())
        first = min(parse_ts(w["release_at"]) for w in cat["workloads"])
        t0 = first.replace(hour=0, minute=0, second=0, microsecond=0)
        log("time base inferred (no saved state):", t0.isoformat())
        return t0

    # -- infrastructure
    def client(self, zone):
        if zone not in self.clients:
            if not self.zone_ep:
                z = with_timeout(Client(self.account["key"], self.account["secret"], url=self.account["endpoint"])).list_zones()
                self.zone_ep = {x["name"]: check_endpoint(x["api-endpoint"], self.account["endpoint"]) for x in z["zones"]}
            self.clients[zone] = with_timeout(Client(self.account["key"], self.account["secret"], url=self.zone_ep[zone]))
        return self.clients[zone]

    def template(self, zone):
        if zone not in self.templates:
            self.templates[zone] = self.client(zone).list_templates(visibility="public")["templates"][0]["id"]
        return self.templates[zone]

    def event(self, kind, **kw):
        with open(self.state_dir / "events.jsonl", "a") as f:
            f.write(json.dumps({"wall": time.time(), "kind": kind, **kw}, default=str) + "\n")

    # -- clock / buffer / quota
    def clock(self):
        st, wall = self.poller.latest()
        vt = self.cat.h(st["virtual_time"])
        if st["state"] == "RUNNING":
            age = (time.monotonic() - wall) * st["speed_multiplier"] / 3600
            return vt + age, age, st
        return vt, 0.0, st

    def buffer(self):
        if len(self.staleness) < 10:
            return 2.0
        d = np.sort(np.array(self.staleness[-300:]))
        return 1.0 + float(d[int(len(d) * 0.95):].mean())

    def used(self):
        u = {r: 0 for r in self.cat.caps}
        for cfg in [r["cfg"] for r in list(self.reg.values())] + list(self.inflight.values()):
            for r, v in cfg.use.items():
                u[r] = u.get(r, 0) + v
        return u

    def reserved(self, now_h, exclude):
        """Quota held back for workloads that must start soon but are not yet created."""
        res = {r: 0 for r in self.cat.caps}
        if PROTECT_MODE == "none":
            return res
        if PROTECT_MODE == "urgency":
            buf = self.buffer()
            for wid, w in self.cat.workloads.items():
                if wid == exclude or wid in self.reg or wid in self.inflight or wid in self.delivered:
                    continue
                rem = self.remaining.get(wid, float(w["required_work"]))
                plan = self.plans.get(wid)
                cands = self.cat.configs(wid)
                if not cands or rem <= 0:
                    continue
                if plan and plan[1] and plan[2]:
                    cfg = plan[1]
                elif RESERVE_SMALLEST_FEASIBLE:     # v3.1: reserve the least-quota config that can still make it
                    feas = [c for c in cands if self.feasible(c, rem, now_h, buf)]
                    cfg = min(feas or cands, key=lambda c: (c.use["vcpu"], -c.speed()))
                else:
                    cfg = max(cands, key=lambda c: c.speed())
                need = rem / cfg.speed() + cfg.O + cfg.S
                latest = cfg.dl_h - buf - need                      # latest safe creation time
                soon = plan and plan[2] and plan[2]["create_h"] is not None and plan[2]["create_h"] <= now_h + 1
                if now_h >= latest - cfg.slack() or soon:
                    for r, v in cfg.use.items():
                        res[r] = res.get(r, 0) + v
            return res
        for wid in self.cat.protected:
            if wid == exclude or wid in self.reg or wid in self.inflight or wid in self.delivered:
                continue
            w = self.cat.workloads[wid]
            plan = self.plans.get(wid)
            single = [c for c in self.cat.configs(wid) if c.n == 1]
            # reserve what the plan wants; with no feasible plan, reserve the fastest single VM
            # (deadline rescue: uses every card of the family)
            cfg = plan[1] if plan and plan[1] and plan[2] else (max(single, key=lambda c: c.speed()) if single else None)
            if cfg is None:
                continue
            if now_h >= self.cat.h(w["release_at"]) - cfg.S - 2:
                for r, v in cfg.use.items():
                    res[r] = res.get(r, 0) + v
        return res

    def fits(self, cfg, now_h):
        u, res = self.used(), self.reserved(now_h, cfg.wid)
        return all(u.get(r, 0) + res.get(r, 0) + v <= self.cat.caps.get(r, 10 ** 9) for r, v in cfg.use.items())

    def feasible(self, c, remaining, now_h, buf, staged=False):
        return remaining / c.rate(c.n) + c.O + (0 if staged else c.S) < c.dl_h - buf - now_h

    # -- planning
    def replan(self, now_h, st, iterations=1, dirty=()):
        """Thread-safe: the periodic planner thread and event triggers share one lock."""
        if not hasattr(self, "plan_lock"):
            self.plan_lock = threading.Lock()
        with self.plan_lock:
            return self._replan(now_h, st, iterations, set(dirty))

    def detect_events(self, prev, st, prev_h, now_h):
        """Reasons to re-plan immediately: release, finish, stop/delete, deadline miss."""
        if prev is None:
            return []
        reasons = []
        for wid, w in self.cat.workloads.items():
            if prev_h < self.cat.h(w["release_at"]) <= now_h:
                reasons.append(f"released {wid}")
        pw = {w["id"]: w for w in prev["workloads"]}
        for w in st["workloads"]:
            o = pw.get(w["id"])
            if o is None:
                continue
            if w["final_output_delivered"] and not o["final_output_delivered"]:
                reasons.append(f"delivered {w['id']}")
            req = float(w["required_work"])
            if float(w["completed_work"]) >= req - 1e-9 > float(o["completed_work"]):
                reasons.append(f"compute-finished {w['id']}")
            if w.get("state") == "deadline_missed" and o.get("state") != "deadline_missed":
                reasons.append(f"deadline-missed {w['id']}")
            before = {a["vm_id"]: a["state"] for a in o["assignments"]}
            after = {a["vm_id"]: a["state"] for a in w["assignments"]}
            for vid, stt in before.items():
                if after.get(vid) is None:
                    reasons.append(f"vm-gone {w['id']}")
                elif after[vid] == "stopped" and stt != "stopped":
                    own = self.pending.get(vid, ("", 0))[0] == "stop" if DECISION_PERIOD_H else False
                    if not own:                              # our own stop is not new information
                        reasons.append(f"stopped {w['id']}")
        return reasons

    def _replan(self, now_h, st, iterations=1, dirty=frozenset()):
        t0 = time.time()
        if not hasattr(self, "cfg_cache"):
            self.cfg_cache = {}                  # wid -> (cfg, sim-hour of last full search)
        full_budget = FULL_SEARCH_PER_PLAN
        stale_order = sorted((v[1], wid) for wid, v in self.cfg_cache.items())
        rotate = {wid for _, wid in stale_order[:full_budget]}
        self.series = self.grid.series(now_h)
        wl = {w["id"]: w for w in st["workloads"]}
        buf, start_h, H = self.buffer(), int(math.floor(now_h)), self.cat.hmax
        if SCORE_WEIGHTING and getattr(self.cat, "ref_kwh", None):
            pending = [k for k, x in wl.items() if not x["final_output_delivered"]]
            est = {}                                     # kg: spent so far + last plan's remaining cost
            for k in self.cat.workloads:
                spent = getattr(self, "carbon", {}).get(k, 0.0) / 1000
                last = getattr(self, "plans", {}).get(k)
                rest = last[0] / 1000 if last and last[0] < INF else 0.2 * self.cat.ref_kwh.get(k, 0) * 0.15
                est[k] = spent + (rest if k in pending else 0.0)
            self.score_w = score_weights(self.cat.ref_kwh, est, pending)
        sw = getattr(self, "score_w", {}) if SCORE_WEIGHTING else {}
        for it in range(iterations):
            plans = {}
            cards = {r: np.full(H, float(c)) for r, c in self.cat.caps.items() if r.startswith("card:")}

            def mask(cfg):                                 # GPU_JOINT: hold cards only where the timetable is free
                if not (GPU_JOINT and cfg.cards):
                    return None
                r = f"card:{cfg.family}"
                return cards[r] >= cfg.use[r] if r in cards else None

            def book(cfg, plan):
                if not (GPU_JOINT and cfg.cards and plan):
                    return
                r = f"card:{cfg.family}"
                c0 = plan["create_h"] if plan.get("create_h") is not None else start_h
                c1 = (plan["finish_h"] if plan.get("finish_h") is not None else int(math.ceil(cfg.dl_h))) + cfg.o_h
                if r in cards:
                    cards[r][max(0, c0):min(H, c1 + 1)] -= cfg.use[r]

            def urgency_key(wid):
                w = self.cat.workloads[wid]
                if wid in self.reg:
                    return (0, 0.0)                        # existing VMs book their cards first
                gpu = any(c.cards for c in self.cat.configs(wid))
                return (1 if gpu else 2, self.cat.h(w["deadline_at"]) - self.cat.h(w["release_at"]))
            order = sorted(self.cat.workloads, key=urgency_key) if GPU_JOINT else list(self.cat.workloads)
            for wid in order:
                w = self.cat.workloads[wid]
                s = wl.get(wid)
                if s is None or s["final_output_delivered"]:
                    continue
                done = float(s["completed_work"]) / float(w["required_work"])
                if done >= 1.0:
                    continue
                if wid in self.reg:
                    rec = self.reg[wid]
                    val, plan = plan_solve(rec["cfg"], now_h, done, True,
                                           rec.get("stage_left", 0.0) if SETUP_PAUSE else rec["ready_h"],
                                           self.series[rec["cfg"].area][1], self.prices, buf, H, wscale=sw.get(wid, 1.0))
                    plans[wid] = (val, rec["cfg"], plan)
                    book(rec["cfg"], plan)
                    continue
                cached = self.cfg_cache.get(wid)
                if iterations == 1 and cached and wid not in dirty and wid not in rotate:
                    val, plan = plan_solve(cached[0], now_h, 0.0, False, None, self.series[cached[0].area][1],
                                           self.prices, buf, H, hold_ok=mask(cached[0]), wscale=sw.get(wid, 1.0))
                    if plan is not None:                       # cached option still feasible: done
                        plans[wid] = (val, cached[0], plan)
                        book(cached[0], plan)
                        continue
                cands = self.cat.configs(wid)
                if not cands:
                    continue
                lo = max(start_h, int(self.cat.h(w["release_at"])))
                hi = max(lo + 1, int(self.cat.h(w["deadline_at"])))
                areas = sorted({c.area for c in cands}, key=lambda a: self.series[a][1][lo:hi].mean())[:TOP_AREAS]
                best = (INF, None, None)
                for cfg in cands:
                    if cfg.area in areas:
                        val, plan = plan_solve(cfg, now_h, 0.0, False, None, self.series[cfg.area][1], self.prices,
                                               buf, H, hold_ok=mask(cfg), wscale=sw.get(wid, 1.0))
                        if EFFICIENT_TIEBREAK:
                            # near-ties (within EFF_TOL): prefer the lower energy per unit of work
                            if best[1] is None and val < INF or val < best[0] * (1 - EFF_TOL) or \
                                    (val <= best[0] * (1 + EFF_TOL) and best[1] is not None
                                     and cfg.energy_per_unit() < best[1].energy_per_unit()):
                                best = (val, cfg, plan)
                        # near-ties (within 1%): prefer the faster config (fuller accelerator use, more slack)
                        elif val < best[0] * 0.99 or (val <= best[0] * 1.01 and best[1] is not None
                                                       and cfg.speed() > best[1].speed()) or best[1] is None and val < INF:
                            best = (val, cfg, plan)
                plans[wid] = best
                if best[1] is not None:
                    self.cfg_cache[wid] = (best[1], now_h)
                    book(best[1], best[2])
            use = {r: np.zeros(H) for r in self.cat.caps}
            for val, cfg, plan in plans.values():
                if plan is None:
                    continue
                c0 = plan["create_h"] if plan["create_h"] is not None else start_h
                c1 = (plan["finish_h"] if plan["finish_h"] is not None else c0) + cfg.o_h
                for r, v in cfg.use.items():
                    if r in use:
                        use[r][max(0, c0):min(H, c1 + 1)] += v
            for r, cap in self.cat.caps.items():
                over = np.maximum(0, use[r] - cap) / max(cap, 1)
                under = (use[r] < 0.9 * cap).astype(float)
                self.prices[r][start_h:] = np.maximum(0.0, self.prices[r][start_h:] + 0.6 * over[start_h:]
                                                      - 0.05 * under[start_h:])
        self.plans = plans
        return time.time() - t0, {r: int(use[r][start_h:].max()) for r in ("vcpu", "vm")}, buf

    # -- actions
    def vm_call(self, kind, wid, vm, now_h):
        try:
            c = self.client(vm["zone"])
            {"start": c.start_instance, "stop": c.stop_instance, "delete": c.delete_instance}[kind](id=vm["id"])
            self.event(kind, wid=wid, vm=vm["id"], h=now_h)
        except Exception as e:
            log(f"{kind} error {wid} {vm['id'][:8]}: {type(e).__name__} {str(e)[:100]}")
            self.pending.pop(vm["id"], None)          # allow retry

    def submit(self, kind, wid, vm, now_h):
        p = self.pending.get(vm["id"])
        if p and p[0] == kind and time.monotonic() - p[1] < 5:
            return
        self.pending[vm["id"]] = (kind, time.monotonic())
        if kind in ("start", "stop"):
            self.last_toggle[vm["id"]] = now_h
        self.pool.submit(self.vm_call, kind, wid, vm, now_h)

    def create(self, wid, cfg, now_h):
        try:
            p = self.cat.profiles[cfg.pid]
            if not self.cat.legal(wid, cfg.zone, p):
                log(f"REFUSED illegal placement {wid}->{cfg.zone}")
                return
            binding = base64.b64encode(json.dumps(
                {"greenops": {"schema_version": "1.0", "workload_id": wid}}).encode()).decode()
            vms = []
            for i in range(cfg.n):
                name = f"{VM_PREFIX}{wid}-{i}"
                try:
                    op = self.client(cfg.zone).create_instance(
                        name=name, instance_type={"id": cfg.pid}, template={"id": self.template(cfg.zone)},
                        disk_size=10, user_data=binding)
                    vid = op["reference"]["id"]
                except Exception as e:
                    log("create error", wid, type(e).__name__, str(e)[:100])
                    found = [v for v in self.client(cfg.zone).list_instances()["instances"] if v["name"] == name]
                    if not found:
                        continue
                    vid = found[0]["id"]
                vms.append({"id": vid, "zone": cfg.zone, "name": name})
            if vms:
                if len(vms) != cfg.n:
                    cfg = Config(self.cat, wid, cfg.zone, cfg.pid, len(vms))
                self.reg[wid] = {"cfg": cfg, "vms": vms, "ready_h": now_h + cfg.S,
                                 "stage_left": cfg.S, "stage_t": now_h}
                log(f"CREATE {wid} -> {cfg.label()} at h={now_h:.2f}")
                self.event("create", wid=wid, cfg=cfg.label(), h=now_h, vms=[v["id"] for v in vms])
        finally:
            self.inflight.pop(wid, None)
            self.creating.discard(wid)

    def reconcile(self, now_h, st):
        """Server is the source of truth: adopt own VMs, delete VMs of delivered workloads."""
        wl = {w["id"]: w for w in st["workloads"]}

        def list_zone(z):
            try:
                return z, self.client(z).list_instances()["instances"]
            except Exception:
                return z, None
        live = {}
        for z, vms in self.pool.map(list_zone, list(self.cat.zones)):
            if vms is None:
                return                                    # incomplete view: skip this pass
            for v in vms:
                if v["name"].startswith(VM_PREFIX):
                    wid = v["name"][len(VM_PREFIX):].rsplit("-", 1)[0]
                    live.setdefault(wid, []).append({"id": v["id"], "zone": z, "name": v["name"],
                                                     "pid": v["instance-type"]["id"]})
        for wid, vms in live.items():
            s = wl.get(wid)
            if s is None or s["final_output_delivered"]:
                for vm in vms:
                    self.submit("delete", wid, vm, now_h)
                self.reg.pop(wid, None)
                continue
            if wid in self.inflight:
                continue
            known = {vm["id"] for vm in self.reg.get(wid, {}).get("vms", [])}
            if {vm["id"] for vm in vms} != known:
                cfg = Config(self.cat, wid, vms[0]["zone"], vms[0]["pid"], len(vms))
                staged = float(s["completed_work"]) > 0
                ready = self.reg[wid]["ready_h"] if wid in self.reg else (now_h - 1 if staged else now_h + cfg.S)
                left = self.reg[wid].get("stage_left", cfg.S) if wid in self.reg else (0.0 if staged else cfg.S)
                self.reg[wid] = {"cfg": cfg, "vms": [{k: vm[k] for k in ("id", "zone", "name")} for vm in vms],
                                 "ready_h": ready, "stage_left": left, "stage_t": now_h}
                log(f"ADOPTED {wid} {cfg.label()} ({len(vms)} VM)")
        for wid in list(self.reg):
            if wid not in live and wid not in self.inflight:
                self.reg.pop(wid, None)                   # VMs vanished server-side

    def act(self, now_h, st, allow_toggle=True):
        h, buf = int(math.floor(now_h)), self.buffer()
        self.delivered = {w["id"] for w in st["workloads"] if w["final_output_delivered"]}
        self.remaining = {w["id"]: float(w["required_work"]) - float(w["completed_work"]) for w in st["workloads"]}
        order = sorted(self.cat.workloads, key=lambda wid: self.cat.h(self.cat.workloads[wid]["deadline_at"]))
        if SCORE_ORDER:
            def urgency(wid):                            # hours of slack at full speed (small = urgent)
                w = self.cat.workloads[wid]
                cands = self.cat.configs(wid)
                if not cands:
                    return INF
                c = max(cands, key=lambda x: x.speed())
                rem = self.remaining.get(wid, float(w["required_work"]))
                return (c.dl_h - buf - now_h) - (rem / c.speed() + c.O + (0 if wid in self.reg else c.S))
            sw = getattr(self, "score_w", {})
            order = sorted(order, key=lambda wid: (0 if urgency(wid) < 6.0 else 1, -sw.get(wid, 1.0)))
        for wid in order:
            s = next((x for x in st["workloads"] if x["id"] == wid), None)
            if s is None:
                continue
            w, rec = self.cat.workloads[wid], self.reg.get(wid)
            states = {a["vm_id"]: a["state"] for a in s["assignments"]}
            if s["final_output_delivered"]:
                if rec:
                    for vm in rec["vms"]:
                        self.submit("delete", wid, vm, now_h)
                    log(f"DELIVERED {wid} at h={now_h:.2f} est {self.carbon.get(wid, 0) / 1000:.2f} kg")
                    del self.reg[wid]
                continue
            remaining = float(w["required_work"]) - float(s["completed_work"])
            if s.get("state") == "deadline_missed" or now_h >= self.cat.h(w["deadline_at"]):
                # Progress no longer counts after the deadline: stop (not delete, which could
                # discard uncheckpointed partial credit) so the VMs draw 0 W.
                for vm in (rec["vms"] if rec else []):
                    if states.get(vm["id"]) == "running":
                        self.submit("stop", wid, vm, now_h)
                continue
            if rec is None:
                if wid in self.creating or self.cat.h(w["deadline_at"]) <= now_h:
                    continue
                val, cfg, plan = self.plans.get(wid, (INF, None, None))
                ci = {a: self.series[a][0][min(h, self.cat.hmax - 1)] for a in self.cat.areas}
                if cfg is None or plan is None:
                    cands = self.cat.configs(wid)
                    feas = [c for c in cands if self.feasible(c, remaining, now_h, buf)]
                    feas_now = [c for c in feas if self.fits(c, now_h)]
                    if feas_now:
                        cfg = pick_green_fast(feas_now, ci)
                    elif feas:
                        self._blocked(wid, now_h, "quota full (feasible size exists)")
                        continue                          # a feasible size exists: wait for quota
                    else:
                        fit = [c for c in cands if self.fits(c, now_h)]
                        if not fit:
                            continue
                        cfg = max(fit, key=lambda c: c.rate(c.n))
                        log(f"INFEASIBLE {wid}: fastest fitting {cfg.label()} for partial credit")
                    plan = {"create_h": h}
                must = remaining / cfg.rate(cfg.n) + cfg.O + cfg.S >= cfg.dl_h - buf - now_h - 1.0
                if plan["create_h"] is None or not (plan["create_h"] <= h or must):
                    continue
                if CREATE_GUARD and st.get("state") == "RUNNING" and \
                        now_h < cfg.rel_h - cfg.S - (12.0 if SETUP_PAUSE else 1.0):
                    continue                              # never park a VM long before its release
                    # (only once the clock runs: the clock starts at the first VM creation, so
                    #  blocking creations while READY would freeze the run at h0 forever)
                if not self.fits(cfg, now_h):
                    alt = [c for c in self.cat.configs(wid) if self.fits(c, now_h)
                           and self.feasible(c, remaining, now_h, buf)]
                    if not alt:
                        self._blocked(wid, now_h, f"quota full for {cfg.label()}")
                        continue
                    cfg = pick_green_fast(alt, ci)
                self.creating.add(wid)
                self.inflight[wid] = cfg
                self.pool.submit(self.create, wid, cfg, now_h)
                continue
            cfg = rec["cfg"]
            plan = self.plans.get(wid, (INF, None, None))[2]
            if SETUP_PAUSE:
                n_run = sum(1 for vm in rec["vms"] if states.get(vm["id"]) == "running")
                if rec.get("stage_left", 0.0) > 0 and n_run == len(rec["vms"]):
                    rec["stage_left"] = max(0.0, rec["stage_left"] - max(0.0, now_h - rec.get("stage_t", now_h)))
                if float(s["completed_work"]) > 0:
                    rec["stage_left"] = 0.0
                rec["stage_t"] = now_h
                staged = rec.get("stage_left", 0.0) <= 0.02
                must = remaining / cfg.rate(cfg.n) + cfg.O + rec.get("stage_left", 0.0) >= cfg.dl_h - buf - now_h - 1.0
                if remaining <= 1e-9 or must or plan is None:
                    want = cfg.n
                else:
                    want = plan["run"].get(h, 0)          # setup and compute both follow the plan (setup may pause)
            else:
                staged = now_h >= rec["ready_h"]
                must = remaining / cfg.rate(cfg.n) + cfg.O >= cfg.dl_h - buf - now_h - 1.0
                want = cfg.n if (remaining <= 1e-9 or not staged or must or plan is None) else plan["run"].get(h, 0)
            if staged and remaining > 1e-9 and now_h < cfg.rel_h:
                want = 0                              # staged before release: nothing to compute, don't idle
            running = [vm for vm in rec["vms"] if states.get(vm["id"]) in ("running", "creating")]
            stopped = [vm for vm in rec["vms"] if states.get(vm["id"]) == "stopped"]
            dwell_ok = (lambda vm: allow_toggle) if DECISION_PERIOD_H else \
                (lambda vm: now_h - self.last_toggle.get(vm["id"], -9) >= DWELL_H)
            for vm in stopped[:max(0, want - len(running))]:
                if must or (OUTPUT_DWELL_EXEMPT and remaining <= 1e-9) or dwell_ok(vm):
                    self.submit("start", wid, vm, now_h)
            if STALE_STOP_FREEZE_H is not None and getattr(self, "status_age", 0.0) > STALE_STOP_FREEZE_H:
                continue                                  # stale view: never stop on outdated progress
            for vm in running[want:]:
                if states.get(vm["id"]) == "running" and dwell_ok(vm):
                    self.submit("stop", wid, vm, now_h)

    def _blocked(self, wid, now_h, why):
        if not BLOCK_LOG:
            return
        last = getattr(self, "_blk", {}).get(wid, -99)
        if now_h - last >= 3.0:                           # throttle: once per 3 sim-hours per workload
            self._blk = getattr(self, "_blk", {})
            self._blk[wid] = now_h
            log(f"BLOCKED {wid} at h={now_h:.1f}: {why}")

    def account_carbon(self, st):
        now_h = self.cat.h(st["virtual_time"])
        if self.last_carbon_t is None or self.series is None:
            self.last_carbon_t = now_h
            return
        dt, self.last_carbon_t = now_h - self.last_carbon_t, now_h
        if dt <= 0:
            return
        h = min(self.cat.hmax - 1, int(now_h))
        on = {a["vm_id"] for w in st["workloads"] for a in w["assignments"] if a["state"] in ("running", "creating")}
        for wid, rec in list(self.reg.items()):
            cfg = rec["cfg"]
            n = sum(1 for vm in rec["vms"] if vm["id"] in on)
            self.carbon[wid] = self.carbon.get(wid, 0.0) + n * cfg.watts * cfg.pue / 1000 * self.series[cfg.area][0][h] * dt

    # -- main loop
    def run(self):
        st, _ = self.poller.latest()
        if st["state"] in ("COMPLETED", "INVALIDATED", "UNAVAILABLE"):
            log("run already ended:", st["state"])
            return True
        self.delivered, self.remaining = set(), {}
        now_h = self.cat.h(st["virtual_time"])
        self.reconcile(now_h, st)
        self.grid.refresh(self.pool)
        self.fc_hour = int(now_h)
        took, peak, buf = self.replan(now_h, st, iterations=4)
        log(f"initial plan {took:.2f}s peak={peak}")
        if self.args.dry_run:
            for wid, (val, cfg, p) in sorted(self.plans.items()):
                print(f"  {wid:24} " + ("NO FEASIBLE PLAN" if p is None else
                      f"{cfg.label():34} create h{p['create_h']}  compute {sum(1 for k in p['run'].values() if k)}h "
                      f"finish h{p['finish_h']}  slack {cfg.slack():.1f}h"))
            return True
        if not self.poller.is_alive():
            self.poller.start()
        self.stop_planner = False
        self.plan_stats = {"n": 0, "last": None}

        def planner():
            while not self.stop_planner:
                t0 = time.monotonic()
                try:
                    now_h, _, st = self.clock()
                    if st["state"] == "RUNNING":
                        took, peak, buf = self.replan(now_h, st)
                        self.plan_stats = {"n": self.plan_stats["n"] + 1, "last": (now_h, took, peak, buf)}
                except Exception as e:
                    log("planner error (continuing):", type(e).__name__, str(e)[:120])
                time.sleep(max(0.0, REPLAN_WALL_S - (time.monotonic() - t0)))
        if not DECISION_PERIOD_H:          # v3.2: re-planning happens at the decision points instead
            threading.Thread(target=planner, daemon=True).start()
        last_replan = last_rec = 0.0
        last_log = -1
        t_end = time.monotonic() + self.args.wall_minutes * 60
        slog = open(self.state_dir / "status.jsonl", "a")
        last_st = None
        prev_seen, prev_seen_h = None, 0.0
        self.event_replans = 0
        while time.monotonic() < t_end:
            loop0 = time.monotonic()
            try:
                now_h, age, st = self.clock()
                if st is not last_st:
                    slog.write(json.dumps(st) + "\n")
                    slog.flush()
                    last_st = st
                    self.account_carbon(st)
                self.status_age = age
                if st["state"] == "RUNNING":
                    self.staleness.append(age)
                if st["qualification"] != "QUALIFIED":
                    log("!!! DISQUALIFIED - stopping")
                    return True
                if st["state"] in ("COMPLETED", "INVALIDATED", "UNAVAILABLE"):
                    log("run ended:", st["state"])
                    return True
                if all(w["final_output_delivered"] for w in st["workloads"]) and not self.reg:
                    log(f"all {len(st['workloads'])} workloads delivered, VMs deleted at h={now_h:.2f}")
                    return True
                if int(now_h) > self.fc_hour and (self.fc_thread is None or not self.fc_thread.is_alive()):
                    self.fc_hour = int(now_h)
                    self.fc_thread = threading.Thread(target=self.grid.refresh, args=(self.pool,), daemon=True)
                    self.fc_thread.start()
                if time.monotonic() - last_rec >= RECONCILE_WALL_S:
                    self.reconcile(now_h, st)
                    last_rec = time.monotonic()
                if st is not prev_seen:
                    reasons = self.detect_events(prev_seen, st, prev_seen_h, now_h) if prev_seen else []
                    prev_seen, prev_seen_h = st, now_h
                    if reasons:
                        dirty = {r.split(" ", 1)[1] for r in reasons if r.startswith("released ")}
                        took, peak, buf = self.replan(now_h, st, dirty=dirty)
                        self.event_replans += 1
                        log(f"EVENT re-plan h={now_h:.2f} ({took:.2f}s): " + ", ".join(reasons[:4])
                            + (f" +{len(reasons) - 4} more" if len(reasons) > 4 else ""))
                if int(now_h) // 6 > last_log and self.plan_stats["last"]:
                    last_log = int(now_h) // 6
                    _, took, peak, buf = self.plan_stats["last"]
                    log(f"h={now_h:.1f} status-age {age:.2f}h timeouts {self.poller.timeouts} re-plans {self.plan_stats['n']} "
                        f"(last {took:.2f}s) buf {buf:.2f}h peak {peak} est {sum(self.carbon.values()) / 1000:.2f} kg")
                if DECISION_PERIOD_H:
                    epoch = int(now_h // DECISION_PERIOD_H)
                    boundary = epoch != getattr(self, "_epoch", None)
                    if boundary:
                        self._epoch = epoch
                        self.replan(now_h, st)                # fresh plan exactly at the decision point
                    self.act(now_h, st, allow_toggle=boundary)
                else:
                    self.act(now_h, st)
            except Exception as e:
                log("iteration error (continuing):", type(e).__name__, str(e)[:160])
                self.event("error", trace=traceback.format_exc()[-2000:])
            time.sleep(max(0.0, (0.2 if DECISION_PERIOD_H else 0.5) - (time.monotonic() - loop0)))
        log("wall-time limit reached")
        self.stop_planner = True
        return True

    def summary(self):
        log("estimated carbon (kg): " + json.dumps({k: round(v / 1000, 3) for k, v in sorted(self.carbon.items())}))
        log(f"estimated total: {sum(self.carbon.values()) / 1000:.2f} kg CO2e")


def enable_v31():
    """v3.1 = v3 + general fixes from the 9a33c75dac live run (no ledger)."""
    global OUTPUT_DWELL_EXEMPT, STALE_STOP_FREEZE_H, CREATE_GUARD, BLOCK_LOG, RESERVE_SMALLEST_FEASIBLE
    OUTPUT_DWELL_EXEMPT, STALE_STOP_FREEZE_H, CREATE_GUARD = True, 1.0, True
    BLOCK_LOG, RESERVE_SMALLEST_FEASIBLE = True, True


def enable_v32():
    """v3.2 = v3.1 + 15-sim-minute decision points (no hold time), 0.25 s status polling,
    own stops not treated as events."""
    global DECISION_PERIOD_H, STATUS_POLL_S
    enable_v31()
    DECISION_PERIOD_H, STATUS_POLL_S = 0.25, 0.25


def enable_v33():
    """v3.3 (experimental) = v3.2 + simulator-tested settings (all worker counts, alpha 0.6,
    tighter slack) + score-ordered admission. Simulator, 14 unseen scenarios, paired vs v3.2:
    +30 +/- 20 points, better in 11/14. Score-weighted planning is available but off."""
    global WORKER_OPTIONS, C_ALPHA, BEYOND_SIGMA, SLACK_MIN_H, SLACK_FRAC, SCORE_WEIGHTING, SCORE_ORDER
    enable_v32()
    WORKER_OPTIONS = (1, 2, 3, 4, 5, 6, 8, 10)
    C_ALPHA = NormalDist().pdf(NormalDist().inv_cdf(0.6)) / (1 - 0.6)
    BEYOND_SIGMA, SLACK_MIN_H, SLACK_FRAC = 0.12, 1.0, 0.05
    SCORE_ORDER = True
    SCORE_WEIGHTING = os.environ.get("GREENOPS_SCORE_WEIGHTING", "0") == "1"   # off: no consistent gain in tests


def enable_v34():
    """v3.4 = v3.3 + energy-efficient tie-break (live: 22,026.17 vs 21,993.73).
    Experimental, off by default: pausable setup (GREENOPS_SETUP_PAUSE=1; deadline misses on broad scenarios)
    and joint GPU card timetable (GREENOPS_GPU_JOINT=1; neutral live, +61 on broad simulated scenarios)."""
    global SETUP_PAUSE, GPU_JOINT
    enable_v33()
    SETUP_PAUSE = os.environ.get("GREENOPS_SETUP_PAUSE", "0") == "1"
    GPU_JOINT = os.environ.get("GREENOPS_GPU_JOINT", "0") == "1"
    global EFFICIENT_TIEBREAK
    EFFICIENT_TIEBREAK = os.environ.get("GREENOPS_EFFICIENT", "1") == "1"


def main():
    if os.environ.get("GREENOPS_V34", "1") == "1":
        enable_v34()
    elif os.environ.get("GREENOPS_V33", "1") == "1":
        enable_v33()
    elif os.environ.get("GREENOPS_V32", "1") == "1":
        enable_v32()
    elif os.environ.get("GREENOPS_V31", "1") == "1":
        enable_v31()
    p = argparse.ArgumentParser(description="GreenOps autonomous DP orchestrator v3")
    p.add_argument("--config", default=os.environ.get("GREENOPS_CONFIG"), help="exo CLI JSON (else env vars)")
    p.add_argument("--catalog", default=str(Path(__file__).resolve().parent / "greenops-public-catalog.json"),
                   help="fallback catalog file if /api/catalog is unreachable")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--wall-minutes", type=float, default=float(os.environ.get("GREENOPS_WALL_MINUTES", 14.5)))
    args = p.parse_args()
    deadline = time.monotonic() + args.wall_minutes * 60
    for attempt in range(20):                              # supervisor: restart after a crash
        try:
            orch = Orchestrator(args)
            args.wall_minutes = max(0.1, (deadline - time.monotonic()) / 60)
            done = orch.run()
            orch.summary()
            if done:
                return
        except Exception as e:
            log(f"CRASH attempt {attempt}: {type(e).__name__} {str(e)[:200]} - restarting with recovery")
            traceback.print_exc()
            time.sleep(2)
        if time.monotonic() >= deadline:
            return


if __name__ == "__main__":
    main()
