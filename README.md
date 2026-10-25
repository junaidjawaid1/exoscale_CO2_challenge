# GreenOps carbon-aware orchestrator (v3.4)

Autonomous controller for the Exoscale GreenOps Challenge (The Energy Bridge Hackathon, Vienna 2026).
It completes the 15 workloads within the 120 simulated hours while minimising electricity carbon,
respecting residency rules, quotas and deadlines. It runs unattended from start to finish.

**Submission:** this repository (v3.4, `orchestrator.py` md5 `d25bc1de418de854837d4b246cba7bcf`).
**Identified practice run:** `800420f75345a744da9d6683`, **22,026.17 points**, 15/15 delivered by their
deadlines, 13.329 kg electricity (360×). **Step-by-step reproduction: [REPRODUCE.md](REPRODUCE.md).**

> **Recommended: 360× practice runs** (set `GREENOPS_WALL_MINUTES=30`). The same code at 720× measured about
> 0.6–0.7 kg more carbon (controller latency, see REPRODUCE.md). It is correct and fully autonomous at both speeds.

## Run (quick start)

```sh
set -a; . ./greenops-<run>.env; set +a          # portal -> API credentials -> Download environment file
docker build -t greenops-orchestrator:v3.4 .
docker run --rm -e EXOSCALE_API_KEY -e EXOSCALE_API_SECRET -e GREENOPS_CLOUD_ENDPOINT \
  -e GREENOPS_GRID_SECRET -e GREENOPS_GRID_ENDPOINT -e GREENOPS_WORK_ENDPOINT \
  -e GREENOPS_WALL_MINUTES=30 greenops-orchestrator:v3.4            # 360×; omit WALL_MINUTES at 720×
```
Add `--dry-run` first to plan without creating anything. Without Docker: `pip install -r requirements.txt &&
python orchestrator.py` (same env vars). Needs 2 CPUs / < 1 GiB; writes state only under `$GREENOPS_STATE_DIR`
(default `/tmp/greenops-<run>`). Exits when every workload is delivered and its VMs are deleted.

## How it decides

* **Inputs (all run-local):** workload/profile/zone catalog from the portal's public `/api/catalog`
  (bundled `greenops-public-catalog.json` as fallback), public scoring reference energies from
  `/api/rules` (bundled `rules.json` as fallback), the run's work status, and the run's own
  Electricity Maps-style carbon-intensity forecasts (24 h and 6 h; the 6 h values are used for hours
  1-6 whenever they differ) and observations. No external or cross-run data.
* **Carbon model:** per country, observed values for the past, the API forecast for the next 24 h, and
  "same hour yesterday" beyond that. Planning uses a risk-adjusted intensity
  `mu * (1 + c_alpha * sigma(lead))` (CVaR, alpha = 0.6).
* **Per-workload dynamic programme:** state = (hour, progress, VM created?), action = workers running
  this hour. Cost = watts x PUE x risk-adjusted intensity (setup and output phases included, stopped
  VMs cost 0 W) plus hourly quota prices. Hard deadline with planned slack (max(1 h, 5% of the window))
  plus a buffer from measured status staleness. The best legal (zone, profile, worker count 1..max)
  configuration is chosen per workload; residency is checked before every create. When two
  configurations are within 2% on carbon, the one with the lower energy per unit of work
  (watts x PUE x workers / effective throughput, i.e. incl. coordination overhead) wins, not the faster.
* **Coordination:** hourly Lagrangian prices on vCPU / VM / GPU-card quotas, urgency-based quota
  reservation, must-run override when slack is exhausted. Contested capacity goes to urgent workloads
  first, then to the workload with the most leaderboard points to gain per kg (fitted score formula).
* **Execution:** start/stop decisions at fixed 15-simulated-minute decision points; re-plans at each
  decision point and on real events (release, compute finished, delivery, deadline missed). VMs are
  deleted as soon as their final output is delivered; VMs of missed workloads are stopped.
* **Self-healing:** reconciles against the live VM list (adopts its own `dp-*` VMs after a restart,
  deletes leftovers of delivered workloads), retries with timeouts, restarts itself after a crash.

## Results (practice runs, hands-off, no intervention)

| Run | Version | Speed | Delivered | Result |
|---|---|---|---|---|
| `262ffaffb9…` | v3.1 | 360× | 15/15 | 21,890.1 points (14.07 kg) |
| `d6e7aa3c57…` | v3.3 | 360× | 15/15 | 21,993.73 points (13.456 kg) |
| **`800420f753…`** | **v3.4** | **360×** | **15/15** | **22,026.17 points (13.329 kg electricity)**, est 13.77 kg |
| `d99d4c6018…` | v3.4 | 360× | 15/15 | est 13.89 kg |
| `9384ffbf12…` | v3.4 | 360× | 15/15 | est 14.28 kg |
| `d571c922ae…` | v3.4 | 720× | 15/15 | est 14.51 kg |

"est" = the controller's own estimate (reads ~3% above the official electricity figure).
Every v3.4 run delivered all 15 workloads by their deadlines and stayed QUALIFIED.

**What we tried and rejected after v3.4** (simulator on 36 unseen scenarios + 4 more live runs): flat or
risk-neutral risk premium, a CICS-style capacity LP for quota prices, a selection value for faster
configurations (all, GPU-only, equal-efficiency only, long-window GPU jobs only), late binding, checkpoint-timed
migration, a self-calibrating forecast-error model, adaptive slack. None beat v3.4. The shared quota (held by
VMs until deletion, also when stopped) is the binding constraint: changes that defer work or raise quota use
collide with it.

## Limitations
- **Run-to-run spread ≈ ±0.25 kg** on identical code and data: decision timing (network round-trips), near-tie
  configuration choices and quota cascades (e.g. one workload taking 2 VMs instead of 1 delays another by 7 h).
- **Speed sensitivity:** worse at 720× than at 360× (≈ +0.6 kg est). The cause is latency, not compute: planning
  takes ~0.02 s per re-plan, while a status round-trip takes ~0.5 s.
- Beyond the 24 h forecast the controller assumes "same hour yesterday", so multi-day grid trends are not anticipated.
- Large GPU jobs run on the most efficient single-card RTX size; bigger sizes only pay off with clean windows
  that are visible in the forecast.

## Switches (env, all optional)
`GREENOPS_V34=0` runs v3.3 behaviour; `GREENOPS_EFFICIENT=0` turns the energy-efficient
tie-break off; experimental and off by default: `GREENOPS_GPU_JOINT=1` (joint per-family GPU card timetable),
`GREENOPS_SETUP_PAUSE=1` (pausable setup scheduled into clean hours; caused deadline misses on broad
simulated scenarios). With `GREENOPS_V34=0`: `GREENOPS_V33=0` runs v3.2 behaviour; `GREENOPS_SCORE_WEIGHTING=1` enables
score-weighted planning (implemented, off: no consistent gain in tests). Simulator evidence for v3.3
vs v3.2 (14 unseen scenarios, paired): +30 ± 20 points, better in 11/14 (`tests/sim_v33_results.txt`).

## Rules compliance

Only this run's published electricity data is used; no historical actuals, no cross-run caches,
no hard-coded schedules. Residency is validated before every VM creation. Credentials are read from
the environment and never logged or committed.
