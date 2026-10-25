# Reproducing the v3.4 results

Step-by-step instructions to build the controller and run it on a GreenOps practice run, exactly as we did.

> **Recommended: run on a 360× practice run.** All our best results are at 360×, including the identified
> run `800420f75345a744da9d6683` (22,026.17 points). At 720× the same code scored about 0.6–0.7 kg higher
> carbon (see [Speed](#speed-360-recommended)). If you use 360×, set `GREENOPS_WALL_MINUTES=30` (step 4).

## 0. What you need
- Docker (any recent version), **or** Python ≥ 3.10 (tested: 3.10.9 on the host, 3.13.16 in the image).
- A fresh GreenOps practice run whose clock has not started (state `READY`). Each attempt needs a new run.
- About 2 CPUs and < 1 GiB RAM. Network access to `greenops.containerstack.net`.

## 1. Get the run's credentials
Portal → your run → **API credentials** → **Download environment file**. You get `greenops-<run>.env`
with 6 variables: `EXOSCALE_API_KEY`, `EXOSCALE_API_SECRET`, `GREENOPS_CLOUD_ENDPOINT`,
`GREENOPS_GRID_SECRET`, `GREENOPS_GRID_ENDPOINT`, `GREENOPS_WORK_ENDPOINT`. Keep it out of git (`.gitignore`
already excludes `*.env`).

```sh
set -a; . ./greenops-<run>.env; set +a
```

## 2. Verify you have the exact code
```sh
md5sum orchestrator.py greenops-public-catalog.json rules.json
# d25bc1de418de854837d4b246cba7bcf  orchestrator.py
# 24061247463e7e3eaf77d9564e227af5  greenops-public-catalog.json
# 652d10a06350e314b814a945f5340d27  rules.json
```
`orchestrator.py` is logically identical to the code of the best run (`800420f753…`). That run used the same
file with experimental switches set off by environment variables, which are now the defaults. No environment
switches are needed.

## 3. Build
```sh
docker build -t greenops-orchestrator:v3.4 .
```
The base image is pinned by digest (`python:3.13-slim-bookworm@sha256:a1165e27…`) and Python packages are
pinned in `requirements.txt` (exoscale 0.16.3, requests 2.34.2, requests-exoscale-auth 1.1.2, numpy 2.2.6).
On our machine the build gives image ID `6de344ec4337`. Check the code inside the image:
```sh
docker run --rm --entrypoint md5sum greenops-orchestrator:v3.4 /app/orchestrator.py   # d25bc1de…
```

## 4. Dry run (read-only: plans, creates nothing, does not start the clock)
```sh
docker run --rm -e EXOSCALE_API_KEY -e EXOSCALE_API_SECRET -e GREENOPS_CLOUD_ENDPOINT \
  -e GREENOPS_GRID_SECRET -e GREENOPS_GRID_ENDPOINT -e GREENOPS_WORK_ENDPOINT \
  greenops-orchestrator:v3.4 --dry-run
```
Expect a line like `run <id> state=READY speed=360 …` and `initial plan …s peak={'vcpu': 296, 'vm': 8}`,
followed by one planned line per workload. **Check `speed=`** to know which wall limit to use below.

## 5. Real run (hands-off)
`GREENOPS_WALL_MINUTES` is the controller's wall-clock budget. The default **14.5** fits 720× (120 sim-h ≈ 10
min). **At 360× the run lasts ≈ 20 min, so set 30**, otherwise the controller stops at about sim-hour 87.

```sh
# 360× (recommended)
docker run --rm -e EXOSCALE_API_KEY -e EXOSCALE_API_SECRET -e GREENOPS_CLOUD_ENDPOINT \
  -e GREENOPS_GRID_SECRET -e GREENOPS_GRID_ENDPOINT -e GREENOPS_WORK_ENDPOINT \
  -e GREENOPS_WALL_MINUTES=30 \
  greenops-orchestrator:v3.4 | tee run.log

# 720× (judging speed): same command without GREENOPS_WALL_MINUTES (default 14.5)
```
Without Docker:
```sh
python -m venv .venv && .venv/bin/pip install -r requirements.txt
GREENOPS_WALL_MINUTES=30 .venv/bin/python orchestrator.py | tee run.log      # add --dry-run first
```
Do not intervene during the run. The first VM creation starts the clock. The controller exits after the last
delivery, once all VMs are deleted. It is restart-safe: if it is killed, restart the same command and it
adopts its own `dp-*` VMs. State is written only under `$GREENOPS_STATE_DIR` (default `/tmp/greenops-<run>`).

## 6. What to expect
- Log lines `CREATE …`, `DELIVERED w-xx … est … kg`, occasionally `BLOCKED …` (waiting for quota) and rarely a
  `create error … 409 quota exceeded` (a create racing a just-issued delete; it is retried within seconds).
- End of log: `all 15 workloads delivered, VMs deleted at h≈115` and `estimated total: … kg CO2e`.
- The controller's estimate reads ~3% higher than the official electricity figure (13.77 est → 13.329 official).
- The final official score appears on the dashboard after sim-hour 120.

| Run (v3.4, hands-off) | Speed | Delivered | Estimated electricity | Official |
|---|---|---|---|---|
| `800420f75345a744da9d6683` (identified run) | 360× | 15/15 | 13.77 kg | **22,026.17 points, 13.329 kg** |
| `d99d4c60185d93b8a4fadc1e` | 360× | 15/15 | 13.89 kg | dashboard |
| `9384ffbf1206978a97286ee1` | 360× | 15/15 | 14.28 kg | dashboard |
| `d571c922aed3f468b06d95c1` | **720×** | 15/15 | **14.51 kg** | dashboard |

Run-to-run spread on identical code and data is about ±0.25 kg (see README, "Limitations").

## Speed: 360× recommended
At 720× every second of controller latency is 0.2 simulated hours (0.1 at 360×). One work-status round-trip
is ~0.5 s (0.11 sim-h at 720×), so at 720× the controller sees freed quota and progress later, blocked
workloads start later, and near-tie decisions shift. Measured on the same scenario: 360× runs 13.77 / 13.89 /
14.28 kg (est), 720× 14.51 kg (est). The controller is correct at both speeds (15/15, qualified, no manual
steps), but its carbon result is better at 360×.
