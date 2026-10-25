# GreenOps orchestrator (v3.2). Build outside the timed run; the entry point launches the controller.
FROM python:3.13-slim-bookworm@sha256:a1165e272e578941b84abc79e4ab38a0305cd12803a5c4247979ac7655f4d641
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    GREENOPS_STATE_DIR=/tmp/greenops
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY orchestrator.py greenops-public-catalog.json rules.json ./
# Runs unprivileged; only /tmp needs to be writable. Credentials come from env vars:
# EXOSCALE_API_KEY, EXOSCALE_API_SECRET, GREENOPS_CLOUD_ENDPOINT, GREENOPS_GRID_SECRET, GREENOPS_GRID_ENDPOINT
USER 65534:65534
ENTRYPOINT ["python", "/app/orchestrator.py"]
