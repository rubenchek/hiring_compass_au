# Hiring Compass AU

Experimental system to automate and track the Australian job application pipeline. Scope and rules live in `docs/design-doc.md`.

## Prerequisites
- Python >= 3.11
- Recent `pip`
- (Optional) Docker + Docker Compose for container runs

## Quick install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Initialize the workspace
Creates the expected folders (`data`, `models`, `reports`, `logs`, etc.):
```bash
python scripts/bootstrap_workspace.py
```

## Run via Docker (dev/prod)
Dev (override auto):
```bash
docker compose up
docker compose run --rm job-alerts
```
Prod:
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm \
  job-alerts -- --no-promote
```

## Run job enrichment via Docker (dev/prod)
Dev:
```bash
docker compose run --rm job-enrichment --limit 10 --max-batches 1
```
Prod:
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm \
  job-enrichment --limit 10 --max-batches 1
```

## Prod smoke test (cloned DB)
```bash
scripts/run_prod_smoke_test.sh
```

## Current structure
```
.
├── configs/                   # YAML sources (e.g., sources.yaml)
├── docs/                      # design docs
├── notebooks/                 # explorations
├── scripts/                   # local utilities (bootstrap, init DB, docker)
├── src/hiring_compass_au/     # Python package
│   ├── config/                # settings (env vars, paths)
│   ├── domain/                # domain models and normalization
│   ├── infra/                 # storage (db, stores)
│   ├── services/job_alerts/   # job alerts pipeline
│   ├── services/job_enrichment/ # job enrichment pipeline
│   └── workspace.py           # workspace paths
├── tests/                     # unit, integration, pipelines
└── run/                       # runtime dev/prod (gitignored)
```

Notes:
- `configs/` (YAML files) is distinct from `src/hiring_compass_au/config/` (code settings).
- Runtime data (`run/`, `data/`, `logs/`, `models/`, `reports/`) are ignored by Git.
- Docker Compose uses `docker-compose.yml` + `docker-compose.override.yml` (dev auto).

## Suggested next steps
- Add new ingestion sources using `configs/sources.yaml`.
- Document key business flows in `docs/`.
- Extend the pipeline (new parsers, enrichments, outputs).
