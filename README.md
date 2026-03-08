# Hiring Compass AU

Système expérimental pour piloter et automatiser la chaîne de candidature en Australie, en restant traçable et contrôlable. Le périmètre et les règles sont décrits dans `docs/design-doc.md`.

## Prérequis
- Python >= 3.11
- `pip` récent
- (Optionnel) Docker + docker compose pour exécuter via `scripts/run_job_alerts.sh`

## Installation rapide
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Initialiser l'espace de travail
Crée les dossiers attendus (`data`, `models`, `reports`, `logs`, etc.) :
```bash
python scripts/bootstrap_workspace.py
```

## Exécuter la pipeline job alerts (local)
```bash
python -m hiring_compass_au.services.job_alerts
```

Arguments utiles (exemples) :
```bash
python -m hiring_compass_au.services.job_alerts --no-fetch --no-promote
```

## Exécuter via Docker (dev/prod)
```bash
./scripts/run_job_alerts.sh dev
./scripts/run_job_alerts.sh prod -- --no-promote
```

## Structure actuelle
```
.
├── configs/                   # YAMLs de sources (ex: sources.yaml)
├── docs/                      # docs de cadrage
├── notebooks/                 # explorations
├── scripts/                   # utilitaires locaux (bootstrap, init DB, docker)
├── src/hiring_compass_au/     # package Python
│   ├── config/                # settings (env vars, chemins)
│   ├── domain/                # schémas et normalisation métier
│   ├── infra/                 # stockage (db, stores)
│   ├── services/job_alerts/   # pipeline job alerts
│   └── workspace.py           # gestion des chemins workspace
├── tests/                     # unit, integration, pipelines
└── run/                       # runtime dev/prod (gitignored)
```

Notes :
- `configs/` (fichiers YAML) est distinct de `src/hiring_compass_au/config/` (code settings).
- Les données/runtime (`run/`, `data/`, `logs/`, `models/`, `reports/`) sont ignorés par Git.

## Prochaines étapes suggérées
- Ajouter de nouvelles sources d'ingestion en s'appuyant sur `configs/sources.yaml`.
- Documenter les flux métier clés dans `docs/`.
- Étendre la pipeline (ex: nouveaux parsers, enrichissements, outputs).
