# Hiring Compass AU

Système expérimental pour piloter et automatiser la chaîne de candidature en Australie, en restant traçable et contrôlable. Le périmètre et les règles sont décrits dans `docs/design-doc.md`.

## Prérequis
- Python >= 3.11
- `pip` récent

## Installation rapide
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Initialiser l'espace de travail
Crée les dossiers attendus (`data`, `models`, `metrics`, `configs`, `notebook`, `logs`, etc.) :
```bash
python scripts/bootstrap_workspace.py
```

## Structure actuelle
- `docs/` : documents de cadrage (`design-doc.md`).
- `data/raw` : dumps des offres ou données collectées (ignorées par Git).
- `data/processed` : données nettoyées/features prêtes à l'emploi (ignorées par Git).
- `notebook/` : explorations et essais rapides.
- `src/hiring_compass/` : code Python du projet (workspace utils dans `workspace.py`).
- `scripts/` : scripts utilitaires (`bootstrap_workspace.py`).
- `models/` : artefacts de modèles/embeddings (ignorés par Git).
- `metrics/` : résultats et évaluations (ignorés par Git).
- `configs/` : configuration locale (ex. `*.yaml`, secrets dans `.env` à ne pas commit).
- `logs/` : traces d'exécution (ignorées par Git).

## Prochaines étapes suggérées
- Ajouter les premières sources d'ingestion (scrapers ou loaders) sous `src/hiring_compass/`.
- Documenter un flux minimal de scoring/candidature dans `docs/` ou `configs/`.
- Tracer les métriques de base dans `metrics/` dès les premiers essais.
