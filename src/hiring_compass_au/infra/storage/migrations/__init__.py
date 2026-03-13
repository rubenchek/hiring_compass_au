from __future__ import annotations

import sqlite3

from .migration_0001_job_ads_columns import apply as apply_0001_job_ads_columns
from .migration_0002_job_ad_enrichment_in_progress import (
    apply as apply_0002_job_ad_enrichment_in_progress,
)

_MIGRATIONS = (
    ("0001_job_ads_columns", apply_0001_job_ads_columns),
    (
        "0002_job_ad_enrichment_in_progress",
        apply_0002_job_ad_enrichment_in_progress,
    ),
)


def apply_migrations(conn: sqlite3.Connection) -> bool:
    applied_any = False
    for _name, apply_fn in _MIGRATIONS:
        applied = apply_fn(conn)
        if applied:
            applied_any = True
    if applied_any:
        conn.commit()
    return applied_any
