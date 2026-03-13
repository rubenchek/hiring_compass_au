from __future__ import annotations

import sqlite3

from ._utils import column_exists


def apply(conn: sqlite3.Connection) -> bool:
    applied = False

    if not column_exists(conn, "job_ads", "company_id"):
        conn.execute("ALTER TABLE job_ads ADD COLUMN company_id INTEGER")
        applied = True

    if not column_exists(conn, "job_ads", "listing_date_utc"):
        conn.execute("ALTER TABLE job_ads ADD COLUMN listing_date_utc TEXT")
        applied = True

    return applied
