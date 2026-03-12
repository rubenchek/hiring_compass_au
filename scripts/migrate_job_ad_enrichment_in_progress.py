from __future__ import annotations

import sqlite3

from hiring_compass_au.config.settings import WorkspaceSettings
from hiring_compass_au.infra.storage.db import get_connection


def _table_has_in_progress(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = 'job_ad_enrichment'
        """
    ).fetchone()
    if row is None or row[0] is None:
        return False
    return "in_progress" in row[0]


def migrate(conn: sqlite3.Connection) -> bool:
    """
    Rebuilds job_ad_enrichment to add 'in_progress' to enrich_status CHECK constraint.
    Returns True if migration applied, False if already up-to-date.
    """
    if _table_has_in_progress(conn):
        return False

    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("BEGIN;")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_ad_enrichment_new(
                job_id          INTEGER,
                enrich_type     TEXT,
                enrich_status   TEXT NOT NULL DEFAULT 'pending'
                    CHECK (enrich_status IN ('pending','in_progress','ok','retry','error')),
                http_status     INTEGER,
                attempt_count   INTEGER NOT NULL DEFAULT 0,
                next_retry_at   TEXT,
                last_attempt_at TEXT,
                error           TEXT,
                fetched_at      TEXT,
                
                PRIMARY KEY (job_id, enrich_type),
                FOREIGN KEY (job_id) REFERENCES job_ads(id) ON DELETE CASCADE
            );
            """
        )

        conn.execute(
            """
            INSERT INTO job_ad_enrichment_new (
                job_id,
                enrich_type,
                enrich_status,
                http_status,
                attempt_count,
                next_retry_at,
                last_attempt_at,
                error,
                fetched_at
            )
            SELECT
                job_id,
                enrich_type,
                enrich_status,
                http_status,
                attempt_count,
                next_retry_at,
                last_attempt_at,
                error,
                fetched_at
            FROM job_ad_enrichment
            """
        )

        conn.execute("DROP TABLE job_ad_enrichment;")
        conn.execute("ALTER TABLE job_ad_enrichment_new RENAME TO job_ad_enrichment;")
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON;")

    return True


def main(ws: WorkspaceSettings = None) -> None:
    if not ws:
        ws = WorkspaceSettings()
    conn = get_connection(ws.db_path)
    try:
        applied = migrate(conn)
        if applied:
            print("Migration applied: job_ad_enrichment now supports 'in_progress'.")
        else:
            print("No migration needed: job_ad_enrichment already supports 'in_progress'.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
