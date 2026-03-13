from __future__ import annotations

import sqlite3

from ._utils import table_sql


def apply(conn: sqlite3.Connection) -> bool:
    sql = table_sql(conn, "job_ad_enrichment")
    if not sql:
        return False

    if "in_progress" in sql:
        return False

    conn.execute("ALTER TABLE job_ad_enrichment RENAME TO job_ad_enrichment_old")

    conn.execute(
        """
        CREATE TABLE job_ad_enrichment(
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
        INSERT INTO job_ad_enrichment(
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
        FROM job_ad_enrichment_old
        """
    )

    conn.execute("DROP TABLE job_ad_enrichment_old")
    return True
