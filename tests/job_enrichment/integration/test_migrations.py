from __future__ import annotations

import sqlite3

from hiring_compass_au.infra.storage.migrations import apply_migrations

OLD_JOB_ADS_SCHEMA = """
CREATE TABLE job_ads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    external_job_id TEXT,
    fingerprint     TEXT,
    title           TEXT,
    company         TEXT,
    suburb          TEXT,
    city            TEXT,
    state           TEXT,
    location_raw    TEXT,
    salary_min      INTEGER,
    salary_max      INTEGER,
    salary_period   TEXT,
    salary_raw      TEXT,
    description     TEXT,
    job_status      TEXT DEFAULT 'new',
    canonical_url   TEXT NOT NULL,
    first_seen_at   TEXT,
    last_seen_at    TEXT,
    UNIQUE(source, canonical_url),
    UNIQUE(source, external_job_id)
);
"""

OLD_JOB_AD_ENRICHMENT_SCHEMA = """
CREATE TABLE job_ad_enrichment(
    job_id          INTEGER,
    enrich_type     TEXT,
    enrich_status   TEXT NOT NULL DEFAULT 'pending'
        CHECK (enrich_status IN ('pending','ok','retry','error')),
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


def _create_old_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(OLD_JOB_ADS_SCHEMA)
    conn.executescript(OLD_JOB_AD_ENRICHMENT_SCHEMA)


def test_apply_migrations_upgrades_old_schema():
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        _create_old_schema(conn)

        conn.execute("INSERT INTO job_ads (source, canonical_url) VALUES ('seek', 'u1')")
        job_id = conn.execute("SELECT id FROM job_ads").fetchone()[0]
        conn.execute(
            """
            INSERT INTO job_ad_enrichment (job_id, enrich_type, enrich_status)
            VALUES (?, 'jobDetails', 'pending')
            """,
            (job_id,),
        )
        conn.commit()

        applied = apply_migrations(conn)
        assert applied is True

        cols = {row[1] for row in conn.execute("PRAGMA table_info(job_ads)").fetchall()}
        assert "company_id" in cols
        assert "listing_date_utc" in cols

        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='job_ad_enrichment'"
        ).fetchone()[0]
        assert "in_progress" in sql

        row = conn.execute(
            "SELECT enrich_status FROM job_ad_enrichment WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        assert row[0] == "pending"
    finally:
        conn.close()
