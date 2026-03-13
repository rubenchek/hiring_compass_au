from __future__ import annotations

import sqlite3
import sys

import hiring_compass_au.services.job_enrichment.__main__ as main_mod
from hiring_compass_au.services.job_enrichment.models import BatchSummary

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


def _create_old_schema(db_path):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(OLD_JOB_ADS_SCHEMA)
        conn.executescript(OLD_JOB_AD_ENRICHMENT_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def test_main_applies_migrations_when_needed(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    db_path = tmp_path / "data" / "local" / "state.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _create_old_schema(db_path)

    monkeypatch.setattr(sys, "argv", ["prog", "--limit", "1", "--max-batches", "1"])
    monkeypatch.setattr(main_mod, "run_enrichment", lambda *a, **k: BatchSummary())

    exit_code = main_mod.main()
    assert exit_code == 0

    conn = sqlite3.connect(db_path)
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(job_ads)").fetchall()}
        assert "company_id" in cols
        assert "listing_date_utc" in cols

        sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='job_ad_enrichment'"
        ).fetchone()[0]
        assert "in_progress" in sql
    finally:
        conn.close()
