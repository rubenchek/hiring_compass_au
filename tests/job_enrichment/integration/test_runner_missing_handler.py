from __future__ import annotations

from hiring_compass_au.services.job_enrichment.dispatcher import HandlerNotFoundError
from hiring_compass_au.services.job_enrichment.models import MissingSessionError
from hiring_compass_au.services.job_enrichment.runner import run_enrichment_batch


def _seed_pending(conn):
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
    return job_id


def test_runner_marks_missing_handler(conn, monkeypatch):
    job_id = _seed_pending(conn)

    def boom(**_k):
        raise HandlerNotFoundError("nope")

    monkeypatch.setattr(
        "hiring_compass_au.services.job_enrichment.runner.dispatch_handler",
        boom,
    )

    summary = run_enrichment_batch(
        conn,
        enrich_type="jobDetails",
        limit=10,
        sessions_cache={"seek": object()},
        request_sleep_range=None,
        throttle_state=None,
        throttle_statuses=None,
        throttle_sleep_range=None,
        throttle_error_limit=None,
    )

    assert summary.skipped == 1
    row = conn.execute(
        "SELECT enrich_status, error FROM job_ad_enrichment WHERE job_id = ?", (job_id,)
    ).fetchone()
    assert row["enrich_status"] == "error"
    assert "missing_handler" in row["error"]


def test_runner_marks_missing_session(conn, monkeypatch):
    job_id = _seed_pending(conn)

    def boom(**_k):
        raise MissingSessionError("no session")

    monkeypatch.setattr(
        "hiring_compass_au.services.job_enrichment.runner.dispatch_handler",
        boom,
    )

    summary = run_enrichment_batch(
        conn,
        enrich_type="jobDetails",
        limit=10,
        sessions_cache={"seek": None},
        request_sleep_range=None,
        throttle_state=None,
        throttle_statuses=None,
        throttle_sleep_range=None,
        throttle_error_limit=None,
    )

    assert summary.skipped == 1
    row = conn.execute(
        "SELECT enrich_status, error FROM job_ad_enrichment WHERE job_id = ?", (job_id,)
    ).fetchone()
    assert row["enrich_status"] == "error"
    assert "missing_session" in row["error"]
