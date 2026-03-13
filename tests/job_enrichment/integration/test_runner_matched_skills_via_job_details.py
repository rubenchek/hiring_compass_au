from __future__ import annotations

from hiring_compass_au.domain.models import JobAdData
from hiring_compass_au.services.job_enrichment.handlers.seek.job_details import (
    handler as handler_mod,
)
from hiring_compass_au.services.job_enrichment.handlers.seek.source_models import (
    SeekEnrichmentData,
)
from hiring_compass_au.services.job_enrichment.models import FetchResult, ParseResult
from hiring_compass_au.services.job_enrichment.runner import run_enrichment_batch


def test_job_details_marks_matched_skills_success(conn, monkeypatch):
    conn.execute(
        """
        INSERT INTO job_ads (source, canonical_url)
        VALUES ('seek', 'u1')
        """
    )
    job_id = conn.execute("SELECT id FROM job_ads").fetchone()[0]

    conn.executemany(
        """
        INSERT INTO job_ad_enrichment (job_id, enrich_type, enrich_status)
        VALUES (?, ?, 'pending')
        """,
        [
            (job_id, "jobDetails"),
            (job_id, "matchedSkills"),
        ],
    )
    conn.commit()

    def fake_fetch(_target, session):
        return FetchResult(http_status=200, headers={}, payload={})

    def fake_parse(_fetch_result, _target):
        job_ad = JobAdData(title="Engineer")
        source = SeekEnrichmentData(skills=["Python"])
        return ParseResult(job_ad_patch=job_ad, source_patch=source)

    monkeypatch.setattr(handler_mod, "fetch_job_details", fake_fetch)
    monkeypatch.setattr(handler_mod, "parse_job_details", fake_parse)

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

    assert summary.success == 2

    statuses = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT enrich_type, enrich_status FROM job_ad_enrichment WHERE job_id = ?",
            (job_id,),
        ).fetchall()
    }
    assert statuses["jobDetails"] == "ok"
    assert statuses["matchedSkills"] == "ok"
