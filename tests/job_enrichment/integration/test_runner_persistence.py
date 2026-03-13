from __future__ import annotations

from hiring_compass_au.domain.models import CompanyData, JobAdData
from hiring_compass_au.services.job_enrichment.handlers.seek.source_models import (
    SeekEnrichmentData,
)
from hiring_compass_au.services.job_enrichment.models import (
    EnrichmentResult,
    FetchResult,
    ParseResult,
)
from hiring_compass_au.services.job_enrichment.runner import run_enrichment_batch


class DummyHandler:
    def enrich(self, _target):
        job_ad = JobAdData(
            external_job_id="ext-1",
            title="Engineer",
            company="ACME",
            location_raw="Sydney NSW",
            salary_raw="$100k",
            listing_date_utc="2026-01-01T00:00:00Z",
        )
        source = SeekEnrichmentData(
            advertiser_id="123",
            work_types="Full time",
            skills=["Python"],
        )
        company = CompanyData(
            name="ACME",
            seek_company_id="123",
        )
        return EnrichmentResult(
            fetch_result=FetchResult(http_status=200, headers={}, payload={}),
            parse_result=ParseResult(
                job_ad_patch=job_ad,
                source_patch=source,
                company_patch=company,
            ),
        )

    def persist_source_patch(self, conn, job_id: int, patch: dict):
        from hiring_compass_au.infra.storage.seek_enrichment_store import (
            upsert_seek_enrichment,
        )

        upsert_seek_enrichment(conn, job_id=job_id, patch=patch)

    def post_persist(self, *, conn, target, result) -> int:
        return 1


def test_runner_persists_patches_and_links_company(conn, monkeypatch):
    conn.execute(
        """
        INSERT INTO job_ads (source, canonical_url)
        VALUES ('seek', 'u1')
        """
    )
    job_id = conn.execute("SELECT id FROM job_ads").fetchone()[0]
    conn.execute(
        """
        INSERT INTO job_ad_enrichment (job_id, enrich_type, enrich_status)
        VALUES (?, 'jobDetails', 'pending')
        """,
        (job_id,),
    )
    conn.commit()

    monkeypatch.setattr(
        "hiring_compass_au.services.job_enrichment.runner.dispatch_handler",
        lambda **_k: DummyHandler(),
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

    assert summary.success == 2  # 1 item + post_persist extra

    row = conn.execute("SELECT title, company_id FROM job_ads WHERE id = ?", (job_id,)).fetchone()
    assert row["title"] == "Engineer"
    assert row["company_id"] is not None

    c_row = conn.execute("SELECT name FROM company WHERE id = ?", (row["company_id"],)).fetchone()
    assert c_row["name"] == "ACME"

    se_row = conn.execute(
        "SELECT advertiser_id FROM seek_enrichment WHERE job_id = ?", (job_id,)
    ).fetchone()
    assert se_row["advertiser_id"] == 123

    e_row = conn.execute(
        "SELECT enrich_status FROM job_ad_enrichment WHERE job_id = ?", (job_id,)
    ).fetchone()
    assert e_row["enrich_status"] == "ok"
