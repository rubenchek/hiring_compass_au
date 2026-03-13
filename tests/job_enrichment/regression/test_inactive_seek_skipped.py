from __future__ import annotations

from hiring_compass_au.infra.storage.enrichment_store import get_ready_enrichment_batch


def test_inactive_seek_status_is_skipped(conn):
    conn.execute("INSERT INTO job_ads (source, canonical_url) VALUES ('seek', 'u1')")
    job_id = conn.execute("SELECT id FROM job_ads").fetchone()[0]
    conn.execute(
        "INSERT INTO job_ad_enrichment (job_id, enrich_type, enrich_status) "
        "VALUES (?, 'jobDetails', 'pending')",
        (job_id,),
    )
    conn.execute(
        "INSERT INTO seek_enrichment (job_id, status) VALUES (?, 'Expired')",
        (job_id,),
    )
    conn.commit()

    rows = get_ready_enrichment_batch(conn=conn, limit=10, enrich_type="jobDetails")
    assert rows == []
