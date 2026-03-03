from __future__ import annotations

from hiring_compass_au.data.storage.job_store import update_job_ad_enrichment, update_job_ads


def _insert_hit(conn, *, hit_id: int, canonical_url: str):
    conn.execute(
        """
        INSERT INTO email_job_hits(
            hit_id, message_id, out_url, source, canonical_status, promote_status, canonical_url
        )
        VALUES (?, 'm', ?, 'seek', 'ok', 'pending', ?)
        """,
        (hit_id, f"o{hit_id}", canonical_url),
    )


def test_update_job_ads_creates_enrichment_pending_and_is_idempotent(conn):
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed','x')"
    )
    _insert_hit(conn, hit_id=1, canonical_url="c1")
    _insert_hit(conn, hit_id=2, canonical_url="c2")

    hits = list(
        conn.execute(
            "SELECT hit_id, external_job_id, source, "
            "canonical_url, fingerprint, title, company, suburb, city, state, "
            "location_raw, salary_min, salary_max, salary_period, salary_raw "
            "FROM email_job_hits ORDER BY hit_id"
        )
    )

    promoted, upserted, failed, attempted = update_job_ads(conn, hits, attempted_keys=set())
    assert len(promoted) == 2
    assert len(upserted) == 2
    assert len(failed) == 0
    assert len(attempted) == 2

    update_job_ad_enrichment(conn, promoted)

    rows = conn.execute(
        "SELECT enrich_type, enrich_status FROM job_ad_enrichment ORDER BY job_id, enrich_type"
    ).fetchall()
    assert len(rows) == 4  # 2 jobs * (jobDetails + matchedSkills)
    assert {r["enrich_type"] for r in rows} == {"jobDetails", "matchedSkills"}
    assert all(r["enrich_status"] == "pending" for r in rows)

    # idempotent: calling again doesn't duplicate (PK(job_id,enrich_type))
    update_job_ad_enrichment(conn, promoted)
    rows2 = conn.execute("SELECT COUNT(*) AS n FROM job_ad_enrichment").fetchone()
    assert rows2["n"] == 4
