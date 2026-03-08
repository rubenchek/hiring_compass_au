from __future__ import annotations

from hiring_compass_au.services.job_alerts.promote.runner import run_promote_job_ad


def test_run_promote_creates_pending_enrichment(conn):
    conn.execute(
        """
        INSERT INTO emails(message_id, thread_id, status, indexed_at) 
        VALUES ('m','t','indexed','x')
        """
    )
    conn.executemany(
        """
        INSERT INTO email_job_hits(
            message_id, 
            out_url, 
            source, 
            promote_status, 
            canonical_status, 
            canonical_url
        )
        VALUES ('m', ?, 'seek', 'pending', 'ok', ?)
        """,
        [("o1", "c1"), ("o2", "c2")],
    )
    conn.commit()

    new, updated, failed = run_promote_job_ad(conn)
    assert (new, updated, failed) == (2, 0, 0)

    n = conn.execute("SELECT COUNT(*) AS n FROM job_ad_enrichment").fetchone()["n"]
    assert n == 4  # 2 jobs * 2 enrich types

    # idempotence: running again doesn't add more enrichment rows
    run_promote_job_ad(conn)
    n2 = conn.execute("SELECT COUNT(*) AS n FROM job_ad_enrichment").fetchone()["n"]
    assert n2 == 4
