from __future__ import annotations

from hiring_compass_au.infra.storage.hit_store import (
    update_job_hit_canonicalization,
    update_promoted_job_hits,
    upsert_email_job_hits,
)


def _insert_indexed_email(conn, message_id: str = "m"):
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES (?, 't', 'indexed', 'x')",
        (message_id,),
    )


def test_upsert_email_job_hits_inserts_and_updates(conn):
    _insert_indexed_email(conn)

    parser_cfg = {"source": "seek", "parser_name": "seek_mail_parser", "parser_version": "v1"}
    hits = [
        {
            "out_url": "u1",
            "title": "t1",
            "company": "c1",
            "debug_lines": ["a", "b"],
            "hit_confidence": 90,
        }
    ]

    n1 = upsert_email_job_hits(conn, "m", hits, parser_cfg)
    assert n1 == 1

    # same out_url, updated fields -> should UPDATE (ON CONFLICT)
    hits2 = [
        {
            "out_url": "u1",
            "title": "t2",
            "company": "c2",
            "debug_lines": ["x"],
            "hit_confidence": 50,
        }
    ]
    n2 = upsert_email_job_hits(conn, "m", hits2, parser_cfg)
    assert n2 == 1

    row = conn.execute(
        "SELECT out_url, title, company FROM email_job_hits WHERE message_id='m' AND out_url='u1'"
    ).fetchone()
    assert row["title"] == "t2"
    assert row["company"] == "c2"


def test_update_job_hit_canonicalization_ok_sets_pending(conn):
    _insert_indexed_email(conn)
    conn.execute(
        "INSERT INTO email_job_hits(message_id, out_url, source) VALUES ('m', 'u1', 'seek')"
    )
    hit_id = conn.execute("SELECT hit_id FROM email_job_hits").fetchone()["hit_id"]

    update_job_hit_canonicalization(
        conn,
        hit_id=hit_id,
        outcome="ok",
        http_status=302,
        canonical_url="https://www.seek.com.au/job/1",
        external_job_id="1",
        canon_error=None,
    )

    row = conn.execute(
        "SELECT canonical_status, attempt_count, promote_status FROM email_job_hits WHERE hit_id=?",
        (hit_id,),
    ).fetchone()
    assert row["canonical_status"] == "ok"
    assert row["attempt_count"] == 1
    assert row["promote_status"] == "pending"


def test_update_job_hit_canonicalization_retry_sets_next_retry_at(conn):
    _insert_indexed_email(conn)
    conn.execute(
        "INSERT INTO email_job_hits(message_id, out_url, source) VALUES ('m', 'u1', 'seek')"
    )
    hit_id = conn.execute("SELECT hit_id FROM email_job_hits").fetchone()["hit_id"]

    update_job_hit_canonicalization(conn, hit_id=hit_id, outcome="retry", canon_error="timeout")

    row = conn.execute(
        "SELECT canonical_status, next_retry_at, attempt_count FROM email_job_hits WHERE hit_id=?",
        (hit_id,),
    ).fetchone()
    assert row["canonical_status"] == "retry"
    assert row["attempt_count"] == 1
    assert row["next_retry_at"] is not None


def test_update_job_hit_canonicalization_error_rejects(conn):
    _insert_indexed_email(conn)
    conn.execute(
        "INSERT INTO email_job_hits(message_id, out_url, source) VALUES ('m', 'u1', 'seek')"
    )
    hit_id = conn.execute("SELECT hit_id FROM email_job_hits").fetchone()["hit_id"]

    update_job_hit_canonicalization(conn, hit_id=hit_id, outcome="error", canon_error="404")

    row = conn.execute(
        "SELECT canonical_status, promote_status FROM email_job_hits WHERE hit_id=?",
        (hit_id,),
    ).fetchone()
    assert row["canonical_status"] == "error"
    assert row["promote_status"] == "rejected"


def test_update_promoted_job_hits_returns_rowcount(conn):
    _insert_indexed_email(conn)
    conn.executemany(
        "INSERT INTO email_job_hits(message_id, out_url, source, promote_status) "
        "VALUES ('m', ?, 'seek', 'pending')",
        [("u1",), ("u2",)],
    )
    hit_ids = [
        r["hit_id"] for r in conn.execute("SELECT hit_id FROM email_job_hits ORDER BY hit_id")
    ]

    updated = update_promoted_job_hits(conn, hits_upserted=[hit_ids[0]], hits_failed=[hit_ids[1]])
    assert updated == 2
