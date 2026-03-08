from __future__ import annotations

from hiring_compass_au.infra.storage.mail_store import (
    update_fetched_email_metadata,
    update_parsed_email,
    upsert_indexed_emails,
)


def test_upsert_indexed_emails_is_idempotent(conn):
    messages = [{"id": "m1", "threadId": "t1"}, {"id": "m2", "threadId": "t2"}]

    inserted1 = upsert_indexed_emails(conn, messages)
    inserted2 = upsert_indexed_emails(conn, messages)

    assert inserted1 == 2
    assert inserted2 == 0

    rows = conn.execute("SELECT message_id, status FROM emails ORDER BY message_id").fetchall()
    assert [(r["message_id"], r["status"]) for r in rows] == [("m1", "indexed"), ("m2", "indexed")]


def test_update_fetched_email_metadata_sets_fetched_status_and_received_at(conn):
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed','x')"
    )

    n = update_fetched_email_metadata(
        conn,
        [
            {
                "message_id": "m",
                "from_email": "jobmail@s.seek.com.au",
                "subject": "subj",
                "internal_date_ms": 1000,
                "html_raw": "<html/>",
                "error": None,
            }
        ],
    )
    assert n == 1

    row = conn.execute(
        "SELECT status, received_at, internal_date_ms, error FROM emails WHERE message_id='m'"
    ).fetchone()
    assert row["status"] == "fetched"
    assert row["internal_date_ms"] == 1000
    assert row["received_at"] is not None
    assert row["error"] is None


def test_update_fetched_email_metadata_allows_null_internal_date_ms(conn):
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed','x')"
    )

    n = update_fetched_email_metadata(
        conn,
        [
            {
                "message_id": "m",
                "from_email": None,
                "subject": None,
                "internal_date_ms": None,
                "html_raw": None,
                "error": "boom",
            }
        ],
    )
    assert n == 1

    row = conn.execute(
        "SELECT status, received_at, error FROM emails WHERE message_id='m'"
    ).fetchone()
    assert row["status"] == "fetch_error"
    assert row["received_at"] is None
    assert row["error"] == "boom"


def test_update_parsed_email_status_transitions(conn):
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','fetched','x')"
    )

    assert update_parsed_email(conn, "m", supported=False) == 1
    status = conn.execute("SELECT status FROM emails WHERE message_id='m'").fetchone()["status"]
    assert status == "parsed_unsupported"

    conn.execute("UPDATE emails SET status='fetched' WHERE message_id='m'")
    assert update_parsed_email(conn, "m", supported=True, hit_parsed_count=0) == 1
    status = conn.execute("SELECT status FROM emails WHERE message_id='m'").fetchone()["status"]
    assert status == "parsed_empty"

    conn.execute("UPDATE emails SET status='fetched' WHERE message_id='m'")
    assert update_parsed_email(conn, "m", supported=True, hit_parsed_count=2) == 1
    status = conn.execute("SELECT status FROM emails WHERE message_id='m'").fetchone()["status"]
    assert status == "parsed"

    conn.execute("UPDATE emails SET status='fetched' WHERE message_id='m'")
    assert update_parsed_email(conn, "m", error="x") == 1
    status = conn.execute("SELECT status FROM emails WHERE message_id='m'").fetchone()["status"]
    assert status == "parsed_error"
