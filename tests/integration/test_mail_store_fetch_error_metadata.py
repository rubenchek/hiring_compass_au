from __future__ import annotations

import sqlite3

from hiring_compass_au.data.storage.mail_store import update_fetched_email_metadata
from hiring_compass_au.data.storage.schema import init_all_tables


def test_update_fetched_email_metadata_allows_null_internal_date_ms():
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    init_all_tables(conn)

    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed','2026-01-01T00:00:00+00:00')"
    )
    conn.commit()

    updated = update_fetched_email_metadata(
        conn,
        [
            {
                "message_id": "m",
                "from_email": None,
                "subject": None,
                "internal_date_ms": None,
                "html_raw": None,
                "error": "Fetch error: boom",
            }
        ],
    )
    assert updated == 1
