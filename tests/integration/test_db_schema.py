from __future__ import annotations

import sqlite3

import pytest

from hiring_compass_au.data.storage.schema import init_all_tables


def test_db_schema_invariants_fk_unique_check_enforced():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    init_all_tables(conn)

    # Seed minimal parent row for FK tests
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed','x')"
    )

    # FK enforced: child cannot reference missing parent
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO email_job_hits(message_id, out_url, source) "
            "VALUES ('missing', 'u', 'seek')"
        )

    # UNIQUE enforced (idempotence invariant)
    conn.execute(
        "INSERT INTO email_job_hits(message_id, out_url, source) VALUES ('m', 'u', 'seek')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO email_job_hits(message_id, out_url, source) VALUES ('m', 'u', 'seek')"
        )

    # CHECK enforced (state machine invariant)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE email_job_hits SET canonical_status='bad' WHERE message_id='m'")

    # No FK violations in DB
    violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
    assert violations == []
