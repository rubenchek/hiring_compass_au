from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from hiring_compass_au.data.storage.db import utc_now_iso

# ----------------------------
# Fill database
# ----------------------------


def upsert_indexed_emails(conn: sqlite3.Connection, messages: list[dict]) -> int:
    """
    Insert new message_ids into emails with status='indexed' and indexed_at=now.
    Do not overwrite existing rows.
    Return number of newly inserted rows.
    """
    if not messages:
        return 0

    now = utc_now_iso()

    rows = []
    for m in messages:
        msg_id = m.get("id")
        thread_id = m.get("threadId")
        if not msg_id:
            continue
        rows.append((msg_id, thread_id, now, "indexed"))

    if not rows:
        return 0

    before = conn.total_changes
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT OR IGNORE INTO emails (message_id, thread_id, indexed_at, status)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()
    after = conn.total_changes
    return after - before


# ----------------------------
# Update database
# ----------------------------


def update_fetched_email_metadata(conn: sqlite3.Connection, fetched_mail_batch: list[dict]) -> int:
    # TODO ajouter template

    if not fetched_mail_batch:
        return 0

    now = utc_now_iso()

    rows = []
    for m in fetched_mail_batch:
        message_id = m.get("message_id")
        if not message_id:
            continue

        from_email = m.get("from_email")
        subject = m.get("subject")
        internal_date_ms = m.get("internal_date_ms")
        if internal_date_ms is None:
            received_at = None
        else:
            received_at = datetime.fromtimestamp(
                internal_date_ms / 1000,
                tz=UTC,
            ).isoformat(timespec="seconds")
        html_raw = m.get("html_raw")
        error = m.get("error")

        status = "fetch_error" if error else "fetched"

        rows.append(
            (
                from_email,
                subject,
                internal_date_ms,
                received_at,
                html_raw,
                error,
                status,
                now,
                message_id,
            )
        )

    if not rows:
        return 0

    before = conn.total_changes
    cur = conn.cursor()

    cur.executemany(
        """
        UPDATE emails 
        SET
            from_email       = ?, 
            subject          = ?, 
            internal_date_ms = ?,
            received_at      = ?,
            html_raw         = ?,
            error            = ?,
            status           = ?,
            fetched_at       = ?
        WHERE message_id = ? 
          AND status = 'indexed'
        """,
        rows,
    )

    conn.commit()
    after = conn.total_changes
    return after - before


def update_parsed_email(
    conn: sqlite3.Connection,
    message_id: str,
    parser_cfg: dict = None,
    hit_parsed_count: int = 0,
    parsed_confidence: int = 0,
    supported: bool = True,
    error: str = None,
) -> int:
    """
    Update email row after parsing.

    - supported=False  => status='parsed_unsupported' (no parser registered)
    - supported=True and parsed_count==0 => status='parsed_empty'
    - supported=True and parsed_count>0  => status='parsed'
    """
    now = utc_now_iso()

    if parser_cfg:
        parser_name = parser_cfg["parser_name"]
        parser_version = parser_cfg["parser_version"]
    else:
        parser_name = parser_version = None

    if error:
        status = "parsed_error"
    elif not supported:
        status = "parsed_unsupported"
    elif hit_parsed_count == 0:
        status = "parsed_empty"
    else:
        status = "parsed"

    cur = conn.execute(
        """
        UPDATE emails
        SET
            status = ?,
            parsed_at = ?,
            parser_name = ?,
            parser_version = ?,
            hit_extract_count = ?,
            parsed_confidence = ?,
            error = ?
        WHERE message_id = ?
        """,
        (
            status,
            now,
            parser_name,
            parser_version,
            int(hit_parsed_count),
            parsed_confidence,
            error,
            message_id,
        ),
    )

    return int(cur.rowcount or 0)


# ----------------------------
# Request database
# ----------------------------


def get_last_internal_date_ms(conn: sqlite3.Connection, from_email: str) -> int | None:
    cur = conn.cursor()

    if from_email is None:
        cur.execute(
            """
            SELECT MAX(internal_date_ms) AS last_internal_date
            FROM emails
            WHERE internal_date_ms IS NOT NULL
            """,
        )
    else:
        cur.execute(
            """
            SELECT MAX(internal_date_ms) AS last_internal_date
            FROM emails
            WHERE internal_date_ms IS NOT NULL
                AND from_email = ?;
            """,
            (from_email,),
        )

    (value,) = cur.fetchone()
    return value


def get_non_fetched_email_list(conn: sqlite3.Connection) -> list:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_id FROM emails
        WHERE fetched_at IS NULL
        ORDER BY indexed_at
        """
    )
    rows = cur.fetchall()
    ids = [row[0] for row in rows]
    return ids


def get_fetched_emails_to_parse(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT message_id, from_email, internal_date_ms, html_raw
        FROM emails
        WHERE status = 'fetched' AND html_raw IS NOT NULL
        ORDER BY internal_date_ms
        """
    )
    yield from cur
