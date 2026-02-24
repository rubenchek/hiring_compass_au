from __future__ import annotations

import sqlite3
import json
from hiring_compass_au.data.storage.db import compute_backoff_minutes, utc_now_iso
from datetime import datetime, timedelta, timezone


# ----------------------------
# Fill database
# ----------------------------

def upsert_email_job_hits(
    conn: sqlite3.Connection,
    message_id: str,
    valid_hits: list,
    parser_cfg: dict,
) -> int:
    """
    Upsert all hits for a single email.
    - No commit here (runner owns transaction).
    Returns number of parsed emails.
    """ 
    
    sql = """
    INSERT INTO email_job_hits (
        message_id,
        source,
        fingerprint,
        out_url,
        title,
        company,
        suburb,
        city,
        state,
        location_raw,
        salary_min,
        salary_max,
        salary_period,
        salary_raw,
        debug_lines,
        hit_confidence,
        parser_name,
        parser_version
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(message_id, out_url) DO UPDATE SET
        source = excluded.source,
        fingerprint = excluded.fingerprint,
        title = excluded.title,
        company = excluded.company,
        suburb = excluded.suburb,
        city = excluded.city,
        state = excluded.state,
        location_raw = excluded.location_raw,
        salary_min = excluded.salary_min,
        salary_max = excluded.salary_max,
        salary_period = excluded.salary_period,
        salary_raw = excluded.salary_raw,
        debug_lines = excluded.debug_lines,
        hit_confidence = excluded.hit_confidence,
        parser_name = excluded.parser_name,
        parser_version = excluded.parser_version
    """
    rows = []
    for hit in valid_hits:       
        debug_lines = hit.get("debug_lines")
        debug_lines_json = json.dumps(debug_lines) if isinstance(debug_lines, list) else None
        
        row = (
            message_id,
            parser_cfg["source"],
            hit.get("fingerprint"),
            hit.get("out_url"),
            hit.get("title"),
            hit.get("company"),
            hit.get("suburb"),
            hit.get("city"),
            hit.get("state"),
            hit.get("location_raw"),
            hit.get("salary_min"),
            hit.get("salary_max"),
            hit.get("salary_period"),
            hit.get("salary_raw"),
            debug_lines_json,
            int(hit.get("hit_confidence") or 0),
            parser_cfg["parser_name"],
            parser_cfg["parser_version"],
        )
        rows.append(row)

    if rows:
        conn.executemany(sql, rows)
    
    return len(rows)


# ----------------------------
# Update database
# ----------------------------

def update_job_hit_canonicalization(
    conn,
    hit_id: int,                      # ou (message_id, hit_rank) etc selon ta PK
    outcome: str,                     # "ok" | "retry" | "error"
    http_status: int | None = None,
    canonical_url: str | None = None,
    external_job_id: str | None = None,
    canon_error: str | None = None,
) -> None:
    if outcome not in {"ok", "retry", "error"}:
        raise ValueError(f"Invalid outcome: {outcome}")
    
    now = utc_now_iso()
    
    # Fetch current attempt_count so backoff uses the incremented value
    row = conn.execute(
        "SELECT attempt_count FROM email_job_hits WHERE hit_id = ?",
        (hit_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"email_job_hits hit_id not found: {hit_id}")
    
    attempt_count_before = int(row["attempt_count"]) 
    attempt_count_after = attempt_count_before + 1
    
    next_retry_at = None
    
    if outcome == "ok":
        canon_error = None
        next_retry_at = None
        promote_status = 'pending'

    elif outcome == "retry":
        delay_min = compute_backoff_minutes(attempt_count_after)
        dt = datetime.now(timezone.utc) + timedelta(minutes=delay_min)
        next_retry_at = dt.replace(microsecond=0).isoformat()
        promote_status = 'pending'
    
    else:
        next_retry_at = None
        promote_status = 'rejected'

    conn.execute(
        """
        UPDATE email_job_hits
        SET
            external_job_id = ?,
            canonical_url = ?,
            canonical_status = ?,
            http_status = ?,
            attempt_count = ?,
            next_retry_at = ?,
            last_attempt_at = ?,
            canon_error = ?,
            promote_status = ?
        WHERE hit_id = ?
        """,
        (
            external_job_id,
            canonical_url,
            outcome,
            http_status,
            attempt_count_after,
            next_retry_at,
            now,
            canon_error,
            promote_status,
            hit_id,
        ),
    )
    
    
def update_promoted_job_hits(
    conn: sqlite3.Connection, 
    hits_upserted: list, 
    hits_failed: list,
    failed_reason = None):
    sql= """
    UPDATE email_job_hits
    SET promote_status = ?, promote_reason = ?
    WHERE hit_id = ?
    """

    rows: list[tuple[str, str | None, int]] = []

    for hit_id in hits_upserted:
        if hit_id is None:
            continue
        rows.append(("promoted", None, int(hit_id)))

    for hit_id in hits_failed:
        if hit_id is None:
            continue
        rows.append(("rejected", failed_reason, int(hit_id)))

    if rows:
        conn.executemany(sql, rows)


# ----------------------------
# Request database
# ----------------------------

def count_urls_to_canonicalize(conn: sqlite3.Connection, max_attempts: int = 10) -> int:
    now = utc_now_iso()
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM email_job_hits
        WHERE
            canonical_status IN ('pending','retry')
            AND attempt_count < ?
            AND (next_retry_at IS NULL OR next_retry_at <= ?)
            AND TRIM(out_url) <> ''
        """,
        (max_attempts, now),
    ).fetchone()
    return int(row[0])



def get_batch_url_to_canonicalize(conn: sqlite3.Connection, limit: int, max_attempts: int = 10,) -> list[sqlite3.Row]:
    """
    Returns a batch of email_job_hits rows to canonicalize.
    Expects sqlite row_factory=sqlite3.Row.
    """
    now = utc_now_iso()
    
    return conn.execute(
        """
        SELECT hit_id, out_url
        FROM email_job_hits
        WHERE
            canonical_status IN ('pending','retry')
            AND promote_status IN ('new')
            AND attempt_count < ?
            AND (next_retry_at IS NULL OR next_retry_at <= ?)
            AND TRIM(out_url) <> ''
        ORDER BY
            -- prefer oldest never-attempted
            CASE WHEN last_attempt_at IS NULL THEN 0 ELSE 1 END ASC,
            CASE WHEN last_attempt_at IS NULL THEN hit_id END ASC,
            -- then oldest next_retry_at/last_attempt_at first
            CASE WHEN last_attempt_at IS NOT NULL THEN next_retry_at END ASC,
            hit_id ASC
        LIMIT ?
        """,
        (max_attempts, now, limit),
    ).fetchall()


def get_promote_pending_job_hits(conn: sqlite3.Connection, limit: int = 200):
    return conn.execute(
    """
    SELECT 
        hit_id,
        external_job_id, 
        source,
        canonical_url,
        fingerprint,
        title,
        company,
        suburb,
        city,
        state,
        location_raw,
        salary_min,
        salary_max,
        salary_period,
        salary_raw
    FROM email_job_hits   
    WHERE
        promote_status = 'pending'
        AND canonical_status = 'ok'
        AND canonical_url IS NOT NULL
        AND TRIM(canonical_url) <> ''
        AND source IS NOT NULL
        AND TRIM(source) <> ''
    ORDER BY hit_id
    LIMIT ?
    """,
    (limit,),
    )