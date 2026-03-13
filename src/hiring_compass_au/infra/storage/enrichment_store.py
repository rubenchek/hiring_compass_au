import sqlite3
from datetime import UTC, datetime, timedelta

from hiring_compass_au.infra.storage.db import compute_backoff_minutes, utc_now_iso


def add_to_job_ad_enrichment_queue(conn: sqlite3.Connection, promoted_jobs: list):
    for row in promoted_jobs:
        if row["source"] == "seek":
            for enrichment in ["jobDetails", "matchedSkills"]:
                conn.execute(
                    """
                INSERT INTO job_ad_enrichment (
                    job_id,
                    enrich_type,
                    enrich_status)
                VALUES (?, ?, ?)
                ON CONFLICT (job_id, enrich_type) DO NOTHING
                """,
                    (row["id"], enrichment, "pending"),
                )


def get_pending_enrichment_types(
    conn: sqlite3.Connection,
    *,
    max_attempts: int = 10,
) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT enrich_type
        FROM job_ad_enrichment
        WHERE
            enrich_status IN ('pending','retry')
            AND attempt_count < ?
        ORDER BY enrich_type ASC
        """,
        (max_attempts,),
    ).fetchall()
    return [r[0] for r in rows]


def get_pending_enrichment_counts(
    conn: sqlite3.Connection,
    *,
    max_attempts: int = 10,
) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT enrich_type, COUNT(*) AS n
        FROM job_ad_enrichment
        WHERE
            enrich_status IN ('pending','retry')
            AND attempt_count < ?
        GROUP BY enrich_type
        ORDER BY enrich_type ASC
        """,
        (max_attempts,),
    ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def get_ready_enrichment_batch(
    conn: sqlite3.Connection,
    limit: int = 50,
    *,
    max_attempts: int = 10,
    enrich_type: str | None = None,
) -> list[sqlite3.Row]:
    """
    Atomically reserves a batch of job_ad_enrichment rows for processing by
    setting enrich_status='in_progress' in a single transaction.
    Expects sqlite row_factory=sqlite3.Row.
    """
    now = utc_now_iso()
    started_tx = False

    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE;")
        started_tx = True

    try:
        enrich_type_filter = ""
        params: list = [now, max_attempts, now, limit]
        if enrich_type is not None:
            enrich_type_filter = "AND enrich_type = ?"
            params = [now, enrich_type, max_attempts, now, limit]

        conn.execute(
            f"""
            UPDATE job_ad_enrichment
            SET
                enrich_status = 'in_progress',
                attempt_count = attempt_count + 1,
                last_attempt_at = ?,
                next_retry_at = NULL
            WHERE rowid IN (
                SELECT e.rowid
                FROM job_ad_enrichment e
                JOIN job_ads j ON j.id = e.job_id
                LEFT JOIN seek_enrichment se ON se.job_id = e.job_id
                WHERE
                    e.enrich_status IN ('pending','retry')
                    {enrich_type_filter}
                    AND e.attempt_count < ?
                    AND (e.next_retry_at IS NULL OR e.next_retry_at <= ?)
                    AND (se.status IS NULL OR se.status NOT IN ('Expired'))
                ORDER BY
                    -- prefer newest jobs first; fallback to id when listing_date_utc is null
                    (j.listing_date_utc IS NULL) ASC,
                    j.listing_date_utc DESC,
                    j.id DESC,
                    enrich_type ASC
                LIMIT ?
            )
            """,
            params,
        )

        rows = conn.execute(
            f"""
            SELECT
                e.job_id,
                e.enrich_type,
                e.attempt_count,
                e.last_attempt_at,
                j.source,
                j.external_job_id,
                j.canonical_url
            FROM job_ad_enrichment e
            JOIN job_ads j ON j.id = e.job_id
            LEFT JOIN seek_enrichment se ON se.job_id = e.job_id
            WHERE
                e.enrich_status = 'in_progress'
                AND e.last_attempt_at = ?
                {enrich_type_filter}
                AND (se.status IS NULL OR se.status NOT IN ('Expired'))
            ORDER BY e.job_id ASC, e.enrich_type ASC
            """,
            [now, *([enrich_type] if enrich_type is not None else [])],
        ).fetchall()

        if started_tx:
            conn.execute("COMMIT;")

        return rows
    except Exception:
        if started_tx:
            conn.execute("ROLLBACK;")
        raise


def mark_enrichment_failed(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    enrich_type: str,
    http_status: int | None,
    error_code: str,
    error_message: str,
) -> None:
    now = utc_now_iso()
    error_text = f"{error_code}: {error_message}" if error_code else error_message
    conn.execute(
        """
        UPDATE job_ad_enrichment
        SET
            enrich_status = 'error',
            http_status = ?,
            error = ?,
            last_attempt_at = ?,
            next_retry_at = NULL
        WHERE job_id = ? AND enrich_type = ?
        """,
        (http_status, error_text, now, job_id, enrich_type),
    )


def mark_enrichment_success(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    enrich_type: str,
    http_status: int | None,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE job_ad_enrichment
        SET
            enrich_status = 'ok',
            http_status = ?,
            fetched_at = ?,
            last_attempt_at = ?
        WHERE job_id = ? AND enrich_type = ?
        """,
        (http_status, now, now, job_id, enrich_type),
    )


def compute_next_retry_at(*, attempt_count: int) -> str:
    now = utc_now_iso()
    dt = datetime.fromisoformat(now)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    minutes = compute_backoff_minutes(attempt_count)
    return (dt + timedelta(minutes=minutes)).isoformat()


def mark_enrichment_retry(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    enrich_type: str,
    http_status: int | None,
    error_code: str,
    error_message: str,
    next_retry_at_utc: str,
) -> None:
    now = utc_now_iso()
    error_text = f"{error_code}: {error_message}" if error_code else error_message
    conn.execute(
        """
        UPDATE job_ad_enrichment
        SET
            enrich_status = 'retry',
            http_status = ?,
            error = ?,
            last_attempt_at = ?,
            next_retry_at = ?
        WHERE job_id = ? AND enrich_type = ?
        """,
        (http_status, error_text, now, next_retry_at_utc, job_id, enrich_type),
    )
