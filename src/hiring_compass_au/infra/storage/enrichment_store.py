import sqlite3

from hiring_compass_au.infra.storage.db import utc_now_iso


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


def get_ready_enrichment_batch(
    conn: sqlite3.Connection,
    limit: int = 50,
    *,
    max_attempts: int = 10,
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
        conn.execute(
            """
            UPDATE job_ad_enrichment
            SET
                enrich_status = 'in_progress',
                attempt_count = attempt_count + 1,
                last_attempt_at = ?,
                next_retry_at = NULL
            WHERE rowid IN (
                SELECT rowid
                FROM job_ad_enrichment
                WHERE
                    enrich_status IN ('pending','retry')
                    AND attempt_count < ?
                    AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY
                    -- prefer oldest never-attempted
                    CASE WHEN last_attempt_at IS NULL THEN 0 ELSE 1 END ASC,
                    CASE WHEN last_attempt_at IS NULL THEN job_id END ASC,
                    -- then oldest next_retry_at/last_attempt_at first
                    CASE WHEN last_attempt_at IS NOT NULL THEN next_retry_at END ASC,
                    job_id ASC,
                    enrich_type ASC
                LIMIT ?
            )
            """,
            (now, max_attempts, now, limit),
        )

        rows = conn.execute(
            """
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
            WHERE
                e.enrich_status = 'in_progress'
                AND e.last_attempt_at = ?
            ORDER BY e.job_id ASC, e.enrich_type ASC
            """,
            (now,),
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
