import logging
import sqlite3

from hiring_compass_au.infra.storage.hit_store import (
    get_promote_pending_job_hits,
    update_promoted_job_hits,
)
from hiring_compass_au.infra.storage.job_store import update_job_ad_enrichment, update_job_ads

logger = logging.getLogger(__name__)


def run_promote_job_ad_batch(
    conn: sqlite3.Connection,
    limit: int = 200,
    attempted_keys: set[tuple[str, str]] | None = None,
) -> tuple[int, int, set]:
    hits = list(get_promote_pending_job_hits(conn, limit=limit))
    n = len(hits)

    if n == 0:
        return 0, 0, attempted_keys

    try:
        conn.execute("BEGIN;")
        promoted_jobs, hits_upserted, hits_failed, attempted_keys = update_job_ads(
            conn, hits, attempted_keys
        )

        expected_updates = len(hits_upserted) + len(hits_failed)
        updated_rows = update_promoted_job_hits(conn, hits_upserted, hits_failed)
        if updated_rows != expected_updates:
            logger.warning(
                "Promotion persisted less than expected: expected=%d updated=%d",
                expected_updates,
                updated_rows,
            )

        update_job_ad_enrichment(conn, promoted_jobs)
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise

    return len(hits_failed), n, attempted_keys


def run_promote_job_ad(conn: sqlite3.Connection, limit: int = 200) -> tuple[int, int, int]:
    existing_keys = {
        (r[0], r[1]) for r in conn.execute("SELECT source, canonical_url FROM job_ads").fetchall()
    }
    attempted_keys = set()
    failed_total = 0

    while True:
        n_failed_hit_b, n, attempted_keys = run_promote_job_ad_batch(conn, limit, attempted_keys)
        failed_total += n_failed_hit_b

        if n == 0:
            break

    new_job_ad_total = len(attempted_keys - existing_keys)
    updated_total = len(attempted_keys & existing_keys)

    logger.info(
        "job_hit promotion finished: new=%d updated=%d failed=%d",
        new_job_ad_total,
        updated_total,
        failed_total,
    )

    return new_job_ad_total, updated_total, failed_total
