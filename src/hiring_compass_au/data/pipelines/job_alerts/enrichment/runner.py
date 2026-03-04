from __future__ import annotations

import logging
import random
import sqlite3
import time

import requests
from tqdm import tqdm

from hiring_compass_au.data.pipelines.job_alerts.enrichment.url_canonicalizer import (
    CanonicalizeError,
    resolve_to_canonical,
)
from hiring_compass_au.data.storage.hit_store import (
    count_urls_to_canonicalize,
    get_batch_url_to_canonicalize,
    update_job_hit_canonicalization,
)

logger = logging.getLogger(__name__)

RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}


def run_url_canonicalization_batch(
    conn: sqlite3,
    session,
    limit: int = 200,
    timeout: float = 15.0,
    *,
    global_bar=None,
) -> tuple[int, int, int, int]:
    """
    Fetch a batch of pending/retry hits, resolve -> canonicalize, update DB status fields.
    Assumes:
      - get_url_to_canonicalize(conn, limit) returns rows with at least: id, out_url
      - update_job_hit_canonicalization() applies attempt_count/next_retry_at/last_attempt_at
    """
    hits = get_batch_url_to_canonicalize(conn, limit=limit)

    if not hits:
        return 0, 0, 0, 0

    ok = retry = err = 0

    try:
        for hit in hits:
            hit_id = hit["hit_id"]
            out_url = hit["out_url"]
            sleep_s = 0.2 + random.uniform(0, 0.2)

            if not out_url.strip():
                update_job_hit_canonicalization(
                    conn,
                    hit_id,
                    outcome="error",
                    http_status=None,
                    canonical_url=None,
                    external_job_id=None,
                    canon_error="Empty out_url",
                )
                err += 1
                continue

            try:
                external_job_id, canonical_url, http_status = resolve_to_canonical(
                    session, out_url, timeout=timeout
                )
                update_job_hit_canonicalization(
                    conn,
                    hit_id,
                    external_job_id=external_job_id,
                    canonical_url=canonical_url,
                    http_status=http_status,
                    canon_error=None,
                    outcome="ok",
                )
                ok += 1

            except (requests.Timeout, requests.ConnectionError) as e:
                logger.warning("Retryable network error for out_url=%s: %s", out_url, e)
                sleep_s = max(sleep_s, 2 + random.uniform(0, 2))
                update_job_hit_canonicalization(
                    conn,
                    hit_id,
                    external_job_id=None,
                    canonical_url=None,
                    http_status=None,
                    canon_error=f"{type(e).__name__}: {e}",
                    outcome="retry",
                )
                retry += 1

            except CanonicalizeError as e:
                http_status = getattr(e, "http_status", None)
                if http_status in RETRYABLE_HTTP_STATUSES:
                    sleep_s = max(sleep_s, 2 + random.uniform(0, 2))
                    outcome = "retry"
                    logger.info("Canonicalizable error due to http_status=%s", http_status)
                    retry += 1
                else:
                    outcome = "error"
                    logger.warning("Non-canonicalizable out_url=%s: %s", out_url, e)
                    err += 1

                update_job_hit_canonicalization(
                    conn,
                    hit_id,
                    http_status=http_status,
                    canonical_url=None,
                    external_job_id=None,
                    canon_error=str(e),
                    outcome=outcome,
                )

            except Exception as e:
                logger.exception("Unexpected error for out_url=%s", out_url)
                sleep_s = max(sleep_s, 2 + random.uniform(0, 2))
                update_job_hit_canonicalization(
                    conn,
                    hit_id,
                    http_status=None,
                    canonical_url=None,
                    external_job_id=None,
                    canon_error=f"Unexpected: {type(e).__name__}: {e}",
                    outcome="retry",
                )
                retry += 1

            finally:
                time.sleep(sleep_s)
                if global_bar is not None:
                    global_bar.update(1)

        conn.commit()
        return ok, retry, err, len(hits)

    except Exception:
        conn.rollback()
        raise


def run_url_canonicalization(
    conn: sqlite3.Connection,
    *,
    batch_size: int = 200,
    timeout: float = 15.0,
    max_batches: int | None = None,
    progress: bool = False,
) -> tuple[int, int, int, int]:
    total_start = count_urls_to_canonicalize(conn)

    if total_start == 0:
        logger.info("URL canonicalization: up-to-date")
        return 0, 0, 0, 0

    session = requests.Session()
    session.headers.update(
        {"User-Agent": "Mozilla/5.0 (compatible; HiringCompassAU/0.1; +https://example.invalid)"}
    )

    batches = 0
    ok = retry = err = 0
    treated = 0

    global_bar = None
    if progress:
        global_bar = tqdm(
            total=total_start, desc="URL enrichment", unit="hit", position=0, leave=True
        )

    try:
        while True:
            if max_batches is not None and batches >= max_batches:
                break

            ok_b, retry_b, err_b, treated_b = run_url_canonicalization_batch(
                conn,
                session,
                limit=batch_size,
                timeout=timeout,
                global_bar=global_bar,
            )

            ok += ok_b
            retry += retry_b
            err += err_b
            treated += treated_b

            if treated_b == 0:
                break

            if progress:
                tqdm.write(
                    f"URL enrichment batch {batches} done: ok={ok_b} retry={retry_b} error={err_b} "
                    f"(batch_size={batch_size})"
                )
            else:
                pct_success = 100 * ok_b / treated_b
                pct_treated = 100 * treated / total_start
                logger.info(
                    "URL canonicalization batch %d done : %.1f%% success (total_progress=%.1f%%)",
                    batches,
                    pct_success,
                    pct_treated,
                )
            batches += 1

    finally:
        if global_bar is not None:
            global_bar.close()

    logger.info(
        "URL canonicalization finished: ok=%d retry=%d error=%d (total_start=%d)",
        ok,
        retry,
        err,
        total_start,
    )
    return total_start, ok, retry, err
