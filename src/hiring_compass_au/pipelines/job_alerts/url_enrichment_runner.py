from __future__ import annotations

import logging
import requests
import sqlite3
import time, random
from tqdm import tqdm
 
from hiring_compass_au.storage.mail_store import get_connection, get_batch_url_to_canonicalize, update_job_hit_canonicalization, count_urls_to_canonicalize
from hiring_compass_au.enrichment.url_canonicalizer import resolve_to_canonical, CanonicalizeError

logger = logging.getLogger(__name__)


def run_url_enrichment(db_path, session, limit: int = 200, timeout: float = 15.0,
                       *, batch_index=0, global_bar=None) -> tuple[int,int,int,int]:
    """
    Fetch a batch of pending/retry hits, resolve -> canonicalize, update DB status fields.
    Assumes:
      - get_url_to_canonicalize(conn, limit) returns rows with at least: id, out_url
      - update_job_hit_canonicalization() applies attempt_count/next_retry_at/last_attempt_at
    """
    
    with get_connection(db_path, sqlite3.Row) as conn:
        try:
            hits = get_batch_url_to_canonicalize(conn, limit=limit)
            
            if not hits:
                logger.info("URL enrichment: no hits to process.")
                return 0, 0, 0, 0
            
            ok = retry = err = 0

            
            for hit in hits:
                hit_id = hit["hit_id"]
                out_url = hit["out_url"]
                sleep_s = 0.2 + random.uniform(0, 0.2)

                # optionnel mais utile si "" possible
                if not out_url.strip():
                    update_job_hit_canonicalization(
                        conn,
                        hit_id,
                        outcome="error",
                        http_status=None,
                        canonical_url=None,
                        job_id=None,
                        canon_error="Empty out_url",
                    )
                    err += 1
                    continue
                
                try:
                    job_id, canonical_url, http_status  = resolve_to_canonical(session, out_url, timeout=timeout)
                    update_job_hit_canonicalization(
                        conn, hit_id, job_id=job_id, canonical_url=canonical_url, http_status=http_status, canon_error=None, outcome="ok",
                        )
                    ok += 1

                
                except (requests.Timeout, requests.ConnectionError) as e:
                    logger.warning("Retryable network error for out_url=%s: %s", out_url, e)
                    sleep_s = max(sleep_s, 2 + random.uniform(0, 2))
                    update_job_hit_canonicalization(
                        conn, hit_id, job_id=None, canonical_url=None, http_status=None, 
                        canon_error=f"{type(e).__name__}: {e}", outcome="retry",
                        )
                    retry += 1
                    
                except CanonicalizeError as e:
                    http_status = getattr(e, "http_status", None)
                    if http_status in (429, 500, 502, 503, 504):
                        sleep_s = max(sleep_s, 2 + random.uniform(0, 2))
                        outcome = "retry"
                        logger.info("Canonicalizable error due to http_status=%s", http_status)
                        retry += 1
                    else:
                        outcome = "error"
                        logger.info("Non-canonicalizable out_url=%s: %s", out_url, e)
                        err += 1
                    update_job_hit_canonicalization(
                        conn, hit_id, http_status=http_status, canonical_url=None, job_id=None, canon_error=str(e), outcome=outcome,
                        )
                    
                
                except Exception as e:
                    logger.exception("Unexpected error for out_url=%s", out_url)
                    update_job_hit_canonicalization(
                        conn, hit_id, http_status=None, canonical_url=None, job_id=None, 
                        canon_error=f"Unexpected: {type(e).__name__}: {e}", outcome="retry",
                        )
                    retry += 1
                    sleep_s = max(sleep_s, 2 + random.uniform(0, 2))
                    
                finally:
                    time.sleep(sleep_s)
                    if global_bar is not None:
                        global_bar.update(1)
                    
            conn.commit()
            return ok, retry, err, len(hits)
    
        except Exception:
            conn.rollback()
            raise

    
    
def run_url_enrichment_all(db_path, *, batch_size: int = 200, timeout: float = 15.0, max_batches: int | None = None) -> tuple[int,int,int]:
    with get_connection(db_path, sqlite3.Row) as conn:
        total = count_urls_to_canonicalize(conn)
    if total == 0:
        logger.info("URL enrichment: up-to-date")
        return 0, 0, 0
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; HiringCompassAU/0.1; +https://example.invalid)"
    })
    
    batches = 0
    ok = retry = err = 0
    
    global_bar = tqdm(total=total, desc="URL enrichment (global)", unit="hit", position=0, leave=True)
    
    try:
        while True:
            if max_batches is not None and batches >= max_batches:
                break
            
            ok_b, retry_b, err_b, treated_b = run_url_enrichment(
                db_path, session, limit=batch_size, timeout=timeout, batch_index=batches,
                global_bar=global_bar)
            
            ok += ok_b; retry += retry_b; err += err_b
            batches += 1
            
            tqdm.write(f"URL enrichment batch {batches-1} done: ok={ok_b} retry={retry_b} error={err_b} (limit={batch_size})")

            if treated_b == 0:
                break
    finally:
        global_bar.close()
        
    logger.info("URL enrichment finished: ok=%d retry=%d error=%d (limit=%d)", ok, retry, err, batch_size) 
    return ok, retry, err
