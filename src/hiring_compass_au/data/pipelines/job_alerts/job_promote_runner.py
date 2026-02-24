import sqlite3
import logging

from hiring_compass_au.data.storage.hit_store import get_promote_pending_job_hits, update_promoted_job_hits
from hiring_compass_au.data.storage.job_store import update_job_ads, update_job_ad_enrichment

logger = logging.getLogger(__name__)


def run_promote_job_ad_batch(conn: sqlite3.Connection, limit: int = 200) -> tuple[int,int]:
    hits = list(get_promote_pending_job_hits(conn, limit=limit))
    n = len(hits)
    
    if n == 0:
        logger.info("No new job_hit to promote")
        return 0, 0
    
    try:
        conn.execute("BEGIN;")
        promoted_jobs, hits_upserted, hits_failed = update_job_ads(conn, hits)
        update_promoted_job_hits(conn, hits_upserted, hits_failed)
        update_job_ad_enrichment(conn, promoted_jobs)
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise  
    
    return len(hits_failed), n
    
    
    
def run_promote_job_ad(conn: sqlite3.Connection, limit: int = 200):
    existing_keys = {
        (r[0], r[1]) for r in conn.execute("SELECT source, canonical_url FROM job_ads").fetchall()
    }
    
    failed_total = 0
    
    while True:
        n_failed_hit_b, n = run_promote_job_ad_batch(conn, limit)
        failed_total += n_failed_hit_b
        
        if n == 0:
                break
            
    final_keys = {
        (r[0], r[1]) for r in conn.execute("SELECT source, canonical_url FROM job_ads").fetchall()
    }
    new_job_ad_total = len(final_keys) - len(existing_keys)
    updated_total = len(final_keys & existing_keys)
            
    logger.info(
        "job_hit promotion finished: new=%d updated=%d failed=%d",    # updated count est faux
        new_job_ad_total, updated_total, failed_total
    )

    return new_job_ad_total, updated_total, failed_total
        