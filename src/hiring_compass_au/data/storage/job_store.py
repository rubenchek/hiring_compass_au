from __future__ import annotations

from hiring_compass_au.data.storage.db import utc_now_iso

import sqlite3


# ----------------------------
# Fill database
# ----------------------------

def update_job_ads(
    conn: sqlite3.Connection,
    hits: list,
) -> int:    
    now = utc_now_iso()
    
    sql = """
    INSERT INTO job_ads (
        source,
        external_job_id,
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
        salary_raw,
        canonical_url,
        first_seen_at,
        last_seen_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(source, canonical_url) DO UPDATE SET
        -- don't overwrite good data with NULLs
        external_job_id = COALESCE(excluded.external_job_id, job_ads.external_job_id),
        fingerprint     = COALESCE(excluded.fingerprint,     job_ads.fingerprint),
        title           = COALESCE(excluded.title,           job_ads.title),
        company         = COALESCE(excluded.company,         job_ads.company),
        suburb          = COALESCE(excluded.suburb,          job_ads.suburb),
        city            = COALESCE(excluded.city,            job_ads.city),
        state           = COALESCE(excluded.state,           job_ads.state),
        location_raw    = COALESCE(excluded.location_raw,    job_ads.location_raw),
        salary_min      = COALESCE(excluded.salary_min,      job_ads.salary_min),
        salary_max      = COALESCE(excluded.salary_max,      job_ads.salary_max),
        salary_period   = COALESCE(excluded.salary_period,   job_ads.salary_period),
        salary_raw      = COALESCE(excluded.salary_raw,      job_ads.salary_raw),
        
        -- keep first_seen_at as-is, refresh last_seen_at
        last_seen_at = excluded.last_seen_at
    RETURNING id, source
    ;
    """
    hits_upserted = []
    hits_failed = []
    promoted_jobs =[]
    keys = set()
    
    
    for hit in hits:
        hit_id = hit["hit_id"]
        source = hit["source"]
        canonical_url = hit["canonical_url"]
        row = []
        
        if not source or not canonical_url:
            hits_failed.append(hit_id)
            continue
        
        keys.add((source, canonical_url))
          
        row = (
            source,
            hit["external_job_id"],
            hit["fingerprint"],
            hit["title"],
            hit["company"],
            hit["suburb"],
            hit["city"],
            hit["state"],
            hit["location_raw"],
            hit["salary_min"],
            hit["salary_max"],
            hit["salary_period"],
            hit["salary_raw"],
            canonical_url,
            now,
            now,
        )   
        hits_upserted.append(hit_id)
        promoted_jobs.append(conn.execute(sql, row).fetchone())

    return promoted_jobs, hits_upserted, hits_failed


def update_job_ad_enrichment(conn: sqlite3.Connection, promoted_jobs):
    for row in promoted_jobs:
        if row["source"]=="seek":
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
                (row["id"], enrichment, "pending")
                )