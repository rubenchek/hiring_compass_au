from __future__ import annotations

import sqlite3


def init_email_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS emails (
            -- From index
            message_id        TEXT PRIMARY KEY,
            thread_id         TEXT,
            
            -- From fetch
            from_email        TEXT,                
            subject           TEXT,
            internal_date_ms  INTEGER,
            received_at       TEXT,
            html_raw          TEXT,    
            
            -- Informations       
            template          TEXT,
            parser_name       TEXT,
            parser_version    TEXT,
            status            TEXT NOT NULL,
            indexed_at        TEXT NOT NULL,
            fetched_at        TEXT,
            parsed_at         TEXT,
            intended_use      TEXT NOT NULL DEFAULT 'prod',
            error             TEXT,
            
            -- Metrics
            parsed_confidence INTEGER,
            hit_extract_count INTEGER NOT NULL DEFAULT 0,
            persist_count     INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    conn.commit()


def init_email_job_hits_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS email_job_hits (
            -- Primary and Foreign key
            hit_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id        TEXT NOT NULL,
            fingerprint       TEXT,
            template          TEXT,
            
            -- From parsed
            title             TEXT,
            company           TEXT,
            suburb            TEXT,
            city              TEXT,
            state             TEXT,
            location_raw      TEXT,
            salary_min        INTEGER,
            salary_max        INTEGER,
            salary_period     TEXT,
            salary_raw        TEXT,
            debug_lines       TEXT,
            out_url           TEXT NOT NULL,
            out_url_norm      TEXT,
            hit_context       TEXT,
            source            TEXT NOT NULL,
            
            --Metrics
            hit_confidence    INTEGER,
            parser_name       TEXT,
            parser_version    TEXT,
            promote_status    TEXT NOT NULL DEFAULT 'new'
                CHECK (promote_status IN ('new','pending','promoted','skipped','rejected')),
            promote_reason    TEXT,
            
            -- Canonicalisation
            external_job_id   TEXT,
            canonical_url     TEXT,
            canonical_status  TEXT NOT NULL DEFAULT 'pending'
                CHECK (canonical_status IN ('pending','ok','retry','error')),
            http_status       INTEGER,
            attempt_count     INTEGER NOT NULL DEFAULT 0,
            next_retry_at     TEXT,
            last_attempt_at   TEXT,
            canon_error       TEXT,

            FOREIGN KEY (message_id) REFERENCES emails(message_id) ON DELETE CASCADE,
            UNIQUE(message_id, out_url)
        );
        """
    )
    conn.commit()


def init_job_ads_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS job_ads (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source          TEXT NOT NULL,
            external_job_id TEXT,
            fingerprint     TEXT,
            title           TEXT,
            company         TEXT,
            company_id      INTEGER,
            suburb          TEXT,
            city            TEXT,
            state           TEXT,
            location_raw    TEXT,
            listing_date_utc TEXT,
            salary_min      INTEGER,
            salary_max      INTEGER,
            salary_period   TEXT,
            salary_raw      TEXT,
            description     TEXT,
            job_status      TEXT DEFAULT 'new',
            canonical_url   TEXT NOT NULL,
            
            first_seen_at   TEXT,
            last_seen_at    TEXT,
            
            UNIQUE(source, canonical_url),
            UNIQUE(source, external_job_id),
            FOREIGN KEY (company_id) REFERENCES company(id)
        );
        """
    )
    conn.commit()


def init_company_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            name                        TEXT,
            industry                    TEXT,
            description                 TEXT,
            size                        TEXT,
            website_url                 TEXT,
            seek_company_id             INTEGER,
            seek_rating_value           REAL,
            seek_review_count           INTEGER,
            seek_company_url            TEXT,
            
            
            UNIQUE(seek_company_id)
        );
        """
    )
    conn.commit()


def init_job_ad_enrichment(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS job_ad_enrichment(
            job_id          INTEGER,
            enrich_type     TEXT,
            enrich_status   TEXT NOT NULL DEFAULT 'pending'
                CHECK (enrich_status IN ('pending','in_progress','ok','retry','error')),
            http_status     INTEGER,
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            next_retry_at   TEXT,
            last_attempt_at TEXT,
            error           TEXT,
            fetched_at      TEXT,
            
            PRIMARY KEY (job_id, enrich_type),
            FOREIGN KEY (job_id) REFERENCES job_ads(id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()


def init_seek_enrichment_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS seek_enrichment (
            job_id                      INTEGER PRIMARY KEY,
            advertiser_id               INTEGER,
            role_id                     TEXT,
            classification_ids          TEXT,
            classification_labels       TEXT,
            subclassification_ids       TEXT,
            subclassification_labels    TEXT,
            seo_normalised_role_title   TEXT,
            work_types                  TEXT,
            work_arrangement_types      TEXT,
            badges                      TEXT,
            description_raw             TEXT,
            teaser                      TEXT,
            bullet_points               TEXT,
            questionnaire_questions     TEXT,
            skills                      TEXT,
            expires_at_utc              TEXT,
            insights_volume_label       TEXT,
            insights_count              INTEGER,
            status                      TEXT,
            FOREIGN KEY (job_id) REFERENCES job_ads(id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()


def init_email_job_ads_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS email_job_ads(
            message_id    TEXT NOT NULL,
            job_ad_id     INTEGER NOT NULL,
            out_url       TEXT,
            
            PRIMARY KEY (message_id, job_ad_id),
            
            FOREIGN KEY (message_id) REFERENCES emails(message_id) ON DELETE CASCADE,
            FOREIGN KEY (job_ad_id) REFERENCES job_ads(id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()


def init_all_tables(conn):
    init_email_table(conn)
    init_email_job_hits_table(conn)
    init_company_table(conn)
    init_job_ads_table(conn)
    init_job_ad_enrichment(conn)
    init_seek_enrichment_table(conn)
    init_email_job_ads_table(conn)
