from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

# ----------------------------
# Connection
# ----------------------------

def get_connection(db_path: Path, row_factory=None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    if row_factory is not None:
        conn.row_factory = row_factory
    return conn

# ----------------------------
# Utils
# ----------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _compute_backoff_minutes(attempt_count_after_increment: int) -> int:
    """
    Exponential backoff in minutes, capped.
    attempt_count_after_increment: 1,2,3,...
    """
    # 1->2min, 2->4min, 3->8min, ... cap at 1440min (24h)
    minutes = 2 ** attempt_count_after_increment
    return min(minutes, 24 * 60)

# ----------------------------
# Schema init
# ----------------------------

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
            out_url_norm TEXT,
            hit_context       TEXT,
            source            TEXT NOT NULL,
            
            --Metrics
            hit_confidence    INTEGER,
            parser_name       TEXT,
            parser_version    TEXT,
            promote_status    TEXT NOT NULL DEFAULT 'pending'
                CHECK (promote_status IN ('pending','promoted','skipped','rejected')),
            promote_reason    TEXT,
            
            -- Canonicalisation
            job_id            TEXT,
            canonical_url     TEXT,
            canonical_status  TEXT NOT NULL DEFAULT 'pending',
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
            job_id          TEXT PRIMARY KEY,
            source          TEXT NOT NULL,
            canonical_url   TEXT NOT NULL,
            figerprint      TEXT,
            title           TEXT,
            company         TEXT,
            suburb          TEXT,
            city            TEXT,
            state           TEXT,
            location_raw    TEXT,
            salary_min      INTEGER,
            salary_max      INTEGER,
            salary_period   TEXT,
            salary_raw     TEXT,
            description     TEXT,
            job_status      TEXT DEFAULT 'new',
            
            first_seen_at   TEXT,
            last_seen_at    TEXT,
            debug_lines     TEXT,
            
            UNIQUE(source, canonical_url)
        );
        """
    )
    conn.commit()
    

def init_job_ad_enrichment(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS job_ad_enrichment(
            job_id          TEXT PRIMARY KEY,
            enrich_status   TEXT NOT NULL DEFAULT 'pending',
            http_status     INTEGER,
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            next_retry_at   TEXT,
            last_attempt_at TEXT,
            error           TEXT,
            fetched_at      TEXT,
            
            FOREIGN KEY (job_id) REFERENCES job_ads(job_id) ON DELETE CASCADE
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
            job_id        TEXT NOT NULL,
            out_url       TEXT,
            
            PRIMARY KEY (message_id, job_id),
            
            FOREIGN KEY (message_id) REFERENCES emails(message_id) ON DELETE CASCADE,
            FOREIGN KEY (job_id)     REFERENCES job_ads(job_id)   ON DELETE CASCADE
        );
        """
    )
    conn.commit()

    
def init_all_tables(conn):
    init_email_table(conn)
    init_email_job_hits_table(conn)
    init_job_ads_table(conn)
    init_job_ad_enrichment(conn)
    init_email_job_ads_table(conn)
    

# ----------------------------
# Update / fill database
# ----------------------------

def upsert_indexed_emails(conn: sqlite3.Connection, messages: list[dict]) -> int:
    """
    Insert new message_ids into emails with status='indexed' and indexed_at=now.
    Do not overwrite existing rows.
    Return number of newly inserted rows.
    """
    if not messages:
        return 0
    
    now = _utc_now_iso()
    
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


def update_fetched_email_metadata(conn: sqlite3.Connection, fetched_mail_batch: list[dict]) -> int:
    # TODO ajouter template
    
    if not fetched_mail_batch:
        return 0
    
    now = _utc_now_iso()
    
    rows = []
    for m in fetched_mail_batch:
        message_id = m.get("message_id")
        if not message_id:
            continue
        
        from_email = m.get("from_email")
        subject = m.get("subject")
        internal_date_ms = m.get("internal_date_ms")
        received_at = datetime.fromtimestamp(internal_date_ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")
        html_raw = m.get("html_raw")
        error = m.get("error")
        
        status = "fetch_error" if error else "fetched"

            
        rows.append((from_email, subject, internal_date_ms, received_at, html_raw, error, status, now, message_id))
    
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
    parsed_confidence : int = 0,
    supported: bool = True,
    error: str = None
) -> int:
    """
    Update email row after parsing.

    - supported=False  => status='parsed_unsupported' (no parser registered)
    - supported=True and parsed_count==0 => status='parsed_empty'
    - supported=True and parsed_count>0  => status='parsed'
    """
    now = _utc_now_iso()
    if error:
        status = "parsed_error"
    elif not supported:
        status = "parsed_unsupported"
    elif hit_parsed_count == 0:
        status = "parsed_empty"
    else:
        status = "parsed"

    conn.execute(
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
        (status, now, parser_cfg["parser_name"], parser_cfg["parser_version"], int(hit_parsed_count), parsed_confidence, error, message_id),
    )


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


def update_job_hit_canonicalization(
    conn,
    hit_id: int,                      # ou (message_id, hit_rank) etc selon ta PK
    outcome: str,                     # "ok" | "retry" | "error"
    http_status: int | None = None,
    canonical_url: str | None = None,
    job_id: str | None = None,
    canon_error: str | None = None,
) -> None:
    if outcome not in {"ok", "retry", "error"}:
        raise ValueError(f"Invalid outcome: {outcome}")
    
    now = _utc_now_iso()
    
    # Fetch current attempt_count so backoff uses the incremented value
    row = conn.execute(
        "SELECT attempt_count FROM email_job_hits WHERE hit_id = ?",
        (hit_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"email_job_hits hit_id not found: {hit_id}")
    
    attempt_count_before = int(row["attempt_count"]) 
    attempt_count_after = attempt_count_before + 1
    
    if outcome == "ok":
        canon_error = None
        next_retry_at = None

    if outcome == "retry":
        delay_min = _compute_backoff_minutes(attempt_count_after)
        dt = datetime.now(timezone.utc) + timedelta(minutes=delay_min)
        next_retry_at = dt.replace(microsecond=0).isoformat()
    
    if outcome == "error":
        next_retry_at = None

    conn.execute(
        """
        UPDATE email_job_hits
        SET
            job_id = ?,
            canonical_url = ?,
            canonical_status = ?,
            http_status = ?,
            attempt_count = ?,
            next_retry_at = ?,
            last_attempt_at = ?,
            canon_error = ?
        WHERE hit_id = ?
        """,
        (
            job_id,
            canonical_url,
            outcome,
            http_status,
            attempt_count_after,
            next_retry_at,
            now,
            canon_error,
            hit_id,
        ),
    )

# ----------------------------
# Request database
# ----------------------------

def get_last_internal_date_ms(conn: sqlite3.Connection, from_email: str) -> int | None:
    cur= conn.cursor()
    
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
    for row in cur:
        yield row


def count_urls_to_canonicalize(conn: sqlite3.Connection, max_attempts: int = 10) -> int:
    now = _utc_now_iso()
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
    now = _utc_now_iso()
    
    return conn.execute(
        """
        SELECT hit_id, out_url
        FROM email_job_hits
        WHERE
            canonical_status IN ('pending','retry')
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