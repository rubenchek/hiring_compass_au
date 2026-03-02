import logging
import sqlite3
from pathlib import Path

from hiring_compass_au.data.ingestion.gmail.auth_and_build import authenticate_and_build_service
from hiring_compass_au.data.ingestion.gmail.mail_fetch import run_mail_fetch
from hiring_compass_au.data.ingestion.gmail.mail_index import run_mail_index
from hiring_compass_au.data.pipelines.job_alerts.job_promote_runner import run_promote_job_ad
from hiring_compass_au.data.pipelines.job_alerts.mail_parse_runner import run_mail_parse
from hiring_compass_au.data.pipelines.job_alerts.url_canonicalize_runner import run_url_enrichment

logger = logging.getLogger(__name__)


def run_job_alert_pipeline(
    conn: sqlite3.Connection,
    client_secret_path: Path, token_path: Path, *,
    index: bool = True, fetch: bool = True, 
    parse: bool = True, canonicalize: bool = True, promote: bool = True,
    senders: list[str] | None = None):
    
    senders = senders or ["jobmail@s.seek.com.au"]
    
    if index or fetch:
        service = authenticate_and_build_service(client_secret_path, token_path)
        
        if index:
            logger.info("Start indexing emails (%d sender(s))", len(senders))
            for sender in senders:
                run_mail_index(from_email=sender, service=service, conn=conn)
        if fetch:
            logger.info("Start fetching emails")
            run_mail_fetch(service=service, conn=conn)
    
    if parse:
        logger.info("Start parsing emails")
        run_mail_parse(conn=conn)

    if canonicalize:
        logger.info("Start canonicalize url")
        run_url_enrichment(conn=conn)

    if promote:
        logger.info("Start promoting job ads")
        run_promote_job_ad(conn=conn)