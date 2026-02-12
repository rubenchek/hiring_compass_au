import logging
import sqlite3

from hiring_compass_au.workspace import get_workspace

from hiring_compass_au.data.ingestion.gmail.auth_and_build import authenticate_and_build_service
from hiring_compass_au.data.ingestion.gmail.mail_index import run_mail_index
from hiring_compass_au.data.ingestion.gmail.mail_fetch import run_mail_fetch

from hiring_compass_au.data.storage.mail_store import get_connection

from hiring_compass_au.data.pipelines.job_alerts.mail_parse_runner import run_mail_parse
from hiring_compass_au.data.pipelines.job_alerts.url_enrichment_runner import run_url_enrichment_all

logger = logging.getLogger(__name__)

SENDERS = ["jobmail@s.seek.com.au"]

def run_job_alert_pipeline(index=True, fetch=True, parse=True, enrich=True, promote=False):
    logger.info("Pipeline flags: index=%s fetch=%s parse=%s enrich=%s promote=%s",
    index, fetch, parse, enrich, promote)
    
    ws = get_workspace()
    db_path = ws.db_path
    
    with get_connection(db_path, sqlite3.Row) as conn:
        if index or fetch:
            client_secret_path = ws.root / "secrets/google_client_secret.json"
            token_path = ws.root / "secrets/gmail_token.json"
            service = authenticate_and_build_service(client_secret_path, token_path)
            
            if index:
                logger.info("Start indexing emails (%d sender(s))", len(SENDERS))
                for sender in SENDERS:
                    run_mail_index(from_email=sender, service=service, conn=conn)
            if fetch:
                logger.info("Start fetching emails")
                run_mail_fetch(service=service, conn=conn)
        
        if parse:
            logger.info("Start parsing emails")
            run_mail_parse(conn=conn)

        if enrich:
            logger.info("Start canonicalize url")
            run_url_enrichment_all(conn=conn)
        # TODO
        # if promote:
        # run_promote_job_ad()