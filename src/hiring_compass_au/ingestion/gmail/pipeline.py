import logging

from hiring_compass_au.workspace import get_workspace

from hiring_compass_au.ingestion.gmail.auth import authenticate_and_build_service
from hiring_compass_au.ingestion.gmail.mail_index import run_mail_index
from hiring_compass_au.ingestion.gmail.mail_fetch import run_mail_fetch
from hiring_compass_au.ingestion.gmail.mail_parse_runner import run_mail_parse

logger = logging.getLogger(__name__)

SENDERS = ["jobmail@s.seek.com.au"]

def run_mail_ingestion_pipeline():
    ws = get_workspace()
    
    client_secret_path = ws.root / "secrets/google_client_secret.json"
    token_path = ws.root / "secrets/gmail_token.json"
    db_path = ws.db_path
    
    service = authenticate_and_build_service(client_secret_path, token_path)
    
    logger.info("Start indexing emails (%d sender(s))", len(SENDERS))
    for sender in SENDERS:
        run_mail_index(from_email=sender, service=service, db_path=db_path)
    
    logger.info("Start fetching emails")
    run_mail_fetch(service=service, db_path=db_path)
    
    logger.info("Start parsing emails")
    run_mail_parse(db_path=db_path)
