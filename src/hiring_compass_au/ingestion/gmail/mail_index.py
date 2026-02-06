import logging
from pathlib import Path

from hiring_compass_au.ingestion.gmail.auth import authenticate_and_build_service
from hiring_compass_au.storage.mail_store import get_connection, init_all_tables, upsert_indexed_emails, get_last_internal_date_ms
from hiring_compass_au.workspace import get_workspace

logger = logging.getLogger(__name__)
    

def list_message_refs(service, query):
    all = []
    token = None
    i=0
    logger.info("Indexing emails with query: %s", query)
    
    while True:
        resp = service.users().messages().list(
            userId="me",
            q=query,
            pageToken=token,
            maxResults=500,
            ).execute()
        all.extend(resp.get("messages", []))
        token = resp.get("nextPageToken", None)
        i+=1
        if token is None: break
    logger.info("%s page(s) fetched", i)
    return all
    
def run_mail_index(from_email):
    ws = get_workspace()
    
    client_secret_path = ws.root / "secrets/google_client_secret.json"
    token_path = ws.root / "secrets/gmail_token.json"
    db_path = ws.db_path
    
    service = authenticate_and_build_service(client_secret_path, token_path)
    
    with get_connection(db_path) as conn:
        init_all_tables(conn)
        last_internal_date_ms = get_last_internal_date_ms(conn, from_email)
        
        if last_internal_date_ms is not None:
            after_seconds = last_internal_date_ms // 1000 - 3600
            query = f"from:({from_email}) after:{after_seconds}"
        else:
            query = f"from:({from_email})"
        messages = list_message_refs(service, query)
        inserted = upsert_indexed_emails(conn, messages)
        
    logger.info(
        "Mail index: found %d messages, inserted %d new",
        len(messages),
        inserted,
    )
    return inserted