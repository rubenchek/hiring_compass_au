import logging
import sqlite3

from hiring_compass_au.data.storage.mail_store import upsert_indexed_emails, get_last_internal_date_ms

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
    
def run_mail_index(from_email, service, conn: sqlite3.Connection):   
    last_internal_date_ms = get_last_internal_date_ms(conn, from_email)
    
    if last_internal_date_ms is not None:
        after_seconds = last_internal_date_ms // 1000 - 3600
        query = f"from:({from_email}) after:{after_seconds}"
    else:
        query = f"from:({from_email})"
    messages = list_message_refs(service, query)
    inserted = upsert_indexed_emails(conn, messages)
        
    logger.info(
        "Mail index for %s: found %d messages, inserted %d new",
        from_email,
        len(messages),
        inserted,
    )
    return inserted