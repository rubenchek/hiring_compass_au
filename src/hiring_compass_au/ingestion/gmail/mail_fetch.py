import logging
import base64
from email.utils import parseaddr

from hiring_compass_au.ingestion.gmail.auth import authenticate_and_build_service
from hiring_compass_au.storage.mail_store import get_connection, get_non_fetched_email_list, update_fetched_email_metadata
from hiring_compass_au.workspace import get_workspace

logger = logging.getLogger(__name__)


def _decode_base64url(data: str) -> str:
    # Gmail envoie du base64url parfois sans padding (=)
    data = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8", errors="replace")


def _walk_parts(payload: dict):
    # DFS sur la structure MIME
    stack = [payload]
    while stack:
        part = stack.pop()
        yield part
        for p in part.get("parts", []) or []:
            stack.append(p)
    

def load_message(service, message_id) -> dict:
    message = service.users().messages().get(
        userId="me",
        id=message_id,
        format="full",
    ).execute()
    return message


def extract_message_fields(message : dict):
    internal_date_ms = int(message.get("internalDate")) if message.get("internalDate") else None
    
    from_email = None
    subject = None
    html_parts = []

    payload = message.get("payload") or {}
    headers = payload.get("headers") or []
    
    # collect metadata
    for h in headers:
        name = h.get("name")
        value = h.get("value") or ""
        
        if name == "From":
            _, from_email = parseaddr(value)
        elif name == "Subject":
            subject = value
            
        if from_email is not None and subject is not None:
            break
    
    # collect html
    for part in _walk_parts(payload):
        if (part.get("mimeType") or "").lower() != "text/html":
            continue
        
        body = part.get("body") or {}
        if part.get("filename") or body.get("attachmentId"):
            continue
        data = body.get("data")
        if data:
            html_parts.append(_decode_base64url(data))
    if html_parts:
        html_raw = max(html_parts, key=len)
    else:
        html_raw = None
    
    message_fields = {
        "from_email": from_email, 
        "subject": subject, 
        "internal_date_ms": internal_date_ms, 
        "html_raw": html_raw,
        "error": None,
        }
    
    return message_fields


def chunked(iterable, size: int):
    batch = []
    for x in iterable:
        batch.append(x)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
    
            
def run_mail_fetch(batch_size: int = 50):
    ws = get_workspace()
    
    client_secret_path = ws.root / "secrets/google_client_secret.json"
    token_path = ws.root / "secrets/gmail_token.json"
    db_path = ws.db_path
    
    service = authenticate_and_build_service(client_secret_path, token_path)
    
    with get_connection(db_path) as conn:
        message_id_list = get_non_fetched_email_list(conn)
        if message_id_list:
            logger.info("%s new messages to fetch", len(message_id_list))
        else:
            logger.info("No new message to fetch")
        
        for id_batch in chunked(message_id_list, batch_size):
            rows = []
            
            for message_id in id_batch:
                d={"message_id": message_id}
                
                try:
                    message = load_message(service, message_id)
                    d.update(extract_message_fields(message))            
                except Exception as e:
                    d.update(
                        {
                            "from_email": None,
                            "subject": None, 
                            "internal_date_ms": None, 
                            "html_raw": None,
                            "error": f"Fetched error: {repr(e)}"
                            }
                        )
                    logger.exception("Failed to fetch message_id=%s", message_id)
                    
                rows.append(d)
                
            update_fetched_email_metadata(conn, rows)