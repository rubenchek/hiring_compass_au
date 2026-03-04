import base64
import logging
import sqlite3
from email.utils import parseaddr

from hiring_compass_au.data.storage.mail_store import (
    get_non_fetched_email_list,
    update_fetched_email_metadata,
)

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
    message = (
        service.users()
        .messages()
        .get(
            userId="me",
            id=message_id,
            format="full",
        )
        .execute()
    )
    return message


def extract_message_fields(message: dict):
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


def run_mail_fetch(
    service, conn: sqlite3.Connection, batch_size: int = 50
) -> tuple[int, int, int, int]:
    to_fetch = get_non_fetched_email_list(conn)
    total = len(to_fetch)

    if total:
        logger.info("%d new messages to fetch", total)
    else:
        logger.info("No new message to fetch")
        return 0, 0, 0, 0

    fetched_ok_total = 0
    fetch_errors_total = 0
    persisted_total = 0

    for id_batch in chunked(to_fetch, batch_size):
        rows = []

        for message_id in id_batch:
            d = {"message_id": message_id}

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
                        "error": f"Fetched error: {repr(e)}",
                    }
                )
                logger.exception("Failed to fetch message_id=%s", message_id)

            rows.append(d)

        updated_rows_b = update_fetched_email_metadata(conn, rows)

        fetched_ok_b = sum(1 for r in rows if not r.get("error"))
        fetch_errors_b = len(rows) - fetched_ok_b
        fetched_ok_total += fetched_ok_b
        fetch_errors_total += fetch_errors_b
        persisted_total += updated_rows_b

        if updated_rows_b != len(rows):
            logger.warning(
                "Fetch batch persisted less than fetched: fetched=%d persisted=%d "
                "(possible status mismatch or missing rows)",
                len(rows),
                updated_rows_b,
            )

    logger.info(
        "Mail fetch finished: ok=%d error=%d persisted=%d",
        fetched_ok_total,
        fetch_errors_total,
        persisted_total,
    )
    if persisted_total != total:
        logger.warning(
            "Mail fetch persisted less than to_fetch: to_fetched=%d persisted=%d "
            "(possible status mismatch or missing rows)",
            total,
            persisted_total,
        )

    fetch_result = (total, fetched_ok_total, fetch_errors_total, persisted_total)
    return fetch_result
