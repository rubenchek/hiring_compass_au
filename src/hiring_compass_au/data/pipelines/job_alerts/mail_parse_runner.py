import logging
import sqlite3

from hiring_compass_au.data.parsers.gmail.parser_registry import parse_email
from hiring_compass_au.data.storage.hit_store import upsert_email_job_hits
from hiring_compass_au.data.storage.mail_store import (
    get_fetched_emails_to_parse,
    update_parsed_email,
)

logger = logging.getLogger(__name__)


def compute_parsed_confidence(hit_confidences: list[int], hits_expected=None) -> int:
    n = len(hit_confidences)
    if n == 0:
        return 10  # supported but empty = suspicious

    avg = sum(hit_confidences) / n
    frac_good = sum(1 for x in hit_confidences if x >= 80) / n
    frac_bad = sum(1 for x in hit_confidences if x < 50) / n

    score = avg + 10 * frac_good - 15 * frac_bad

    if hits_expected:
        lo, hi = hits_expected
        if lo <= n <= hi:
            score += 5

        # soft penalty if far from expected
        ok_low = max(1, lo // 3)
        ok_high = int(hi * 1.5)
        if n < ok_low:
            score -= 10
        elif n > ok_high:
            score -= 5
    if n < 3:
        score -= 10

    return int(max(0, min(100, round(score))))


def run_mail_parse(conn: sqlite3.Connection) -> tuple[int, int, int, int, int]:
    mail_total = 0
    unsupported_total = 0
    hits_upserted_total = 0
    empty_total = 0
    error_total = 0
    emails_updated_total = 0

    for message in get_fetched_emails_to_parse(conn):
        mail_total += 1
        message_id = message["message_id"]
        from_email = message["from_email"]
        html_raw = message["html_raw"]
        parser_cfg = None

        try:
            it, parser_cfg = parse_email(from_email, html_raw)

            if it is None:
                emails_updated_total += update_parsed_email(conn, message_id, supported=False)
                conn.commit()
                unsupported_total += 1
                continue

            hits = list(it)
            valid_hits = [h for h in hits if h.get("out_url")]

            hit_confidences = [int(h.get("hit_confidence") or 0) for h in valid_hits]
            parsed_confidence = compute_parsed_confidence(
                hit_confidences,
                hits_expected=parser_cfg.get("hits_expected"),
            )

            hit_parsed_count = upsert_email_job_hits(
                conn=conn,
                message_id=message_id,
                valid_hits=valid_hits,
                parser_cfg=parser_cfg,
            )
            emails_updated_total += update_parsed_email(
                conn=conn,
                message_id=message_id,
                parser_cfg=parser_cfg,
                hit_parsed_count=hit_parsed_count,
                parsed_confidence=parsed_confidence,
            )
            conn.commit()

            hits_upserted_total += hit_parsed_count
            if hit_parsed_count == 0:
                empty_total += 1

        except Exception as e:
            conn.rollback()
            logger.exception("Parsing failed for message_id=%s", message_id)

            emails_updated_total += update_parsed_email(conn, message_id, error=str(e))
            conn.commit()
            error_total += 1

    logger.info(
        "Mail parse finished: emails=%d hits_upserted=%d empty=%d error=%d unsupported=%d",
        mail_total,
        hits_upserted_total,
        empty_total,
        error_total,
        unsupported_total,
    )

    if emails_updated_total != mail_total:
        logger.warning(
            "Mail parse updated less rows than processed: processed=%d updated=%d",
            mail_total,
            emails_updated_total,
        )

    if mail_total and hits_upserted_total == 0 and error_total == 0:
        logger.warning("Mail parse produced 0 hits (possible template change?)")

    return mail_total, hits_upserted_total, empty_total, error_total, unsupported_total
