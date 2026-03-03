import logging
import sqlite3
import time
from pathlib import Path

from hiring_compass_au.data.ingestion.gmail.auth_and_build import authenticate_and_build_service
from hiring_compass_au.data.ingestion.gmail.mail_fetch import run_mail_fetch
from hiring_compass_au.data.ingestion.gmail.mail_index import run_mail_index
from hiring_compass_au.data.pipelines.job_alerts.job_promote_runner import run_promote_job_ad
from hiring_compass_au.data.pipelines.job_alerts.mail_parse_runner import run_mail_parse
from hiring_compass_au.data.pipelines.job_alerts.url_canonicalize_runner import (
    run_url_canonicalization,
)

logger = logging.getLogger(__name__)


def _record_stage_error(results: dict, stage: str, e: Exception) -> None:
    results["failed_stage"] = stage
    results["stage_error"] = {"type": type(e).__name__, "message": str(e)}


def run_job_alert_pipeline(
    conn: sqlite3.Connection,
    client_secret_path: Path,
    token_path: Path,
    *,
    index: bool = True,
    fetch: bool = True,
    parse: bool = True,
    canonicalize: bool = True,
    promote: bool = True,
    senders: list[str] | None = None,
    fetch_batch_size: int = 50,
    canon_batch_size: int = 200,
    canon_timeout_s: float = 15,
    canon_max_batches: int | None = None,
    progress: bool = True,
) -> dict:
    senders = senders or ["jobmail@s.seek.com.au"]

    results: dict = {
        "senders": senders,
        "index": None,
        "fetch": None,
        "parse": None,
        "canonicalize": None,
        "promote": None,
        "durations_s": {},
    }

    service = None
    if index or fetch:
        t0 = time.monotonic()
        try:
            service = authenticate_and_build_service(client_secret_path, token_path)
        except Exception as e:
            _record_stage_error(results, "gmail_auth", e)
            e.hc_results = results
            raise
        finally:
            results["durations_s"]["gmail_auth"] = round(time.monotonic() - t0, 3)

        if index:
            t0 = time.monotonic()
            try:
                inserted_total = 0
                found_total = 0
                logger.info("Start indexing emails (%d sender(s))", len(senders))
                for sender in senders:
                    inserted, found = run_mail_index(from_email=sender, service=service, conn=conn)
                    inserted_total += inserted
                    found_total += found
                results["index"] = {
                    "senders": senders,
                    "found": found_total,
                    "inserted": inserted_total,
                }
            except Exception as e:
                _record_stage_error(results, "index", e)
                e.hc_results = results
                raise
            finally:
                results["durations_s"]["index"] = round(time.monotonic() - t0, 3)

        if fetch:
            t0 = time.monotonic()
            try:
                logger.info("Start fetching emails")
                to_fetch, ok, error, persisted = run_mail_fetch(
                    service=service, conn=conn, batch_size=fetch_batch_size
                )
                results["fetch"] = {
                    "to_fetch": to_fetch,
                    "ok": ok,
                    "error": error,
                    "persisted": persisted,
                }
            except Exception as e:
                _record_stage_error(results, "fetch", e)
                e.hc_results = results
                raise
            finally:
                results["durations_s"]["fetch"] = round(time.monotonic() - t0, 3)

    if parse:
        t0 = time.monotonic()
        try:
            logger.info("Start parsing emails")
            emails, hits_upserted, empty, error, unsupported = run_mail_parse(conn=conn)
            results["parse"] = {
                "emails": emails,
                "hits_upserted": hits_upserted,
                "empty": empty,
                "error": error,
                "unsupported": unsupported,
            }
        except Exception as e:
            _record_stage_error(results, "parse", e)
            e.hc_results = results
            raise
        finally:
            results["durations_s"]["parse"] = round(time.monotonic() - t0, 3)

    if canonicalize:
        t0 = time.monotonic()
        try:
            logger.info("Start canonicalize url")
            total_start, ok, retry, error = run_url_canonicalization(
                conn=conn,
                batch_size=canon_batch_size,
                timeout=canon_timeout_s,
                max_batches=canon_max_batches,
                progress=progress,
            )
            results["canonicalize"] = {
                "total_start": total_start,
                "ok": ok,
                "retry": retry,
                "error": error,
            }
        except Exception as e:
            _record_stage_error(results, "canonicalize", e)
            e.hc_results = results
            raise
        finally:
            results["durations_s"]["canonicalize"] = round(time.monotonic() - t0, 3)

    if promote:
        t0 = time.monotonic()
        try:
            logger.info("Start promoting job ads")
            new, updated, failed = run_promote_job_ad(conn=conn)
            results["promote"] = {"new": new, "updated": updated, "failed": failed}
        except Exception as e:
            _record_stage_error(results, "promote", e)
            e.hc_results = results
            raise
        finally:
            results["durations_s"]["promote"] = round(time.monotonic() - t0, 3)

    return results
