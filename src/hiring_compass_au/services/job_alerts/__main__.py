from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import uuid
from datetime import UTC, datetime

import requests

from hiring_compass_au.config.settings import WorkspaceSettings
from hiring_compass_au.infra.storage.db import get_connection
from hiring_compass_au.infra.storage.schema import init_all_tables
from hiring_compass_au.services.job_alerts.pipeline import run_job_alert_pipeline
from hiring_compass_au.services.job_alerts.settings import JobAlertsSettings
from hiring_compass_au.workspace import WorkspacePaths, ensure_workspace

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_TEMPFAIL = 75

RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}


def _google_http_status(e) -> int | None:
    resp = getattr(e, "resp", None)
    if resp is not None:
        return getattr(resp, "status", None)
    return getattr(e, "status_code", None)


def classify_exception_to_exit_code(e: Exception) -> int:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return EXIT_TEMPFAIL

    try:
        from googleapiclient.errors import HttpError
    except Exception:
        HttpError = None

    try:
        from google.auth.exceptions import TransportError
    except Exception:
        TransportError = None

    if TransportError is not None and isinstance(e, TransportError):
        return EXIT_TEMPFAIL

    if HttpError is not None and isinstance(e, HttpError):
        status = _google_http_status(e)
        if status in RETRYABLE_HTTP_STATUSES:
            return EXIT_TEMPFAIL
        return EXIT_FAIL

    return EXIT_FAIL


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--no-index", action="store_true")
    p.add_argument("--no-fetch", action="store_true")
    p.add_argument("--no-parse", action="store_true")
    p.add_argument("--no-canonicalize", action="store_true")
    p.add_argument("--no-promote", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    logger = logging.getLogger(__name__)

    ws = WorkspaceSettings()
    cfg = JobAlertsSettings()

    ensure_workspace(WorkspacePaths(root=ws.root), minimal=True)

    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC).replace(microsecond=0).isoformat()

    results = None
    exit_code = EXIT_OK
    error_type = None
    error_message = None

    needs_gmail = (not args.no_index) or (not args.no_fetch)

    startup = {
        "event": "pipeline_start",
        "run_id": run_id,
        "root": str(ws.root),
        "db_path": str(ws.db_path),
        "logs_dir": str(ws.logs_dir),
        "needs_gmail": needs_gmail,
        "flags": {
            "index": not args.no_index,
            "fetch": not args.no_fetch,
            "parse": not args.no_parse,
            "canonicalize": not args.no_canonicalize,
            "promote": not args.no_promote,
        },
    }
    logger.info("%s", json.dumps(startup, ensure_ascii=False))

    if needs_gmail and not cfg.gmail_client_secret_path.exists():
        exit_code = EXIT_FAIL
        error_type = "FileNotFoundError"
        error_message = (
            f"Missing Gmail client secret file: {cfg.gmail_client_secret_path} "
            "(set HC_GMAIL_CLIENT_SECRET to override)"
        )
        logger.error("%s", error_message)

    if exit_code == EXIT_OK:
        if needs_gmail and not cfg.gmail_token_path.exists():
            logger.warning(
                "Gmail token not found; OAuth flow may be required: %s", cfg.gmail_token_path
            )
        try:
            with get_connection(ws.db_path, sqlite3.Row) as conn:
                init_all_tables(conn)

                results = run_job_alert_pipeline(
                    conn,
                    cfg.gmail_client_secret_path,
                    cfg.gmail_token_path,
                    cfg.gmail_oauth_host,
                    cfg.gmail_oauth_port,
                    cfg.gmail_oauth_open_browser,
                    index=not args.no_index,
                    fetch=not args.no_fetch,
                    fetch_batch_size=cfg.fetch_batch_size,
                    parse=not args.no_parse,
                    canonicalize=not args.no_canonicalize,
                    canon_batch_size=cfg.canon_batch_size,
                    canon_timeout_s=cfg.canon_timeout_s,
                    canon_max_batches=cfg.canon_max_batches,
                    promote=not args.no_promote,
                    senders=cfg.senders,
                    progress=cfg.progress,
                )
        except Exception as e:
            results = getattr(e, "hc_results", results)

            exit_code = classify_exception_to_exit_code(e)
            error_type = type(e).__name__
            error_message = str(e)

            if exit_code == EXIT_TEMPFAIL:
                logger.warning("Pipeline failed (temporary network error)", exc_info=True)
            else:
                logger.exception("Pipeline failed")

    finished_at = datetime.now(UTC).replace(microsecond=0).isoformat()

    summary = {
        "event": "pipeline_summary",
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "exit_code": exit_code,
        "db_path": str(ws.db_path),
        "logs_dir": str(ws.logs_dir),
        "results": results,
        "error_type": error_type,
        "error_message": error_message,
    }

    line = json.dumps(summary, ensure_ascii=False)
    logger.info("%s", line)

    jsonl_path = ws.logs_dir / "pipeline_runs.jsonl"
    with jsonl_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
