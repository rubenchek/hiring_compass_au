import argparse
import logging
import sqlite3

from hiring_compass_au.data.pipelines.job_alerts.job_alerts_pipeline import run_job_alert_pipeline
from hiring_compass_au.data.pipelines.job_alerts.settings import JobAlertsSettings
from hiring_compass_au.data.storage.db import get_connection
from hiring_compass_au.data.storage.schema import init_all_tables
from hiring_compass_au.settings import WorkspaceSettings
from hiring_compass_au.workspace import WorkspacePaths, ensure_workspace


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--no-index", action="store_true")
    p.add_argument("--no-fetch", action="store_true")
    p.add_argument("--no-parse", action="store_true")
    p.add_argument("--no-canonicalize", action="store_true")
    p.add_argument("--no-promote", action="store_true")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    ws = WorkspaceSettings()
    cfg = JobAlertsSettings(root=ws.root)

    ensure_workspace(WorkspacePaths(root=ws.root))
    
    if not args.no_index or not args.no_fetch:
        if not cfg.gmail_client_secret.exists():
            logger.error(
                "Missing Gmail client secret file: %s (set HC_GMAIL_CLIENT_SECRET to override)",
                cfg.gmail_client_secret,
            )
            return 1
        if not cfg.gmail_token.exists():
            logger.error(
                "Missing Gmail token file: %s (set HC_GMAIL_TOKEN to override)",
                cfg.gmail_token,
            )
            return 1

    with get_connection(ws.db_path, sqlite3.Row) as conn:
        init_all_tables(conn)

        run_job_alert_pipeline(
            conn, 
            cfg.gmail_client_secret,
            cfg.gmail_token,
            index=not args.no_index,
            fetch=not args.no_fetch,
            parse=not args.no_parse,
            canonicalize=not args.no_canonicalize,
            promote=not args.no_promote,
            senders=cfg.senders,
        )

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
