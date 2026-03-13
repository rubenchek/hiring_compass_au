from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import uuid
from dataclasses import asdict
from datetime import UTC, datetime

from hiring_compass_au.config.settings import WorkspaceSettings
from hiring_compass_au.infra.storage.db import get_connection
from hiring_compass_au.infra.storage.migrations import apply_migrations
from hiring_compass_au.infra.storage.schema import init_all_tables
from hiring_compass_au.services.job_enrichment.models import (
    RetryableEnrichmentError,
    TerminalEnrichmentError,
)
from hiring_compass_au.services.job_enrichment.runner import run_enrichment
from hiring_compass_au.workspace import WorkspacePaths, ensure_workspace

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_TEMPFAIL = 75


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--max-batches", type=int, default=None)
    p.add_argument("--request-sleep-min", type=float, default=0.4)
    p.add_argument("--request-sleep-max", type=float, default=1.2)
    p.add_argument("--batch-sleep-min", type=float, default=3.0)
    p.add_argument("--batch-sleep-max", type=float, default=10.0)
    p.add_argument("--batch-size-min", type=int, default=25)
    p.add_argument("--batch-size-max", type=int, default=50)
    p.add_argument("--throttle-sleep-min", type=float, default=30.0)
    p.add_argument("--throttle-sleep-max", type=float, default=90.0)
    p.add_argument("--throttle-error-limit", type=int, default=3)
    p.add_argument("--log-every-batches", type=int, default=2)
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    logger = logging.getLogger(__name__)

    ws = WorkspaceSettings()
    ensure_workspace(WorkspacePaths(root=ws.root), minimal=True)

    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    logger.info(
        "%s",
        json.dumps(
            {
                "event": "enrichment_start",
                "run_id": run_id,
                "db_path": str(ws.db_path),
                "limit": args.limit,
                "max_batches": args.max_batches,
            },
            ensure_ascii=False,
        ),
    )

    summary = None
    exit_code = EXIT_OK
    error_type = None
    error_message = None
    try:
        with get_connection(ws.db_path, sqlite3.Row) as conn:
            init_all_tables(conn)
            apply_migrations(conn)
            summary = run_enrichment(
                conn,
                limit=args.limit,
                max_batches=args.max_batches,
                request_sleep_range=(args.request_sleep_min, args.request_sleep_max),
                batch_sleep_range=(args.batch_sleep_min, args.batch_sleep_max),
                batch_size_range=(args.batch_size_min, args.batch_size_max),
                throttle_sleep_range=(args.throttle_sleep_min, args.throttle_sleep_max),
                throttle_error_limit=args.throttle_error_limit,
                log_every_batches=args.log_every_batches,
            )
    except Exception as exc:
        logger.exception("Enrichment service failed")
        error_type = type(exc).__name__
        error_message = str(exc)
        if isinstance(exc, RetryableEnrichmentError):
            exit_code = EXIT_TEMPFAIL
        elif isinstance(exc, TerminalEnrichmentError):
            exit_code = EXIT_FAIL
        else:
            exit_code = EXIT_FAIL

    finished_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    payload = {
        "event": "enrichment_summary",
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "exit_code": exit_code,
        "db_path": str(ws.db_path),
        "logs_dir": str(ws.logs_dir),
        "summary": asdict(summary) if summary is not None else None,
        "error_type": error_type,
        "error_message": error_message,
    }
    line = json.dumps(payload, ensure_ascii=False)
    logger.info("%s", line)

    jsonl_path = ws.logs_dir / "enrichment_runs.jsonl"
    with jsonl_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
