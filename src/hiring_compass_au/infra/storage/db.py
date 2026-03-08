from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

# ----------------------------
# Connection
# ----------------------------


def get_connection(db_path: Path, row_factory=None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    if row_factory is not None:
        conn.row_factory = row_factory
    return conn


# ----------------------------
# Utils
# ----------------------------


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def compute_backoff_minutes(attempt_count_after_increment: int) -> int:
    """
    Exponential backoff in minutes, capped.
    attempt_count_after_increment: 1,2,3,...
    """
    # 1->2min, 2->4min, 3->8min, ... cap at 1440min (24h)
    minutes = 2**attempt_count_after_increment
    return min(minutes, 24 * 60)
