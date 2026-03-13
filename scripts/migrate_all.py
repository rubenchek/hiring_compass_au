from __future__ import annotations

import pathlib
import sqlite3
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hiring_compass_au.infra.storage.migrations import apply_migrations


def main(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        apply_migrations(conn)
        conn.commit()
        print("OK: migrations applied")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/migrate_all.py <db_path>")
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
