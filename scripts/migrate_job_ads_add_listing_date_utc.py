from __future__ import annotations

import sqlite3
import sys


def main(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE job_ads
            ADD COLUMN listing_date_utc TEXT;
            """
        )
        conn.commit()
        print("OK: added job_ads.listing_date_utc")
        return 0
    except sqlite3.OperationalError as exc:
        # Column already exists or other DDL issue
        msg = str(exc).lower()
        if "duplicate column" in msg or "already exists" in msg:
            print("OK: job_ads.listing_date_utc already exists")
            return 0
        print(f"ERROR: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/migrate_job_ads_add_listing_date_utc.py <db_path>")
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
