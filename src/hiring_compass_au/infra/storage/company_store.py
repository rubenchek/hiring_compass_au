from __future__ import annotations

import json
import sqlite3
from typing import Any


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True)


def upsert_company_from_patch(
    conn: sqlite3.Connection,
    *,
    patch: dict[str, Any],
) -> int | None:
    seek_company_id = patch.get("seek_company_id")
    if not seek_company_id:
        return None

    values = {
        "name": patch.get("name"),
        "industry": patch.get("industry"),
        "description": _json_or_none(patch.get("description")),
        "size": patch.get("size"),
        "website_url": patch.get("website_url"),
        "seek_company_id": seek_company_id,
        "seek_rating_value": patch.get("seek_rating_value"),
        "seek_review_count": patch.get("seek_review_count"),
        "seek_company_url": patch.get("seek_company_url"),
    }

    columns = [
        "name",
        "industry",
        "description",
        "size",
        "website_url",
        "seek_company_id",
        "seek_rating_value",
        "seek_review_count",
        "seek_company_url",
    ]
    sql_columns = ", ".join(columns)
    sql_placeholders = ", ".join(["?"] * len(columns))
    sql_updates = ", ".join([f"{col}=COALESCE(excluded.{col}, company.{col})" for col in columns])

    row = conn.execute(
        f"""
        INSERT INTO company ({sql_columns})
        VALUES ({sql_placeholders})
        ON CONFLICT(seek_company_id) DO UPDATE SET
            {sql_updates}
        RETURNING id
        """,
        [values[col] for col in columns],
    ).fetchone()

    return int(row[0]) if row else None
