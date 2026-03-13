from __future__ import annotations

import json
import sqlite3
from typing import Any


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True)


def upsert_seek_enrichment(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    patch: dict[str, Any],
) -> None:
    columns = [
        "advertiser_id",
        "role_id",
        "classification_ids",
        "classification_labels",
        "subclassification_ids",
        "subclassification_labels",
        "seo_normalised_role_title",
        "work_types",
        "work_arrangement_types",
        "badges",
        "description_raw",
        "teaser",
        "bullet_points",
        "questionnaire_questions",
        "skills",
        "expires_at_utc",
        "insights_volume_label",
        "insights_count",
        "status",
    ]

    values = {
        "advertiser_id": patch.get("advertiser_id"),
        "role_id": patch.get("role_id"),
        "classification_ids": _json_or_none(patch.get("classification_ids")),
        "classification_labels": _json_or_none(patch.get("classification_labels")),
        "subclassification_ids": _json_or_none(patch.get("subclassification_ids")),
        "subclassification_labels": _json_or_none(patch.get("subclassification_labels")),
        "seo_normalised_role_title": patch.get("seo_normalised_role_title"),
        "work_types": patch.get("work_types"),
        "work_arrangement_types": _json_or_none(patch.get("work_arrangement_types")),
        "badges": _json_or_none(patch.get("badges")),
        "description_raw": patch.get("description_raw"),
        "teaser": patch.get("teaser"),
        "bullet_points": _json_or_none(patch.get("bullet_points")),
        "questionnaire_questions": _json_or_none(patch.get("questionnaire_questions")),
        "skills": _json_or_none(patch.get("skills")),
        "expires_at_utc": patch.get("expires_at_utc"),
        "insights_volume_label": patch.get("insights_volume_label"),
        "insights_count": patch.get("insights_count"),
        "status": patch.get("status"),
    }

    sql_columns = ", ".join(["job_id", *columns])
    sql_placeholders = ", ".join(["?"] * (1 + len(columns)))
    sql_updates = ", ".join(
        [f"{col}=COALESCE(excluded.{col}, seek_enrichment.{col})" for col in columns]
    )

    conn.execute(
        f"""
        INSERT INTO seek_enrichment ({sql_columns})
        VALUES ({sql_placeholders})
        ON CONFLICT(job_id) DO UPDATE SET
            {sql_updates}
        """,
        [job_id, *[values[col] for col in columns]],
    )
