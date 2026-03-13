from __future__ import annotations

from typing import Any

from hiring_compass_au.infra.storage.enrichment_store import mark_enrichment_success
from hiring_compass_au.infra.storage.seek_enrichment_store import upsert_seek_enrichment
from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.fetch import (
    fetch_job_details,
)
from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.parse import (
    parse_job_details,
)
from hiring_compass_au.services.job_enrichment.models import (
    EnrichmentResult,
    MissingSessionError,
)


class SeekJobDetailsHandler:
    enrich_type = "jobDetails"
    source = "seek"

    def __init__(self, session=None) -> None:
        self._session = session
        if self._session is None:
            raise MissingSessionError("SeekJobDetailsHandler requires a session")

    def enrich(self, target) -> EnrichmentResult:
        fetch_result = fetch_job_details(target, session=self._session)
        parse_result = parse_job_details(fetch_result, target)
        return EnrichmentResult(
            fetch_result=fetch_result,
            parse_result=parse_result,
        )

    def persist_source_patch(
        self,
        conn,
        job_id: int,
        patch: dict[str, Any],
    ) -> None:
        upsert_seek_enrichment(conn, job_id=job_id, patch=patch)

    def post_persist(self, *, conn, target, result) -> int:
        source = result.parse_result.source_patch
        if source is None:
            return 0
        skills = getattr(source, "skills", None)
        if skills is None:
            return 0
        if isinstance(skills, list) and len(skills) == 0:
            return 0
        if target.enrich_type == "jobDetails":
            mark_enrichment_success(
                conn=conn,
                job_id=target.job_id,
                enrich_type="matchedSkills",
                http_status=result.fetch_result.http_status,
            )
            return 1
        return 0
