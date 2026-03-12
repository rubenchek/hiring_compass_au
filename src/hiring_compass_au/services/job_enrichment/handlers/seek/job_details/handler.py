from __future__ import annotations

from typing import Any

from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.fetch import (
    fetch_job_details,
)
from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.parse import (
    parse_job_details,
)
from hiring_compass_au.services.job_enrichment.models import EnrichmentResult


class SeekJobDetailsHandler:
    enrich_type = "jobDetails"
    source = "seek"

    def __init__(self, session=None) -> None:
        self._session = session
        if not self._session:
            raise

    def enrich(self, target) -> EnrichmentResult:
        fetch_result = fetch_job_details(target, session=self._session)
        parse_result = parse_job_details(fetch_result, target)
        return EnrichmentResult(
            fetch_result=fetch_result,
            parse_result=parse_result,
        )

    def persist_source_patch(self, conn, job_id: int, patch: dict[str, Any], now_utc: str) -> None:
        """
        Persist source-specific patch. Placeholder.
        """
        _ = (conn, job_id, patch, now_utc)
