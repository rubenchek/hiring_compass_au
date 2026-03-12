from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from hiring_compass_au.domain.models import CompanyData, JobAdData


class SourcePatch(Protocol):
    def to_patch(self) -> dict[str, Any] | None: ...


@dataclass(slots=True)
class FetchResult:
    http_status: int | None
    headers: dict[str, str]
    payload: Any


@dataclass(slots=True)
class ParseResult:
    job_ad_patch: JobAdData | dict[str, Any] | None = None
    source_patch: SourcePatch | dict[str, Any] | None = None
    company_patch: CompanyData | dict[str, Any] | None = None


@dataclass(slots=True)
class EnrichmentResult:
    fetch_result: FetchResult
    parse_result: ParseResult
