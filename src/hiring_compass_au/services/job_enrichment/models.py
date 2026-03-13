from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from hiring_compass_au.domain.models import CompanyData, JobAdData


class SourcePatch(Protocol):
    def to_patch(self) -> dict[str, Any] | None: ...


@dataclass(slots=True)
class BatchSummary:
    selected: int = 0
    success: int = 0
    retry: int = 0
    failed: int = 0
    skipped: int = 0


@dataclass(slots=True)
class EnrichmentTarget:
    job_id: int
    enrich_type: str
    source: str
    external_job_id: str | None
    canonical_url: str | None


@dataclass(slots=True)
class FetchResult:
    http_status: int | None
    headers: dict[str, str]
    payload: Any


@dataclass(slots=True)
class ParseResult:
    job_ad_patch: JobAdData | None = None
    source_patch: SourcePatch | None = None
    company_patch: CompanyData | None = None


@dataclass(slots=True)
class EnrichmentResult:
    fetch_result: FetchResult
    parse_result: ParseResult


class RetryableEnrichmentError(Exception):
    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        error_code: str = "retryable_error",
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.error_code = error_code


class TerminalEnrichmentError(Exception):
    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        error_code: str = "terminal_error",
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.error_code = error_code


class MissingSessionError(ValueError):
    def __init__(self, message: str = "Handler requires a session") -> None:
        super().__init__(message)
