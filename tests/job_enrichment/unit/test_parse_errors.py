from __future__ import annotations

import pytest

from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.parse import (
    parse_job_details,
)
from hiring_compass_au.services.job_enrichment.models import (
    FetchResult,
    TerminalEnrichmentError,
)


class DummyTarget:
    external_job_id = "123"


def test_parse_raises_on_invalid_payload():
    fetch = FetchResult(http_status=200, headers={}, payload="bad")
    with pytest.raises(TerminalEnrichmentError):
        parse_job_details(fetch, DummyTarget())


def test_parse_raises_on_missing_job():
    fetch = FetchResult(http_status=200, headers={}, payload={"data": {}})
    with pytest.raises(TerminalEnrichmentError):
        parse_job_details(fetch, DummyTarget())
