from __future__ import annotations

import json

import pytest
import requests

from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.fetch import (
    fetch_job_details,
)
from hiring_compass_au.services.job_enrichment.models import (
    RetryableEnrichmentError,
    TerminalEnrichmentError,
)


class DummyResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.headers = {}
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, response):
        self._response = response
        self.headers = {}
        self.seek_session_id = "sess"
        self.seek_visitor_id = "visit"

    def post(self, *_args, **_kwargs):
        return self._response


class ErrorSession(DummySession):
    def post(self, *_args, **_kwargs):
        raise requests.Timeout("boom")


class DummyTarget:
    external_job_id = "123"


def test_fetch_retryable_on_timeout():
    session = ErrorSession(None)
    with pytest.raises(RetryableEnrichmentError):
        fetch_job_details(DummyTarget(), session=session)


def test_fetch_retryable_on_429():
    session = DummySession(DummyResponse(429, {"data": {}}))
    with pytest.raises(RetryableEnrichmentError):
        fetch_job_details(DummyTarget(), session=session)


def test_fetch_retryable_on_500():
    session = DummySession(DummyResponse(500, {"data": {}}))
    with pytest.raises(RetryableEnrichmentError):
        fetch_job_details(DummyTarget(), session=session)


def test_fetch_terminal_on_404():
    session = DummySession(DummyResponse(404, {"data": {}}))
    with pytest.raises(TerminalEnrichmentError):
        fetch_job_details(DummyTarget(), session=session)


def test_fetch_terminal_on_graphql_errors():
    session = DummySession(DummyResponse(200, {"errors": [{"message": "nope"}]}))
    with pytest.raises(TerminalEnrichmentError):
        fetch_job_details(DummyTarget(), session=session)
