from __future__ import annotations

import pytest

from hiring_compass_au.services.job_enrichment.dispatcher import (
    HandlerNotFoundError,
    dispatch_handler,
)
from hiring_compass_au.services.job_enrichment.handlers.seek.job_details.handler import (
    SeekJobDetailsHandler,
)


def test_dispatcher_raises_when_handler_missing():
    with pytest.raises(HandlerNotFoundError):
        dispatch_handler(source="seek", enrich_type="missing", session=object())


def test_dispatcher_returns_handler_instance():
    handler = dispatch_handler(source="seek", enrich_type="jobDetails", session=object())
    assert isinstance(handler, SeekJobDetailsHandler)
