from __future__ import annotations

from types import SimpleNamespace

import pytest

from hiring_compass_au.data.pipelines.job_alerts.enrichment.url_canonicalizer import (
    CanonicalizeError,
    canonicalize_seek_location,
    resolve_to_canonical,
)


def test_canonicalize_seek_location_strips_to_job_id_path():
    job_id, canonical = canonicalize_seek_location("https://www.seek.com.au/job/12345?foo=1")
    assert job_id == "12345"
    assert canonical == "https://www.seek.com.au/job/12345"


def test_resolve_to_canonical_uses_location_header():
    class FakeSession:
        def head(self, out_url, allow_redirects, timeout):
            return SimpleNamespace(
                status_code=302, headers={"Location": "https://www.seek.com.au/job/99999?x=1"}
            )

        def get(self, out_url, allow_redirects, timeout):
            raise AssertionError("GET should not be called when HEAD has Location")

    job_id, canonical, status = resolve_to_canonical(
        FakeSession(), "https://email.s.seek.com.au/uni/ss/c/x"
    )
    assert job_id == "99999"
    assert canonical == "https://www.seek.com.au/job/99999"
    assert status == 302


def test_resolve_to_canonical_raises_when_no_location():
    class FakeSession:
        def head(self, out_url, allow_redirects, timeout):
            return SimpleNamespace(status_code=200, headers={})

        def get(self, out_url, allow_redirects, timeout):
            return SimpleNamespace(status_code=200, headers={})

    with pytest.raises(CanonicalizeError):
        resolve_to_canonical(FakeSession(), "https://email.s.seek.com.au/uni/ss/c/x")
