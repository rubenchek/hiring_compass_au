from __future__ import annotations

from urllib.parse import urlparse, urlunparse
import logging
import re
import requests

logger = logging.getLogger(__name__)

SEEK_JOB_PATH_RE = re.compile(r"^/job/(?P<job_id>\d+)(?:/.*)?$")


class CanonicalizeError(Exception):
    def __init__(self, message: str, http_status: int | None = None):
        super().__init__(message)
        self.http_status = http_status


def head_location(
    session: requests.Session, 
    out_url: str, 
    timeout: float = 15.0,
    ) -> tuple[int,str | None]:    
    
    r = session.head(out_url, allow_redirects=False, timeout=timeout)
    loc = r.headers.get("Location")
    if loc:
        return r.status_code, loc
    
    r = session.get(out_url, allow_redirects=False, timeout=timeout)
    return r.status_code, r.headers.get("Location")


def canonicalize_seek_location(location_url: str) -> tuple[str, str]:
    """
    location_url is expected to be a SEEK job URL (after redirect).
    Returns (job_id, canonical_url).
    """
    p = urlparse(location_url)
    m = SEEK_JOB_PATH_RE.match(p.path)
    if not m:
        raise ValueError(f"Not a SEEK job URL: {location_url}")

    job_id = m.group("job_id")
    canonical_path = f"/job/{job_id}"
    canonical_url = urlunparse((p.scheme, p.netloc, canonical_path, "", "", ""))    
    return job_id, canonical_url 



def resolve_to_canonical(
    session: requests.Session,
    out_url: str,
    timeout: float = 15.0) -> tuple[str, str, int]:
    """
    out_url = url from email (tracking).
    Returns (job_id, canonical_url, http_status).
    """
    http_status, location = head_location(session, out_url, timeout=timeout)
    if not location:
        raise CanonicalizeError(
            f"No Location header (status={http_status}) for out_url={out_url}",
            http_status=http_status,
            )
    try:
        job_id, canonical_url = canonicalize_seek_location(location)
    except ValueError as e:
        raise CanonicalizeError(str(e), http_status=http_status) from e
    
    return job_id, canonical_url, http_status
    