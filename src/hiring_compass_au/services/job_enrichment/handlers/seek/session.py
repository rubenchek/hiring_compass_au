from __future__ import annotations

import uuid

import requests


def build_seek_session(
    bearer_token: str | None = None, cookie: str | None = None
) -> requests.Session:
    session = requests.Session()
    session.seek_session_id = str(uuid.uuid4())
    session.seek_visitor_id = str(uuid.uuid4())
    session_id = session.seek_session_id
    session.headers.update(
        {
            "Accept": "*/*",
            "Accept-encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-AU,en;q=0.9",
            "Content-Type": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            "Origin": "https://www.seek.com.au",
            "Referer": "https://www.seek.com.au/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Sec-CH-UA": ('"Not:A-Brand";v="99", "Brave";v="145", "Chromium";v="145"'),
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"macOS"',
            "Sec-GPC": "1",
            "X-Seek-EC-SessionId": session_id,
            "X-Seek-EC-VisitorId": session_id,
            "X-Seek-Site": "chalice",
        }
    )
    if not bearer_token:
        bearer_token = input("SEEK bearer token (optional, press Enter to skip): ").strip() or None
    if bearer_token:
        token = bearer_token.strip()
        if not token.lower().startswith("bearer "):
            token = f"Bearer {token}"
        session.headers["Authorization"] = token

    if not cookie:
        cookie = input("SEEK cookie (optional, press Enter to skip): ").strip() or None
    if cookie:
        session.headers["Cookie"] = cookie

    return session
