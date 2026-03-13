from __future__ import annotations

import os
import uuid
from pathlib import Path

import requests

ENV_BEARER = "HC_SEEK_BEARER"
ENV_COOKIE = "HC_SEEK_COOKIE"


def _persist_env_file(path: Path, values: dict[str, str]) -> None:
    existing: dict[str, str] = {}
    if path.exists():
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            existing[key.strip()] = value.strip()

    existing.update(values)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{k}={v}" for k, v in sorted(existing.items())]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _secrets_env_path() -> Path:
    env_dir = Path(os.environ.get("HC_SECRETS_DIR") or "secrets")
    if not env_dir.is_absolute():
        env_dir = env_dir.resolve()
    return env_dir / "seek.env"


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
        os.environ[ENV_BEARER] = token

    if not cookie:
        cookie = input("SEEK cookie (optional, press Enter to skip): ").strip() or None
    if cookie:
        session.headers["Cookie"] = cookie
        os.environ[ENV_COOKIE] = cookie

    persist_values = {}
    if bearer_token:
        persist_values[ENV_BEARER] = os.environ[ENV_BEARER]
    if cookie:
        persist_values[ENV_COOKIE] = os.environ[ENV_COOKIE]
    if persist_values:
        env_path = _secrets_env_path()
        _persist_env_file(env_path, persist_values)

    return session
