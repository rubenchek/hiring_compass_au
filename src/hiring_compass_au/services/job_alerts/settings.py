from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_repo_root() -> Path:
    # Explicit override (useful in Docker)
    env_repo = os.environ.get("HC_REPO_ROOT")
    if env_repo:
        return Path(env_repo).expanduser().resolve()

    # Fallback: workspace root (HC_ROOT) or CWD (/app in Docker)
    env_root = os.environ.get("HC_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    return Path.cwd().resolve()


class JobAlertsSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HC_",
        env_file=(".env.local", ".env"),
        extra="ignore",
        enable_decoding=False,
    )

    repo_root: Path = Field(default_factory=_resolve_repo_root)
    secrets_dir: Path = Path("secrets")
    gmail_client_secret_path: Path = Path("google_client_secret.json")
    gmail_token_path: Path = Path("gmail_token.json")

    gmail_oauth_host: str = "127.0.0.1"
    gmail_oauth_port: int = 0
    gmail_oauth_open_browser: bool = True

    senders: list[str] = ["jobmail@s.seek.com.au"]
    fetch_batch_size: int = 50

    canon_batch_size: int = 200
    canon_timeout_s: float = 15
    canon_max_batches: int | None = None
    progress: bool = False

    @field_validator("senders", mode="before")
    @classmethod
    def _parse_senders(cls, v):
        if v is None:
            return v
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("["):
                try:
                    return json.loads(s)
                except json.JSONDecodeError:
                    pass
            return [x.strip() for x in s.replace(";", ",").split(",") if x.strip()]
        return v

    @model_validator(mode="after")
    def _resolve_relative_paths(self) -> JobAlertsSettings:
        self.repo_root = self.repo_root.resolve()

        if not self.secrets_dir.is_absolute():
            self.secrets_dir = (self.repo_root / self.secrets_dir).resolve()

        if not self.gmail_client_secret_path.is_absolute():
            self.gmail_client_secret_path = (
                self.secrets_dir / self.gmail_client_secret_path
            ).resolve()

        if not self.gmail_token_path.is_absolute():
            self.gmail_token_path = (self.secrets_dir / self.gmail_token_path).resolve()

        return self
