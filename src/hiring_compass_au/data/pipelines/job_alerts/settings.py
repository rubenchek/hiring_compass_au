from __future__ import annotations

import json
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from hiring_compass_au.settings import WorkspacePaths, WorkspaceSettings


class JobAlertsSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HC_",
        env_file=(".env.local", ".env"),
        extra="ignore",
        enable_decoding=False,
    )

    root: Path = WorkspacePaths().root
    gmail_client_secret: Path = Path("secrets/google_client_secret.json")
    gmail_token: Path = Path("secrets/gmail_token.json")
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
        ws = WorkspaceSettings()
        if not self.gmail_client_secret.is_absolute():
            self.gmail_client_secret = (ws.root / self.gmail_client_secret).resolve()

        if not self.gmail_token.is_absolute():
            self.gmail_token = (ws.root / self.gmail_token).resolve()

        return self
