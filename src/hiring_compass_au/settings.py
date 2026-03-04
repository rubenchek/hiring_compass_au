from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_default_root() -> Path:
    env_root = os.environ.get("HC_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    # try: repo root by walking up from CWD (works in local dev)
    root = Path.cwd().resolve()
    cur = root
    while True:
        if (cur / "pyproject.toml").exists():
            return cur
        if cur == cur.parent:
            break
        cur = cur.parent

    # fallback: CWD (works in Docker where WORKDIR=/app)
    return root


class WorkspaceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HC_",
        env_file=(".env.local", ".env"),
        extra="ignore",
    )
    root: Path = Field(default_factory=_resolve_default_root)
    db_path: Path = Path("data/local/state.sqlite")
    logs_dir: Path = Path("logs")

    @model_validator(mode="after")
    def _resolve_relative_paths(self) -> WorkspaceSettings:
        if not self.root.is_absolute():
            self.root = self.root.resolve()

        if not self.db_path.is_absolute():
            self.db_path = (self.root / self.db_path).resolve()

        if not self.logs_dir.is_absolute():
            self.logs_dir = (self.root / self.logs_dir).resolve()

        return self
