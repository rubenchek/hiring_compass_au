from __future__ import annotations

from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from hiring_compass_au.workspace import WorkspacePaths


class WorkspaceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HC_",
        env_file=(".env.local", ".env"),
        extra="ignore",
    )
    root: Path = WorkspacePaths().root
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