from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from hiring_compass_au.config.settings import WorkspaceSettings


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


@dataclass(frozen=True)
class WorkspacePaths:
    """Represents the canonical workspace layout for the project."""

    root: Path = _resolve_default_root()

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def db_path(self) -> Path:
        return self.data / "local" / "state.sqlite"

    @property
    def models(self) -> Path:
        return self.root / "models"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    def iter_dirs_minimal(self) -> Iterable[Path]:
        return (
            self.data,
            self.db_path.parent,
            self.logs,
        )

    def iter_dirs(self) -> Iterable[Path]:
        return (
            self.data,
            self.db_path.parent,
            self.models,
            self.reports,
            self.logs,
        )


def ensure_workspace(
    paths: WorkspacePaths | None = None, *, minimal: bool = False
) -> list[tuple[Path, bool]]:
    """
    Make sure the expected workspace directories exist.

    Returns a list of tuples: (path, created) for basic logging/diagnostics.
    """
    if paths is None:
        ws = WorkspaceSettings()
        paths = WorkspacePaths(root=ws.root)
        dirs = (
            paths.data,
            ws.db_path.parent,
            paths.models,
            paths.reports,
            ws.logs_dir,
        )
    elif minimal:
        dirs = paths.iter_dirs_minimal()
    else:
        dirs = paths.iter_dirs()

    created_state: list[tuple[Path, bool]] = []

    for path in dirs:
        existed_before = path.exists()
        path.mkdir(parents=True, exist_ok=True)
        created_state.append((path, not existed_before))

    return created_state


def format_created_state(state: list[tuple[Path, bool]], root: Path) -> str:
    lines = []
    root = root.resolve()
    for path, created in state:
        prefix = "created" if created else "ok"
        try:
            shown = path.resolve().relative_to(root)
        except ValueError:
            shown = path
        lines.append(f"{prefix:>7}  {shown}")
    return "\n".join(lines)
