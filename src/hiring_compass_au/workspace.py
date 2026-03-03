from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

# Base repository directory; used to derive standard folders.
ROOT_DIR = Path(__file__).resolve()
while not (ROOT_DIR / "pyproject.toml").exists():
    ROOT_DIR = ROOT_DIR.parent
    if ROOT_DIR == ROOT_DIR.parent:
        raise RuntimeError("pyproject.toml not found")


@dataclass(frozen=True)
class WorkspacePaths:
    """Represents the canonical workspace layout for the project."""

    root: Path = ROOT_DIR

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

    def iter_dirs(self) -> Iterable[Path]:
        return (
            self.data,
            self.db_path.parent,
            self.models,
            self.reports,
            self.logs,
        )


def ensure_workspace(paths: WorkspacePaths | None = None) -> list[tuple[Path, bool]]:
    """
    Make sure the expected workspace directories exist.

    Returns a list of tuples: (path, created) for basic logging/diagnostics.
    """
    paths = paths or WorkspacePaths()
    created_state: list[tuple[Path, bool]] = []

    for path in paths.iter_dirs():
        existed_before = path.exists()
        path.mkdir(parents=True, exist_ok=True)
        created_state.append((path, not existed_before))

    return created_state


def format_created_state(state: list[tuple[Path, bool]]) -> str:
    """Pretty-print the ensure_workspace result."""
    lines = []
    for path, created in state:
        prefix = "created" if created else "ok"
        lines.append(f"{prefix:>7}  {path.relative_to(ROOT_DIR)}")
    return "\n".join(lines)
