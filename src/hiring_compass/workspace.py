from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# Base repository directory; used to derive standard folders.
ROOT_DIR = Path(__file__).resolve()
while not (ROOT_DIR / "pyproject.toml").exists():
    ROOT_DIR = ROOT_DIR.parent


@dataclass(frozen=True)
class WorkspacePaths:
    """Represents the canonical workspace layout for the project."""

    root: Path = ROOT_DIR
    data_raw: Path = ROOT_DIR / "data" / "raw"
    data_processed: Path = ROOT_DIR / "data" / "processed"
    models: Path = ROOT_DIR / "models"
    metrics: Path = ROOT_DIR / "metrics"
    notebooks: Path = ROOT_DIR / "notebooks"
    configs: Path = ROOT_DIR / "configs"
    logs: Path = ROOT_DIR / "logs"
    scripts: Path = ROOT_DIR / "scripts"
    docs: Path = ROOT_DIR / "docs"

    def iterable(self) -> Iterable[Path]:
        return (
            self.data_raw,
            self.data_processed,
            self.models,
            self.metrics,
            self.notebooks,
            self.configs,
            self.logs,
            self.scripts,
            self.docs,
        )


def ensure_workspace(paths: WorkspacePaths | None = None) -> list[tuple[Path, bool]]:
    """
    Make sure the expected workspace directories exist.

    Returns a list of tuples: (path, created) for basic logging/diagnostics.
    """
    paths = paths or WorkspacePaths()
    created_state: list[tuple[Path, bool]] = []

    for path in paths.iterable():
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
