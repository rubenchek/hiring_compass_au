"""
Initialize the local workspace structure (directories, placeholders).

Usage:
    python scripts/bootstrap_workspace.py
"""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    # Allow running the script before installing the package in editable mode.
    sys.path.insert(0, str(SRC_DIR))

from hiring_compass.workspace import WorkspacePaths, ensure_workspace, format_created_state  # noqa: E402


def main() -> None:
    state = ensure_workspace(WorkspacePaths())
    print("Workspace ready:")
    print(format_created_state(state))


if __name__ == "__main__":
    main()
