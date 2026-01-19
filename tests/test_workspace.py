from __future__ import annotations

from hiring_compass_au.workspace import WorkspacePaths, ensure_workspace

def make_tmp_workspace(tmp_path):
    root = tmp_path
    return WorkspacePaths(
        root=root,
        data_raw=root / "data" / "raw",
        data_processed=root / "data" / "processed",
        models=root / "models",
        reports=root / "reports",
        logs=root / "logs",
    )
    
def test_ensure_workspace_creates_then_is_idempotent(tmp_path):
    ws = make_tmp_workspace(tmp_path)

    # 1) First run: directories should be created
    state1 = ensure_workspace(ws)
    assert all(path.exists() for path, _ in state1)
    assert any(created for _, created in state1)

    # 2) Second run: nothing new should be created
    state2 = ensure_workspace(ws)
    assert all(path.exists() for path, _ in state2)
    assert all(created is False for _, created in state2)