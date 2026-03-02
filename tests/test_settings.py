from __future__ import annotations

from hiring_compass_au.settings import WorkspaceSettings
from hiring_compass_au.workspace import WorkspacePaths

from hiring_compass_au.data.pipelines.job_alerts.settings import JobAlertsSettings


def test_workspace_root_autodetected_contains_pyproject():
    ws = WorkspaceSettings()
    assert (ws.root / "pyproject.toml").exists()
    assert ws.root == WorkspacePaths().root


def test_workspace_settings_defaults_are_absolute_and_under_root():
    ws = WorkspaceSettings()
    assert ws.root.is_absolute()
    assert ws.db_path.is_absolute()
    assert ws.logs_dir.is_absolute()
    assert str(ws.db_path).startswith(str(ws.root))


def test_workspace_settings_env_overrides_relative_db_path(monkeypatch, tmp_path):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/test.sqlite")

    ws = WorkspaceSettings()
    assert ws.root == tmp_path
    assert ws.db_path == (tmp_path / "data/local/test.sqlite").resolve()


def test_job_alerts_settings_resolves_secrets_under_root_and_parses_senders(monkeypatch, tmp_path):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_SENDERS", "a@x,b@y")

    ws = WorkspaceSettings()
    cfg = JobAlertsSettings(root=ws.root)

    assert cfg.gmail_token == (tmp_path / "secrets/gmail_token.json").resolve()
    assert cfg.gmail_client_secret == (tmp_path / "secrets/google_client_secret.json").resolve()
    assert cfg.senders == ["a@x", "b@y"]
