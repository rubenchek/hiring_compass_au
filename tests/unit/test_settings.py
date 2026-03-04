from __future__ import annotations

from hiring_compass_au.data.pipelines.job_alerts.settings import JobAlertsSettings
from hiring_compass_au.settings import WorkspaceSettings


def test_workspace_root_is_absolute():
    ws = WorkspaceSettings()
    assert ws.root.is_absolute()


def test_workspace_settings_defaults_are_absolute_and_under_root(monkeypatch):
    monkeypatch.delenv("HC_ROOT", raising=False)

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

    assert cfg.gmail_token_path == (cfg.repo_root / "secrets/gmail_token.json").resolve()
    assert (
        cfg.gmail_client_secret_path
        == (cfg.repo_root / "secrets/google_client_secret.json").resolve()
    )
    assert cfg.senders == ["a@x", "b@y"]
