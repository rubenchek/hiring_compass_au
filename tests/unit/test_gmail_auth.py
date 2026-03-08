from __future__ import annotations

from types import SimpleNamespace

import pytest

import hiring_compass_au.services.job_alerts.ingestion.auth_and_build as auth_mod


def test_authenticate_and_build_service_forwards_oauth_params(monkeypatch, tmp_path):
    secret = tmp_path / "client_secret.json"
    secret.write_text("{}", encoding="utf-8")
    token = tmp_path / "token.json"

    seen = {}

    def fake_authenticate_gmail(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace()  # dummy creds

    monkeypatch.setattr(auth_mod, "authenticate_gmail", fake_authenticate_gmail)
    monkeypatch.setattr(auth_mod, "build_gmail_service", lambda _creds: "SERVICE")

    service = auth_mod.authenticate_and_build_service(
        client_secret_path=secret,
        token_path=token,
        oauth_host="0.0.0.0",
        oauth_port=8080,
        oauth_open_browser=False,
    )

    assert service == "SERVICE"
    assert seen["client_secret_path"] == secret
    assert seen["token_path"] == token
    assert seen["oauth_host"] == "0.0.0.0"
    assert seen["oauth_port"] == 8080
    assert seen["oauth_open_browser"] is False


def test_authenticate_gmail_rejects_port_0_when_listening_on_all_interfaces(tmp_path):
    secret = tmp_path / "client_secret.json"
    secret.write_text("{}", encoding="utf-8")
    token = tmp_path / "token.json"

    with pytest.raises(ValueError):
        auth_mod.authenticate_gmail(
            client_secret_path=secret,
            token_path=token,
            oauth_host="0.0.0.0",
            oauth_port=0,
            oauth_open_browser=False,
        )
