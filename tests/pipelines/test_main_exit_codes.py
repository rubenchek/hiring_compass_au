from __future__ import annotations

import json
import sys

import requests

import hiring_compass_au.data.pipelines.job_alerts.__main__ as main_mod


def _read_last_jsonl(path):
    lines = path.read_text(encoding="utf-8").splitlines()
    return json.loads(lines[-1])


def test_main_exit_0_and_writes_jsonl(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(sys, "argv", ["prog", "--no-index", "--no-fetch"])
    monkeypatch.setattr(main_mod, "run_job_alert_pipeline", lambda *a, **k: {"ok": True})

    exit_code = main_mod.main()
    assert exit_code == 0

    jsonl = tmp_path / "logs" / "pipeline_runs.jsonl"
    last = _read_last_jsonl(jsonl)
    assert last["exit_code"] == 0
    assert last["results"] == {"ok": True}


def test_main_exit_75_on_timeout(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(sys, "argv", ["prog", "--no-index", "--no-fetch"])

    def boom(*_a, **_k):
        raise requests.Timeout("t")

    monkeypatch.setattr(main_mod, "run_job_alert_pipeline", boom)

    exit_code = main_mod.main()
    assert exit_code == 75

    jsonl = tmp_path / "logs" / "pipeline_runs.jsonl"
    last = _read_last_jsonl(jsonl)
    assert last["exit_code"] == 75
    assert last["results"] is None


def test_main_exit_1_on_non_network_error(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(sys, "argv", ["prog", "--no-index", "--no-fetch"])

    def boom(*_a, **_k):
        raise ValueError("nope")

    monkeypatch.setattr(main_mod, "run_job_alert_pipeline", boom)

    exit_code = main_mod.main()
    assert exit_code == 1

    jsonl = tmp_path / "logs" / "pipeline_runs.jsonl"
    last = _read_last_jsonl(jsonl)
    assert last["exit_code"] == 1
    assert last["results"] is None
