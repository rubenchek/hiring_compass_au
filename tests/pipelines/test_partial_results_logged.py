from __future__ import annotations

import json
import sys

import hiring_compass_au.data.pipelines.job_alerts.__main__ as main_mod


def test_main_logs_partial_results_when_exception_has_hc_results(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(sys, "argv", ["prog", "--no-index", "--no-fetch"])

    partial = {
        "parse": {"emails": 10},
        "canonicalize": {"total_start": 10, "ok": 3, "retry": 7, "error": 0},
    }

    def boom(*_a, **_k):
        e = RuntimeError("canonicalize crashed")
        e.hc_results = partial
        raise e

    monkeypatch.setattr(main_mod, "run_job_alert_pipeline", boom)

    exit_code = main_mod.main()
    assert exit_code == 1  # non-network

    jsonl = tmp_path / "logs" / "pipeline_runs.jsonl"
    last = json.loads(jsonl.read_text(encoding="utf-8").splitlines()[-1])
    assert last["exit_code"] == 1
    assert last["results"] == partial
    assert last["error_type"] == "RuntimeError"
