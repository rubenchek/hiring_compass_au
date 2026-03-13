from __future__ import annotations

import json
import sys

import hiring_compass_au.services.job_enrichment.__main__ as main_mod
from hiring_compass_au.services.job_enrichment.models import (
    BatchSummary,
    RetryableEnrichmentError,
    TerminalEnrichmentError,
)


def _read_last_jsonl(path):
    lines = path.read_text(encoding="utf-8").splitlines()
    return json.loads(lines[-1])


def test_main_exit_0_and_writes_jsonl(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(sys, "argv", ["prog", "--limit", "1", "--max-batches", "1"])
    monkeypatch.setattr(main_mod, "run_enrichment", lambda *a, **k: BatchSummary())

    exit_code = main_mod.main()
    assert exit_code == 0

    jsonl = tmp_path / "logs" / "enrichment_runs.jsonl"
    last = _read_last_jsonl(jsonl)
    assert last["exit_code"] == 0
    assert last["summary"] is not None


def test_main_exit_75_on_retryable_error(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(sys, "argv", ["prog", "--limit", "1", "--max-batches", "1"])

    def boom(*_a, **_k):
        raise RetryableEnrichmentError("t")

    monkeypatch.setattr(main_mod, "run_enrichment", boom)

    exit_code = main_mod.main()
    assert exit_code == 75

    jsonl = tmp_path / "logs" / "enrichment_runs.jsonl"
    last = _read_last_jsonl(jsonl)
    assert last["exit_code"] == 75


def test_main_exit_1_on_terminal_error(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(sys, "argv", ["prog", "--limit", "1", "--max-batches", "1"])

    def boom(*_a, **_k):
        raise TerminalEnrichmentError("nope")

    monkeypatch.setattr(main_mod, "run_enrichment", boom)

    exit_code = main_mod.main()
    assert exit_code == 1

    jsonl = tmp_path / "logs" / "enrichment_runs.jsonl"
    last = _read_last_jsonl(jsonl)
    assert last["exit_code"] == 1
