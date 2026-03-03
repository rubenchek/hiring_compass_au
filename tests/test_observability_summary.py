from __future__ import annotations

import sqlite3
from pathlib import Path
import json
import sys


from hiring_compass_au.data.storage.schema import init_all_tables
import hiring_compass_au.data.pipelines.job_alerts.job_alerts_pipeline as pipeline_mod
import hiring_compass_au.data.pipelines.job_alerts.__main__ as main_mod


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    init_all_tables(conn)
    return conn


def test_run_job_alert_pipeline_returns_expected_shape_without_gmail(monkeypatch):
    conn = _conn()

    monkeypatch.setattr(pipeline_mod, "run_mail_parse", lambda conn: (2, 7, 1, 0, 0))
    monkeypatch.setattr(pipeline_mod, "run_url_canonicalization", lambda **_: (3, 1, 1, 1))
    monkeypatch.setattr(pipeline_mod, "run_promote_job_ad", lambda conn: (1, 1, 0))

    results = pipeline_mod.run_job_alert_pipeline(
        conn,
        client_secret_path=Path("ignored"),
        token_path=Path("ignored"),
        index=False,
        fetch=False,
        parse=True,
        canonicalize=True,
        promote=True,
        senders=["jobmail@s.seek.com.au"],
        fetch_batch_size=50,
        canon_batch_size=200,
        canon_timeout_s=1.0,
        canon_max_batches=1,
        progress=False,
    )

    assert isinstance(results, dict)
    assert results["index"] is None
    assert results["fetch"] is None

    assert results["parse"] == {
        "emails": 2,
        "hits_upserted": 7,
        "empty": 1,
        "error": 0,
        "unsupported": 0,
    }
    assert results["canonicalize"] == {"total_start": 3, "ok": 1, "retry": 1, "error": 1}
    assert results["promote"] == {"new": 1, "updated": 1, "failed": 0}

    # Durations: on vérifie juste présence + type
    for k in ("parse", "canonicalize", "promote"):
        assert k in results["durations_s"]
        assert isinstance(results["durations_s"][k], float)
        assert results["durations_s"][k] >= 0.0


def test_main_appends_fourth_jsonl_run(tmp_path, monkeypatch):
    monkeypatch.setenv("HC_ROOT", str(tmp_path))
    monkeypatch.setenv("HC_DB_PATH", "data/local/state.sqlite")
    monkeypatch.setenv("HC_LOGS_DIR", "logs")

    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--no-index", "--no-fetch", "--no-parse", "--no-canonicalize", "--no-promote"],
    )

    fake_results = {"durations_s": {"parse": 0.1}, "parse": {"emails": 2}}
    monkeypatch.setattr(main_mod, "run_job_alert_pipeline", lambda *a, **k: fake_results)

    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = logs_dir / "pipeline_runs.jsonl"

    existing = [
        {"event": "pipeline_summary", "run_id": "r1", "exit_code": 0, "results": {"x": 1}},
        {"event": "pipeline_summary", "run_id": "r2", "exit_code": 1, "results": None},
        {"event": "pipeline_summary", "run_id": "r3", "exit_code": 0, "results": {"y": 2}},
    ]
    jsonl_path.write_text("\n".join(json.dumps(x) for x in existing) + "\n", encoding="utf-8")

    exit_code = main_mod.main()
    assert exit_code == 0

    lines = jsonl_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4

    last = json.loads(lines[-1])
    assert last["event"] == "pipeline_summary"
    assert last["exit_code"] == 0
    assert last["results"] == fake_results
    assert last["run_id"] not in {"r1", "r2", "r3"}
