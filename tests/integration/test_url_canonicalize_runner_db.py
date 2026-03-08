from __future__ import annotations

import time

import requests

import hiring_compass_au.services.job_alerts.enrichment.runner as mod
from hiring_compass_au.services.job_alerts.enrichment.runner import (
    run_url_canonicalization,
)
from hiring_compass_au.services.job_alerts.enrichment.url_canonicalizer import (
    CanonicalizeError,
)


def test_run_url_canonicalization_updates_db_statuses(conn, monkeypatch):
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed','x')"
    )
    conn.executemany(
        "INSERT INTO email_job_hits(message_id, out_url, source) VALUES ('m', ?, 'seek')",
        [("u1",), ("u2",), ("u3",)],
    )

    def fake_resolve(_session, out_url, timeout=15.0):
        if out_url == "u1":
            return "123", "https://www.seek.com.au/job/123", 302
        if out_url == "u2":
            raise requests.Timeout("t")
        raise CanonicalizeError("nope", http_status=404)

    monkeypatch.setattr(mod, "resolve_to_canonical", fake_resolve)
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    total, ok, retry, err = run_url_canonicalization(
        conn, batch_size=50, timeout=1.0, max_batches=10, progress=False
    )
    assert (total, ok, retry, err) == (3, 1, 1, 1)

    rows = conn.execute(
        "SELECT out_url, canonical_status FROM email_job_hits ORDER BY out_url"
    ).fetchall()
    statuses = {r["out_url"]: r["canonical_status"] for r in rows}
    assert statuses == {"u1": "ok", "u2": "retry", "u3": "error"}
