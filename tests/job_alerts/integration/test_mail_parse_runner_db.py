from __future__ import annotations

import hiring_compass_au.services.job_alerts.parsers.runner as mod
from hiring_compass_au.services.job_alerts.parsers.runner import run_mail_parse


def test_run_mail_parse_persists_hits_and_updates_email_status(conn, monkeypatch):
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at, html_raw, from_email) "
        "VALUES ('m1','t1','fetched','x','<html/>','jobmail@s.seek.com.au')"
    )

    def fake_parse_email(_from_email, _html_raw):
        parser_cfg = {"source": "seek", "parser_name": "seek_mail_parser", "parser_version": "v1"}
        hits = iter([{"out_url": "u1", "hit_confidence": 90}])
        return hits, parser_cfg

    monkeypatch.setattr(mod, "parse_email", fake_parse_email)

    emails, hits_upserted, empty, error, unsupported, confidence = run_mail_parse(conn)
    assert (emails, hits_upserted, empty, error, unsupported) == (1, 1, 0, 0, 0)
    assert confidence is not None
    assert 0.0 <= confidence <= 100.0

    email = conn.execute(
        "SELECT status, hit_extract_count FROM emails WHERE message_id='m1'"
    ).fetchone()
    assert email["status"] == "parsed"
    assert email["hit_extract_count"] == 1

    hit = conn.execute("SELECT canonical_status, promote_status FROM email_job_hits").fetchone()
    assert hit["canonical_status"] == "pending"
    assert hit["promote_status"] in {"new", "pending"}
