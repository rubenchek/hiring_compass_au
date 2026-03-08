from __future__ import annotations

import sqlite3
import time

import requests

from hiring_compass_au.infra.storage.schema import init_all_tables
from hiring_compass_au.services.job_alerts.enrichment import runner as canon_mod
from hiring_compass_au.services.job_alerts.enrichment.url_canonicalizer import (
    CanonicalizeError,
)
from hiring_compass_au.services.job_alerts.ingestion import mail_fetch as mail_fetch_mod
from hiring_compass_au.services.job_alerts.parsers import runner as mail_parse_mod
from hiring_compass_au.services.job_alerts.promote.runner import run_promote_job_ad


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    init_all_tables(conn)
    return conn


def test_mail_fetch_counters(monkeypatch):
    conn = _conn()
    now = "2026-01-01T00:00:00+00:00"
    conn.executemany(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) VALUES (?, ?, 'indexed', ?)",
        [("m1", "t1", now), ("m2", "t2", now), ("m3", "t3", now)],
    )
    conn.commit()

    def fake_extract(_message):
        return {
            "from_email": "jobmail@s.seek.com.au",
            "subject": "x",
            "internal_date_ms": 1,
            "html_raw": "<html></html>",
            "error": None,
        }

    def fake_load(_service, message_id):
        if message_id == "m2":
            raise RuntimeError("boom")
        return {"id": message_id}

    monkeypatch.setattr(mail_fetch_mod, "extract_message_fields", fake_extract)
    monkeypatch.setattr(mail_fetch_mod, "load_message", fake_load)

    to_fetch, ok, err, persisted = mail_fetch_mod.run_mail_fetch(
        service=object(), conn=conn, batch_size=50
    )
    assert (to_fetch, ok, err, persisted) == (3, 2, 1, 3)


def test_mail_parse_counters(monkeypatch):
    conn = _conn()
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at, html_raw, from_email) "
        "VALUES ('m1','t1','fetched','2026-01-01T00:00:00+00:00','<html/>','jobmail@s.seek.com.au')"
    )
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at, html_raw, from_email) "
        "VALUES ('m2','t2','fetched','2026-01-01T00:00:00+00:00','<html/>','unknown@x')"
    )
    conn.commit()

    def fake_parse_email(from_email, _html_raw):
        if from_email == "unknown@x":
            return None, None
        parser_cfg = {
            "source": "seek",
            "parser_name": "seek_mail_parser",
            "parser_version": "v1",
            "hits_expected": (1, 2),
        }
        hits = iter([{"out_url": "https://example.invalid/x", "hit_confidence": 90}])
        return hits, parser_cfg

    monkeypatch.setattr(mail_parse_mod, "parse_email", fake_parse_email)

    emails, hits_upserted, empty, error, unsupported, confidence = mail_parse_mod.run_mail_parse(
        conn
    )
    assert (emails, hits_upserted, empty, error, unsupported) == (2, 1, 0, 0, 1)
    assert confidence is not None


def test_url_canonicalize_counters(monkeypatch):
    conn = _conn()
    now = "2026-01-01T00:00:00+00:00"
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed',?)",
        (now,),
    )
    conn.executemany(
        "INSERT INTO email_job_hits(message_id, out_url, source, canonical_status) "
        "VALUES (?, ?, 'seek', 'pending')",
        [("m", "u1"), ("m", "u2"), ("m", "u3")],
    )
    conn.commit()

    def fake_resolve(_session, out_url, timeout=15.0):
        if out_url == "u1":
            return "123", "https://www.seek.com.au/job/123", 302
        if out_url == "u2":
            raise requests.Timeout("t")
        raise CanonicalizeError("nope", http_status=404)

    monkeypatch.setattr(canon_mod, "resolve_to_canonical", fake_resolve)
    monkeypatch.setattr(time, "sleep", lambda *_: None)

    total_start, ok, retry, err = canon_mod.run_url_canonicalization(
        conn, batch_size=50, timeout=1.0, max_batches=10, progress=False
    )
    assert (total_start, ok, retry, err) == (3, 1, 1, 1)


def test_promote_counters_new_vs_updated():
    conn = _conn()
    now = "2026-01-01T00:00:00+00:00"
    conn.execute(
        "INSERT INTO emails(message_id, thread_id, status, indexed_at) "
        "VALUES ('m','t','indexed',?)",
        (now,),
    )
    # job_ads existing (will make one hit count as "updated")
    conn.execute(
        "INSERT INTO job_ads(source, canonical_url, first_seen_at, last_seen_at) "
        "VALUES ('seek', 'c1', ?, ?)",
        (now, now),
    )
    # two hits to promote: one existing key, one new key
    conn.executemany(
        """
        INSERT INTO email_job_hits(
            message_id, out_url, source, promote_status, canonical_status, canonical_url
        ) 
        VALUES ('m', ?, 'seek', 'pending', 'ok', ?)
        """,
        [("o1", "c1"), ("o2", "c2")],
    )
    conn.commit()

    new, updated, failed = run_promote_job_ad(conn)
    assert (new, updated, failed) == (1, 1, 0)
