from __future__ import annotations

from pathlib import Path

from hiring_compass_au.services.job_alerts.parsers.seek_mail_parser import parse_seek_email


def test_parse_seek_email_from_fixture_extracts_one_hit():
    fixtures_dir = Path(__file__).resolve().parents[1] / "fixtures"
    html = (fixtures_dir / "seek_alert_minimal.html").read_text(encoding="utf-8")

    hits = list(parse_seek_email(html))
    assert len(hits) == 1

    hit = hits[0]

    assert hit["out_url"].startswith("https://email.s.seek.com.au/uni/ss/c/")
    assert hit["title"] == "Data Engineer"
    assert hit["company"] == "Acme Pty Ltd"
    assert hit["location_raw"] == "Sydney NSW"
    assert isinstance(hit["hit_confidence"], int)
    assert 0 <= hit["hit_confidence"] <= 100
