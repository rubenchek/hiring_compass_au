from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

from hiring_compass_au.settings import WorkspaceSettings


def _get_env(name: str) -> str | None:
    v = os.environ.get(name)
    if v is None:
        return None
    v = v.strip()
    return v or None


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


def _read_last_jsonl(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"JSONL not found: {path}")
    last = None
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                last = line
    if last is None:
        raise ValueError(f"JSONL is empty: {path}")
    try:
        return json.loads(last)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSONL last line in {path}: {e}") from e


def _parse_iso_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _db_counts(db_path: str | None, *, fallback_db_path: Path) -> dict[str, int | None]:
    # Prefer db_path from JSONL if it exists; otherwise fallback to workspace db
    if (not db_path) or (not Path(db_path).exists()):
        db_path = str(fallback_db_path)

    p = Path(db_path)
    if not p.exists():
        return {"job_ads_total": None, "job_ad_enrichment_pending": None}

    try:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=2.0)
    except Exception:
        return {"job_ads_total": None, "job_ad_enrichment_pending": None}

    try:
        cur = conn.cursor()

        try:
            job_ads_total = int(cur.execute("SELECT COUNT(*) FROM job_ads;").fetchone()[0])
        except Exception:
            job_ads_total = None

        try:
            job_ad_enrichment_pending = int(
                cur.execute(
                    "SELECT COUNT(DISTINCT job_id) "
                    "FROM job_ad_enrichment "
                    "WHERE enrich_status='pending';"
                ).fetchone()[0]
            )
        except Exception:
            job_ad_enrichment_pending = None

        return {
            "job_ads_total": job_ads_total,
            "job_ad_enrichment_pending": job_ad_enrichment_pending,
        }
    finally:
        conn.close()


def _fmt_int(v: int | None) -> str:
    return "n/a" if v is None else str(v)


def _build_message(
    summary: dict, *, exit_code: int | None, attempts: int | None, fallback_db_path: Path
) -> str:
    def _mdv2_escape(s: str) -> str:
        for ch in r"_*[]()~`>#+-=|{}.!":
            s = s.replace(ch, "\\" + ch)
        return s

    def _esc(v) -> str:
        return _mdv2_escape("n/a" if v is None else str(v))

    started_at = summary.get("started_at")
    finished_at = summary.get("finished_at")
    db_path = summary.get("db_path")
    results = summary.get("results") or {}

    effective_exit = int(exit_code) if exit_code is not None else int(summary.get("exit_code", 1))
    if effective_exit == 0:
        status_emoji = "✅"
        status_text = "run ok"
    elif effective_exit == 75:
        status_emoji = "🟡"
        status_text = "tempfail, retry prévu"
    else:
        status_emoji = "❌"
        status_text = "run failed"

    t0 = _parse_iso_dt(started_at)
    t1 = _parse_iso_dt(finished_at)
    duration_s = int((t1 - t0).total_seconds()) if (t0 and t1) else None

    counts = _db_counts(db_path, fallback_db_path=fallback_db_path)

    index = results.get("index") or {}
    index_found = index.get("found")
    index_inserted = index.get("inserted")

    parse = results.get("parse") or {}
    parse_emails = parse.get("emails")
    parse_hits = parse.get("hits_upserted")
    parse_conf = parse.get("confidence_mean")
    parse_unsupported = parse.get("unsupported")
    parse_error = parse.get("error")

    canon = results.get("canonicalize") or {}
    canon_total = canon.get("total_start")
    canon_ok = canon.get("ok")
    canon_retry = canon.get("retry")
    canon_error = canon.get("error")

    promote = results.get("promote") or {}
    promote_new = promote.get("new")
    promote_updated = promote.get("updated")
    promote_failed = promote.get("failed")

    failed_stage = results.get("failed_stage")
    stage_error = results.get("stage_error") or {}
    err_type = summary.get("error_type") or stage_error.get("type")
    err_msg = summary.get("error_message") or stage_error.get("message")

    if parse_conf is None:
        conf_str = "n/a"
    else:
        try:
            conf_str = f"{float(parse_conf):.1f}%"
        except Exception:
            conf_str = str(parse_conf)

    meta_bits: list[str] = []
    if duration_s is not None:
        meta_bits.append(f"⏱️ {_esc(duration_s)}s")
    if attempts is not None and attempts > 1:
        meta_bits.append(f"🔁 {_esc(attempts)} tentatives")

    lines: list[str] = []
    lines.append(f"{status_emoji} *HiringCompassAU job alerts* — {_esc(status_text)}")
    if meta_bits:
        lines.append(" · ".join(meta_bits))

    lines.append("")
    lines.append("*📊 Aujourd’hui*")
    lines.append(
        f"• 📥 Gmail: \\+{_esc(_fmt_int(index_inserted))} nouveaux emails, "
        f"vus {_esc(_fmt_int(index_found))}"
    )
    lines.append(
        f"• 🧩 Parsing: {_esc(_fmt_int(parse_emails))} emails, {_esc(_fmt_int(parse_hits))} hits, "
        f"conf {_esc(conf_str)}, unsupported {_esc(_fmt_int(parse_unsupported))}, "
        f"errors {_esc(_fmt_int(parse_error))}"
    )
    lines.append(
        f"• 🔗 Canonicalisation: {_esc(_fmt_int(canon_ok))}/{_esc(_fmt_int(canon_total))} ok, "
        f"retry {_esc(_fmt_int(canon_retry))}, errors {_esc(_fmt_int(canon_error))}"
    )
    lines.append(
        f"• 🚀 Promotion: \\+{_esc(_fmt_int(promote_new))} nouvelles, "
        f"{_esc(_fmt_int(promote_updated))} updates,"
        f"failed {_esc(_fmt_int(promote_failed))}"
    )

    lines.append("")
    lines.append("*🗄️ Base de données*")
    lines.append(f"• 📦 Annonces uniques: {_esc(_fmt_int(counts.get('job_ads_total')))}")
    lines.append(
        "• ⛏️ Enrichment en attente, jobs: "
        f"{_esc(_fmt_int(counts.get('job_ad_enrichment_pending')))}"
    )

    if effective_exit != 0:
        lines.append("")
        lines.append("*🧯 Détails erreur*")
        if failed_stage:
            lines.append(f"• étape: {_esc(failed_stage)}")
        if err_type:
            lines.append(f"• type: {_esc(err_type)}")
        if err_msg:
            msg = str(err_msg).strip().replace("\n", " ")
            if len(msg) > 350:
                msg = msg[:350] + "…"
            lines.append(f"• message: {_esc(msg)}")

    return "\n".join(lines)


def _send_telegram(text: str, *, bot_token: str, chat_id: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=10)
    data = r.json()
    if not r.ok or not data.get("ok", False):
        raise RuntimeError(f"Telegram sendMessage failed: status={r.status_code} resp={data}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl_path", type=Path, default=None)
    ap.add_argument("--exit-code", type=int, default=None)
    ap.add_argument("--attempts", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ws = WorkspaceSettings()
    jsonl_path = args.jsonl_path or (ws.logs_dir / "pipeline_runs.jsonl")

    secrets_dir = Path(_get_env("HC_SECRETS_DIR") or "secrets")
    _load_env_file(secrets_dir / "telegram.env")

    bot_token = _get_env("HC_TELEGRAM_BOT_TOKEN")
    chat_id = _get_env("HC_TELEGRAM_CHAT_ID")

    if not args.dry_run:
        if not bot_token:
            raise RuntimeError("Missing env HC_TELEGRAM_BOT_TOKEN")
        if not chat_id:
            raise RuntimeError("Missing env HC_TELEGRAM_CHAT_ID")

    summary = _read_last_jsonl(jsonl_path)

    # Prefer CLI, else allow env injected by wrapper
    effective_exit = args.exit_code
    if effective_exit is None:
        env_exit = _get_env("HC_EXIT_CODE")
        effective_exit = int(env_exit) if env_exit is not None else None

    effective_attempts = args.attempts
    if effective_attempts is None:
        env_attempts = _get_env("HC_ATTEMPTS")
        effective_attempts = int(env_attempts) if env_attempts is not None else None

    text = _build_message(
        summary,
        exit_code=effective_exit,
        attempts=effective_attempts,
        fallback_db_path=ws.db_path,
    )

    if args.dry_run:
        print(text)
        return 0

    _send_telegram(text, bot_token=bot_token, chat_id=chat_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
