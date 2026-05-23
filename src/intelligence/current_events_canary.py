"""Current Events canary.

Maintains a compact current-events surface from official scheduled catalysts,
released macro actuals and high-impact unscheduled news. The canary queues
Claude/research-note review through data_change_events, but never rewrites
predictions directly.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.core.changes import enqueue_change_event
from src.core.db import connect_writable, table_exists
from src.fetchers import gdelt

CURRENT_EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS current_events (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key              TEXT NOT NULL,
    event_type             TEXT NOT NULL,
    title                  TEXT NOT NULL,
    summary                TEXT,
    event_time             TEXT NOT NULL,
    region                 TEXT,
    category               TEXT,
    priority               REAL NOT NULL DEFAULT 1.0,
    status                 TEXT NOT NULL DEFAULT 'active',
    object_id              TEXT NOT NULL,
    source_table           TEXT NOT NULL,
    source_id              TEXT,
    labels_json            TEXT,
    affected_assets_json   TEXT,
    display_title          TEXT,
    display_summary        TEXT,
    why_text               TEXT,
    source_quality         TEXT,
    metadata_json          TEXT,
    oracle_review_required INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at             TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at             TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_current_events_key
    ON current_events(event_key);
CREATE INDEX IF NOT EXISTS idx_current_events_time
    ON current_events(status, event_time, priority);
CREATE INDEX IF NOT EXISTS idx_current_events_type
    ON current_events(event_type, status, priority);

CREATE TABLE IF NOT EXISTS gdelt_canary_files (
    file_stamp       TEXT PRIMARY KEY,
    url              TEXT NOT NULL,
    status           TEXT NOT NULL,
    rows_seen        INTEGER NOT NULL DEFAULT 0,
    error_message    TEXT,
    fetched_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

MARKET_MOVING_STREAMS = {
    "policy_rates", "conflict_security", "trade_sanctions_supply",
    "energy_commodities", "market_stress",
}
MACRO_NEWS_STREAMS = {"economy_news", "policy_rates"}
REVIEW_PRIORITY = 4.6
BREAKING_THRESHOLD = 3.5
GDELT_EVENT_TOTAL_CAP = 10
GDELT_EVENT_PER_STREAM_CAP = 2
GDELT_EXISTING_EVENT_CAP = GDELT_EVENT_TOTAL_CAP
GDELT_INCREMENTAL_EVENT_CAP = GDELT_EVENT_TOTAL_CAP

RELEASE_FAMILY_MAP = {
    "us_cpi": "cpi", "se_cpi_cpif": "cpif", "gb_cpi": "cpi",
    "eu_hicp": "hicp", "us_ppi": "ppi", "us_pce": "pce",
    "us_payrolls": "payrolls", "us_unemployment": "unemployment",
    "us_jolts": "jolts", "us_gdp": "gdp", "us_eia_oil": "eia",
    "fomc_rate": "fomc", "ecb_policy_rate": "ecb",
    "riksbank_policy_rate": "riksbank", "boe_policy_rate": "boe",
    "boj_policy_rate": "boj",
}

NEWS_RULES: tuple[tuple[str, float, tuple[str, ...], tuple[str, ...]], ...] = (
    (r"\bsummit\b", 1.00, ("theme:political", "theme:market_moving_news"), ("SPY", "GC=F")),
    (r"\btrump\b", 0.85, ("theme:political", "theme:market_moving_news"), ("SPY", "GC=F")),
    (r"\bxi\b|\bchina\b", 0.75, ("theme:political", "theme:trade"), ("SPY", "MCHI", "GC=F")),
    (r"\btariff\b|\btariffs\b|trade war", 1.20, ("theme:trade", "theme:market_moving_news"), ("SPY", "MCHI", "DX-Y.NYB")),
    (r"\bsanction\b|\bsanctions\b", 1.15, ("theme:sanctions", "theme:market_moving_news"), ("SPY", "GC=F", "CL=F")),
    (r"\bwar\b|\bconflict\b|\bmissile\b|\binvasion\b", 1.20, ("theme:conflict", "theme:market_moving_news"), ("SPY", "GC=F", "CL=F")),
    (r"oil shock|\bcrude\b|\boil\b|\bopec\b", 1.05, ("theme:energy", "theme:oil_price", "theme:market_moving_news", "asset_impact:oil"), ("CL=F", "BZ=F", "XLE")),
    (r"central bank|rate decision|\bfed\b|federal reserve|\becb\b|riksbank|bank of england|\bboj\b", 1.05, ("theme:central_bank", "theme:rates", "theme:macro_news", "theme:market_moving_news"), ("TLT", "GC=F", "SPY", "DX-Y.NYB")),
    (r"\bdefault\b|banking stress|bank failure|credit stress|liquidity crisis", 1.45, ("theme:banking", "theme:market_moving_news"), ("SPY", "XLF", "TLT", "GC=F")),
    (r"\bgdp\b|payrolls|unemployment|\bcpi\b|\bpce\b|\bppi\b|\bpmi\b", 0.75, ("theme:macro_news",), ("SPY", "TLT", "GC=F")),
)

STREAM_DISPLAY = {
    "economy_news": "Economy news",
    "policy_rates": "Rates and central banks",
    "major_disaster": "Disaster risk",
    "political_risk": "Political risk",
    "conflict_security": "War and security risk",
    "trade_sanctions_supply": "Trade friction",
    "energy_commodities": "Oil and energy",
    "market_stress": "Market stress",
}

ASSET_DISPLAY = {
    "SPY": "S&P 500",
    "GC=F": "Gold",
    "GLD": "Gold",
    "CL=F": "WTI crude",
    "BZ=F": "Brent crude",
    "XLE": "Energy stocks",
    "XLF": "Banks",
    "TLT": "long bonds",
    "DX-Y.NYB": "US dollar",
    "^VIX": "volatility",
    "MCHI": "China equities",
}


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(CURRENT_EVENTS_SCHEMA)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(current_events)")}
    for name, sql_type in (
        ("display_title", "TEXT"),
        ("display_summary", "TEXT"),
        ("why_text", "TEXT"),
        ("source_quality", "TEXT"),
    ):
        if name not in existing:
            conn.execute(f"ALTER TABLE current_events ADD COLUMN {name} {sql_type}")


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, default=str)


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    if isinstance(parsed, list):
        return [str(v) for v in parsed]
    return []


def _stable_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:18]


def _dt(value: Any) -> dt.datetime | None:
    if not value:
        return None
    raw = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        try:
            parsed = dt.datetime.fromisoformat(raw[:10] + "T00:00:00+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _iso(value: dt.datetime) -> str:
    return value.astimezone(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _event_time(date_value: Any, scheduled_at_utc: Any = None, updated_at: Any = None) -> str:
    for candidate in (scheduled_at_utc, updated_at):
        parsed = _dt(candidate)
        if parsed:
            return _iso(parsed)
    raw = str(date_value or dt.date.today().isoformat())[:10]
    return f"{raw}T00:00:00Z"


def _expires(event_time: str, days: float) -> str:
    base = _dt(event_time) or dt.datetime.now(dt.UTC)
    return _iso(base + dt.timedelta(days=days))


def _release_family(release_key: str | None) -> str | None:
    stem = str(release_key or "").split(":", 1)[0]
    return RELEASE_FAMILY_MAP.get(stem, stem.split("_", 1)[-1] if stem else None)


def _labels_for_release(category: str | None, release_key: str | None) -> list[str]:
    labels: list[str] = []
    if category:
        labels.append(f"theme:{category}")
    family = _release_family(release_key)
    if family:
        labels.append(f"release_family:{family}")
    return sorted(set(labels))


def _assets_for_labels(labels: list[str], text: str = "") -> list[str]:
    assets: set[str] = set()
    lower = text.lower()
    mapping = {
        "asset_impact:sp500": ("SPY",),
        "asset_impact:gold": ("GC=F",),
        "asset_impact:oil": ("CL=F", "BZ=F"),
        "theme:rates": ("TLT", "GC=F", "DX-Y.NYB"),
        "theme:central_bank": ("TLT", "GC=F", "SPY", "DX-Y.NYB"),
        "theme:inflation": ("TLT", "GC=F", "SPY"),
        "theme:energy": ("CL=F", "BZ=F", "XLE"),
        "theme:oil_price": ("CL=F", "BZ=F", "XLE"),
        "theme:trade": ("SPY", "MCHI", "DX-Y.NYB"),
        "theme:conflict": ("SPY", "GC=F", "CL=F"),
        "theme:sanctions": ("SPY", "GC=F", "CL=F"),
    }
    for label in labels:
        assets.update(mapping.get(label, ()))
    if "oil" in lower or "crude" in lower:
        assets.update(("CL=F", "BZ=F"))
    if "gold" in lower:
        assets.add("GC=F")
    if "s&p" in lower or "stock" in lower or "equity" in lower:
        assets.add("SPY")
    return sorted(assets)


def _stream_display(stream: Any) -> str:
    key = str(stream or "").strip().lower().replace(" ", "_")
    return STREAM_DISPLAY.get(key, key.replace("_", " ").title() if key else "News")


def _scope_display(scope: Any) -> str:
    raw = str(scope or "Global").strip() or "Global"
    return raw.upper() if len(raw) <= 3 else raw


def _asset_display_list(assets: Sequence[Any], limit: int = 4) -> str:
    names = [ASSET_DISPLAY.get(str(asset), str(asset)) for asset in assets if str(asset).strip()]
    unique = list(dict.fromkeys(names).keys())
    return ", ".join(unique[:limit]) if len(unique) <= limit else ", ".join(unique[:limit]) + ", more"


def _gdelt_why(
    stream: str,
    labels: Sequence[str],
    count: int,
    severity: float | None,
    impact: float | None,
    z_score: float | None,
    example_domain: str | None = None,
) -> str:
    stream_key = str(stream or "")
    if stream_key == "trade_sanctions_supply":
        driver = "trade, sanctions or supply-chain stress is clustering in recent news"
    elif stream_key == "energy_commodities":
        driver = "oil or energy headlines are clustering strongly"
    elif stream_key == "conflict_security":
        driver = "conflict and security headlines are clustering"
    elif stream_key == "market_stress":
        driver = "market-stress language is showing up together"
    elif stream_key == "policy_rates":
        driver = "policy-rate and central-bank headlines are clustering"
    elif stream_key == "major_disaster":
        driver = "disaster headlines are unusually concentrated"
    elif stream_key == "political_risk":
        driver = "political-risk headlines are unusually concentrated"
    else:
        driver = "macro-relevant headlines are clustering"
    metrics: list[str] = []
    if count:
        metrics.append(f"{count:,} articles")
    if z_score is not None and z_score >= 2.5:
        metrics.append(f"z-score {z_score:.1f}")
    elif severity is not None:
        metrics.append(f"severity {severity:.1f}")
    if impact is not None and impact >= 4.0:
        metrics.append(f"impact {impact:.1f}")
    if example_domain:
        metrics.append(f"example from {example_domain}")
    suffix = f" ({'; '.join(metrics)})" if metrics else ""
    return f"{driver}{suffix}."


def _event_display_fields(event: Mapping[str, Any]) -> dict[str, str]:
    metadata = dict(event.get("metadata") or {})
    title = str(event.get("title") or "Event")
    summary = str(event.get("summary") or "").strip()
    labels = [str(label) for label in event.get("labels") or []]
    assets = list(event.get("affected_assets") or [])
    source_table = str(event.get("source_table") or "")
    if source_table in {"gdelt_streams", "gdelt_canary_files"}:
        _date, stream, scope = _gdelt_event_parts(event)
        why = str(event.get("why_text") or metadata.get("why_text") or "").strip()
        if not why:
            why = _gdelt_why(
                stream,
                labels,
                int(metadata.get("article_count") or 0),
                metadata.get("severity"),
                metadata.get("societal_impact_score"),
                metadata.get("z_score"),
                metadata.get("example_domain"),
            )
        display_title = f"{_stream_display(stream)}: {_scope_display(scope)}"
        asset_text = _asset_display_list(assets)
        display_summary = why if not asset_text else f"{why} Watch {asset_text}."
        return {
            "display_title": display_title,
            "display_summary": display_summary,
            "why_text": why,
            "source_quality": str(event.get("source_quality") or f"GDELT aggregate; {metadata.get('article_count') or 0} articles"),
        }
    if source_table == "news_items":
        why = str(event.get("why_text") or "").strip()
        if not why:
            reasons = metadata.get("reasons") or []
            why = f"Matched market-moving terms: {', '.join(str(r) for r in reasons[:3])}." if reasons else "High-priority news matched market-moving rules."
        return {
            "display_title": str(event.get("display_title") or title),
            "display_summary": str(event.get("display_summary") or summary or why),
            "why_text": why,
            "source_quality": str(event.get("source_quality") or metadata.get("source") or "news wire"),
        }
    if source_table == "macro_release_actuals":
        return {
            "display_title": str(event.get("display_title") or title),
            "display_summary": str(event.get("display_summary") or summary or "Official macro event."),
            "why_text": str(event.get("why_text") or summary or "Official macro data can move the rate, growth and inflation baseline."),
            "source_quality": str(event.get("source_quality") or "official calendar/release"),
        }
    return {
        "display_title": str(event.get("display_title") or title),
        "display_summary": str(event.get("display_summary") or summary),
        "why_text": str(event.get("why_text") or summary),
        "source_quality": str(event.get("source_quality") or "source event"),
    }


def _has_concrete_why(event: Mapping[str, Any]) -> bool:
    why = str(event.get("why_text") or _event_display_fields(event).get("why_text") or "").strip().lower()
    if len(why) < 24:
        return False
    generic = ("needs a look", "deserves review", "crossed the high-impact gate")
    return not any(token in why for token in generic)


def _should_review_event(event: Mapping[str, Any], *, high_impact: bool = True, min_count: int = 0) -> bool:
    if not high_impact or float(event.get("priority") or 0.0) < REVIEW_PRIORITY:
        return False
    metadata = dict(event.get("metadata") or {})
    count = int(metadata.get("article_count") or metadata.get("count") or 0)
    if min_count and count < min_count:
        return False
    return _has_concrete_why(event)


def _priority_cap(value: float) -> float:
    return round(max(1.0, min(5.0, value)), 2)


def _upsert_event(conn: sqlite3.Connection, event: Mapping[str, Any]) -> tuple[int, int]:
    ensure_schema(conn)
    display = _event_display_fields(event)
    before = conn.total_changes
    conn.execute(
        """INSERT INTO current_events (
             event_key, event_type, title, summary, event_time, region, category,
             priority, status, object_id, source_table, source_id, labels_json,
             affected_assets_json, display_title, display_summary, why_text,
             source_quality, metadata_json, oracle_review_required, expires_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(event_key) DO UPDATE SET
             event_type=excluded.event_type,
             title=excluded.title,
             summary=excluded.summary,
             event_time=excluded.event_time,
             region=excluded.region,
             category=excluded.category,
             priority=excluded.priority,
             status=excluded.status,
             object_id=excluded.object_id,
             source_table=excluded.source_table,
             source_id=excluded.source_id,
             labels_json=excluded.labels_json,
             affected_assets_json=excluded.affected_assets_json,
             display_title=excluded.display_title,
             display_summary=excluded.display_summary,
             why_text=excluded.why_text,
             source_quality=excluded.source_quality,
             metadata_json=excluded.metadata_json,
             oracle_review_required=excluded.oracle_review_required,
             expires_at=excluded.expires_at,
             updated_at=datetime('now')""",
        (
            event["event_key"], event["event_type"], event["title"], event.get("summary"),
            event["event_time"], event.get("region"), event.get("category"),
            float(event.get("priority") or 1.0), event.get("status", "active"),
            event["object_id"], event["source_table"], str(event.get("source_id")) if event.get("source_id") is not None else None,
            _json(event.get("labels") or []), _json(event.get("affected_assets") or []),
            display["display_title"], display["display_summary"], display["why_text"], display["source_quality"],
            _json(event.get("metadata") or {}), int(bool(event.get("oracle_review_required"))),
            event.get("expires_at"),
        ),
    )
    changed = conn.total_changes - before
    queued = 0
    if event.get("oracle_review_required") and _should_review_event(event):
        source_id = conn.execute("SELECT id FROM current_events WHERE event_key = ?", (event["event_key"],)).fetchone()
        queued = enqueue_change_event(
            conn,
            object_id="current.event",
            source_table="current_events",
            source_id=source_id[0] if source_id else event["event_key"],
            event_type=str(event.get("event_type") or "upsert"),
            priority=float(event.get("priority") or 1.0),
            labels=list(event.get("labels") or []),
            metadata={
                **dict(event.get("metadata") or {}),
                "title": event.get("title"),
                "event_time": event.get("event_time"),
                "event_key": event.get("event_key"),
            },
            event_key=f"current.event:{event['event_key']}",
            oracle_review_required=True,
        )
    return changed, queued


def _scheduled_from_macro_actuals(conn: sqlite3.Connection, now: dt.datetime, days_forward: int) -> list[dict[str, Any]]:
    if not table_exists(conn, "macro_release_actuals"):
        return []
    rows = conn.execute(
        """SELECT id, calendar_event_id, release_key, region, category, title,
                  scheduled_date, scheduled_time_local, importance, expected_value,
                  expected_text, expected_unit, previous_value, status, metadata_json, updated_at
           FROM macro_release_actuals
           WHERE scheduled_date BETWEEN date(?) AND date(?, ?)
             AND status != 'released'
           ORDER BY scheduled_date ASC, importance DESC, region, title""",
        (now.date().isoformat(), now.date().isoformat(), f"+{int(days_forward)} days"),
    ).fetchall()
    events = []
    tomorrow = now.date() + dt.timedelta(days=1)
    for row in rows:
        event_time = _event_time(row["scheduled_date"])
        labels = _labels_for_release(row["category"], row["release_key"])
        priority = float(row["importance"] or 3)
        scheduled_date = dt.date.fromisoformat(str(row["scheduled_date"])[:10])
        if scheduled_date <= tomorrow:
            priority += 0.35
        summary_bits = []
        if row["expected_text"]:
            summary_bits.append(str(row["expected_text"]))
        if row["previous_value"] is not None:
            summary_bits.append(f"Previous: {row['previous_value']}")
        assets = _assets_for_labels(labels, row["title"])
        events.append({
            "event_key": f"current:scheduled:{row['release_key']}:{row['scheduled_date']}",
            "event_type": "scheduled_catalyst",
            "title": row["title"],
            "summary": " | ".join(summary_bits) or "Scheduled macro catalyst.",
            "event_time": event_time,
            "region": row["region"],
            "category": row["category"],
            "priority": _priority_cap(priority),
            "object_id": "calendar.event",
            "source_table": "macro_release_actuals",
            "source_id": row["id"],
            "labels": labels,
            "affected_assets": assets,
            "metadata": {"release_key": row["release_key"], "status": row["status"], "scheduled_date": row["scheduled_date"]},
            "why_text": f"{row['category']} release can update the macro baseline for {row['region']}.",
            "source_quality": "official calendar/release",
            "oracle_review_required": False,
            "expires_at": _expires(event_time, 1.0),
        })
    return events


def _scheduled_from_calendar(conn: sqlite3.Connection, now: dt.datetime, days_forward: int) -> list[dict[str, Any]]:
    if not table_exists(conn, "calendar_events"):
        return []
    skip_ids = set()
    if table_exists(conn, "macro_release_actuals"):
        skip_ids = {str(row[0]) for row in conn.execute("SELECT DISTINCT calendar_event_id FROM macro_release_actuals WHERE calendar_event_id IS NOT NULL")}
    rows = conn.execute(
        """SELECT id, date, time_local, region, category, importance, title,
                  expected, market_note, source, url, event_key, release_family,
                  scheduled_at_utc, labels_json, status
           FROM calendar_events
           WHERE date BETWEEN date(?) AND date(?, ?)
             AND status != 'cancelled'
           ORDER BY date ASC, importance DESC, region, title""",
        (now.date().isoformat(), now.date().isoformat(), f"+{int(days_forward)} days"),
    ).fetchall()
    events = []
    tomorrow = now.date() + dt.timedelta(days=1)
    for row in rows:
        if str(row["id"]) in skip_ids:
            continue
        event_time = _event_time(row["date"], row["scheduled_at_utc"])
        labels = _json_list(row["labels_json"])
        if row["category"] and f"theme:{row['category']}" not in labels:
            labels.append(f"theme:{row['category']}")
        if row["release_family"] and f"release_family:{row['release_family']}" not in labels:
            labels.append(f"release_family:{row['release_family']}")
        scheduled_date = dt.date.fromisoformat(str(row["date"])[:10])
        priority = float(row["importance"] or 3) + (0.35 if scheduled_date <= tomorrow else 0)
        assets = _assets_for_labels(labels, row["title"])
        events.append({
            "event_key": f"current:calendar:{row['event_key'] or row['id']}:{row['date']}",
            "event_type": "scheduled_catalyst",
            "title": row["title"],
            "summary": row["expected"] or row["market_note"] or "Scheduled calendar catalyst.",
            "event_time": event_time,
            "region": row["region"],
            "category": row["category"],
            "priority": _priority_cap(priority),
            "object_id": "calendar.event",
            "source_table": "calendar_events",
            "source_id": row["id"],
            "labels": sorted(set(labels)),
            "affected_assets": assets,
            "metadata": {"source": row["source"], "url": row["url"], "status": row["status"]},
            "why_text": f"{row['category']} catalyst can update the macro baseline for {row['region']}.",
            "source_quality": str(row["source"] or "calendar"),
            "oracle_review_required": False,
            "expires_at": _expires(event_time, 1.0),
        })
    return events


def _released_actuals(conn: sqlite3.Connection, now: dt.datetime) -> list[dict[str, Any]]:
    if not table_exists(conn, "macro_release_actuals"):
        return []
    rows = conn.execute(
        """SELECT id, release_key, region, category, title, scheduled_date,
                  scheduled_time_local, importance, actual_value, expected_value,
                  expected_text, previous_value, surprise_value, surprise_text,
                  unit, status, updated_at
           FROM macro_release_actuals
           WHERE status = 'released'
             AND scheduled_date >= date(?, '-3 days')
           ORDER BY scheduled_date DESC, importance DESC, region, title""",
        (now.date().isoformat(),),
    ).fetchall()
    events = []
    for row in rows:
        updated = _dt(row["updated_at"])
        age_hours = (now - updated).total_seconds() / 3600 if updated else 999
        event_time = _event_time(row["scheduled_date"], updated_at=row["updated_at"])
        labels = _labels_for_release(row["category"], row["release_key"])
        priority = float(row["importance"] or 3)
        priority = priority + 0.6 if age_hours <= 12 else priority - 1.2
        actual = row["actual_value"]
        unit = row["unit"] or ""
        summary = f"Actual: {actual}{unit}" if actual is not None else "Released macro actual."
        if row["surprise_text"]:
            summary += f" | Surprise: {row['surprise_text']}"
        elif row["surprise_value"] is not None:
            summary += f" | Surprise: {row['surprise_value']}"
        event = {
            "event_key": f"current:released:{row['release_key']}:{row['scheduled_date']}",
            "event_type": "released_actual",
            "title": row["title"],
            "summary": summary,
            "event_time": event_time,
            "region": row["region"],
            "category": row["category"],
            "priority": _priority_cap(priority),
            "object_id": "macro.release.actual",
            "source_table": "macro_release_actuals",
            "source_id": row["id"],
            "labels": labels,
            "affected_assets": _assets_for_labels(labels, row["title"]),
            "metadata": {"release_key": row["release_key"], "status": row["status"], "scheduled_date": row["scheduled_date"]},
            "why_text": summary,
            "source_quality": "official release",
            "oracle_review_required": False,
            "expires_at": _iso(now + dt.timedelta(hours=72)),
        }
        event["oracle_review_required"] = _should_review_event(event, high_impact=age_hours <= 24)
        events.append(event)
    return events


def _score_news_text(title: str, summary: str | None, category: str | None, source: str | None, published_at: str | None, now: dt.datetime) -> tuple[float, list[str], list[str], list[str]]:
    text = f"{title} {summary or ''} {category or ''} {source or ''}".lower()
    labels: set[str] = set()
    assets: set[str] = set()
    reasons: list[str] = []
    score = 0.0
    for pattern, weight, rule_labels, rule_assets in NEWS_RULES:
        if re.search(pattern, text, flags=re.IGNORECASE):
            score += weight
            labels.update(rule_labels)
            assets.update(rule_assets)
            reasons.append(pattern.replace("\\b", ""))
    if "trump" in text and ("xi" in text or "china" in text or "beijing" in text):
        score += 1.2
        labels.update(("theme:trade", "theme:political", "theme:market_moving_news"))
        assets.update(("SPY", "MCHI", "GC=F"))
        reasons.append("US-China leadership link")
    if category in {"central_bank", "inflation", "labour", "gdp", "energy"}:
        score += 0.45
        labels.add("theme:macro_news")
    published = _dt(published_at)
    if published:
        age_hours = max((now - published).total_seconds() / 3600, 0)
        if age_hours <= 6:
            score += 0.35
        elif age_hours <= 24:
            score += 0.15
    if not labels and score <= 0:
        return 0.0, [], [], []
    priority = _priority_cap(2.55 + score)
    return priority, sorted(labels), sorted(assets), reasons


def _rss_breaking_news(conn: sqlite3.Connection, now: dt.datetime) -> list[dict[str, Any]]:
    if not table_exists(conn, "news_items"):
        return []
    rows = conn.execute(
        """SELECT id, published_at, fetched_at, source, title, summary, url, region, category
           FROM news_items
           WHERE COALESCE(published_at, fetched_at) >= datetime(?, '-48 hours')
           ORDER BY COALESCE(published_at, fetched_at) DESC
           LIMIT 500""",
        (_iso(now),),
    ).fetchall()
    events = []
    for row in rows:
        priority, labels, assets, reasons = _score_news_text(row["title"], row["summary"], row["category"], row["source"], row["published_at"] or row["fetched_at"], now)
        if priority < BREAKING_THRESHOLD:
            continue
        event_time = _event_time(row["published_at"] or row["fetched_at"], updated_at=row["published_at"] or row["fetched_at"])
        key_token = row["url"] or row["title"]
        why = f"Matched market-moving terms: {', '.join(reasons[:3])}." if reasons else "High-priority news matched market-moving rules."
        event = {
            "event_key": f"current:news:rss:{_stable_hash(str(key_token).lower())}",
            "event_type": "breaking_news",
            "title": row["title"],
            "summary": row["summary"] or f"High-impact news from {row['source']}.",
            "event_time": event_time,
            "region": row["region"] or "Global",
            "category": row["category"] or "news",
            "priority": priority,
            "object_id": "news.item",
            "source_table": "news_items",
            "source_id": row["id"],
            "labels": labels,
            "affected_assets": sorted(set(assets) | set(_assets_for_labels(labels, row["title"]))),
            "metadata": {"source": row["source"], "url": row["url"], "reasons": reasons},
            "why_text": why,
            "source_quality": str(row["source"] or "news wire"),
            "oracle_review_required": False,
            "expires_at": _iso(now + dt.timedelta(hours=48)),
        }
        event["oracle_review_required"] = _should_review_event(event, high_impact=True)
        events.append(event)
    return events


def _labels_for_stream(stream: str, top_themes: list[str], raw_labels: Any = None) -> list[str]:
    labels = _json_list(raw_labels)
    try:
        inferred = gdelt._stream_label_ids(stream, top_themes)  # type: ignore[attr-defined]
    except AttributeError:
        inferred = [f"stream:{stream}"]
    labels.extend(inferred)
    if stream in MARKET_MOVING_STREAMS:
        labels.append("theme:market_moving_news")
    if stream in MACRO_NEWS_STREAMS:
        labels.append("theme:macro_news")
    return sorted(set(label for label in labels if label))


def _gdelt_priority(stream: str, labels: list[str], count: int, severity: float | None, impact: float | None, z_score: float | None, event_time: str, now: dt.datetime) -> float:
    base = max(float(severity or 0), float(impact or 0), 0.0)
    if z_score is not None and z_score >= 3.0 and count >= 10:
        base = max(base, 4.1 + min(0.5, (z_score - 3.0) / 6.0))
    if "theme:market_moving_news" in labels:
        base += 0.25
    if any(label.startswith("asset_impact:") for label in labels):
        base += 0.2
    if stream in {"conflict_security", "trade_sanctions_supply", "energy_commodities", "market_stress"}:
        base += 0.15
    parsed = _dt(event_time)
    if parsed and (now - parsed).total_seconds() <= 12 * 3600:
        base += 0.15
    if count:
        base += min(0.2, math.log10(max(count, 1)) / 10)
    return _priority_cap(base)


def _gdelt_event_parts(event: Mapping[str, Any]) -> tuple[str, str, str]:
    metadata = dict(event.get("metadata") or {})
    raw_key = str(event.get("event_key") or "")
    parts = raw_key.split(":")
    date = str(metadata.get("date") or "")
    stream = str(metadata.get("stream") or event.get("category") or "")
    region = str(event.get("region") or metadata.get("region") or "")
    country = str(metadata.get("country") or "")
    if not date and len(parts) >= 5 and parts[0] == "current" and parts[1] == "gdelt":
        date = parts[2]
        stream = stream or parts[3]
        region = region or parts[4]
        if len(parts) > 5:
            country = country or parts[5]
    scope = country or region or "Global"
    return date, stream, scope


def _normalise_gdelt_breaking_event(event: dict[str, Any]) -> dict[str, Any]:
    normalised = dict(event)
    metadata = dict(normalised.get("metadata") or {})
    date, stream, scope = _gdelt_event_parts(normalised)
    if not date:
        date = (_dt(normalised.get("event_time")) or dt.datetime.now(dt.UTC)).date().isoformat()
    if not stream:
        stream = str(normalised.get("category") or "gdelt")
    if not scope:
        scope = "Global"
    display_scope = scope.upper() if len(scope) <= 3 else scope
    metadata.update({"date": date, "stream": stream, "canonical_scope": scope})
    normalised["event_key"] = f"current:gdelt:{date}:{stream}:{scope}"
    normalised["title"] = f"{_stream_display(stream)}: {display_scope}"
    normalised["metadata"] = metadata
    display = _event_display_fields(normalised)
    normalised.update(display)
    return normalised


def _dedupe_gdelt_breaking_events(events: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    passthrough: list[dict[str, Any]] = []
    best_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    gdelt_sources = {"gdelt_streams", "gdelt_canary_files"}

    def sort_key(event: Mapping[str, Any]) -> tuple[float, float, float]:
        source_bonus = 1.0 if event.get("source_table") == "gdelt_canary_files" else 0.0
        parsed = _dt(event.get("event_time"))
        timestamp = parsed.timestamp() if parsed else 0.0
        return (source_bonus, float(event.get("priority") or 0.0), timestamp)

    for event in events:
        if event.get("event_type") == "breaking_news" and event.get("source_table") in gdelt_sources:
            normalised = _normalise_gdelt_breaking_event(event)
            key = _gdelt_event_parts(normalised)
            previous = best_by_key.get(key)
            if previous is None or sort_key(normalised) > sort_key(previous):
                best_by_key[key] = normalised
        else:
            passthrough.append(event)

    selected: list[dict[str, Any]] = []
    per_stream: dict[str, int] = {}
    for event in sorted(best_by_key.values(), key=sort_key, reverse=True):
        _, stream, _scope = _gdelt_event_parts(event)
        count = per_stream.get(stream, 0)
        if count >= GDELT_EVENT_PER_STREAM_CAP:
            continue
        selected.append(event)
        per_stream[stream] = count + 1
        if len(selected) >= GDELT_EVENT_TOTAL_CAP:
            break
    return passthrough + selected


def _gdelt_existing_breaking(conn: sqlite3.Connection, now: dt.datetime) -> list[dict[str, Any]]:
    if not table_exists(conn, "gdelt_streams"):
        return []
    rows = conn.execute(
        """SELECT id, date, stream, region, country, article_count, total_articles,
                  article_share, baseline_30d, z_score, severity, societal_impact_score,
                  labels_json, top_theme_codes_json, fetched_at
           FROM gdelt_streams
           WHERE date >= date(?, '-2 days')
           ORDER BY date DESC, societal_impact_score DESC, severity DESC, article_count DESC
           LIMIT 500""",
        (now.date().isoformat(),),
    ).fetchall()
    example_rows: dict[tuple[str, str, str, str], sqlite3.Row] = {}
    if table_exists(conn, "gdelt_stream_examples"):
        for row in conn.execute(
            """SELECT date, stream, region, country, title, url, source_domain, labels_json
               FROM gdelt_stream_examples
               WHERE date >= date(?, '-2 days')
               ORDER BY date DESC, example_rank ASC""",
            (now.date().isoformat(),),
        ).fetchall():
            key = (row["date"], row["stream"], row["region"], row["country"] or "")
            example_rows.setdefault(key, row)
    events = []
    for row in rows:
        top_themes = _json_list(row["top_theme_codes_json"])
        labels = _labels_for_stream(row["stream"], top_themes, row["labels_json"])
        count = int(row["article_count"] or 0)
        severity = float(row["severity"] or 0)
        impact = float(row["societal_impact_score"] or 0)
        z_score = float(row["z_score"]) if row["z_score"] is not None else None
        market_labeled = (
            "theme:market_moving_news" in labels
            or "theme:macro_news" in labels
            or any(label.startswith("asset_impact:") for label in labels)
        )
        high_impact = (
            severity >= REVIEW_PRIORITY
            or impact >= REVIEW_PRIORITY
            or (z_score is not None and z_score >= 3.0 and count >= 10)
        )
        material_market_bucket = market_labeled and (
            (count >= 20 and max(severity, impact) >= 3.5)
            or (z_score is not None and z_score >= 2.5 and count >= 10)
        )
        if not (high_impact or material_market_bucket):
            continue
        event_time = _event_time(row["date"], updated_at=row["fetched_at"])
        priority = _gdelt_priority(row["stream"], labels, count, severity, impact, z_score, event_time, now)
        if priority < BREAKING_THRESHOLD:
            continue
        key = (row["date"], row["stream"], row["region"], row["country"] or "")
        example = example_rows.get(key)
        scope = row["country"] or row["region"] or "Global"
        example_domain = example["source_domain"] if example and example["source_domain"] else None
        title = f"{_stream_display(row['stream'])}: {_scope_display(scope)}"
        why = _gdelt_why(row["stream"], labels, count, severity, impact, z_score, example_domain)
        summary = why
        metadata = {
            "date": row["date"], "stream": row["stream"], "country": row["country"],
            "article_count": count, "severity": severity, "societal_impact_score": impact,
            "z_score": z_score, "top_theme_codes": top_themes,
            "example_url": example["url"] if example else None,
            "example_domain": example_domain,
        }
        event = {
            "event_key": f"current:gdelt:{row['date']}:{row['stream']}:{row['region']}:{row['country'] or ''}",
            "event_type": "breaking_news",
            "title": title,
            "summary": summary,
            "event_time": event_time,
            "region": row["region"] or "Global",
            "category": row["stream"],
            "priority": priority,
            "object_id": "gdelt.stream",
            "source_table": "gdelt_streams",
            "source_id": row["id"],
            "labels": labels,
            "affected_assets": _assets_for_labels(labels, title),
            "metadata": metadata,
            "why_text": why,
            "source_quality": f"GDELT aggregate; {count:,} articles",
            "oracle_review_required": False,
            "expires_at": _iso(now + dt.timedelta(hours=48)),
        }
        event["oracle_review_required"] = _should_review_event(event, high_impact=high_impact, min_count=20)
        events.append(event)
    return sorted(events, key=lambda item: (float(item.get("priority") or 0), item.get("event_time") or ""), reverse=True)[: GDELT_EVENT_TOTAL_CAP * 4]


def _recent_gkg_stamps(now: dt.datetime, hours: int) -> list[tuple[str, str]]:
    now = now.astimezone(dt.UTC)
    minute = (now.minute // 15) * 15
    end = now.replace(minute=minute, second=0, microsecond=0) - dt.timedelta(minutes=30)
    start = end - dt.timedelta(hours=hours)
    stamps = []
    cursor = start
    while cursor <= end:
        stamp = cursor.strftime("%Y%m%d%H%M00")
        url = f"{gdelt.GDELT_BASE}/{stamp}.gkg.csv.zip"
        stamps.append((stamp, url))
        cursor += dt.timedelta(minutes=15)
    return stamps


def _merge_streams(dest: dict[tuple[str, str, str], dict[str, Any]], src: dict[tuple[str, str, str], dict[str, Any]]) -> None:
    for key, item in src.items():
        bucket = dest.setdefault(key, {"count": 0, "themes": Counter(), "examples": []})
        bucket["count"] += int(item.get("count") or 0)
        bucket["themes"].update(item.get("themes") or {})
        seen = {example.get("url") or example.get("title") for example in bucket["examples"]}
        for example in item.get("examples") or []:
            token = example.get("url") or example.get("title")
            if token and token not in seen and len(bucket["examples"]) < 10:
                bucket["examples"].append(example)
                seen.add(token)


def _upsert_incremental_stream_buckets(
    conn: sqlite3.Connection,
    day_iso: str,
    total_increment: int,
    streams: dict[tuple[str, str, str], dict[str, Any]],
) -> int:
    if total_increment <= 0 or not streams or not table_exists(conn, "gdelt_streams"):
        return 0
    before = conn.total_changes
    for (stream, region, country), item in streams.items():
        count_increment = int(item.get("count") or 0)
        if count_increment <= 0:
            continue
        existing = conn.execute(
            """SELECT article_count, total_articles
               FROM gdelt_streams
               WHERE date = ? AND stream = ? AND region = ? AND country = ?""",
            (day_iso, stream, region, country or ""),
        ).fetchone()
        old_count = int(existing["article_count"] or 0) if existing else 0
        old_total = int(existing["total_articles"] or 0) if existing else 0
        article_count = old_count + count_increment
        total_articles = max(old_total + int(total_increment or 0), article_count)
        top_themes = [theme for theme, _n in (item.get("themes") or Counter()).most_common(12)]
        labels = _labels_for_stream(stream, top_themes)
        try:
            share, baseline, z_score, severity, impact = gdelt._stream_scores(  # type: ignore[attr-defined]
                conn, day_iso, stream, region, country or "", article_count, total_articles,
            )
        except AttributeError:
            share = article_count / total_articles if total_articles else 0.0
            baseline = z_score = None
            severity = min(5.0, 2.4 + math.log10(max(article_count, 1)) * 1.25)
            impact = severity
        conn.execute(
            """INSERT INTO gdelt_streams (
                 date, stream, region, country, article_count, total_articles,
                 article_share, baseline_30d, z_score, severity,
                 societal_impact_score, labels_json, top_theme_codes_json, source
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'gdelt_canary')
               ON CONFLICT(date, stream, region, country) DO UPDATE SET
                 article_count=excluded.article_count,
                 total_articles=excluded.total_articles,
                 article_share=excluded.article_share,
                 baseline_30d=excluded.baseline_30d,
                 z_score=excluded.z_score,
                 severity=excluded.severity,
                 societal_impact_score=excluded.societal_impact_score,
                 labels_json=excluded.labels_json,
                 top_theme_codes_json=excluded.top_theme_codes_json,
                 fetched_at=datetime('now')""",
            (
                day_iso, stream, region, country or "", article_count, total_articles,
                share, baseline, z_score, severity, impact, _json(labels), _json(top_themes),
            ),
        )
        if not table_exists(conn, "gdelt_stream_examples"):
            continue
        used_ranks = {
            int(row[0]) for row in conn.execute(
                """SELECT example_rank FROM gdelt_stream_examples
                   WHERE date = ? AND stream = ? AND region = ? AND country = ?""",
                (day_iso, stream, region, country or ""),
            ).fetchall()
        }
        next_rank = 1
        for example in item.get("examples") or []:
            while next_rank in used_ranks and next_rank <= gdelt.MAX_EXAMPLES_PER_BUCKET:
                next_rank += 1
            if next_rank > gdelt.MAX_EXAMPLES_PER_BUCKET:
                break
            conn.execute(
                """INSERT INTO gdelt_stream_examples (
                     date, stream, region, country, example_rank, title, url,
                     source_domain, location_name, theme_codes_json, labels_json, tone, source
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'gdelt_canary')
                   ON CONFLICT(date, stream, region, country, example_rank) DO NOTHING""",
                (
                    day_iso, stream, region, country or "", next_rank,
                    example.get("title"), example.get("url"), example.get("source_domain"),
                    example.get("location_name"), _json(example.get("theme_codes") or []),
                    _json(labels), example.get("tone"),
                ),
            )
            used_ranks.add(next_rank)
            next_rank += 1
    return conn.total_changes - before


def _incremental_gdelt_events(conn: sqlite3.Connection, now: dt.datetime, hours: int = 6) -> tuple[list[dict[str, Any]], int, str | None]:
    ensure_schema(conn)
    streams: dict[tuple[str, str, str], dict[str, Any]] = {}
    rows_seen = 0
    latest_stamp = None
    for stamp, url in _recent_gkg_stamps(now, hours):
        processed = conn.execute("SELECT status FROM gdelt_canary_files WHERE file_stamp = ?", (stamp,)).fetchone()
        if processed and processed[0] == "success":
            continue
        total, _counts, _disasters, file_streams, err = gdelt.fetch_file_counts(url)
        status = "failure" if err else ("success" if total > 0 else "missing")
        conn.execute(
            """INSERT INTO gdelt_canary_files (file_stamp, url, status, rows_seen, error_message)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(file_stamp) DO UPDATE SET
                 url=excluded.url,
                 status=excluded.status,
                 rows_seen=excluded.rows_seen,
                 error_message=excluded.error_message,
                 fetched_at=datetime('now')""",
            (stamp, url, status, int(total or 0), err),
        )
        if status != "success":
            continue
        rows_seen += int(total or 0)
        latest_stamp = stamp
        _merge_streams(streams, file_streams)
    if rows_seen and streams:
        _upsert_incremental_stream_buckets(conn, now.date().isoformat(), rows_seen, streams)
    events: list[dict[str, Any]] = []
    for (stream, region, country), item in streams.items():
        count = int(item.get("count") or 0)
        top_themes = [theme for theme, _n in (item.get("themes") or Counter()).most_common(12)]
        labels = _labels_for_stream(stream, top_themes)
        severity = min(5.0, 2.4 + math.log10(max(count, 1)) * 1.25)
        impact = min(5.0, severity + (0.35 if region in {"US", "EU", "Europe", "Asia", "Middle East"} else 0.0))
        market_labeled = (
            "theme:market_moving_news" in labels
            or "theme:macro_news" in labels
            or any(label.startswith("asset_impact:") for label in labels)
        )
        high_impact = severity >= REVIEW_PRIORITY or impact >= REVIEW_PRIORITY
        material_market_bucket = market_labeled and count >= 20 and max(severity, impact) >= 3.5
        if not (high_impact or material_market_bucket):
            continue
        priority = _gdelt_priority(stream, labels, count, severity, impact, None, _iso(now), now)
        if priority < BREAKING_THRESHOLD:
            continue
        example = (item.get("examples") or [{}])[0]
        scope = country or region or "Global"
        title = f"{_stream_display(stream)}: {_scope_display(scope)}"
        why = _gdelt_why(stream, labels, count, severity, impact, None, example.get("source_domain"))
        event = {
            "event_key": f"current:gdelt:{now.date().isoformat()}:{stream}:{region}:{country or ''}",
            "event_type": "breaking_news",
            "title": title,
            "summary": why,
            "event_time": _iso(now),
            "region": region or "Global",
            "category": stream,
            "priority": priority,
            "object_id": "gdelt.stream",
            "source_table": "gdelt_canary_files",
            "source_id": latest_stamp,
            "labels": labels,
            "affected_assets": _assets_for_labels(labels, title),
            "metadata": {"date": now.date().isoformat(), "stream": stream, "region": region, "country": country, "article_count": count, "top_theme_codes": top_themes, "latest_file_stamp": latest_stamp, "example_url": example.get("url"), "example_domain": example.get("source_domain")},
            "why_text": why,
            "source_quality": f"GDELT canary; {count:,} articles",
            "oracle_review_required": False,
            "expires_at": _iso(now + dt.timedelta(hours=48)),
        }
        event["oracle_review_required"] = _should_review_event(event, high_impact=high_impact, min_count=20)
        events.append(event)
    events = sorted(
        events,
        key=lambda item: (float(item.get("priority") or 0), item.get("event_time") or ""),
        reverse=True,
    )[: GDELT_EVENT_TOTAL_CAP * 4]
    return events, rows_seen, latest_stamp


def _expire_old_events(conn: sqlite3.Connection, now: dt.datetime) -> int:
    before = conn.total_changes
    conn.execute(
        """UPDATE current_events
           SET status = 'expired', updated_at = datetime('now')
           WHERE status = 'active'
             AND expires_at IS NOT NULL
             AND datetime(expires_at) < datetime(?)""",
        (_iso(now),),
    )
    return conn.total_changes - before


def _deactivate_missing_breaking_events(conn: sqlite3.Connection, active_keys: set[str]) -> int:
    before = conn.total_changes
    source_filter = "'gdelt_streams','gdelt_canary_files','news_items'"
    if active_keys:
        placeholders = ",".join("?" for _ in active_keys)
        conn.execute(
            f"""UPDATE current_events
                SET status = 'expired', updated_at = datetime('now')
                WHERE status = 'active'
                  AND event_type = 'breaking_news'
                  AND source_table IN ({source_filter})
                  AND event_key NOT IN ({placeholders})""",
            tuple(active_keys),
        )
    else:
        conn.execute(
            f"""UPDATE current_events
                SET status = 'expired', updated_at = datetime('now')
                WHERE status = 'active'
                  AND event_type = 'breaking_news'
                  AND source_table IN ({source_filter})"""
        )
    return conn.total_changes - before


def _sync_current_event_review_queue(conn: sqlite3.Connection) -> int:
    if not table_exists(conn, "data_change_events"):
        return 0
    before = conn.total_changes
    conn.execute(
        """UPDATE data_change_events
           SET status = 'queued', updated_at = datetime('now')
           WHERE object_id = 'current.event'
             AND status = 'superseded'
             AND event_key IN (
               SELECT 'current.event:' || event_key
               FROM current_events
               WHERE status = 'active'
                 AND oracle_review_required = 1
             )"""
    )
    conn.execute(
        """UPDATE data_change_events
           SET status = 'superseded', updated_at = datetime('now')
           WHERE object_id = 'current.event'
             AND status = 'queued'
             AND event_key NOT IN (
               SELECT 'current.event:' || event_key
               FROM current_events
               WHERE status = 'active'
                 AND oracle_review_required = 1
             )"""
    )
    return conn.total_changes - before


def build_events(
    conn: sqlite3.Connection,
    *,
    now: dt.datetime | None = None,
    days_forward: int = 14,
    fetch_incremental_gdelt: bool = True,
    recent_gdelt_hours: int = 6,
) -> tuple[list[dict[str, Any]], int, str | None]:
    conn.row_factory = sqlite3.Row
    now = now or dt.datetime.now(dt.UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.UTC)
    now = now.astimezone(dt.UTC)
    events: list[dict[str, Any]] = []
    events.extend(_scheduled_from_macro_actuals(conn, now, days_forward))
    events.extend(_scheduled_from_calendar(conn, now, days_forward))
    events.extend(_released_actuals(conn, now))
    events.extend(_rss_breaking_news(conn, now))
    events.extend(_gdelt_existing_breaking(conn, now))
    rows_seen = 0
    latest_stamp = None
    if fetch_incremental_gdelt:
        incremental, rows_seen, latest_stamp = _incremental_gdelt_events(conn, now, recent_gdelt_hours)
        events.extend(incremental)
    events = _dedupe_gdelt_breaking_events(events)
    return events, rows_seen, latest_stamp


def generate_and_store(
    conn: sqlite3.Connection | None = None,
    *,
    now: dt.datetime | None = None,
    days_forward: int = 14,
    fetch_incremental_gdelt: bool = True,
    recent_gdelt_hours: int = 6,
) -> dict[str, Any]:
    own_conn = conn is None
    conn = conn or connect_writable(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        if own_conn:
            conn.executescript(SCHEMA)
            add_missing_columns(conn)
        ensure_schema(conn)
        now = now or dt.datetime.now(dt.UTC)
        if now.tzinfo is None:
            now = now.replace(tzinfo=dt.UTC)
        events, gdelt_rows_seen, latest_stamp = build_events(
            conn,
            now=now,
            days_forward=days_forward,
            fetch_incremental_gdelt=fetch_incremental_gdelt,
            recent_gdelt_hours=recent_gdelt_hours,
        )
        before = conn.total_changes
        queued = 0
        for event in events:
            _changed, event_queued = _upsert_event(conn, event)
            queued += event_queued
        active_breaking_keys = {
            str(event.get("event_key"))
            for event in events
            if event.get("event_type") == "breaking_news"
            and event.get("source_table") in {"gdelt_streams", "gdelt_canary_files", "news_items"}
        }
        expired = _expire_old_events(conn, now)
        expired += _deactivate_missing_breaking_events(conn, active_breaking_keys)
        review_superseded = _sync_current_event_review_queue(conn)
        conn.commit()
        changed = conn.total_changes - before
        try:
            log(f"Current events canary: {len(events)} candidate(s), {queued} review queue event(s).", module="current_events")
        except OSError:
            pass
        return {
            "rows_seen": len(events) + gdelt_rows_seen,
            "rows_inserted": changed,
            "rows_updated": expired,
            "latest_source_ts": latest_stamp or _iso(now),
            "metadata": {"events": len(events), "queued_reviews": queued, "expired": expired, "review_superseded": review_superseded, "gdelt_rows_seen": gdelt_rows_seen},
        }
    finally:
        if own_conn:
            conn.close()


if __name__ == "__main__":
    generate_and_store()
