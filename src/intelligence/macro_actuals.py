"""Deterministic macro release actual matching.

This turns scheduled calendar rows into a freshness-safe release tape by
linking them to already-ingested official indicator rows when possible.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import sqlite3
from typing import Any

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.core.changes import queue_high_impact_macro_release
from src.core.db import connect_writable, table_exists


MATCH_WINDOW_DAYS = 90
PENDING_GRACE_DAYS = {
    "central_bank": 3,
    "interest": 3,
    "rates": 3,
    "energy": 7,
    "labour": 14,
    "gdp": 30,
    "inflation": 14,
    "retail_sales": 14,
    "sentiment": 14,
    "industry": 14,
}
EXPECTED_FULL_RE = re.compile(
    r"^\s*(?:expected|consensus|forecast|prior|previous)?\s*[:=]?\s*"
    r"(?P<value>[-+]?\d+(?:[.,]\d+)?)\s*(?P<unit>%|bp|bps|index|rate)?\s*$",
    re.IGNORECASE,
)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_") or "event"


def _parse_expected(text: str | None) -> tuple[float | None, str | None, str | None, str]:
    raw = str(text or "").strip()
    if not raw:
        return None, None, None, "empty"
    match = EXPECTED_FULL_RE.match(raw)
    if not match:
        return None, None, raw, "prose"
    try:
        value = float(match.group("value").replace(",", "."))
    except ValueError:
        return None, None, raw, "invalid"
    unit = match.group("unit") or None
    if unit and unit.lower() == "bps":
        unit = "bp"
    return value, unit, raw, "numeric"


def _rule(event: sqlite3.Row) -> dict[str, Any] | None:
    title = str(event["title"] or "")
    lower = title.lower()
    region = str(event["region"] or "").upper()
    category = str(event["category"] or "").lower()
    if category == "inflation" and region == "US" and "cpi" in lower:
        return {
            "country": "US",
            "category": "inflation",
            "patterns": ("BLS CPI-U All Items NSA YoY", "CPI-U All Items NSA YoY", "CPI"),
            "unit": "%",
            "actual_type": "yoy_pct",
            "preferred_terms": ("yoy", "all items"),
            "avoid_terms": ("core", "mom", "index"),
            "release_key": "us_cpi",
            "window_days": 70,
        }
    if category == "inflation" and region == "US" and "ppi" in lower:
        return {
            "country": "US",
            "category": "inflation",
            "patterns": ("BLS PPI", "Producer Price Index", "PPI"),
            "unit": "%",
            "actual_type": "yoy_pct",
            "preferred_terms": ("ppi", "producer"),
            "avoid_terms": ("mom", "index"),
            "release_key": "us_ppi",
            "window_days": 70,
        }
    if category == "inflation" and region == "US" and "pce" in lower:
        return {
            "country": "US",
            "category": "inflation",
            "patterns": ("PCE Price Index", "Core PCE", "Personal Consumption Expenditures"),
            "unit": "%",
            "actual_type": "yoy_pct",
            "preferred_terms": ("pce", "price"),
            "avoid_terms": ("mom", "index"),
            "release_key": "us_pce",
            "window_days": 70,
        }
    if category == "inflation" and region == "SE" and ("cpi" in lower or "cpif" in lower):
        return {
            "country": "SE",
            "category": "inflation",
            "patterns": ("SCB CPIF, annual changes", "SCB Annual changes", "Sweden CPI YoY", "SCB CPI"),
            "unit": "%",
            "actual_type": "yoy_pct",
            "preferred_terms": ("annual changes", "cpif"),
            "avoid_terms": ("monthly", "index", "excl"),
            "release_key": "se_cpi_cpif",
            "window_days": 70,
        }
    if category == "inflation" and region == "UK" and "cpi" in lower:
        return {
            "country": "GB",
            "category": "inflation",
            "patterns": ("UK CPI", "Eurostat HICP YoY (GB)"),
            "unit": "%",
            "actual_type": "yoy_pct",
            "preferred_terms": ("yoy", "cpi", "hicp"),
            "avoid_terms": ("mom", "index"),
            "release_key": "gb_cpi",
            "window_days": 70,
        }
    if category == "inflation" and region == "EU" and ("hicp" in lower or "inflation" in lower):
        return {
            "country": "EU",
            "category": "inflation",
            "patterns": ("Eurozone HICP", "Eurostat HICP YoY (EU)"),
            "unit": "%",
            "actual_type": "yoy_pct",
            "preferred_terms": ("yoy", "hicp"),
            "avoid_terms": ("mom", "index"),
            "release_key": "eu_hicp",
            "window_days": 70,
        }
    if category == "labour" and region == "US" and ("payroll" in lower or "employment" in lower):
        return {
            "country": "US",
            "category": "labour",
            "patterns": ("Nonfarm Payrolls", "Payrolls", "Employment Situation"),
            "unit": "thousands",
            "actual_type": "level",
            "preferred_terms": ("payroll", "employment"),
            "avoid_terms": ("unemployment", "rate"),
            "release_key": "us_payrolls",
            "window_days": 45,
        }
    if category == "labour" and region == "US" and ("unemployment" in lower or "jolts" in lower):
        return {
            "country": "US",
            "category": "labour",
            "patterns": ("JOLTS", "Job Openings", "Unemployment Rate", "Unemployment"),
            "unit": "%",
            "actual_type": "level",
            "preferred_terms": ("jolts", "job openings") if "jolts" in lower else ("unemployment", "rate"),
            "avoid_terms": (),
            "release_key": "us_jolts" if "jolts" in lower else "us_unemployment",
            "window_days": 45,
        }
    if category == "gdp" and region == "US":
        return {
            "country": "US",
            "category": "gdp",
            "patterns": ("GDP", "Gross Domestic Product"),
            "unit": "%",
            "actual_type": "annualized_pct",
            "preferred_terms": ("gdp", "gross domestic product"),
            "avoid_terms": (),
            "release_key": "us_gdp",
            "window_days": 120,
        }
    if category == "energy" and region == "US" and ("eia" in lower or "petroleum" in lower or "oil" in lower):
        return {
            "country": "US",
            "category": "energy",
            "patterns": ("EIA Crude Oil Stocks", "EIA Weekly Petroleum Status", "Crude Oil Inventories"),
            "unit": "barrels",
            "actual_type": "inventory_change",
            "preferred_terms": ("crude", "inventor", "stocks"),
            "avoid_terms": (),
            "release_key": "us_eia_oil",
            "window_days": 10,
        }
    if category in {"retail_sales", "retail"} and region == "US":
        return {
            "country": "US",
            "category": "retail_sales",
            "patterns": ("Retail Sales", "Advance Retail Sales"),
            "unit": "%",
            "actual_type": "mom_pct",
            "preferred_terms": ("retail", "sales"),
            "avoid_terms": ("index",),
            "release_key": "us_retail_sales",
            "window_days": 45,
        }
    if category in {"sentiment", "confidence"} and region == "US":
        return {
            "country": "US",
            "category": "sentiment",
            "patterns": ("Consumer Sentiment", "Consumer Confidence", "University of Michigan", "Conference Board"),
            "unit": "Index",
            "actual_type": "index",
            "preferred_terms": ("confidence", "sentiment", "consumer"),
            "avoid_terms": (),
            "release_key": "us_confidence",
            "window_days": 45,
        }
    if category in {"industry", "pmi"} and region in {"US", "EU", "SE", "UK", "JP"}:
        country = {"UK": "GB"}.get(region, region)
        return {
            "country": country,
            "category": "industry",
            "patterns": ("PMI", "Purchasing Managers", "Manufacturing PMI", "Industrial Production"),
            "unit": "Index",
            "actual_type": "index",
            "preferred_terms": ("pmi", "manufacturing", "industrial"),
            "avoid_terms": (),
            "release_key": f"{country.lower()}_pmi",
            "window_days": 45,
        }
    if category in {"central_bank", "interest", "rates"} and region == "EU":
        return {
            "country": "EU",
            "category": "interest",
            "patterns": ("ECB Deposit Rate", "ECB Refi Rate"),
            "unit": "%",
            "actual_type": "policy_rate_pct",
            "preferred_terms": ("rate",),
            "avoid_terms": (),
            "release_key": "ecb_policy_rate",
            "window_days": 5,
        }
    if category in {"central_bank", "interest", "rates"} and region == "US":
        return {
            "country": "US",
            "category": "interest",
            "patterns": ("Fed Funds Rate",),
            "unit": "%",
            "actual_type": "policy_rate_pct",
            "preferred_terms": ("rate",),
            "avoid_terms": (),
            "release_key": "fomc_rate",
            "window_days": 5,
        }
    if category in {"central_bank", "interest", "rates"} and region == "SE":
        return {
            "country": "SE",
            "category": "interest",
            "patterns": ("Riksbank Policy Rate", "Riksbank Repo Rate", "Policy Rate"),
            "unit": "%",
            "actual_type": "policy_rate_pct",
            "preferred_terms": ("rate",),
            "avoid_terms": (),
            "release_key": "riksbank_policy_rate",
            "window_days": 5,
        }
    if category in {"central_bank", "interest", "rates"} and region == "UK":
        return {
            "country": "GB",
            "category": "interest",
            "patterns": ("Bank of England Bank Rate", "BoE Bank Rate", "Policy Rate"),
            "unit": "%",
            "actual_type": "policy_rate_pct",
            "preferred_terms": ("rate",),
            "avoid_terms": (),
            "release_key": "boe_policy_rate",
            "window_days": 5,
        }
    if category in {"central_bank", "interest", "rates"} and region == "JP":
        return {
            "country": "JP",
            "category": "interest",
            "patterns": ("Bank of Japan Policy Rate", "BoJ Policy Rate", "Policy Rate"),
            "unit": "%",
            "actual_type": "policy_rate_pct",
            "preferred_terms": ("rate",),
            "avoid_terms": (),
            "release_key": "boj_policy_rate",
            "window_days": 5,
        }
    return None


def _indicator_match(conn: sqlite3.Connection, scheduled_date: str, rule: dict[str, Any]) -> sqlite3.Row | None:
    if not table_exists(conn, "indicators"):
        return None
    window_days = int(rule.get("window_days") or MATCH_WINDOW_DAYS)
    clauses = ["country = ?", "category = ?", "date <= ?", "date >= date(?, ?)"]
    params: list[Any] = [
        rule["country"],
        rule["category"],
        scheduled_date,
        scheduled_date,
        f"-{window_days} days",
    ]
    pattern_clauses = []
    for pattern in rule["patterns"]:
        pattern_clauses.append("indicator_name LIKE ?")
        params.append(f"%{pattern}%")
    clauses.append(f"({' OR '.join(pattern_clauses)})")
    rows = conn.execute(
        f"""SELECT id, date, country, category, indicator_name, value, unit, expected_value
            FROM indicators
            WHERE {' AND '.join(clauses)}
            ORDER BY date DESC, indicator_name ASC""",
        params,
    ).fetchall()
    if not rows:
        return None
    priorities = {pattern: idx for idx, pattern in enumerate(rule["patterns"])}

    def score(row: sqlite3.Row) -> int:
        name = str(row["indicator_name"] or "")
        lower = name.lower()
        row_unit = str(row["unit"] or "").lower()
        expected_unit = str(rule.get("unit") or "").lower()
        points = 0
        if expected_unit and row_unit == expected_unit:
            points += 500
        elif expected_unit:
            points -= 500
        priority = min(
            (idx for pattern, idx in priorities.items() if pattern.lower() in lower),
            default=999,
        )
        points += max(0, 300 - priority * 30)
        for term in rule.get("preferred_terms", ()):
            if str(term).lower() in lower:
                points += 80
        for term in rule.get("avoid_terms", ()):
            if str(term).lower() in lower:
                points -= 120
        actual_type = rule.get("actual_type")
        if actual_type == "yoy_pct":
            if "yoy" in lower or "annual changes" in lower:
                points += 250
            if "mom" in lower or "monthly" in lower:
                points -= 250
            if "index" in lower:
                points -= 250
        if actual_type == "mom_pct":
            if "mom" in lower or "monthly" in lower:
                points += 150
            if "yoy" in lower or "annual" in lower:
                points -= 150
        return points

    def rank(row: sqlite3.Row) -> tuple[str, int]:
        return (str(row["date"]), score(row))

    return sorted(rows, key=rank, reverse=True)[0]


def _previous_value(conn: sqlite3.Connection, row: sqlite3.Row | None) -> float | None:
    if row is None:
        return None
    previous = conn.execute(
        """SELECT value FROM indicators
           WHERE country = ? AND indicator_name = ? AND date < ?
           ORDER BY date DESC LIMIT 1""",
        (row["country"], row["indicator_name"], row["date"]),
    ).fetchone()
    return float(previous[0]) if previous and previous[0] is not None else None


def _source_rows_exist(conn: sqlite3.Connection, rule: dict[str, Any] | None) -> bool:
    if rule is None or not table_exists(conn, "indicators"):
        return False
    row = conn.execute(
        """SELECT 1 FROM indicators
           WHERE country = ? AND category = ?
           LIMIT 1""",
        (rule["country"], rule["category"]),
    ).fetchone()
    return row is not None


def _unmatched_status(conn: sqlite3.Connection, rule: dict[str, Any] | None, scheduled_date: str, today: dt.date) -> str:
    if scheduled_date > today.isoformat():
        return "waiting"
    if rule is None:
        return "unmatched_rule_gap"
    if not _source_rows_exist(conn, rule):
        return "unmatched_source_missing"
    try:
        days_since = (today - dt.date.fromisoformat(scheduled_date[:10])).days
    except ValueError:
        days_since = 999
    grace = int(rule.get("pending_days") or PENDING_GRACE_DAYS.get(str(rule.get("category")), 14))
    if days_since <= grace:
        return "unmatched_pending"
    return "unmatched_rule_gap"


def _actual_row(conn: sqlite3.Connection, event: sqlite3.Row, today: dt.date) -> tuple:
    rule = _rule(event)
    scheduled_date = str(event["date"])[:10]
    release_key = f"{rule['release_key']}:{event['id']}" if rule else f"{event['region']}:{_slug(event['title'])}:{event['id']}"
    expected_value, expected_unit, expected_text, expected_parse_status = _parse_expected(event["expected"])
    actual = None if scheduled_date > today.isoformat() or rule is None else _indicator_match(conn, scheduled_date, rule)
    previous_value = _previous_value(conn, actual)
    actual_value = float(actual["value"]) if actual and actual["value"] is not None else None
    actual_unit = actual["unit"] if actual else None
    units_compatible = (
        actual_value is not None
        and expected_value is not None
        and (not expected_unit or not actual_unit or str(expected_unit).lower() == str(actual_unit).lower())
    )
    surprise_value = (
        actual_value - expected_value
        if units_compatible
        else None
    )
    if actual is not None:
        status = "released"
    else:
        status = _unmatched_status(conn, rule, scheduled_date, today)
    metadata = {
        "rule": rule,
        "calendar_source": event["source"],
        "calendar_url": event["url"],
        "expected_parse_status": expected_parse_status,
    }
    return (
        event["id"], release_key, event["region"], event["category"], event["title"],
        scheduled_date, event["time_local"], int(event["importance"] or 3),
        actual_value, expected_value, expected_text, expected_unit,
        previous_value, surprise_value,
        "above_expected" if surprise_value and surprise_value > 0 else ("below_expected" if surprise_value and surprise_value < 0 else None),
        actual_unit,
        "indicators" if actual else None,
        actual["id"] if actual else None,
        actual["indicator_name"] if actual else None,
        actual["date"] if actual else None,
        status,
        json.dumps(metadata, sort_keys=True, default=str),
    )


def generate_and_store(days_back: int = 14, days_forward: int = 45) -> dict[str, int]:
    conn = connect_writable(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        if not table_exists(conn, "calendar_events"):
            return {"rows_seen": 0, "rows_inserted": 0, "rows_updated": 0}
        today = dt.date.today()
        rows = conn.execute(
            """SELECT id, date, time_local, region, category, importance,
                      title, expected, market_note, source, url
               FROM calendar_events
               WHERE date BETWEEN date('now', ?) AND date('now', ?)
               ORDER BY date ASC, importance DESC""",
            (f"-{int(days_back)} days", f"+{int(days_forward)} days"),
        ).fetchall()
        payload = [_actual_row(conn, row, today) for row in rows]
        before = conn.total_changes
        if payload:
            conn.executemany(
                """INSERT INTO macro_release_actuals (
                     calendar_event_id, release_key, region, category, title,
                     scheduled_date, scheduled_time_local, importance,
                     actual_value, expected_value, expected_text, expected_unit,
                     previous_value, surprise_value, surprise_text,
                     unit, source_table, source_id,
                     source_indicator_name, value_date, status, metadata_json
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(release_key, scheduled_date) DO UPDATE SET
                     calendar_event_id=excluded.calendar_event_id,
                     region=excluded.region,
                     category=excluded.category,
                     title=excluded.title,
                     scheduled_time_local=excluded.scheduled_time_local,
                     importance=excluded.importance,
                     actual_value=excluded.actual_value,
                     expected_value=excluded.expected_value,
                     expected_text=excluded.expected_text,
                     expected_unit=excluded.expected_unit,
                     previous_value=excluded.previous_value,
                     surprise_value=excluded.surprise_value,
                     surprise_text=excluded.surprise_text,
                     unit=excluded.unit,
                     source_table=excluded.source_table,
                     source_id=excluded.source_id,
                     source_indicator_name=excluded.source_indicator_name,
                     value_date=excluded.value_date,
                     status=excluded.status,
                     metadata_json=excluded.metadata_json,
                     updated_at=datetime('now')""",
                payload,
            )
        conn.commit()
        written = conn.total_changes - before
        released = sum(1 for row in payload if row[20] == "released")
        queued = 0
        if released:
            release_rows = conn.execute(
                """SELECT *
                   FROM macro_release_actuals
                   WHERE status = 'released'
                     AND scheduled_date BETWEEN date('now', ?) AND date('now', ?)
                     AND importance >= 4""",
                (f"-{int(days_back)} days", f"+{int(days_forward)} days"),
            ).fetchall()
            for release_row in release_rows:
                queued += queue_high_impact_macro_release(conn, release_row)
            conn.commit()
        log(f"Macro actuals: {len(payload)} calendar row(s), {released} released match(es).", module="macro_actuals")
        return {"rows_seen": len(rows), "rows_inserted": written, "rows_updated": written, "metadata": {"change_events_queued": queued}}
    finally:
        conn.close()


if __name__ == "__main__":
    generate_and_store()
