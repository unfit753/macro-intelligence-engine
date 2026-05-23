"""Generate macro-event scenario ladders and event-level forecasts."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
from functools import lru_cache
from typing import Any

from config.config_fetch import CLAUDE_MODEL, DB_PATH, log
from src.core.db import connect_writable, table_exists
from config.db_setup import SCHEMA, add_missing_columns


MODEL = "deterministic_macro_event_scenarios_v1"
RESULT_MODEL = "deterministic_macro_event_results_v1"
FORECAST_MODEL = "deterministic_macro_event_forecasts_v1"


REGION_ASSETS = {
    "US": ["SPY", "TLT", "DX-Y.NYB", "GC=F", "^OMX", "^STOXX50E"],
    "EU": ["^STOXX50E", "^OMX", "EURUSD=X", "GC=F", "TLT"],
    "SE": ["^OMX", "SEK=X", "^STOXX50E", "GC=F"],
    "UK": ["^FTSE", "GBPUSD=X", "^STOXX50E"],
    "JP": ["^N225", "JPY=X", "SPY"],
    "CN": ["^HSI", "CL=F", "GC=F", "SPY"],
}

COUNTRY_ALIASES = {"UK": "GB", "EZ": "EU", "EUROZONE": "EU", "SWEDEN": "SE"}


EVENT_FORECAST_TOOL = {
    "name": "submit_macro_event_forecast",
    "description": "Submit a forecast for one scheduled macro calendar event.",
    "input_schema": {
        "type": "object",
        "properties": {
            "forecast_direction": {
                "type": "string",
                "description": "Plain short direction, e.g. hotter, cooler, hold, cut_bias, hike_risk, stronger, weaker, mixed.",
            },
            "confidence_0_1": {"type": "number", "minimum": 0, "maximum": 1},
            "summary": {
                "type": "string",
                "description": "One short sentence in plain language about what you think happens.",
            },
            "rationale_md": {
                "type": "string",
                "description": "2-4 concise paragraphs. Use the provided indicator trends and historical analogue dates only.",
            },
            "historical_analogues_used": {
                "type": "array", "items": {"type": "string"},
                "description": "Analogue dates or handles from the prompt you actually relied on.",
            },
            "watch_items": {
                "type": "array", "items": {"type": "string"},
                "description": "2-5 details that would change the view once the release lands.",
            },
        },
        "required": [
            "forecast_direction", "confidence_0_1", "summary", "rationale_md",
            "historical_analogues_used", "watch_items",
        ],
    },
}


def _event_key(event_id: int, release_date: str, region: str, title: str) -> str:
    raw = f"{event_id}:{release_date}:{region}:{title}"
    digest = hashlib.sha256(raw.encode()).hexdigest()[:10]
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in title).strip("-")[:44]
    return f"{release_date}-{region.lower()}-{slug}-{digest}"


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _country(region: str | None) -> str:
    reg = str(region or "").upper()
    return COUNTRY_ALIASES.get(reg, reg)


def _event_family(category: str | None, title: str | None) -> str:
    text = f"{category or ''} {title or ''}".lower()
    if any(x in text for x in ("fomc", "ecb", "riksbank", "boe", "boj", "rate decision", "central_bank")):
        return "central_bank"
    if "pce" in text:
        return "pce_inflation"
    if "ppi" in text or "producer price" in text:
        return "producer_prices"
    if any(x in text for x in ("cpi", "cpif", "hicp", "inflation")):
        return "inflation"
    if any(x in text for x in ("payroll", "employment", "unemployment", "jolts", "jobs", "labour", "labor", "wage")):
        return "labour"
    if "gdp" in text:
        return "gdp"
    if any(x in text for x in ("oil", "petroleum", "eia", "inventor", "energy")):
        return "energy"
    if any(x in text for x in ("retail", "consumer spending")):
        return "consumer"
    if any(x in text for x in ("pmi", "manufactur", "industrial", "production")):
        return "industry"
    return "macro"


def _affected_assets(region: str, category: str, title: str) -> list[str]:
    assets = REGION_ASSETS.get(region.upper(), ["SPY", "^STOXX50E", "^OMX", "GC=F"])
    haystack = f"{category} {title}".lower()
    if "oil" in haystack or "energy" in haystack or "hormuz" in haystack:
        assets += ["CL=F", "BZ=F"]
    if any(x in haystack for x in ("inflation", "cpi", "hicp", "cpif", "pce", "ppi")):
        assets += ["TLT", "GC=F"]
    if any(x in haystack for x in ("fomc", "ecb", "riksbank", "rate")):
        assets += ["TLT", "DX-Y.NYB", "GC=F"]
    return sorted(set(assets))


def _representative_asset(region: str, family: str, assets: list[str]) -> str:
    if family == "energy":
        return "CL=F"
    if family in {"inflation", "pce_inflation", "producer_prices", "central_bank"}:
        return "TLT" if "TLT" in assets else assets[0]
    if region.upper() == "SE" and "^OMX" in assets:
        return "^OMX"
    if region.upper() == "EU" and "^STOXX50E" in assets:
        return "^STOXX50E"
    return assets[0] if assets else "SPY"


def _scenario_ladder(category: str, region: str, title: str) -> tuple[str, list[dict[str, str]]]:
    haystack = f"{category} {title}".lower()
    if any(x in haystack for x in ("inflation", "cpi", "hicp", "cpif", "pce", "ppi")):
        return "inline", [
            {
                "bucket": "below_expectation",
                "macro_read": "Disinflation surprise; lower rate-pressure if confirmed by core details.",
                "likely_market_effect": "Rates/yields down, duration supported, equities usually supported unless growth scare dominates; gold mixed-to-positive on lower real-rate pressure.",
            },
            {
                "bucket": "inline",
                "macro_read": "Consensus broadly confirmed; market reaction should depend on positioning and details.",
                "likely_market_effect": "Lower volatility unless prior positioning was stretched; focus moves to next central-bank communication.",
            },
            {
                "bucket": "above_expectation",
                "macro_read": "Inflation pressure persists; central-bank easing expectations likely pushed out.",
                "likely_market_effect": "Yields and currency usually higher, equities/rate-sensitive sectors pressured, gold depends on real yields versus safe-haven demand.",
            },
        ]
    if any(x in haystack for x in ("central_bank", "fomc", "ecb", "riksbank", "boe", "boj", "rate")):
        return "inline", [
            {
                "bucket": "dovish_surprise",
                "macro_read": "Policy path shifts easier than expected.",
                "likely_market_effect": "Duration and equities supported, local currency softer, gold often supported if real rates fall.",
            },
            {
                "bucket": "inline",
                "macro_read": "Policy message matches consensus.",
                "likely_market_effect": "Reaction depends on press-conference details and prior positioning.",
            },
            {
                "bucket": "hawkish_surprise",
                "macro_read": "Policy path shifts tighter than expected.",
                "likely_market_effect": "Yields/local currency higher, equities and long-duration assets pressured, spillover to global risk if major central bank.",
            },
        ]
    if any(x in haystack for x in ("labour", "labor", "jobs", "payroll", "employment", "unemployment", "jolts")):
        return "inline", [
            {
                "bucket": "weak",
                "macro_read": "Labour cooling; growth risk rises but rate-pressure eases.",
                "likely_market_effect": "Duration supported; equities depend on whether markets treat it as soft landing or recession signal.",
            },
            {
                "bucket": "inline",
                "macro_read": "No clear macro surprise.",
                "likely_market_effect": "Limited reaction unless wages/unemployment details diverge.",
            },
            {
                "bucket": "strong",
                "macro_read": "Labour resilience; growth holds but inflation/rate pressure can rise.",
                "likely_market_effect": "Yields/currency higher; equities mixed, with cyclicals better than long duration.",
            },
        ]
    return "inline", [
        {
            "bucket": "negative_surprise",
            "macro_read": "Data or event outcome is worse for growth/risk appetite than expected.",
            "likely_market_effect": "Risk assets pressured, safe-haven demand higher, local assets underperform if region-specific.",
        },
        {
            "bucket": "inline",
            "macro_read": "Outcome close to expected path.",
            "likely_market_effect": "Reaction mostly positioning-driven.",
        },
        {
            "bucket": "positive_surprise",
            "macro_read": "Outcome is better for growth/risk appetite than expected.",
            "likely_market_effect": "Risk assets supported; safe havens less bid unless inflation/rate implications dominate.",
        },
    ]


def _month_from_title(title: str, fallback_release_date: str) -> str | None:
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    text = title.lower()
    year = None
    for token in text.replace("/", " ").replace("-", " ").split():
        if token.isdigit() and len(token) == 4:
            year = int(token)
            break
    for name, month in months.items():
        if name in text and year:
            return dt.date(year, month, 1).isoformat()
    try:
        released = dt.date.fromisoformat(fallback_release_date)
    except ValueError:
        return None
    month = released.month - 1
    year = released.year
    if month == 0:
        month = 12
        year -= 1
    return dt.date(year, month, 1).isoformat()


def _actual_indicator_preferences(region: str, category: str, title: str) -> list[str]:
    haystack = f"{region} {category} {title}".lower()
    reg = region.upper()
    if "pce" in haystack:
        return ["PCE Price Index"] if reg == "US" else []
    if "ppi" in haystack or "producer price" in haystack:
        return ["PPI All Commodities"] if reg == "US" else []
    if not any(x in haystack for x in ("inflation", "cpi", "hicp", "cpif")):
        return []
    if reg == "US":
        return [
            "BLS CPI-U All Items NSA YoY",
            "BLS Core CPI-U NSA YoY",
            "BLS CPI-U All Items SA MoM",
            "BLS Core CPI-U SA MoM",
            "CPI",
            "Core CPI",
        ]
    if reg in {"SE", "SWEDEN"}:
        return [
            "SCB CPIF, annual changes, 2020=100",
            "SCB Annual changes",
            "SCB CPIF, monthly changes, 2020=100",
            "SCB HICP, annual changes, 2025=100",
        ]
    if reg in {"EU", "EZ"}:
        return ["Eurostat HICP YoY (EU)", "Eurozone HICP"]
    if reg in {"UK", "GB"}:
        return ["IMF Consumer Price Inflation (PCPIPCH)"]
    return []


def _macro_actual_row(conn: sqlite3.Connection, event_id: int, event_key: str) -> dict[str, Any] | None:
    if not table_exists(conn, "macro_release_actuals"):
        return None
    row = conn.execute(
        """SELECT actual_value, surprise_text, status, source_indicator_name,
                  value_date, unit, expected_value, previous_value, metadata_json
           FROM macro_release_actuals
           WHERE (calendar_event_id = ? OR release_key = ?)
             AND status = 'released'
           ORDER BY updated_at DESC
           LIMIT 1""",
        (event_id, event_key),
    ).fetchone()
    if not row:
        return None
    actual_value, surprise_text, status, indicator, value_date, unit, expected, previous, metadata_json = row
    detail = [{
        "date": value_date,
        "indicator": indicator,
        "value": float(actual_value) if actual_value is not None else None,
        "unit": unit,
        "expected_value": expected,
        "previous_value": previous,
        "metadata": json.loads(metadata_json or "{}"),
    }]
    pieces = []
    if indicator and actual_value is not None:
        pieces.append(f"{indicator}: {float(actual_value):.3g}{unit or ''}")
    if surprise_text:
        pieces.append(str(surprise_text))
    return {
        "status": status,
        "actual_value": float(actual_value) if actual_value is not None else None,
        "actual_detail": detail,
        "actual_summary": "; ".join(pieces),
        "actual_surprise": surprise_text or "actual_available",
    }


def _find_actuals(conn: sqlite3.Connection, event_id: int, event_key: str, release_date: str,
                  region: str, category: str, title: str) -> dict[str, Any]:
    matched = _macro_actual_row(conn, event_id, event_key)
    if matched:
        return matched
    preferences = _actual_indicator_preferences(region, category, title)
    if not preferences:
        return {"status": "no_match_rule"}
    target_month = _month_from_title(title, release_date)
    if not target_month:
        return {"status": "no_target_month"}
    country = _country(region)
    values: list[dict[str, Any]] = []
    for name in preferences:
        row = conn.execute(
            """SELECT date, indicator_name, value, unit, impact
               FROM indicators
               WHERE country = ?
                 AND indicator_name = ?
                 AND date <= ?
               ORDER BY date DESC
               LIMIT 1""",
            (country, name, target_month),
        ).fetchone()
        if not row:
            continue
        d, indicator_name, value, unit, impact = row
        if d != target_month:
            continue
        values.append({
            "date": d,
            "indicator": indicator_name,
            "value": float(value) if value is not None else None,
            "unit": unit,
            "impact": impact,
        })
    if not values:
        return {"status": "awaiting_actual", "target_month": target_month}
    actual_value = next((v["value"] for v in values if v["value"] is not None), None)
    pieces = []
    for item in values[:4]:
        val = item["value"]
        if val is None:
            continue
        unit = item["unit"] or ""
        pieces.append(f"{item['indicator']}: {val:.3g}{unit}")
    return {
        "status": "released",
        "target_month": target_month,
        "actual_value": actual_value,
        "actual_detail": values,
        "actual_summary": "; ".join(pieces),
        "actual_surprise": "actual_available_consensus_unstructured",
    }


def _patterns_for_family(family: str, region: str) -> list[tuple[str, list[str]]]:
    if family == "pce_inflation":
        return [("inflation", ["pce price"]), ("inflation", ["cpi", "core cpi"])]
    if family == "producer_prices":
        return [("inflation", ["ppi", "producer price"]), ("inflation", ["cpi", "core cpi"])]
    if family == "inflation":
        if region.upper() in {"SE", "SWEDEN"}:
            return [("inflation", ["cpif", "annual changes", "monthly changes", "hicp"])]
        if region.upper() in {"EU", "EZ"}:
            return [("inflation", ["hicp"])]
        return [("inflation", ["cpi", "core cpi", "inflation"]), ("inflation", ["pce price"])]
    if family == "central_bank":
        return [("interest", ["policy rate", "fed funds", "deposit rate", "refi rate", "short-term rate"])]
    if family == "labour":
        return [("labour", ["payroll", "unemployment", "jolts", "wage", "employment"])]
    if family == "gdp":
        return [("gdp", ["gdp"])]
    if family == "energy":
        return [("inflation", ["ppi"])]
    if family == "consumer":
        return [("retail_sales", ["retail"]), ("sentiment", ["consumer sentiment", "consumer confidence"])]
    if family == "industry":
        return [("industry", ["industrial", "manufacturing", "production"])]
    return [("inflation", ["cpi"]), ("interest", ["yield", "rate"])]


def _indicator_priority(name: str, family: str) -> int:
    text = name.lower()
    score = 0
    if any(x in text for x in ("yoy", "annual changes", "unemployment rate", "policy rate", "deposit rate")):
        score -= 30
    if any(x in text for x in ("mom", "monthly changes", "payroll", "retail", "sentiment")):
        score -= 20
    if any(x in text for x in ("core", "cpif", "hicp", "pce")):
        score -= 8
    if "index" in text and family not in {"pce_inflation", "producer_prices", "gdp", "energy"}:
        score += 18
    if family == "labour" and "unemployment" in text:
        score -= 8
    return score


def _candidate_indicator_names(conn: sqlite3.Connection, country: str, category: str,
                               patterns: list[str], as_of: dt.date, family: str) -> list[str]:
    if not patterns:
        return []
    clauses = " OR ".join("LOWER(indicator_name) LIKE ?" for _ in patterns)
    params: list[Any] = [country, category, as_of.isoformat()] + [f"%{p.lower()}%" for p in patterns]
    rows = conn.execute(
        f"""SELECT indicator_name, MAX(date) AS latest_date, COUNT(*) AS n
            FROM indicators
            WHERE country = ? AND category = ? AND date <= ? AND ({clauses})
            GROUP BY indicator_name
            ORDER BY latest_date DESC, n DESC""",
        params,
    ).fetchall()
    names = [r[0] for r in rows]
    names.sort(key=lambda n: (_indicator_priority(n, family), n.lower()))
    return names[:5]


def _series_for_indicator(conn: sqlite3.Connection, country: str, category: str,
                          name: str, as_of: dt.date, limit: int = 8) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT date, value, unit
           FROM indicators
           WHERE country = ? AND category = ? AND indicator_name = ? AND date <= ?
           ORDER BY date DESC
           LIMIT ?""",
        (country, category, name, as_of.isoformat(), limit),
    ).fetchall()
    return [{"date": d, "value": float(v), "unit": unit} for d, v, unit in rows if v is not None]


def _series_change(name: str, series: list[dict[str, Any]], family: str) -> tuple[float | None, str]:
    if len(series) < 2:
        return None, "not enough history"
    latest = float(series[0]["value"])
    prev = float(series[1]["value"])
    unit = str(series[0].get("unit") or "")
    text = f"{name} {unit}".lower()
    if prev == 0:
        return None, "previous value is zero"
    if any(x in text for x in ("index", "gdp", "pce price", "ppi all commodities", "retail sales", "industrial production", "payrolls")):
        change = (latest / prev - 1.0) * 100.0
        return change, "percent_change"
    return latest - prev, "point_change"


def _trend_from_change(name: str, change: float | None, basis: str, family: str) -> str:
    if change is None:
        return "unknown"
    text = name.lower()
    threshold = 0.05 if basis == "point_change" else 0.15
    if "payroll" in text:
        threshold = 0.03
    if family in {"pce_inflation", "producer_prices", "energy"} and basis == "percent_change":
        threshold = 0.20
    if abs(change) <= threshold:
        return "steady"
    return "rising" if change > 0 else "falling"


def _format_value(value: float | None, unit: str | None) -> str:
    if value is None:
        return "n/a"
    suffix = unit or ""
    if abs(value) >= 1000:
        return f"{value:,.0f}{suffix}"
    return f"{value:.3g}{suffix}"


def _indicator_sentence(name: str, latest: dict[str, Any], previous: dict[str, Any] | None,
                        change: float | None, basis: str, trend: str) -> str:
    latest_value = _format_value(latest.get("value"), latest.get("unit"))
    if not previous or change is None:
        return f"{name}: {latest_value} on {latest.get('date')}."
    prev_value = _format_value(previous.get("value"), latest.get("unit"))
    if basis == "percent_change":
        move = f"{change:+.2f}% from previous"
    else:
        move = f"{change:+.2f} from previous"
    return f"{name}: {latest_value} on {latest.get('date')} vs {prev_value} on {previous.get('date')} ({move}, {trend})."


def _collect_indicator_evidence(conn: sqlite3.Connection, as_of: dt.date,
                                region: str, family: str) -> list[dict[str, Any]]:
    country = _country(region)
    evidence: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for category, patterns in _patterns_for_family(family, region):
        for name in _candidate_indicator_names(conn, country, category, patterns, as_of, family):
            key = (category, name)
            if key in seen:
                continue
            seen.add(key)
            series = _series_for_indicator(conn, country, category, name, as_of)
            if not series:
                continue
            change, basis = _series_change(name, series, family)
            trend = _trend_from_change(name, change, basis, family)
            latest = series[0]
            previous = series[1] if len(series) > 1 else None
            evidence.append({
                "country": country,
                "category": category,
                "indicator": name,
                "latest_date": latest["date"],
                "latest_value": latest["value"],
                "unit": latest.get("unit"),
                "previous_date": previous["date"] if previous else None,
                "previous_value": previous["value"] if previous else None,
                "change": change,
                "change_basis": basis,
                "trend": trend,
                "readable": _indicator_sentence(name, latest, previous, change, basis, trend),
            })
            if len(evidence) >= 5:
                return evidence
    return evidence


def _price_momentum(conn: sqlite3.Connection, symbol: str, as_of: dt.date) -> dict[str, Any] | None:
    if not table_exists(conn, "prices"):
        return None
    rows = conn.execute(
        """SELECT date, price
           FROM prices
           WHERE symbol = ? AND date <= ?
           ORDER BY date DESC
           LIMIT 22""",
        (symbol, as_of.isoformat()),
    ).fetchall()
    if len(rows) < 2:
        return None
    latest_date, latest_price = rows[0]
    prev_date, prev_price = rows[min(len(rows) - 1, 5)]
    start_date, start_price = rows[-1]
    def pct(a: float, b: float) -> float | None:
        return ((a / b) - 1.0) * 100.0 if b else None
    return {
        "symbol": symbol,
        "latest_date": latest_date,
        "latest_price": float(latest_price),
        "move_1w_pct": pct(float(latest_price), float(prev_price)),
        "move_window_pct": pct(float(latest_price), float(start_price)),
        "window_start": start_date,
        "week_start": prev_date,
    }


def _direction_from_evidence(family: str, evidence: list[dict[str, Any]], price: dict[str, Any] | None) -> tuple[str, str]:
    rising = sum(1 for e in evidence if e.get("trend") == "rising")
    falling = sum(1 for e in evidence if e.get("trend") == "falling")
    if family in {"inflation", "pce_inflation", "producer_prices"}:
        if rising > falling:
            return "hotter", "inflation pressure looks more likely to rise or stay sticky"
        if falling > rising:
            return "cooler", "inflation pressure looks more likely to ease"
        return "sticky", "inflation looks close to the recent trend"
    if family == "central_bank":
        inflation_evidence = [e for e in evidence if "inflation" in str(e.get("category"))]
        if inflation_evidence:
            inf_rising = sum(1 for e in inflation_evidence if e.get("trend") == "rising")
            inf_falling = sum(1 for e in inflation_evidence if e.get("trend") == "falling")
            if inf_rising > inf_falling:
                return "hold_hawkish", "rates are more likely to stay firm; cut odds should be treated carefully"
            if inf_falling > inf_rising:
                return "cut_bias", "the setup leans toward easier policy language if officials trust the disinflation"
        return "hold", "the most likely central-bank outcome is no dramatic shift"
    if family == "labour":
        unemployment_rising = any("unemployment" in str(e.get("indicator", "")).lower() and e.get("trend") == "rising" for e in evidence)
        payroll_rising = any("payroll" in str(e.get("indicator", "")).lower() and e.get("trend") == "rising" for e in evidence)
        if unemployment_rising and not payroll_rising:
            return "weaker", "labour looks more likely to cool"
        if payroll_rising and not unemployment_rising:
            return "stronger", "labour still looks resilient"
        return "mixed", "labour signals look mixed rather than one-way"
    if family == "gdp":
        if rising > falling:
            return "stronger", "growth momentum looks firmer"
        if falling > rising:
            return "weaker", "growth momentum looks softer"
        return "mixed", "growth looks close to its recent path"
    if family == "energy":
        move = (price or {}).get("move_1w_pct")
        if move is not None and move > 2.5:
            return "tightening", "oil pressure is rising into the release"
        if move is not None and move < -2.5:
            return "easing", "oil pressure is easing into the release"
        return "mixed", "energy pressure is not clearly one-way"
    if rising > falling:
        return "stronger", "the latest data leans stronger"
    if falling > rising:
        return "weaker", "the latest data leans weaker"
    return "mixed", "the latest data is mixed"


@lru_cache(maxsize=64)
def _fetch_analogues(as_of: dt.date, asset: str, horizon: str = "1m", k: int = 5) -> list[dict[str, Any]]:
    try:
        from src.retrieval.query import fetch_analogues
        rows = fetch_analogues(as_of, asset=asset, horizon=horizon, k=k)
    except Exception as exc:  # pragma: no cover - qdrant is optional in unit tests
        return [{"status": "unavailable", "reason": str(exc)[:180], "asset": asset, "horizon": horizon}]
    analogues: list[dict[str, Any]] = []
    for row in rows or []:
        if isinstance(row, dict):
            analogues.append({k: row.get(k) for k in ("date", "as_of", "score", "distance", "forward_return", "summary") if k in row})
        else:
            analogues.append({"value": str(row)})
    return analogues[:k]


def _watch_items(family: str) -> list[str]:
    if family in {"inflation", "pce_inflation", "producer_prices"}:
        return ["core versus headline", "monthly pace", "services/sticky components", "bond-yield reaction"]
    if family == "central_bank":
        return ["vote split", "statement language", "inflation forecast path", "press-conference tone"]
    if family == "labour":
        return ["headline jobs", "unemployment rate", "wage growth", "participation rate"]
    if family == "energy":
        return ["crude inventory draw/build", "product inventories", "refinery utilisation", "oil price reaction"]
    if family == "gdp":
        return ["consumption", "investment", "inventory contribution", "inflation deflator"]
    return ["headline surprise", "revision details", "market reaction", "follow-up official commentary"]


def _forecast_from_patterns(conn: sqlite3.Connection, as_of: dt.date, region: str,
                            category: str, title: str, importance: int,
                            assets: list[str]) -> dict[str, Any]:
    family = _event_family(category, title)
    evidence = _collect_indicator_evidence(conn, as_of, region, family)
    if family == "central_bank":
        evidence = (evidence + _collect_indicator_evidence(conn, as_of, region, "inflation")[:3])[:6]
    representative_asset = _representative_asset(region, family, assets)
    price = _price_momentum(conn, representative_asset, as_of)
    if family == "energy":
        price = _price_momentum(conn, "CL=F", as_of) or price
    direction, read = _direction_from_evidence(family, evidence, price)
    analogues = _fetch_analogues(as_of, representative_asset, "1m", k=5)
    confidence = 0.48 + min(0.18, 0.035 * len([e for e in evidence if e.get("trend") != "unknown"])) + min(0.10, max(0, importance - 3) * 0.035)
    if not evidence:
        confidence -= 0.12
    if analogues and analogues[0].get("status") != "unavailable":
        confidence += 0.05
    confidence = round(max(0.32, min(0.82, confidence)), 2)
    evidence_text = "; ".join(e.get("readable", "") for e in evidence[:3]) or "No tight same-family indicator trend was found in the local history."
    price_text = ""
    if price and price.get("move_1w_pct") is not None:
        price_text = f" {representative_asset} moved {price['move_1w_pct']:+.1f}% over the recent price window."
    summary = f"{read.capitalize()}."
    rationale = (
        f"{summary} The local read is based on the latest known values on or before {as_of.isoformat()}, so it avoids looking past the forecast date. "
        f"Evidence: {evidence_text}.{price_text}\n\n"
        f"Historical reference handle: {representative_asset} / 1m analogues. Treat the analogue dates as pattern references, not a guarantee of the release number."
    )
    return {
        "family": family,
        "forecast_direction": direction,
        "forecast_confidence": confidence,
        "forecast_summary": summary,
        "forecast_rationale_md": rationale,
        "representative_asset": representative_asset,
        "indicator_evidence": evidence,
        "price_context": price,
        "historical_analogues": analogues,
        "watch_items": _watch_items(family),
    }


def _build_claude_prompt(payload: dict[str, Any]) -> str:
    pattern = payload.get("historical_pattern") or {}
    evidence = pattern.get("indicator_evidence") or []
    analogues = pattern.get("historical_analogues") or []
    watch = pattern.get("watch_items") or []
    lines = [
        "You are Claude acting as Macro Intelligence Engine' macro calendar analyst.",
        "Forecast the scheduled macro event itself: will the data/policy read hotter, cooler, stronger, weaker, hold, cut-bias, or hike-risk?",
        "Use only the provided current indicators and historical analogue handles. Do not invent consensus numbers.",
        "Keep it useful for a reader who does not know macro jargon.",
        "",
        f"Event: {payload.get('title')}",
        f"Date/time: {payload.get('release_date')} {payload.get('release_time_local') or ''}",
        f"Region/category: {payload.get('region')} / {payload.get('category')}",
        f"Importance: {payload.get('importance')}/5",
        f"Expected/context: {payload.get('expected') or 'not structured'}",
        f"Deterministic local read: {payload.get('forecast_summary')}",
        "",
        "Current indicator evidence:",
    ]
    if evidence:
        lines.extend(f"- {item.get('readable')}" for item in evidence[:6])
    else:
        lines.append("- No tight same-family indicator evidence was found.")
    lines.append("")
    lines.append("Historical analogue handles:")
    if analogues:
        for item in analogues[:5]:
            lines.append(f"- {json.dumps(item, sort_keys=True, default=str)}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Watch items after release:")
    lines.extend(f"- {item}" for item in watch[:5])
    lines.append("")
    lines.append("Submit one forecast through the tool. Plain language first, caveats second.")
    return "\n".join(lines)


def _extract_event_tool_use(msg) -> dict[str, Any] | None:
    for block in msg.content:
        if block.type == "tool_use" and block.name == "submit_macro_event_forecast":
            return block.input
    return None


def _call_claude_event_forecast(prompt: str) -> dict[str, Any] | None:
    try:
        from anthropic import Anthropic
        from dotenv import load_dotenv
    except Exception as exc:  # pragma: no cover - optional dependency path
        log(f"Claude forecast unavailable: {exc}", module="macro_events")
        return None
    load_dotenv(override=True)
    if not os.getenv("ANTHROPIC_API_KEY"):
        log("ANTHROPIC_API_KEY not set; keeping deterministic macro-event forecasts.", module="macro_events")
        return None
    client = Anthropic()
    msg = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1200,
        temperature=0.2,
        system=(
            "You are a careful macro analyst. Forecast scheduled releases using the provided local "
            "Macro Intelligence Engine context and historical analogue handles. Do not invent exact consensus numbers. "
            "Use the tool exactly once."
        ),
        tools=[EVENT_FORECAST_TOOL],
        tool_choice={"type": "tool", "name": "submit_macro_event_forecast"},
        messages=[{"role": "user", "content": prompt}],
    )
    return _extract_event_tool_use(msg)


def _apply_claude_forecast(payload: dict[str, Any], forecast: dict[str, Any] | None) -> None:
    if not forecast:
        return
    try:
        conf = float(forecast.get("confidence_0_1"))
    except (TypeError, ValueError):
        conf = payload.get("forecast_confidence") or payload.get("confidence") or 0.5
    payload["forecast_direction"] = str(forecast.get("forecast_direction") or payload.get("forecast_direction") or "mixed")[:80]
    payload["forecast_confidence"] = max(0.0, min(1.0, conf))
    payload["forecast_summary"] = str(forecast.get("summary") or payload.get("forecast_summary") or "")[:500]
    payload["forecast_rationale_md"] = str(forecast.get("rationale_md") or payload.get("forecast_rationale_md") or "")
    payload["claude_forecast"] = forecast
    payload["claude_model"] = CLAUDE_MODEL
    payload["claude_at"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def build_predictions(conn: sqlite3.Connection, as_of: dt.date | None = None,
                      days_forward: int = 45, days_back: int = 7,
                      live_claude: bool = False, claude_limit: int = 0) -> list[dict[str, Any]]:
    as_of = as_of or dt.date.today()
    generated_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    rows = conn.execute(
        """SELECT id, date, time_local, region, category, importance, title,
                  expected, market_note, source, url
           FROM calendar_events
           WHERE date BETWEEN date(?, ?) AND date(?, ?)
             AND importance >= 4
           ORDER BY date ASC, importance DESC, region""",
        (as_of.isoformat(), f"-{days_back} days", as_of.isoformat(), f"+{days_forward} days"),
    ).fetchall()
    out: list[dict[str, Any]] = []
    claude_calls = 0
    for row in rows:
        event_id, release_date, time_local, region, category, importance, title, expected, market_note, source, url = row
        region = region or "Global"
        category = category or "macro"
        title = title or "Macro event"
        event_key = _event_key(event_id, release_date, region, title)
        bucket, scenarios = _scenario_ladder(category, region, title)
        assets = _affected_assets(region, category, title)
        actual = _find_actuals(conn, event_id, event_key, release_date, region, category, title)
        pattern = _forecast_from_patterns(conn, as_of, region, category, title, int(importance or 3), assets)
        rationale = (
            f"Scenario ladder generated for {title}. Baseline expectation/context: "
            f"{expected or 'not structured'}. Market note: {market_note or 'n/a'}. "
            "Use this as macro-event research, not a precise numeric nowcast."
        )
        if actual.get("status") == "released":
            rationale += f"\n\nActual/result crosscheck ({RESULT_MODEL}): {actual.get('actual_summary') or 'actual available'}."
        payload = {
            "calendar_event_id": event_id,
            "event_key": event_key,
            "as_of": as_of.isoformat(),
            "generated_at": generated_at,
            "release_date": release_date,
            "release_time_local": time_local,
            "region": region,
            "country": region if len(str(region or "")) <= 3 else None,
            "category": category,
            "title": title,
            "importance": int(importance or 3),
            "expected": expected,
            "previous_value": None,
            "predicted_surprise_bucket": bucket,
            "confidence": min(0.78, 0.42 + 0.06 * int(importance or 3)),
            "scenario_json": scenarios,
            "affected_assets": assets,
            "rationale_md": rationale,
            "key_risks": [
                "Consensus/expected value may be incomplete or stale.",
                "Initial market reaction can reverse after details or central-bank commentary.",
                "Cross-asset effects depend on positioning and concurrent headlines.",
            ],
            "source": source,
            "url": url,
            "actual_value": actual.get("actual_value"),
            "actual_surprise": actual.get("actual_surprise"),
            "actual_detail": actual.get("actual_detail"),
            "actual_summary": actual.get("actual_summary"),
            "result_status": actual.get("status"),
            "model": MODEL,
            "forecast_direction": pattern["forecast_direction"],
            "forecast_confidence": pattern["forecast_confidence"],
            "forecast_summary": pattern["forecast_summary"],
            "forecast_rationale_md": pattern["forecast_rationale_md"],
            "historical_pattern": pattern,
            "claude_forecast": None,
            "claude_model": None,
            "claude_at": None,
        }
        release_is_future = str(release_date) >= as_of.isoformat()
        if live_claude and release_is_future and (claude_limit <= 0 or claude_calls < claude_limit):
            try:
                forecast = _call_claude_event_forecast(_build_claude_prompt(payload))
                _apply_claude_forecast(payload, forecast)
                if forecast:
                    claude_calls += 1
                    log(f"Claude macro-event forecast added for {title}", module="macro_events")
            except Exception as exc:
                log(f"Claude macro-event forecast failed for {title}: {exc}", module="macro_events")
        payload["input_hash"] = _hash_payload(payload)
        out.append(payload)
    return out


def store_predictions(conn: sqlite3.Connection, predictions: list[dict[str, Any]]) -> int:
    rows = [
        (
            p["calendar_event_id"], p["event_key"], p["as_of"], p["generated_at"],
            p["release_date"], p["release_time_local"], p["region"], p["country"],
            p["category"], p["title"], p["importance"], p["expected"], p["previous_value"],
            p["predicted_surprise_bucket"], p["confidence"], json.dumps(p["scenario_json"]),
            json.dumps(p["affected_assets"]), p["rationale_md"], json.dumps(p["key_risks"]),
            p["source"], p["url"], p["model"], p["input_hash"], p.get("actual_value"),
            p.get("actual_surprise"), json.dumps(p.get("actual_detail") or []),
            p.get("actual_summary"), p.get("result_status"),
            p.get("forecast_direction"), p.get("forecast_confidence"), p.get("forecast_summary"),
            p.get("forecast_rationale_md"), json.dumps(p.get("historical_pattern") or {}),
            json.dumps(p.get("claude_forecast") or {}), p.get("claude_model"), p.get("claude_at"),
        )
        for p in predictions
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO macro_event_predictions (
             calendar_event_id, event_key, as_of, generated_at, release_date,
             release_time_local, region, country, category, title, importance,
             expected, previous_value, predicted_surprise_bucket, confidence,
             scenario_json, affected_assets_json, rationale_md, key_risks_json,
             source, url, model, input_hash, actual_value, actual_surprise,
             actual_detail_json, actual_summary, result_status,
             forecast_direction, forecast_confidence, forecast_summary,
             forecast_rationale_md, historical_pattern_json,
             claude_forecast_json, claude_model, claude_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


def generate_and_store(as_of: dt.date | None = None, days_forward: int = 45, days_back: int = 7,
                       live_claude: bool = False, claude_limit: int = 0) -> int:
    conn = connect_writable(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        predictions = build_predictions(
            conn,
            as_of=as_of,
            days_forward=days_forward,
            days_back=days_back,
            live_claude=live_claude,
            claude_limit=claude_limit,
        )
        n = store_predictions(conn, predictions)
        conn.commit()
        log(f"Generated {n} macro-event forecast(s).", module="macro_events")
        return n
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to today")
    parser.add_argument("--days-forward", type=int, default=45)
    parser.add_argument("--days-back", type=int, default=7)
    parser.add_argument("--live-claude", action="store_true", help="Ask Claude for an overlay forecast for upcoming events.")
    parser.add_argument("--claude-limit", type=int, default=8, help="Maximum Claude event forecasts for this run; 0 means no cap.")
    args = parser.parse_args(argv)
    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else None
    generate_and_store(as_of, args.days_forward, args.days_back, live_claude=args.live_claude, claude_limit=args.claude_limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
