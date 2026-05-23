"""Compile raw feeds into stored "where is the world pulling" packages."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass, asdict
from typing import Any

import pandas as pd

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable
from config.db_setup import SCHEMA, add_missing_columns


MODEL = "deterministic_world_pull_v1"

REGION_ALIASES = {
    "GLOBAL": ("global", "Global", None),
    "US": ("region", "US", "Global"),
    "USA": ("region", "US", "Global"),
    "EU": ("region", "EU", "Global"),
    "EZ": ("region", "EU", "Global"),
    "EUR": ("region", "EU", "Global"),
    "SE": ("nation", "Sweden", "EU"),
    "SWE": ("nation", "Sweden", "EU"),
    "SWEDEN": ("nation", "Sweden", "EU"),
    "DE": ("nation", "Germany", "EU"),
    "FR": ("nation", "France", "EU"),
    "GB": ("nation", "United Kingdom", "Europe"),
    "UK": ("nation", "United Kingdom", "Europe"),
    "JP": ("region", "Asia", "Global"),
    "CN": ("region", "Asia", "Global"),
    "KR": ("region", "Asia", "Global"),
    "HK": ("region", "Asia", "Global"),
    "ASIA": ("region", "Asia", "Global"),
    "IR": ("region", "Energy/Commodities", "Global"),
    "SA": ("region", "Energy/Commodities", "Global"),
    "AE": ("region", "Energy/Commodities", "Global"),
    "YM": ("region", "Energy/Commodities", "Global"),
}

THEME_DIRECTIONS = {
    "inflation": "inflation-risk",
    "central_bank": "rates-volatility",
    "interest": "rates-volatility",
    "monetary": "rates-volatility",
    "gdp": "growth-watch",
    "growth": "growth-watch",
    "labour": "growth-watch",
    "trade": "trade-friction",
    "currency": "fx-volatility",
    "energy": "energy-upside-risk",
    "oil_price": "energy-upside-risk",
    "conflict": "risk-off",
    "war": "risk-off",
    "sanctions": "trade-friction",
    "disaster": "supply-risk",
    "weather": "supply-risk",
    "political": "policy-risk",
    "politics": "policy-risk",
    "banking": "credit-risk",
    "debt": "credit-risk",
    "stockmarket": "risk-appetite-watch",
}

ASSET_IMPACT = {
    "US": ["SPY", "TLT", "GC=F", "DX-Y.NYB", "^OMX", "^STOXX50E"],
    "EU": ["^STOXX50E", "^OMX", "EURUSD=X", "GC=F"],
    "Sweden": ["^OMX", "SEK=X", "^STOXX50E"],
    "Asia": ["^N225", "^HSI", "SPY", "CL=F"],
    "Energy/Commodities": ["CL=F", "BZ=F", "GC=F", "^STOXX50E", "^OMX"],
    "Global": ["SPY", "TLT", "GC=F", "CL=F", "^OMX", "^STOXX50E", "^N225"],
}


@dataclass
class Evidence:
    source: str
    title: str
    detail: str
    weight: float
    date: str | None = None
    url: str | None = None


@dataclass
class Package:
    as_of: str
    generated_at: str
    scope_type: str
    scope: str
    parent_scope: str | None
    theme: str
    direction: str
    severity: float
    confidence: float
    freshness: str
    horizon: str
    evidence: list[Evidence]
    conclusion: str
    affected_assets: list[str]
    prediction_impact: dict[str, Any]
    next_watch: str
    source_refs: list[dict[str, str]]
    model: str = MODEL


def _scope(raw: str | None, default: str = "Global") -> tuple[str, str, str | None]:
    key = str(raw or default).strip().upper()
    return REGION_ALIASES.get(key, ("region" if key != "GLOBAL" else "global", raw or default, "Global" if key != "GLOBAL" else None))


def _theme(value: str | None) -> str:
    text = str(value or "risk").strip().lower().replace(" ", "_")
    if text in {"terror", "crisis"}:
        return "conflict"
    if "inflation" in text or "cpi" in text or "hicp" in text or "cpif" in text:
        return "inflation"
    if "bank" in text or "rate" in text or "monetary" in text:
        return "central_bank"
    return text


def _direction(theme: str, title: str = "") -> str:
    haystack = f"{theme} {title}".lower()
    if "hormuz" in haystack or "red sea" in haystack or "oil" in haystack:
        return "energy-upside-risk"
    for key, direction in THEME_DIRECTIONS.items():
        if key in haystack:
            return direction
    return "macro-pressure"


def _freshness(d: str | None, as_of: dt.date) -> str:
    if not d:
        return "unknown"
    try:
        day = dt.date.fromisoformat(str(d)[:10])
    except ValueError:
        return "unknown"
    delta = (as_of - day).days
    if delta < 0:
        return f"in {abs(delta)}d"
    if delta == 0:
        return "today"
    if delta <= 7:
        return f"{delta}d"
    return f"{delta}d old"


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _add(bucket: dict[tuple[str, str, str], list[Evidence]], scope: str, theme: str, evidence: Evidence) -> None:
    scope_type, scope_name, parent = _scope(scope)
    bucket.setdefault((scope_type, scope_name, theme), []).append(evidence)
    if scope_name != "Global":
        bucket.setdefault(("global", "Global", theme), []).append(
            Evidence(evidence.source, evidence.title, evidence.detail, evidence.weight * 0.35, evidence.date, evidence.url)
        )
    if parent and parent not in {"Global", scope_name}:
        bucket.setdefault(("region", parent, theme), []).append(
            Evidence(evidence.source, evidence.title, evidence.detail, evidence.weight * 0.55, evidence.date, evidence.url)
        )


def collect_evidence(conn: sqlite3.Connection, as_of: dt.date) -> dict[tuple[str, str, str], list[Evidence]]:
    bucket: dict[tuple[str, str, str], list[Evidence]] = {}

    for row in conn.execute(
        """SELECT id, date, time_local, region, category, importance, title,
                  expected, market_note, source, url
           FROM calendar_events
           WHERE date BETWEEN ? AND date(?, '+21 days')
             AND importance >= 4
           ORDER BY date ASC, importance DESC""",
        (as_of.isoformat(), as_of.isoformat()),
    ):
        _id, d, t, region, category, importance, title, expected, note, source, url = row
        theme = _theme(category or title)
        detail = " · ".join(x for x in [expected, note] if x)
        _add(bucket, region, theme, Evidence(
            source or "calendar", title, detail or "Scheduled high-importance catalyst.",
            float(importance), d, url,
        ))

    for row in conn.execute(
        """SELECT name, region, country, category, severity, summary, source, updated_at
           FROM risk_hotspots
           WHERE active = 1
           ORDER BY severity DESC"""
    ):
        name, region, country, category, severity, summary, source, updated_at = row
        theme = _theme(category)
        scope = country or region or "Global"
        _add(bucket, scope, theme, Evidence(
            source or "risk_hotspots", name, summary or "Persistent risk hotspot.",
            float(severity or 3), updated_at, None,
        ))

    for row in conn.execute(
        """SELECT date, country, disaster_type, article_count, total_articles, examples
           FROM gdelt_disaster_signals
           WHERE date >= date(?, '-7 days')
           ORDER BY date DESC, article_count DESC
           LIMIT 40""",
        (as_of.isoformat(),),
    ):
        d, country, disaster_type, count, _total, examples = row
        severity = min(5.0, 1.5 + math.log10(max(float(count or 1), 1)))
        title = f"{country} {str(disaster_type or 'disaster').replace('_', ' ')}"
        detail = f"{int(count or 0)} GDELT article(s). {examples or ''}".strip()
        _add(bucket, country, "disaster", Evidence("gdelt_disaster", title, detail, severity, d, None))

    latest_news = conn.execute(
        "SELECT MAX(date) FROM signals WHERE symbol='_news' AND signal_name LIKE 'news_rate_%'"
    ).fetchone()[0]
    if latest_news:
        rows = conn.execute(
            """SELECT signal_name, value FROM signals
               WHERE symbol='_news' AND date=? AND signal_name LIKE 'news_rate_%'""",
            (latest_news,),
        ).fetchall()
        for signal_name, value in rows:
            theme = _theme(signal_name.replace("news_rate_", ""))
            avg = conn.execute(
                """SELECT AVG(value) FROM signals
                   WHERE symbol='_news' AND signal_name=?
                     AND date >= date(?, '-30 days') AND date < ?""",
                (signal_name, latest_news, latest_news),
            ).fetchone()[0]
            ratio = (float(value) / float(avg)) if avg else 1.0
            if ratio < 1.2 and float(value or 0) < 0.04:
                continue
            severity = min(5.0, max(1.0, ratio * 1.6))
            detail = f"{float(value) * 100:.2f}% of global articles; {ratio:.2f}x trailing 30d average."
            _add(bucket, "Global", theme, Evidence("gdelt_macro_pulse", theme.replace("_", " ").title(), detail, severity, latest_news, None))

    for row in conn.execute(
        """SELECT country, category, indicator_name, date, value, unit, impact
           FROM indicators i
           WHERE category IN ('inflation','interest','gdp','labour','monetary','currency','trade','debt','energy','positioning')
             AND date = (
               SELECT MAX(date) FROM indicators i2
               WHERE i2.country = i.country AND i2.indicator_name = i.indicator_name
                 AND i2.date <= ?
             )
           ORDER BY date DESC
           LIMIT 80""",
        (as_of.isoformat(),),
    ):
        country, category, name, d, value, unit, impact = row
        if not d:
            continue
        try:
            age = (as_of - dt.date.fromisoformat(str(d)[:10])).days
        except ValueError:
            age = 999
        if age > 180:
            continue
        theme = _theme(category or name)
        val = f"{value:,.4g} {unit or ''}".strip() if value is not None else "value n/a"
        detail = f"Latest {name}: {val} ({d}). {impact or ''}".strip()
        weight = 2.0 if age <= 45 else 1.2
        _add(bucket, country, theme, Evidence("official_macro", name, detail, weight, d, None))

    for row in conn.execute(
        """SELECT country, program, product, COUNT(*) AS n, MAX(fetched_at)
           FROM sanctions
           GROUP BY country, program, product
           HAVING n >= 3
           ORDER BY n DESC
           LIMIT 40"""
    ):
        country, program, product, n, latest = row
        scope = country or "Global"
        title = f"{country or 'Unspecified'} sanctions {program or 'program n/a'}"
        product_txt = f"; product hint {product}" if product else ""
        detail = f"{int(n):,} current row(s){product_txt}."
        _add(bucket, scope, "sanctions", Evidence("sanctions", title, detail, min(5.0, 1 + math.log10(max(n, 1))), latest, None))

    return bucket


def build_packages(conn: sqlite3.Connection, as_of: dt.date | None = None) -> list[Package]:
    as_of = as_of or dt.date.today()
    generated_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    evidence = collect_evidence(conn, as_of)
    packages: list[Package] = []
    for (scope_type, scope, theme), items in evidence.items():
        if not items:
            continue
        items = sorted(items, key=lambda e: e.weight, reverse=True)[:8]
        severity = min(5.0, round(sum(i.weight for i in items) / max(1.5, len(items) * 0.85), 2))
        if severity < 1.25:
            continue
        confidence = min(0.92, round(0.42 + 0.08 * len(items) + 0.06 * min(severity, 5), 2))
        strongest = items[0]
        direction = _direction(theme, strongest.title)
        freshness = _freshness(strongest.date, as_of)
        affected = ASSET_IMPACT.get(scope, ASSET_IMPACT.get("Global", []))
        if theme in {"energy", "oil_price", "conflict"}:
            affected = sorted(set(affected + ["CL=F", "BZ=F", "GC=F"]))
        evidence_txt = "; ".join(i.title for i in items[:3])
        conclusion = (
            f"{scope} is showing {theme.replace('_', ' ')} pressure tilted toward "
            f"{direction.replace('-', ' ')}. Main evidence: {evidence_txt}."
        )
        next_watch = strongest.title if strongest.source == "calendar" else "Watch confirmation from prices, official macro releases and follow-up news intensity."
        source_refs = [
            {"source": i.source, "title": i.title, "date": i.date or "", "url": i.url or ""}
            for i in items
        ]
        packages.append(Package(
            as_of=as_of.isoformat(),
            generated_at=generated_at,
            scope_type=scope_type,
            scope=scope,
            parent_scope=None if scope_type == "global" else _scope(scope)[2],
            theme=theme,
            direction=direction,
            severity=severity,
            confidence=confidence,
            freshness=freshness,
            horizon="near_term",
            evidence=items,
            conclusion=conclusion,
            affected_assets=affected,
            prediction_impact={
                "asset_bias": direction,
                "macro_use": "Use as compiled context; not a standalone buy/sell signal.",
            },
            next_watch=next_watch,
            source_refs=source_refs,
        ))
    return sorted(packages, key=lambda p: (p.scope != "Global", -p.severity, p.scope, p.theme))


def store_packages(conn: sqlite3.Connection, packages: list[Package]) -> int:
    rows = []
    for pkg in packages:
        payload = asdict(pkg)
        payload["evidence"] = [asdict(e) for e in pkg.evidence]
        rows.append((
            pkg.as_of, pkg.generated_at, pkg.scope_type, pkg.scope, pkg.parent_scope,
            pkg.theme, pkg.direction, pkg.severity, pkg.confidence, pkg.freshness,
            pkg.horizon, json.dumps([asdict(e) for e in pkg.evidence]),
            pkg.conclusion, json.dumps(pkg.affected_assets),
            json.dumps(pkg.prediction_impact), pkg.next_watch,
            json.dumps(pkg.source_refs), pkg.model, _hash_payload(payload),
        ))
    conn.executemany(
        """INSERT OR REPLACE INTO intelligence_packages (
             as_of, generated_at, scope_type, scope, parent_scope, theme, direction,
             severity, confidence, freshness, horizon, evidence_json, conclusion,
             affected_assets_json, prediction_impact_json, next_watch, source_refs_json,
             model, input_hash
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


def compile_and_store(as_of: dt.date | None = None) -> int:
    conn = connect_writable(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        packages = build_packages(conn, as_of=as_of)
        n = store_packages(conn, packages)
        conn.commit()
        log(f"Compiled {n} intelligence package(s).", module="world_pull")
        return n
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to today")
    args = parser.parse_args(argv)
    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else None
    compile_and_store(as_of)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
