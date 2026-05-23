"""Build Macro Intelligence Engine impact hierarchy and rolled-up index snapshots."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from typing import Any

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable
from config.db_setup import SCHEMA, add_missing_columns

MODEL = "deterministic_impact_graph_v1"
OBSOLETE_ENTITY_IDS = ("region:energy_commodities",)

DEFAULT_ENTITIES: tuple[tuple[str, str, str, str | None, str | None, str | None, str | None, str | None, str], ...] = (
    ("global", "Global Atlas Index", "global", None, None, None, None, None, "Top-level aggregate of macro, risk, liquidity and market pressure."),
    ("region:us", "United States", "region", "global", "US", None, None, None, "US macro and market pull."),
    ("region:eu", "European Union / Euro Area", "region", "global", "EU", None, None, None, "EU and euro-area macro pull."),
    ("region:nordics", "Nordics", "region", "region:eu", "Nordics", None, None, None, "Nordic macro and market pull."),
    ("region:asia", "Asia", "region", "global", "Asia", None, None, None, "Asia session and regional risk pull."),
    ("commodity:energy_commodities", "Energy / Commodities", "commodity", "global", "Energy", None, "Commodities", None, "Commodity supply, energy and inflation channel."),
    ("nation:us", "United States", "nation", "region:us", "US", "US", None, None, "US national macro node."),
    ("nation:se", "Sweden", "nation", "region:nordics", "Nordics", "SE", None, None, "Swedish macro, SEK and OMX node."),
    ("nation:de", "Germany", "nation", "region:eu", "EU", "DE", None, None, "German industrial and DAX exposure."),
    ("nation:fr", "France", "nation", "region:eu", "EU", "FR", None, None, "French macro and CAC exposure."),
    ("nation:gb", "United Kingdom", "nation", "region:eu", "Europe", "GB", None, None, "UK macro and gilt/GBP exposure."),
    ("nation:jp", "Japan", "nation", "region:asia", "Asia", "JP", None, None, "Japan and Nikkei exposure."),
    ("nation:cn", "China", "nation", "region:asia", "Asia", "CN", None, None, "China demand, policy and supply-chain exposure."),
    ("macro:inflation", "Inflation Pressure", "macro_indicator", "global", None, None, None, None, "CPI/HICP/CPIF, price pressure and inflation-surprise channel."),
    ("macro:rates", "Rates Pressure", "macro_indicator", "global", None, None, None, None, "Central-bank and yield-pressure channel."),
    ("macro:growth", "Growth Pulse", "macro_indicator", "global", None, None, None, None, "GDP, labour, PMIs and growth-risk channel."),
    ("macro:risk", "Risk Appetite", "macro_indicator", "global", None, None, None, None, "Conflict, crisis, volatility and broad risk appetite."),
    ("macro:liquidity", "Liquidity / Credit", "macro_indicator", "global", None, None, None, None, "Credit, banking, debt and liquidity stress."),
    ("macro:trade", "Trade / Sanctions", "macro_indicator", "global", None, None, None, None, "Trade restrictions, tariffs, sanctions and shipping friction."),
    ("macro:supply", "Supply Shock", "macro_indicator", "global", None, None, None, None, "Weather, disaster, logistics and physical supply disruption."),
    ("sector:energy", "Energy", "sector", "commodity:energy_commodities", None, None, "Energy", None, "Oil, gas and energy-sensitive equities."),
    ("sector:commodities", "Commodities", "sector", "commodity:energy_commodities", None, None, "Commodities", None, "Gold, oil and broad commodity pressure."),
    ("sector:banks", "Banks / Financials", "sector", "global", None, None, "Financials", None, "Banks, credit and rates-sensitive financials."),
    ("sector:technology", "Technology", "sector", "global", None, None, "Technology", None, "Long-duration growth and semiconductor supply chain."),
    ("sector:defense", "Defense", "sector", "global", None, None, "Defense", None, "Conflict and security spending exposure."),
    ("sector:shipping", "Shipping / Logistics", "sector", "global", None, None, "Shipping", None, "Freight, ports, containers and shipping-lane risk."),
    ("market:sp500", "S&P 500 / SPY", "market", "region:us", "US", None, None, "SPY", "US broad equity market."),
    ("market:omx", "OMXS30", "market", "nation:se", "Nordics", "SE", None, "^OMX", "Swedish equity market."),
    ("market:stoxx50", "Euro Stoxx 50", "market", "region:eu", "EU", None, None, "^STOXX50E", "Eurozone large-cap equity market."),
    ("market:dax", "DAX", "market", "nation:de", "EU", "DE", None, "^GDAXI", "German equity market."),
    ("market:nikkei", "Nikkei 225", "market", "nation:jp", "Asia", "JP", None, "^N225", "Japanese equity market."),
    ("market:bonds_us", "US Duration / TLT", "market", "region:us", "US", None, None, "TLT", "US long-duration bond proxy."),
    ("market:gold", "Gold", "market", "commodity:energy_commodities", None, None, "Commodities", "GC=F", "Gold safe-haven and real-rate asset."),
    ("market:oil", "Oil", "market", "commodity:energy_commodities", None, None, "Energy", "CL=F", "WTI crude and energy inflation channel."),
    ("market:usd", "US Dollar", "market", "region:us", "US", None, None, "DX-Y.NYB", "USD and global liquidity pressure."),
    ("market:sek", "Swedish Krona", "market", "nation:se", "Nordics", "SE", None, "SEK=X", "SEK and Riksbank-sensitive FX channel."),
)

SYMBOL_ENTITY = {
    "SPY": "market:sp500", "^GSPC": "market:sp500", "TLT": "market:bonds_us",
    "GC=F": "market:gold", "CL=F": "market:oil", "BZ=F": "market:oil",
    "^OMX": "market:omx", "^STOXX50E": "market:stoxx50", "^GDAXI": "market:dax",
    "^N225": "market:nikkei", "DX-Y.NYB": "market:usd", "SEK=X": "market:sek",
}

SCOPE_ENTITY = {
    "Global": "global", "US": "region:us", "EU": "region:eu", "Sweden": "nation:se",
    "Nordics/Sweden": "region:nordics", "Asia": "region:asia",
    "Energy/Commodities": "commodity:energy_commodities", "Germany": "nation:de",
    "France": "nation:fr", "United Kingdom": "nation:gb", "Japan": "nation:jp",
    "China": "nation:cn", "SE": "nation:se", "DE": "nation:de", "FR": "nation:fr",
    "GB": "nation:gb", "UK": "nation:gb", "JP": "nation:jp", "CN": "nation:cn",
}

THEME_ENTITIES = {
    "inflation": ["macro:inflation", "macro:rates", "market:bonds_us", "market:gold"],
    "central_bank": ["macro:rates", "macro:liquidity", "market:bonds_us", "market:usd"],
    "interest": ["macro:rates", "market:bonds_us", "market:usd"],
    "monetary": ["macro:rates", "macro:liquidity", "market:bonds_us"],
    "gdp": ["macro:growth", "market:sp500", "market:stoxx50"],
    "labour": ["macro:growth", "macro:rates", "market:sp500"],
    "growth": ["macro:growth", "market:sp500", "market:stoxx50"],
    "conflict": ["macro:risk", "sector:defense", "sector:energy", "market:gold", "market:oil"],
    "sanctions": ["macro:trade", "sector:shipping", "sector:energy", "market:oil"],
    "trade": ["macro:trade", "sector:shipping", "sector:technology"],
    "disaster": ["macro:supply", "sector:commodities", "sector:shipping"],
    "weather": ["macro:supply", "sector:commodities", "market:oil"],
    "energy": ["sector:energy", "market:oil", "macro:inflation"],
    "oil_price": ["sector:energy", "market:oil", "macro:inflation"],
    "banking": ["macro:liquidity", "sector:banks"],
    "debt": ["macro:liquidity", "market:bonds_us"],
    "currency": ["market:usd", "macro:liquidity"],
    "political": ["macro:risk", "macro:trade"],
}

@dataclass
class Impact:
    as_of: str
    generated_at: str
    source_table: str
    source_id: int | None
    evidence_key: str
    theme: str
    entity_id: str
    direction: str
    magnitude: float
    confidence: float
    horizon: str
    freshness: str | None
    summary: str
    source_refs: list[dict[str, Any]]


def _hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


def ensure_entities(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "UPDATE oracle_entities SET active = 0 WHERE entity_id = ?",
        [(entity_id,) for entity_id in OBSOLETE_ENTITY_IDS],
    )
    conn.executemany(
        """INSERT INTO oracle_entities
           (entity_id, label, entity_type, parent_id, region, nation, sector, symbol, description)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(entity_id) DO UPDATE SET
             label=excluded.label,
             entity_type=excluded.entity_type,
             parent_id=excluded.parent_id,
             region=excluded.region,
             nation=excluded.nation,
             sector=excluded.sector,
             symbol=excluded.symbol,
             description=excluded.description,
             active=1""",
        DEFAULT_ENTITIES,
    )


def _json_list(raw: str | None) -> list:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _theme_entities(theme: str) -> list[str]:
    return THEME_ENTITIES.get(theme, ["macro:risk"])


def _symbol_entities(raw: str | None) -> list[str]:
    out = []
    for symbol in _json_list(raw):
        entity = SYMBOL_ENTITY.get(str(symbol))
        if entity:
            out.append(entity)
    return out


def _dedupe(seq: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in seq:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _impact(as_of: str, generated_at: str, source_table: str, source_id: int | None,
            evidence_key: str, theme: str, entity_id: str, direction: str,
            magnitude: float, confidence: float, horizon: str, freshness: str | None,
            summary: str, source_refs: list[dict[str, Any]]) -> Impact:
    return Impact(
        as_of, generated_at, source_table, source_id, evidence_key, theme,
        entity_id, direction, max(0.1, min(5.0, float(magnitude))),
        max(0.05, min(0.98, float(confidence))), horizon, freshness, summary,
        source_refs[:6],
    )


def impacts_from_world_pull(conn: sqlite3.Connection, as_of: dt.date) -> list[Impact]:
    generated_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    latest = conn.execute("SELECT MAX(as_of) FROM intelligence_packages WHERE as_of <= ?", (as_of.isoformat(),)).fetchone()[0]
    if not latest:
        return []
    rows = conn.execute(
        """SELECT id, scope, theme, direction, severity, confidence, freshness,
                  conclusion, affected_assets_json, source_refs_json
           FROM intelligence_packages
           WHERE as_of = ?""",
        (latest,),
    ).fetchall()
    impacts: list[Impact] = []
    for row in rows:
        source_id, scope, theme, direction, severity, confidence, freshness, conclusion, assets_json, refs_json = row
        refs = _json_list(refs_json)
        entities = [SCOPE_ENTITY.get(scope, "global")]
        entities += _theme_entities(theme)
        entities += _symbol_entities(assets_json)
        for entity_id in _dedupe(entities):
            impacts.append(_impact(
                latest, generated_at, "intelligence_packages", source_id,
                f"world_pull:{source_id}", theme, entity_id, direction,
                severity, confidence, "near_term", freshness, conclusion or "Compiled World Pull evidence.", refs,
            ))
    return impacts


def impacts_from_macro_events(conn: sqlite3.Connection, as_of: dt.date) -> list[Impact]:
    generated_at = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    latest = conn.execute("SELECT MAX(as_of) FROM macro_event_predictions WHERE as_of <= ?", (as_of.isoformat(),)).fetchone()[0]
    if not latest:
        return []
    rows = conn.execute(
        """SELECT id, region, category, title, importance, confidence,
                  predicted_surprise_bucket, affected_assets_json, release_date,
                  source, url
           FROM macro_event_predictions
           WHERE as_of = ?
             AND release_date >= ?""",
        (latest, as_of.isoformat()),
    ).fetchall()
    impacts: list[Impact] = []
    for row in rows:
        source_id, region, category, title, importance, confidence, bucket, assets_json, release_date, source, url = row
        theme = "central_bank" if category == "central_bank" else category or "macro_event"
        direction = "rates-volatility" if theme in {"inflation", "central_bank"} else "macro-pressure"
        refs = [{"source": source or "calendar", "title": title, "date": release_date or "", "url": url or ""}]
        entities = [SCOPE_ENTITY.get(region, "global")]
        entities += _theme_entities(theme)
        entities += _symbol_entities(assets_json)
        summary = f"Upcoming {region} {category} event: {title}. Baseline scenario bucket: {bucket}."
        for entity_id in _dedupe(entities):
            impacts.append(_impact(
                latest, generated_at, "macro_event_predictions", source_id,
                f"macro_event:{source_id}", theme, entity_id, direction,
                float(importance or 3), confidence, "event_window", release_date,
                summary, refs,
            ))
    return impacts


def _ancestor_map(conn: sqlite3.Connection) -> dict[str, str | None]:
    rows = conn.execute("SELECT entity_id, parent_id FROM oracle_entities WHERE active = 1").fetchall()
    return {entity_id: parent_id for entity_id, parent_id in rows}


def _entity_meta(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        "SELECT entity_id, label, entity_type, parent_id, symbol FROM oracle_entities WHERE active = 1"
    ).fetchall()
    return {r[0]: {"label": r[1], "type": r[2], "parent": r[3], "symbol": r[4]} for r in rows}


def _expanded_for_rollup(impacts: list[Impact], parents: dict[str, str | None]) -> list[Impact]:
    expanded = list(impacts)
    for impact in impacts:
        parent = parents.get(impact.entity_id)
        decay = 0.62
        while parent:
            expanded.append(_impact(
                impact.as_of, impact.generated_at, impact.source_table, impact.source_id,
                impact.evidence_key + f":parent:{parent}", impact.theme, parent,
                impact.direction, impact.magnitude * decay, impact.confidence * 0.95,
                impact.horizon, impact.freshness, impact.summary, impact.source_refs,
            ))
            parent = parents.get(parent)
            decay *= 0.62
    return expanded



def interpret_snapshot(entity_label: str, entity_type: str, theme: str, direction: str) -> tuple[str, str]:
    label = entity_label.lower()
    theme_l = theme.lower()
    direction_l = direction.lower()

    if "oil" in label or entity_label == "Energy" or "energy" in label:
        if theme_l in {"conflict", "sanctions", "disaster", "weather", "energy", "oil_price"} or "supply" in direction_l or "energy" in direction_l:
            return "bullish/upside risk", "Supply or geopolitical pressure is usually supportive for oil and energy revenues, while it hurts consumers and inflation-sensitive markets."
    if "gold" in label:
        if theme_l in {"conflict", "sanctions", "disaster"} or "risk-off" in direction_l:
            return "bullish/safe haven", "Risk, sanctions or supply stress can support gold through safe-haven demand. Check real yields and USD before treating it as clean bullish."
        if theme_l in {"inflation", "central_bank", "interest"}:
            return "mixed", "Inflation can support gold, but higher real-rate pressure can hurt it. Treat as mixed until rates/USD confirm."
    if "duration" in label or "tlt" in label or "bond" in label:
        if theme_l in {"inflation", "central_bank", "interest"} or "rates" in direction_l:
            return "bearish", "Inflation or hawkish rate pressure is usually negative for long-duration bonds."
        if "risk-off" in direction_l or theme_l in {"conflict", "disaster"}:
            return "mixed/supportive", "Risk-off can support duration, but inflation-linked supply shocks can offset that."
    if any(x in label for x in ["s&p", "omx", "stoxx", "dax", "nikkei", "equity"]):
        if theme_l in {"inflation", "central_bank", "interest"} or "rates" in direction_l:
            return "bearish", "Rate or inflation pressure is usually a headwind for equities, especially rate-sensitive sectors."
        if theme_l in {"conflict", "sanctions", "disaster"} or "risk-off" in direction_l:
            return "bearish/risk-off", "Geopolitical, sanctions or disaster pressure is usually a risk-off headwind for broad equities."
        if theme_l in {"gdp", "growth", "labour"}:
            return "mixed", "Growth strength can support earnings, but may also keep rates higher."
    if "dollar" in label or "usd" in label:
        if theme_l in {"inflation", "central_bank", "interest", "conflict"}:
            return "bullish/volatile", "US rate pressure and risk-off demand often support USD, though the move can reverse if growth fear dominates."
    if "krona" in label or "sek" in label:
        if theme_l in {"inflation", "central_bank", "interest"}:
            return "mixed/SEK supportive", "Swedish inflation or Riksbank repricing can support SEK but pressure OMX rate-sensitive shares."
    if entity_type == "sector":
        if "defense" in label and theme_l == "conflict":
            return "bullish", "Conflict pressure can support defense demand and budgets."
        if "shipping" in label and theme_l in {"sanctions", "trade", "disaster", "conflict"}:
            return "bearish/cost risk", "Trade friction or supply disruption usually raises shipping and logistics risk."
        if "banks" in label and theme_l in {"interest", "central_bank", "inflation"}:
            return "mixed", "Higher rates can help net interest income but hurt credit quality and valuations."
    if entity_type in {"global", "region", "nation"}:
        if theme_l in {"inflation", "central_bank", "interest"}:
            return "macro headwind", "This points to higher rate/inflation pressure for the area, generally negative for broad risk assets."
        if theme_l in {"conflict", "sanctions", "disaster", "trade"}:
            return "risk headwind", "This is a risk or supply-chain headwind for the area, not a crisis call by itself. Confirm with local evidence and market reaction."
        if theme_l in {"gdp", "growth", "labour"}:
            return "growth watch", "This is a growth signal; direction depends on whether markets price it as earnings support or rate pressure."
    if direction_l in {"risk-on", "growth-watch"}:
        return "supportive/watch", "The signal is potentially supportive, but needs confirmation from prices and related macro data."
    return "watch", "This is an evidence cluster routed to this entity. It is not directional enough by itself; use it as a watch item, not a forecast."

def build_index_snapshots(conn: sqlite3.Connection, impacts: list[Impact]) -> list[dict[str, Any]]:
    meta = _entity_meta(conn)
    parents = _ancestor_map(conn)
    expanded = _expanded_for_rollup(impacts, parents)
    grouped: dict[tuple[str, str, str], list[Impact]] = {}
    for impact in expanded:
        if impact.entity_id in meta:
            grouped.setdefault((impact.entity_id, impact.theme, impact.horizon), []).append(impact)
    snapshots = []
    for (entity_id, theme, horizon), items in grouped.items():
        weighted = sum(i.magnitude * i.confidence for i in items)
        score = min(100.0, round(weighted * 10.0 / math.sqrt(max(1, len(items))), 1))
        magnitude = min(5.0, round(sum(i.magnitude for i in items) / len(items), 2))
        confidence = min(0.96, round(sum(i.confidence for i in items) / len(items), 2))
        direction_weights: dict[str, float] = {}
        for i in items:
            direction_weights[i.direction] = direction_weights.get(i.direction, 0.0) + i.magnitude * i.confidence
        direction = max(direction_weights.items(), key=lambda kv: kv[1])[0]
        top = sorted(items, key=lambda i: i.magnitude * i.confidence, reverse=True)[:5]
        affected_assets = sorted({meta[i.entity_id]["symbol"] for i in items if meta.get(i.entity_id, {}).get("symbol")})
        market_bias, plain_read = interpret_snapshot(meta[entity_id]["label"], meta[entity_id]["type"], theme, direction)
        payload = {
            "as_of": items[0].as_of, "generated_at": items[0].generated_at,
            "entity_id": entity_id, "entity_label": meta[entity_id]["label"],
            "entity_type": meta[entity_id]["type"], "parent_id": meta[entity_id]["parent"],
            "theme": theme, "direction": direction, "horizon": horizon,
            "score": score, "magnitude": magnitude, "confidence": confidence,
            "evidence_count": len(items),
            "market_bias": market_bias,
            "plain_read": plain_read,
            "top_evidence": [{"summary": i.summary, "source": i.source_table, "magnitude": i.magnitude} for i in top],
            "affected_assets": affected_assets,
        }
        payload["input_hash"] = _hash(payload)
        snapshots.append(payload)
    return sorted(snapshots, key=lambda s: (s["entity_type"] != "global", -s["score"], s["entity_label"]))


def store_impacts(conn: sqlite3.Connection, impacts: list[Impact]) -> int:
    rows = []
    for impact in impacts:
        payload = impact.__dict__.copy()
        rows.append((
            impact.as_of, impact.generated_at, impact.source_table, impact.source_id,
            impact.evidence_key, impact.theme, impact.entity_id, impact.direction,
            impact.magnitude, impact.confidence, impact.horizon, impact.freshness,
            impact.summary, json.dumps(impact.source_refs), MODEL, _hash(payload),
        ))
    conn.executemany(
        """INSERT OR REPLACE INTO oracle_impacts (
             as_of, generated_at, source_table, source_id, evidence_key, theme,
             entity_id, direction, magnitude, confidence, horizon, freshness,
             summary, source_refs_json, model, input_hash
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


def store_snapshots(conn: sqlite3.Connection, snapshots: list[dict[str, Any]]) -> int:
    rows = [
        (
            s["as_of"], s["generated_at"], s["entity_id"], s["entity_label"],
            s["entity_type"], s["parent_id"], s["theme"], s["direction"],
            s["horizon"], s["score"], s["magnitude"], s["confidence"],
            s["evidence_count"], s.get("market_bias"), s.get("plain_read"),
            json.dumps(s["top_evidence"]), json.dumps(s["affected_assets"]), MODEL, s["input_hash"],
        ) for s in snapshots
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO oracle_index_snapshots (
             as_of, generated_at, entity_id, entity_label, entity_type, parent_id,
             theme, direction, horizon, score, magnitude, confidence,
             evidence_count, market_bias, plain_read, top_evidence_json, affected_assets_json, model, input_hash
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


def build_and_store(as_of: dt.date | None = None) -> tuple[int, int]:
    as_of = as_of or dt.date.today()
    conn = connect_writable(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        ensure_entities(conn)
        impacts = impacts_from_world_pull(conn, as_of) + impacts_from_macro_events(conn, as_of)
        snapshots = build_index_snapshots(conn, impacts)
        n_impacts = store_impacts(conn, impacts)
        n_snapshots = store_snapshots(conn, snapshots)
        conn.commit()
        log(f"Built {n_impacts} impact row(s), {n_snapshots} index snapshot(s).", module="impact_graph")
        return n_impacts, n_snapshots
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to today")
    args = parser.parse_args(argv)
    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else None
    build_and_store(as_of)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
