"""Build compact named prediction context packs for Claude prompts."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from typing import Any

import pandas as pd

from src.core import labels, queries
from src.core.db import table_exists


HORIZON_DESC = {
    "1d": "1 trading day (~24h)",
    "1w": "1 week (~5 trading days)",
    "1m": "1 month (~21 trading days)",
    "3m": "3 months (~63 trading days)",
    "1y": "1 year (~252 trading days)",
    "5y": "5 years (~1,260 trading days)",
}

ASSET_REGION_HINTS = {
    "^OMX": ["SE", "EU", "US"],
    "^STOXX50E": ["EU", "DE", "FR", "SE", "US"],
    "^N225": ["JP", "CN", "KR", "US"],
    "SPY": ["US", "EU", "CN", "JP"],
    "TLT": ["US", "EU"],
    "GC=F": ["US", "EU", "SE", "CN", "JP"],
    "CL=F": ["US", "EU", "CN", "SA", "AE"],
}


def _json_records(df: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    return queries.jsonable_records(df.head(limit))


def _latest_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: str,
    where: str,
    params: tuple[Any, ...],
    order: str,
    limit: int,
) -> list[dict[str, Any]]:
    if not table_exists(conn, table):
        return []
    return queries.jsonable_records(pd.read_sql(
        f"SELECT {columns} FROM {table} WHERE {where} ORDER BY {order} LIMIT ?",
        conn,
        params=(*params, int(limit)),
    ))


def _latest_signal_rows(conn: sqlite3.Connection, symbol: str, as_of: dt.date, limit: int = 24) -> list[dict[str, Any]]:
    return _latest_rows(
        conn,
        "signals",
        "date, symbol, signal_name, value",
        "symbol = ? AND date = (SELECT MAX(date) FROM signals WHERE symbol = ? AND date <= ?)",
        (symbol, symbol, as_of.isoformat()),
        "signal_name",
        limit,
    )


def _macro_signal_rows(conn: sqlite3.Connection, as_of: dt.date, limit: int = 24) -> list[dict[str, Any]]:
    rows = _latest_signal_rows(conn, "_macro", as_of, limit=limit)
    if rows:
        return rows
    return _latest_rows(
        conn,
        "indicators",
        "date, country, category, indicator_name, value, unit",
        "date <= ?",
        (as_of.isoformat(),),
        "date DESC, country, category",
        limit,
    )


def _relevant_regions(asset: str) -> list[str]:
    return ASSET_REGION_HINTS.get(asset, ["US", "EU", "SE", "JP", "CN"])


def _historical_refs(conn: sqlite3.Connection, analogues: list[Any], keys: list[str] | None = None) -> list[dict[str, Any]]:
    if not analogues or not table_exists(conn, "historical_state_values"):
        return []
    refs = []
    for analogue in analogues[:8]:
        rows = queries.historical_state(conn, analogue.date, keys=keys, include_sparse=True)
        refs.append({
            "as_of": analogue.date,
            "cosine": round(float(analogue.cosine), 4),
            "realised_return": analogue.realised_return,
            "historical_state_keys": rows["value_key"].head(18).tolist() if not rows.empty else [],
        })
    return refs


def build_context_pack(
    conn: sqlite3.Connection,
    asset: str,
    horizon: str,
    as_of: dt.date,
    analogues: list[Any],
    *,
    asset_name: str = "",
    profile_id: str = "default",
) -> dict[str, Any]:
    """Build one compact named pack; never dumps the full historical store."""
    regions = _relevant_regions(asset)
    oracle_map = queries.oracle_layer_map(conn, limit=80)
    if not oracle_map.empty:
        oracle_map = oracle_map[
            oracle_map.get("entity_type", pd.Series(dtype=str)).isin(["global", "region", "nation", "market", "commodity", "sector"])
        ].head(18)
    gdelt = queries.gdelt_streams(conn, days=14, limit=80)
    if not gdelt.empty:
        gdelt = gdelt[
            (gdelt["region"].isin(["Global", *regions]))
            | (gdelt["country"].fillna("").isin(regions))
        ].head(16)
    risk = queries.risk_hotspots(conn)
    if not risk.empty:
        risk = risk[
            risk["country"].fillna("").isin(regions) | risk["region"].fillna("").isin(regions)
        ].head(10)
    source_health = queries.source_health(conn)
    if not source_health.empty:
        source_health = source_health[source_health["status"].isin(["stale", "empty", "unknown"])].head(12)
    pack = {
        "contract_version": "prediction_context_pack.v1",
        "asset": asset,
        "asset_name": asset_name or asset,
        "horizon": horizon,
        "horizon_description": HORIZON_DESC.get(horizon, horizon),
        "as_of": as_of.isoformat(),
        "profile_id": profile_id,
        "data_contract": _json_records(queries.data_catalog(conn), limit=28),
        "weighted_label_catalog": _json_records(labels.weighted_label_catalog(conn, profile_id=profile_id), limit=120),
        "sections": {
            "oracle_map_read": _json_records(oracle_map, limit=18),
            "latest_macro_releases": _json_records(queries.latest_macro_releases(conn, hours=72, limit=10), limit=10),
            "next_catalysts": _json_records(queries.next_macro_catalysts(conn, days=45, limit=14), limit=14),
            "current_events": _json_records(queries.current_events(conn, hours_back=48, days_forward=14, limit=14), limit=14),
            "asset_signals": _latest_signal_rows(conn, asset, as_of, limit=24),
            "macro_signals": _macro_signal_rows(conn, as_of, limit=24),
            "gdelt_streams": _json_records(gdelt, limit=16),
            "risk_hotspots": _json_records(risk, limit=10),
            "data_change_events": _json_records(queries.data_change_events(conn, limit=12), limit=12),
            "source_health": _json_records(source_health, limit=12),
            "historical_analogues": [
                {
                    "date": a.date,
                    "cosine": round(float(a.cosine), 4),
                    "realised_return": a.realised_return,
                }
                for a in analogues[:12]
            ],
            "historical_references": _historical_refs(conn, analogues),
        },
        "rules": [
            "Use the named sections and semantic data contract names.",
            "Historical rows are references only; do not infer from unavailable future values.",
            "Treat source health and freshness as discounting information.",
            "Submit the forecast via the submit_forecast tool.",
        ],
    }
    pack["input_hash"] = hashlib.sha256(
        json.dumps(pack, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return pack


def _table(records: list[dict[str, Any]], columns: list[str], limit: int = 10) -> str:
    rows = records[:limit]
    if not rows:
        return "_No current rows._"
    out = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    for row in rows:
        values = []
        for col in columns:
            value = row.get(col)
            text = str(value if value is not None else "n/a").replace("|", "/")
            values.append(text[:220])
        out.append("| " + " | ".join(values) + " |")
    return "\n".join(out)


def render_context_prompt(pack: dict[str, Any]) -> str:
    sections = pack.get("sections", {})
    parts = [
        f"# Macro Intelligence Engine Prediction Context Pack: {pack['asset_name']} ({pack['asset']})",
        f"**As of:** {pack['as_of']}  **Horizon:** {pack.get('horizon_description', pack['horizon'])}  **Profile:** {pack['profile_id']}",
        "",
        "Use these named, compact sections instead of scanning raw historical blocks. Historical state is referenced by analogue dates and keys only.",
        "",
        "## Data Contract Names",
        _table(pack.get("data_contract", []), ["object_id", "display_name", "prompt_name", "frontend_group", "prompt_role"], 24),
        "",
        "## Weighted Label Catalog",
        _table(pack.get("weighted_label_catalog", []), ["label_id", "label_type", "label", "effective_weight"], 18),
        "",
        "## Atlas Map Read",
        _table(sections.get("oracle_map_read", []), ["entity_label", "display_label", "market_bias", "score_label", "plain_read"], 14),
        "",
        "## Latest Macro Releases",
        _table(sections.get("latest_macro_releases", []), ["scheduled_date", "region", "title", "actual_value", "expected_value", "surprise_text"], 10),
        "",
        "## Current Events",
        _table(sections.get("current_events", []), ["event_type", "event_time", "region", "title", "priority", "status"], 12),
        "",
        "## Next Catalysts",
        _table(sections.get("next_catalysts", []), ["scheduled_date", "region", "title", "category", "importance", "status"], 12),
        "",
        "## Asset Signals",
        _table(sections.get("asset_signals", []), ["date", "signal_name", "value"], 24),
        "",
        "## Macro Signals",
        _table(sections.get("macro_signals", []), ["date", "country", "category", "indicator_name", "value", "unit"], 18),
        "",
        "## GDELT Streams And Risk Hotspots",
        _table(sections.get("gdelt_streams", []), ["date", "stream", "region", "country", "article_count", "severity", "societal_impact_score"], 14),
        "",
        _table(sections.get("risk_hotspots", []), ["name", "region", "country", "category", "severity", "summary"], 8),
        "",
        "## High-Impact Data Changes",
        _table(sections.get("data_change_events", []), ["object_id", "source_table", "event_type", "priority", "status", "created_at"], 10),
        "",
        "## Source Health Exceptions",
        _table(sections.get("source_health", []), ["source", "cadence", "latest", "age", "status"], 10),
        "",
        "## Historical Analogues",
        _table(sections.get("historical_analogues", []), ["date", "cosine", "realised_return"], 12),
        "",
        "## Historical Reference IDs",
        "Use these analogue dates and keys as reference handles. Do not request or infer a full historical dump.",
        "```json",
        json.dumps(sections.get("historical_references", []), indent=2, sort_keys=True, default=str)[:6000],
        "```",
        "",
        "## Your Task",
        f"Produce a directional view on **{pack['asset_name']}** over **{pack.get('horizon_description', pack['horizon'])}**. Anchor the answer in the named pack sections, discount stale sources, and be honest about uncertainty.",
    ]
    return "\n".join(parts)


def store_context_pack(conn: sqlite3.Connection, pack: dict[str, Any], prompt_md: str) -> int:
    if not table_exists(conn, "prediction_context_packs"):
        return 0
    refs = pack.get("sections", {}).get("historical_references", [])
    cur = conn.execute(
        """INSERT INTO prediction_context_packs (
             asset, horizon, as_of, profile_id, pack_json, prompt_md,
             historical_refs_json, input_hash
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(asset, horizon, as_of, profile_id, input_hash) DO UPDATE SET
             pack_json=excluded.pack_json,
             prompt_md=excluded.prompt_md,
             historical_refs_json=excluded.historical_refs_json,
             updated_at=datetime('now')""",
        (
            pack["asset"],
            pack["horizon"],
            pack["as_of"],
            pack["profile_id"],
            json.dumps(pack, sort_keys=True, default=str),
            prompt_md,
            json.dumps(refs, sort_keys=True, default=str),
            pack["input_hash"],
        ),
    )
    return int(cur.lastrowid or 0)
