"""Render the per-prediction brief that gets sent to Claude.

Inputs:
    asset, horizon, as_of date
    asset signals snapshot
    macro / regime signals
    upcoming calendar events and persistent risk hotspots
    regional macro snapshot and passive-feed alerts
    top-K analogues with realised forward returns
    recent events (last 90 days)

Output: a single Markdown string. Plain prose where helpful, tables for
numeric snapshots so the model doesn't have to do arithmetic.
"""
from __future__ import annotations

import sqlite3
import json
from datetime import date, timedelta

import pandas as pd

from src.retrieval.query import Analogue


HORIZON_DESC = {
    "1d":  "1 trading day (~24h)",
    "1w":  "1 week (~5 trading days)",
    "1m":  "1 month (~21 trading days)",
    "3m":  "3 months (~63 trading days)",
    "1y":  "1 year (~252 trading days)",
    "5y":  "5 years (~1,260 trading days)",
}

HORIZON_CALENDAR_DAYS = {
    "1d": 3,
    "1w": 10,
    "1m": 45,
    "3m": 90,
    "1y": 365,
    "5y": 365,
}

ASSET_REGION_HINTS = {
    "^OMX": ["SE", "EU", "US"],
    "^STOXX50E": ["EU", "DE", "FR", "SE", "US"],
    "^GDAXI": ["DE", "EU", "US"],
    "^FCHI": ["FR", "EU", "US"],
    "^FTSE": ["GB", "EU", "US"],
    "^N225": ["JP", "CN", "KR", "US"],
    "^HSI": ["CN", "JP", "KR", "US"],
    "SPY": ["US", "EU", "CN", "JP"],
    "TLT": ["US", "EU"],
    "GC=F": ["US", "EU", "SE", "CN", "JP"],
    "CL=F": ["US", "EU", "CN", "SA", "AE"],
    "BZ=F": ["EU", "US", "SA", "AE"],
    "DX-Y.NYB": ["US", "EU", "SE", "JP", "CN"],
}

KEY_INDICATOR_CATEGORIES = (
    "inflation", "interest", "gdp", "labour", "monetary", "currency",
    "trade", "debt", "energy", "positioning",
)


def _signal_snapshot(conn: sqlite3.Connection, symbol: str, as_of: date) -> dict[str, float]:
    rows = conn.execute(
        """SELECT signal_name, value FROM signals
           WHERE symbol = ? AND date = (
             SELECT MAX(date) FROM signals WHERE symbol = ? AND date <= ?
           )""",
        (symbol, symbol, as_of.isoformat()),
    ).fetchall()
    return {name: val for name, val in rows}


def _recent_events(conn: sqlite3.Connection, as_of: date, days: int = 90) -> list[tuple]:
    cutoff = (as_of - timedelta(days=days)).isoformat()
    rows = conn.execute(
        """SELECT date, title, country, type, source FROM events
           WHERE date BETWEEN ? AND ?
           ORDER BY date DESC""",
        (cutoff, as_of.isoformat()),
    ).fetchall()
    return rows


def _asset_regions(asset: str) -> list[str]:
    return ASSET_REGION_HINTS.get(asset, ["US", "EU", "SE", "JP", "CN"])


def _simple_ticker(asset: str) -> str | None:
    if asset.startswith("^") or "=" in asset or "-" in asset:
        return None
    return asset.upper()


def _fmt_pct(x: float | None) -> str:
    return f"{x * 100:+.2f}%" if x is not None else "n/a"


def _fmt_signal(name: str, value: float) -> str:
    if name.startswith("ret_") or name.startswith("px_vs_") or name == "drawdown_252d" or name == "vol_30d_ann":
        return f"{value * 100:+.2f}%"
    if name == "rsi_14":
        return f"{value:.1f}"
    if name in ("ma_50", "ma_200"):
        return f"{value:,.2f}"
    if name in ("ma50_above_ma200", "dxy_above_ma200"):
        return "yes" if value > 0.5 else "no"
    if name == "vix_regime":
        return {0: "low (<15)", 1: "normal (15-25)", 2: "elevated (25-35)", 3: "crisis (>35)"}.get(int(value), "?")
    if name == "yield_curve_10y_ff":
        return f"{value:+.2f} pp"
    if name.startswith("news_rate_"):
        return f"{value * 100:.2f}% of articles"
    if name.startswith("news_count_"):
        return f"{int(value):,} articles"
    if name == "news_total_articles":
        return f"{int(value):,}"
    return f"{value:.4f}"


def _signal_table(snapshot: dict[str, float], order: list[str]) -> str:
    lines = ["| signal | value |", "|---|---|"]
    for k in order:
        if k in snapshot:
            lines.append(f"| `{k}` | {_fmt_signal(k, snapshot[k])} |")
    return "\n".join(lines)


ASSET_SIGNAL_ORDER = [
    "ret_1d", "ret_1w", "ret_1m", "ret_3m", "ret_1y", "ret_5y",
    "drawdown_252d", "rsi_14", "vol_30d_ann",
    "px_vs_ma50", "px_vs_ma200", "ma50_above_ma200",
]
MACRO_SIGNAL_ORDER = ["yield_curve_10y_ff", "vix_regime", "dxy_above_ma200"]

# Order matters: themes most market-relevant first.
NEWS_THEME_ORDER = [
    "central_bank", "monetary_policy", "interest_rates", "inflation",
    "stockmarket", "banking", "bankruptcy", "debt",
    "currency", "oil_price", "housing", "terror",
]


def _news_intensity_table(conn: sqlite3.Connection, as_of: date) -> str:
    """Compare each theme's article share today against its trailing 30d mean."""
    cutoff = (as_of - timedelta(days=30)).isoformat()
    snap = _signal_snapshot(conn, "_news", as_of)
    if not snap:
        return ""

    # Trailing 30d mean for each rate
    means: dict[str, float] = {}
    for theme in NEWS_THEME_ORDER:
        key = f"news_rate_{theme}"
        rows = conn.execute(
            """SELECT AVG(value) FROM signals
               WHERE symbol = '_news' AND signal_name = ?
                 AND date BETWEEN ? AND ?""",
            (key, cutoff, as_of.isoformat()),
        ).fetchone()
        if rows and rows[0] is not None:
            means[theme] = float(rows[0])

    total = snap.get("news_total_articles", 0)
    lines = [
        f"Total articles today: {int(total):,}.",
        "",
        "| theme | today | 30d mean | vs mean |",
        "|---|---|---|---|",
    ]
    for theme in NEWS_THEME_ORDER:
        rate_key = f"news_rate_{theme}"
        if rate_key not in snap:
            continue
        today = snap[rate_key]
        mean = means.get(theme)
        if mean and mean > 0:
            ratio = today / mean
            arrow = "↑" if ratio > 1.2 else "↓" if ratio < 0.8 else "→"
            vs = f"{arrow} {ratio:.2f}×"
            mean_str = f"{mean*100:.2f}%"
        else:
            vs = "—"
            mean_str = "—"
        lines.append(f"| `{theme}` | {today*100:.2f}% | {mean_str} | {vs} |")
    return "\n".join(lines)


def _upcoming_calendar_block(conn: sqlite3.Connection, asset: str, horizon: str, as_of: date) -> str:
    """High-importance scheduled events the model must consider."""
    end = (as_of + timedelta(days=HORIZON_CALENDAR_DAYS.get(horizon, 45))).isoformat()
    regions = _asset_regions(asset)
    rows = conn.execute(
        """SELECT date, time_local, region, category, importance, title,
                  expected, market_note, source
           FROM calendar_events
           WHERE date BETWEEN ? AND ?
             AND importance >= 4
           ORDER BY
             CASE WHEN region IN (%s) THEN 0 ELSE 1 END,
             date ASC, importance DESC, region
           LIMIT 12""" % ",".join("?" for _ in regions),
        (as_of.isoformat(), end, *regions),
    ).fetchall()
    if not rows:
        return ""

    lines = [
        "| date | region | event | expected/context | market note |",
        "|---|---|---|---|---|",
    ]
    for d, t, region, category, importance, title, expected, note, source in rows:
        when = f"{d} {t or ''}".strip()
        event = f"{title} (`{category}`, imp {importance}/5, {source or 'source n/a'})"
        lines.append(f"| {when} | {region} | {event} | {expected or 'n/a'} | {note or 'n/a'} |")
    return "\n".join(lines)


def _risk_hotspots_block(conn: sqlite3.Connection, asset: str) -> str:
    regions = _asset_regions(asset)
    rows = conn.execute(
        """SELECT name, region, country, category, severity, summary, updated_at
           FROM risk_hotspots
           WHERE active = 1
           ORDER BY
             CASE WHEN country IN (%s) THEN 0 ELSE 1 END,
             severity DESC, name
           LIMIT 8""" % ",".join("?" for _ in regions),
        (*regions,),
    ).fetchall()
    if not rows:
        return ""
    lines = []
    for name, region, country, category, severity, summary, updated_at in rows:
        lines.append(
            f"- **{name}** ({country or region}, `{category}`, severity {severity}/5, "
            f"updated {updated_at}): {summary or 'no summary'}"
        )
    return "\n".join(lines)


def _regional_macro_block(conn: sqlite3.Connection, asset: str, as_of: date) -> str:
    regions = _asset_regions(asset)
    placeholders = ",".join("?" for _ in regions)
    categories = ",".join("?" for _ in KEY_INDICATOR_CATEGORIES)
    rows = conn.execute(
        f"""SELECT country, category, indicator_name, date, value, unit
            FROM indicators i
            WHERE country IN ({placeholders})
              AND category IN ({categories})
              AND date = (
                SELECT MAX(date) FROM indicators
                WHERE country = i.country
                  AND indicator_name = i.indicator_name
                  AND date <= ?
              )
            ORDER BY country, category, indicator_name""",
        (*regions, *KEY_INDICATOR_CATEGORIES, as_of.isoformat()),
    ).fetchall()
    if not rows:
        return ""

    lines = ["| country | indicator | latest | value |", "|---|---|---|---|"]
    for country, category, name, d, value, unit in rows[:24]:
        suffix = f" {unit}" if unit else ""
        lines.append(f"| {country} | {name} (`{category}`) | {d} | {value:.4g}{suffix} |")
    return "\n".join(lines)


def _passive_alerts_block(conn: sqlite3.Connection, asset: str, as_of: date) -> str:
    """Summarise passive feeds without pretending they are primary macro drivers."""
    lines: list[str] = []
    ticker = _simple_ticker(asset)
    if ticker:
        rumour = conn.execute(
            """SELECT date, mentions_today, baseline_mean, z_score, sentiment_today
               FROM rumour_signals
               WHERE ticker = ? AND date <= ?
               ORDER BY date DESC LIMIT 1""",
            (ticker, as_of.isoformat()),
        ).fetchone()
        if rumour:
            d, mentions, baseline, z_score, sentiment = rumour
            z = f"{z_score:+.2f}σ" if z_score is not None else "n/a"
            base = f"{baseline:.1f}" if baseline is not None else "n/a"
            sent = f"{sentiment:+.2f}" if sentiment is not None else "n/a"
            lines.append(
                f"- Social/rumour for `{ticker}` on {d}: {mentions} mentions "
                f"vs baseline {base}, z={z}, sentiment={sent}."
            )

        insider = conn.execute(
            """SELECT filing_date, insider_name, transaction_type, value_usd
               FROM insider_trades
               WHERE ticker = ? AND filing_date >= date(?, '-30 days')
                 AND value_usd IS NOT NULL
               ORDER BY ABS(value_usd) DESC LIMIT 3""",
            (ticker, as_of.isoformat()),
        ).fetchall()
        for d, insider_name, transaction_type, value_usd in insider:
            lines.append(
                f"- Insider Form 4 `{ticker}` {d}: {insider_name} "
                f"{transaction_type or '?'} about ${value_usd:,.0f}."
            )

        fund = conn.execute(
            """SELECT period_end, concept, value, unit, form, filed
               FROM company_fundamentals
               WHERE ticker = ?
                 AND concept IN ('Revenue', 'NetIncome', 'OperatingIncome', 'EPS')
               ORDER BY period_end DESC, filed DESC LIMIT 6""",
            (ticker,),
        ).fetchall()
        if fund:
            latest_period = fund[0][0]
            bits = []
            for _period_end, concept, value, unit, form, filed in fund[:4]:
                if concept == "EPS":
                    bits.append(f"{concept}={value:.2f} {unit or ''}".strip())
                else:
                    bits.append(f"{concept}=${value/1e9:.2f}B")
            lines.append(
                f"- Fundamentals `{ticker}` latest period {latest_period}: "
                + "; ".join(bits) + "."
            )

    weather = conn.execute(
        """SELECT asset, location, weather_var, window_days, correlation, n_observations
           FROM weather_correlations
           WHERE asset = ?
           ORDER BY ABS(correlation) DESC LIMIT 3""",
        (asset,),
    ).fetchall()
    for _asset, location, weather_var, window_days, corr, n_obs in weather:
        lines.append(
            f"- Weather correlation `{asset}`/{location}/{weather_var}: "
            f"{corr:+.3f} over {window_days}d (n={n_obs}); exploratory only."
        )

    regions = _asset_regions(asset)
    ph = ",".join("?" for _ in regions)
    disasters = conn.execute(
        f"""SELECT date, country, disaster_type, article_count, examples
            FROM gdelt_disaster_signals
            WHERE date >= date(?, '-14 days') AND country IN ({ph})
            ORDER BY date DESC, article_count DESC LIMIT 5""",
        (as_of.isoformat(), *regions),
    ).fetchall()
    for d, country, disaster_type, count, examples in disasters:
        lines.append(
            f"- GDELT disaster marker {d} `{country}`/{disaster_type}: "
            f"{count} articles; {examples or 'no examples'}."
        )

    sanctions = conn.execute(
        f"""SELECT country, program, product, COUNT(*) AS n
            FROM sanctions
            WHERE country IN ({ph})
            GROUP BY country, program, product
            ORDER BY n DESC LIMIT 6""",
        (*regions,),
    ).fetchall()
    for country, program, product, n in sanctions:
        product_txt = f", product hint={product}" if product else ""
        lines.append(
            f"- Sanctions context `{country}`/{program or 'program n/a'}: "
            f"{n} OFAC row(s){product_txt}; macro context only."
        )

    return "\n".join(lines)


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def _oracle_index_block(conn: sqlite3.Connection, asset: str, as_of: date) -> str:
    if not _has_table(conn, "oracle_index_snapshots"):
        return ""
    rows = conn.execute(
        """SELECT entity_label, entity_type, parent_id, theme, direction,
                  horizon, score, magnitude, confidence, evidence_count,
                  market_bias, plain_read, affected_assets_json
           FROM oracle_index_snapshots
           WHERE as_of = (SELECT MAX(as_of) FROM oracle_index_snapshots WHERE as_of <= ?)
           ORDER BY
             CASE entity_type
               WHEN 'global' THEN 0 WHEN 'region' THEN 1 WHEN 'nation' THEN 2
               WHEN 'macro_indicator' THEN 3 WHEN 'sector' THEN 4
               WHEN 'market' THEN 5 ELSE 6 END,
             score DESC
           LIMIT 18""",
        (as_of.isoformat(),),
    ).fetchall()
    if not rows:
        return ""
    lines = [
        "| entity | type | read | theme | horizon | score | confidence | evidence |",
        "|---|---|---|---|---|---:|---:|---:|",
    ]
    for label, entity_type, parent_id, theme, direction, horizon, score, magnitude, confidence, evidence_count, market_bias, plain_read, affected_json in rows:
        relevance = ""
        try:
            affected = json.loads(affected_json or "[]")
            if asset in affected:
                relevance = f"; directly affects {asset}"
        except json.JSONDecodeError:
            pass
        lines.append(
            f"| {label} | {entity_type} | {market_bias or direction}: {plain_read or direction} | {theme} | {horizon} | "
            f"{float(score):.1f}/100 | {float(confidence):.2f} | {int(evidence_count)}{relevance} |"
        )
    return "\n".join(lines)

def _world_pull_block(conn: sqlite3.Connection, asset: str, as_of: date) -> str:
    if not _has_table(conn, "intelligence_packages"):
        return ""
    regions = set(_asset_regions(asset) + ["Global"])
    ph = ",".join("?" for _ in regions)
    rows = conn.execute(
        f"""SELECT scope, scope_type, theme, direction, severity, confidence,
                  freshness, conclusion, affected_assets_json, next_watch
            FROM intelligence_packages
            WHERE as_of = (SELECT MAX(as_of) FROM intelligence_packages WHERE as_of <= ?)
              AND (scope IN ({ph}) OR scope = 'Global')
            ORDER BY
              CASE WHEN scope = 'Global' THEN 0 ELSE 1 END,
              severity DESC, confidence DESC
            LIMIT 12""",
        (as_of.isoformat(), *regions),
    ).fetchall()
    if not rows:
        return ""

    lines = [
        "| scope | theme | pull | severity | confidence | conclusion | next watch |",
        "|---|---|---|---:|---:|---|---|",
    ]
    for scope, scope_type, theme, direction, severity, confidence, freshness, conclusion, affected_json, next_watch in rows:
        affected = ""
        try:
            assets = json.loads(affected_json or "[]")
            if asset in assets:
                affected = f" Affects `{asset}`."
        except json.JSONDecodeError:
            pass
        lines.append(
            f"| {scope} (`{scope_type}`) | `{theme}` | `{direction}` | "
            f"{float(severity):.1f}/5 | {float(confidence):.2f} | "
            f"{conclusion or ''}{affected} Freshness: {freshness or 'n/a'} | {next_watch or 'n/a'} |"
        )
    return "\n".join(lines)


def _macro_event_scenarios_block(conn: sqlite3.Connection, asset: str, horizon: str, as_of: date) -> str:
    if not _has_table(conn, "macro_event_predictions"):
        return ""
    end = (as_of + timedelta(days=HORIZON_CALENDAR_DAYS.get(horizon, 45))).isoformat()
    regions = set(_asset_regions(asset) + ["Global"])
    ph = ",".join("?" for _ in regions)
    rows = conn.execute(
        f"""SELECT release_date, release_time_local, region, category, title,
                  importance, expected, predicted_surprise_bucket, confidence,
                  scenario_json, affected_assets_json, actual_summary,
                  result_status, local_model_summary
            FROM macro_event_predictions
            WHERE as_of = (SELECT MAX(as_of) FROM macro_event_predictions WHERE as_of <= ?)
              AND release_date BETWEEN ? AND ?
              AND (region IN ({ph}) OR region = 'Global')
            ORDER BY release_date ASC, importance DESC
            LIMIT 8""",
        (as_of.isoformat(), as_of.isoformat(), end, *regions),
    ).fetchall()
    if not rows:
        return ""

    lines = []
    for d, t, region, category, title, importance, expected, bucket, confidence, scenario_json, affected_json, actual_summary, result_status, local_model_summary in rows:
        try:
            scenarios = json.loads(scenario_json or "[]")
        except json.JSONDecodeError:
            scenarios = []
        try:
            affected = json.loads(affected_json or "[]")
        except json.JSONDecodeError:
            affected = []
        relevant = "yes" if asset in affected else "regional/global"
        lines.append(
            f"- **{d} {t or ''} · {region} · {title}** (`{category}`, imp {importance}/5): "
            f"baseline bucket `{bucket}`, confidence {float(confidence):.2f}; "
            f"expected/context: {expected or 'n/a'}; relevance to {asset}: {relevant}."
        )
        if actual_summary:
            lines.append(f"  - Actual/result now available: {actual_summary}")
        if local_model_summary:
            lines.append(f"  - Local result read: {local_model_summary}")
        elif result_status == "awaiting_actual":
            lines.append("  - Actual/result not found in official feeds yet.")
        for scenario in scenarios[:3]:
            lines.append(
                f"  - `{scenario.get('bucket', 'scenario')}`: "
                f"{scenario.get('macro_read', '')} Market effect: {scenario.get('likely_market_effect', '')}"
            )
    return "\n".join(lines)


def _data_freshness_block(conn: sqlite3.Connection, as_of: date) -> str:
    sources = [
        ("prices", "market prices", "date"),
        ("signals", "computed signals", "date"),
        ("events", "curated/live events", "date"),
        ("calendar_events", "macro calendar", "date"),
        ("indicators", "macro indicators", "date"),
        ("social_mentions", "social mentions", "date"),
        ("rumour_signals", "rumour synthesis", "date"),
        ("weather_obs", "weather observations", "date"),
        ("weather_correlations", "weather correlations", "computed_at"),
        ("company_fundamentals", "company fundamentals", "filed"),
        ("insider_trades", "insider trades", "filing_date"),
        ("sanctions", "sanctions lists", "fetched_at"),
        ("gdelt_disaster_signals", "GDELT disaster markers", "date"),
    ]
    lines = ["| source | rows | latest | age vs as_of |", "|---|---:|---|---:|"]
    for table, label, date_col in sources:
        if table == "calendar_events":
            row = conn.execute(
                f"SELECT COUNT(*), MAX({date_col}) FROM {table} WHERE {date_col} >= ?",
                (as_of.isoformat(),),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT COUNT(*), MAX({date_col}) FROM {table} WHERE substr({date_col}, 1, 10) <= ?",
                (as_of.isoformat(),),
            ).fetchone()
        count, latest = row if row else (0, None)
        age = "n/a"
        if latest:
            try:
                latest_day = date.fromisoformat(str(latest)[:10])
                delta = (as_of - latest_day).days
                age = f"{delta}d" if delta >= 0 else f"in {abs(delta)}d"
            except ValueError:
                age = "n/a"
        lines.append(f"| {label} | {int(count):,} | {latest or 'n/a'} | {age} |")
    return "\n".join(lines)


def render_brief(conn: sqlite3.Connection, asset: str, horizon: str, as_of: date,
                 analogues: list[Analogue], asset_name: str = "") -> str:
    try:
        from src.intelligence.context_packs import (
            build_context_pack,
            render_context_prompt,
            store_context_pack,
        )

        pack = build_context_pack(
            conn,
            asset,
            horizon,
            as_of,
            analogues,
            asset_name=asset_name,
        )
        prompt_md = render_context_prompt(pack)
        try:
            store_context_pack(conn, pack, prompt_md)
            conn.commit()
        except sqlite3.Error:
            # Read-only backtests can still use the compact prompt.
            pass
        return prompt_md
    except Exception:
        # Defensive fallback while older local/server DBs receive the additive
        # contract tables.
        pass

    asset_snap = _signal_snapshot(conn, asset, as_of)
    macro_snap = _signal_snapshot(conn, "_macro", as_of)
    events = _recent_events(conn, as_of, days=90)

    parts: list[str] = []
    parts.append(f"# Forecast brief — {asset_name or asset}, {horizon} horizon")
    parts.append(f"**As of:** {as_of.isoformat()}  •  **Horizon:** {HORIZON_DESC[horizon]}")
    parts.append("")

    oracle_index = _oracle_index_block(conn, asset, as_of)
    if oracle_index:
        parts.append("## Atlas impact hierarchy")
        parts.append(
            "This is the rolled-up hierarchy from raw evidence to Global, regions, nations, "
            "macro indicators, sectors, markets and assets. Use it to understand where "
            "the world is pulling before reading individual source rows.\n"
        )
        parts.append(oracle_index)
        parts.append("")

    world_pull = _world_pull_block(conn, asset, as_of)
    if world_pull:
        parts.append("## Compiled World Pull intelligence")
        parts.append(
            "This is the cleaned intelligence package compiled from official macro, "
            "calendar catalysts, risk hotspots, sanctions, GDELT/news and passive feeds. "
            "Use it as the primary macro context before raw source details.\n"
        )
        parts.append(world_pull)
        parts.append("")

    macro_scenarios = _macro_event_scenarios_block(conn, asset, horizon, as_of)
    if macro_scenarios:
        parts.append("## Macro-event scenario ladder")
        parts.append(
            "Known upcoming events with below/inline/above or dovish/inline/hawkish "
            "scenario effects. These are scenario forecasts, not exact numeric nowcasts.\n"
        )
        parts.append(macro_scenarios)
        parts.append("")

    parts.append("## Current asset signals")
    parts.append(_signal_table(asset_snap, ASSET_SIGNAL_ORDER))
    parts.append("")

    parts.append("## Current macro regime")
    parts.append(_signal_table(macro_snap, MACRO_SIGNAL_ORDER))
    parts.append("")

    calendar_block = _upcoming_calendar_block(conn, asset, horizon, as_of)
    if calendar_block:
        parts.append("## Upcoming scheduled market catalysts")
        parts.append(
            "These are known future events inside the relevant horizon window. "
            "Treat them as catalysts or risk windows, not as outcomes.\n"
        )
        parts.append(calendar_block)
        parts.append("")

    risk_block = _risk_hotspots_block(conn, asset)
    if risk_block:
        parts.append("## Persistent geopolitical and supply-chain watchpoints")
        parts.append(risk_block)
        parts.append("")

    regional_macro = _regional_macro_block(conn, asset, as_of)
    if regional_macro:
        parts.append("## Latest regional macro snapshot")
        parts.append(regional_macro)
        parts.append("")

    news_table = _news_intensity_table(conn, as_of)
    if news_table:
        parts.append("## News intensity (GDELT GKG, share of global articles)")
        parts.append("Each theme's article share today vs its trailing 30-day mean. "
                     "An ↑ means the news cycle is paying more attention to this "
                     "topic than usual; ↓ means less. Use this as a cross-check on "
                     "the macro regime above.\n")
        parts.append(news_table)
        parts.append("")

    passive_alerts = _passive_alerts_block(conn, asset, as_of)
    if passive_alerts:
        parts.append("## Passive-feed alerts")
        parts.append(
            "These are secondary signals. Use them only as weak context unless "
            "they line up with price action, macro data or known catalysts.\n"
        )
        parts.append(passive_alerts)
        parts.append("")

    parts.append("## Historical analogues (top by macro-state similarity)")
    parts.append("Each row is a past month-end with similar macro state, plus what "
                 "the same asset actually did over the same forward horizon.\n")
    parts.append("| date | similarity | realised return |")
    parts.append("|---|---|---|")
    for a in analogues:
        parts.append(f"| {a.date} | {a.cosine:.3f} | {_fmt_pct(a.realised_return)} |")
    parts.append("")

    if events:
        parts.append(f"## Recent events (last 90 days, {len(events)} entries)")
        for d, title, country, ev_type, source in events[:25]:
            tag = f"[{ev_type or '?'}/{source or '?'}]"
            country_tag = f" ({country})" if country else ""
            parts.append(f"- **{d}**{country_tag} {tag} {title}")
        if len(events) > 25:
            parts.append(f"- … {len(events) - 25} more")
        parts.append("")

    parts.append("## Data freshness")
    parts.append(
        "Use this to discount stale sources. A source can be displayed in the "
        "client but still be stale or irrelevant for this forecast.\n"
    )
    parts.append(_data_freshness_block(conn, as_of))
    parts.append("")

    parts.append("## Your task")
    parts.append(
        f"Produce a directional view on **{asset_name or asset}** over the "
        f"next **{HORIZON_DESC[horizon]}**, anchored in the macro state and "
        "the historical analogues above. Be honest about uncertainty: if the "
        "analogues are split or the regime is unfamiliar, say so and lower "
        "your confidence. Submit your forecast via the `submit_forecast` tool."
    )
    return "\n".join(parts)
