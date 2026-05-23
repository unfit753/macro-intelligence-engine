"""Materialise daily no-lookahead historical comparison states.

The tables written here are deliberately separate from production scoring.
They make it easy to inspect "what did Macro Intelligence Engine know as of this date?" and then
evaluate later market moves from a separate forward-return table.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import multiprocessing as mp
import os
import re
import sqlite3
from typing import Any, Iterable

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.core.db import connect_writable, table_exists


TRAINING_CUTOFF = dt.date(2009, 11, 30)
DEFAULT_FORWARD_HORIZONS = (5, 21, 63)
DEFAULT_HISTORICAL_WORKERS = max(1, os.cpu_count() or 1)
DEFAULT_SYMBOLS = (
    "GC=F", "CL=F", "SPY", "TLT", "^OMX", "^STOXX50E",
    "^N225", "^VIX", "DX-Y.NYB",
)
CORE_REQUIRED_KEYS = tuple(f"price:{symbol}" for symbol in DEFAULT_SYMBOLS) + (
    "signal:_macro:yield_curve_10y_ff",
    "signal:DX-Y.NYB:px_vs_ma200",
)

FRESHNESS_THRESHOLDS: dict[str, tuple[int, int, int]] = {
    "prices": (3, 7, 30),
    "signals": (3, 7, 30),
    "indicators": (45, 120, 365),
    "gdelt_streams": (2, 7, 30),
    "calendar_events": (1, 7, 30),
    "events": (7, 30, 365),
    "oracle_index_snapshots": (1, 3, 14),
    "sanctions": (3, 14, 60),
    "risk_hotspots": (14, 45, 180),
    "weather_obs": (3, 14, 60),
    "weather_correlations": (14, 45, 180),
    "social_mentions": (2, 7, 30),
    "rumour_signals": (2, 7, 30),
    "news_items": (2, 7, 30),
    "insider_trades": (14, 45, 180),
    "company_fundamentals": (120, 240, 720),
}

FUNDAMENTAL_CONCEPTS = {
    "Revenue", "GrossProfit", "OperatingIncome", "NetIncome", "EPS",
    "Cash", "Assets", "Liabilities", "StockholdersEquity", "LongTermDebt",
}

_LABEL_ASSIGNMENT_CACHE: dict[tuple[Any, ...], dict[tuple[str, str, str, str], set[str]]] = {}
_LABEL_CONN_CACHE: dict[int, dict[tuple[str, str, str, str], set[str]]] = {}


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_") or "value"


def _date_range(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    day = start
    while day <= end:
        yield day
        day += dt.timedelta(days=1)


def _parse_date(value: str | None, fallback: dt.date | None = None) -> dt.date | None:
    if not value:
        return fallback
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return fallback


def _freshness(as_of: dt.date, value_date: str | None) -> int | None:
    if not value_date:
        return None
    try:
        return (as_of - dt.date.fromisoformat(str(value_date)[:10])).days
    except ValueError:
        return None


def _freshness_class(source_table: str, freshness_days: int | None) -> str:
    if freshness_days is None:
        return "archive"
    fresh, aging, stale = FRESHNESS_THRESHOLDS.get(source_table, (7, 30, 365))
    if freshness_days <= fresh:
        return "fresh"
    if freshness_days <= aging:
        return "aging"
    if freshness_days <= stale:
        return "stale"
    return "archive"


def _label_assignment_cache_key(conn: sqlite3.Connection) -> tuple[Any, ...]:
    db_path = conn.execute("PRAGMA database_list").fetchone()[2] or f":memory:{id(conn)}"
    if not table_exists(conn, "data_label_assignments"):
        return (db_path, "missing")
    count, max_id, max_updated = conn.execute(
        """SELECT COUNT(*), COALESCE(MAX(id), 0), COALESCE(MAX(updated_at), '')
           FROM data_label_assignments
           WHERE active = 1"""
    ).fetchone()
    return (db_path, count, max_id, max_updated)


def _label_assignment_index(conn: sqlite3.Connection) -> dict[tuple[str, str, str, str], set[str]]:
    conn_cache_key = id(conn)
    if conn_cache_key in _LABEL_CONN_CACHE:
        return _LABEL_CONN_CACHE[conn_cache_key]
    cache_key = _label_assignment_cache_key(conn)
    if cache_key in _LABEL_ASSIGNMENT_CACHE:
        _LABEL_CONN_CACHE[conn_cache_key] = _LABEL_ASSIGNMENT_CACHE[cache_key]
        return _LABEL_CONN_CACHE[conn_cache_key]
    if len(cache_key) == 2 and cache_key[1] == "missing":
        _LABEL_ASSIGNMENT_CACHE[cache_key] = {}
        _LABEL_CONN_CACHE[conn_cache_key] = _LABEL_ASSIGNMENT_CACHE[cache_key]
        return _LABEL_CONN_CACHE[conn_cache_key]
    index: dict[tuple[str, str, str, str], set[str]] = {}
    for label_id, target_type, target_table, target_column, target_value in conn.execute(
        """SELECT label_id, target_type, COALESCE(target_table, ''),
                  COALESCE(target_column, ''), COALESCE(target_value, '')
           FROM data_label_assignments
           WHERE active = 1"""
    ):
        key = (target_type or "", target_table or "", target_column or "", target_value or "")
        index.setdefault(key, set()).add(label_id)
    _LABEL_ASSIGNMENT_CACHE[cache_key] = index
    _LABEL_CONN_CACHE[conn_cache_key] = index
    return index


def _clear_label_assignment_conn_cache(conn: sqlite3.Connection) -> None:
    _LABEL_CONN_CACHE.pop(id(conn), None)


def _label_ids(
    conn: sqlite3.Connection,
    target_type: str,
    target_table: str,
    target_column: str,
    target_value: str | None,
) -> list[str]:
    index = _label_assignment_index(conn)
    base = (target_type, target_table, target_column)
    labels = set(index.get((*base, ""), set()))
    if target_value:
        labels.update(index.get((*base, target_value), set()))
    return sorted(labels)


def _combined_labels(*items: list[str]) -> list[str]:
    return sorted({label for labels in items for label in labels if label})


def _table_labels(conn: sqlite3.Connection, table: str) -> list[str]:
    return _label_ids(conn, "table", table, "", None)


def _source_labels(conn: sqlite3.Connection, table: str, column: str, value: str | None, target_type: str | None = None) -> list[str]:
    labels = _label_ids(conn, target_type or column, table, column, value)
    labels += _table_labels(conn, table)
    return labels


def _state_row(
    as_of: dt.date,
    value_key: str,
    source_table: str,
    source_id: int | None,
    source_symbol: str | None,
    source_name: str | None,
    region: str | None,
    country: str | None,
    category: str | None,
    label_ids: list[str],
    value: float | None,
    value_text: str | None,
    unit: str | None,
    value_date: str,
    confidence: float = 1.0,
) -> tuple:
    freshness_days = _freshness(as_of, value_date)
    return (
        as_of.isoformat(), value_key, source_table, source_id, source_symbol,
        source_name, region, country, category, json.dumps(label_ids),
        value, value_text, unit, value_date, freshness_days,
        _freshness_class(source_table, freshness_days), confidence,
    )


def _target_symbols(conn: sqlite3.Connection, symbols: Iterable[str] | None) -> list[str]:
    if symbols:
        return sorted({str(s) for s in symbols})
    out = set(DEFAULT_SYMBOLS)
    if table_exists(conn, "targets"):
        rows = conn.execute("SELECT symbol FROM targets WHERE active = 1").fetchall()
        out.update(str(r[0]) for r in rows if r[0])
    return sorted(out)


def _latest_complete_market_date(conn: sqlite3.Connection, symbols: Iterable[str] | None = None) -> dt.date:
    if not table_exists(conn, "prices"):
        return dt.date.today()
    core_symbols = sorted({str(s) for s in (symbols or DEFAULT_SYMBOLS) if s})
    if core_symbols:
        placeholders = ",".join("?" for _ in core_symbols)
        row = conn.execute(
            f"""SELECT MAX(date) FROM (
                  SELECT date
                  FROM prices
                  WHERE symbol IN ({placeholders})
                  GROUP BY date
                  HAVING COUNT(DISTINCT symbol) = ?
                )""",
            (*core_symbols, len(core_symbols)),
        ).fetchone()
        parsed = _parse_date(row[0] if row else None)
        if parsed:
            return parsed
    row = conn.execute("SELECT MAX(date) FROM prices").fetchone()
    return _parse_date(row[0] if row else None, dt.date.today()) or dt.date.today()


def _latest_prices(conn: sqlite3.Connection, as_of: dt.date, symbols: list[str]) -> list[tuple]:
    if not table_exists(conn, "prices") or not symbols:
        return []
    placeholders = ",".join("?" for _ in symbols)
    return conn.execute(
        f"""SELECT p.id, p.date, p.symbol, p.asset_class, p.name, p.price, p.currency
            FROM prices p
            JOIN (
              SELECT symbol, MAX(date) AS max_date
              FROM prices
              WHERE date <= ? AND symbol IN ({placeholders})
              GROUP BY symbol
            ) latest ON latest.symbol = p.symbol AND latest.max_date = p.date
            ORDER BY p.symbol""",
        (as_of.isoformat(), *symbols),
    ).fetchall()


def _latest_indicators(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "indicators"):
        return []
    return conn.execute(
        """SELECT i.id, i.date, i.country, i.category, i.indicator_name,
                  i.value, i.unit
           FROM indicators i
           JOIN (
             SELECT country, indicator_name, MAX(date) AS max_date
             FROM indicators
             WHERE date <= ?
             GROUP BY country, indicator_name
           ) latest
             ON latest.country = i.country
            AND latest.indicator_name = i.indicator_name
            AND latest.max_date = i.date
           ORDER BY i.country, i.category, i.indicator_name""",
        (as_of.isoformat(),),
    ).fetchall()


def _latest_signals(conn: sqlite3.Connection, as_of: dt.date, symbols: list[str]) -> list[tuple]:
    if not table_exists(conn, "signals"):
        return []
    signal_symbols = sorted(set(symbols) | {"_macro"})
    placeholders = ",".join("?" for _ in signal_symbols)
    return conn.execute(
        f"""SELECT s.id, s.date, s.symbol, s.signal_name, s.value
            FROM signals s
            JOIN (
              SELECT symbol, signal_name, MAX(date) AS max_date
              FROM signals
              WHERE date <= ?
                AND symbol IN ({placeholders})
                AND signal_name NOT LIKE 'news_%'
              GROUP BY symbol, signal_name
            ) latest
              ON latest.symbol = s.symbol
             AND latest.signal_name = s.signal_name
             AND latest.max_date = s.date
            ORDER BY s.symbol, s.signal_name""",
        (as_of.isoformat(), *signal_symbols),
    ).fetchall()


def _recent_gdelt_streams(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "gdelt_streams"):
        return []
    return conn.execute(
        """SELECT g.id, g.date, g.stream, g.region, g.country, g.article_count,
                  g.article_share, g.severity, g.societal_impact_score,
                  g.labels_json
           FROM gdelt_streams g
           JOIN (
             SELECT stream, region, country, MAX(date) AS max_date
             FROM gdelt_streams
             WHERE date <= ? AND date >= date(?, '-7 days')
             GROUP BY stream, region, country
           ) latest
             ON latest.stream = g.stream
            AND latest.region = g.region
            AND latest.country = g.country
            AND latest.max_date = g.date
           ORDER BY g.societal_impact_score DESC, g.severity DESC
           LIMIT 120""",
        (as_of.isoformat(), as_of.isoformat()),
    ).fetchall()


def _sanction_clusters(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "sanctions"):
        return []
    return conn.execute(
        """SELECT MIN(id) AS source_id, MAX(substr(fetched_at, 1, 10)) AS value_date,
                  COALESCE(country, '') AS country, COALESCE(program, 'program n/a') AS program,
                  COALESCE(product, '') AS product, COUNT(*) AS rows
           FROM sanctions
           WHERE substr(fetched_at, 1, 10) <= ?
           GROUP BY country, program, product
           HAVING rows >= 3
           ORDER BY rows DESC
           LIMIT 80""",
        (as_of.isoformat(),),
    ).fetchall()


def _active_risk_hotspots(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "risk_hotspots"):
        return []
    return conn.execute(
        """SELECT id, updated_at, name, region, country, category, severity, summary
           FROM risk_hotspots
           WHERE active = 1 AND substr(updated_at, 1, 10) <= ?
           ORDER BY severity DESC, name
           LIMIT 80""",
        (as_of.isoformat(),),
    ).fetchall()


def _latest_weather_obs(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "weather_obs"):
        return []
    return conn.execute(
        """SELECT w.id, w.date, w.location, w.temp_mean_c,
                  w.precipitation_mm, w.wind_max_kmh
           FROM weather_obs w
           JOIN (
             SELECT location, MAX(date) AS max_date
             FROM weather_obs
             WHERE date <= ?
             GROUP BY location
           ) latest ON latest.location = w.location AND latest.max_date = w.date
           ORDER BY w.location
           LIMIT 80""",
        (as_of.isoformat(),),
    ).fetchall()


def _latest_weather_correlations(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "weather_correlations"):
        return []
    return conn.execute(
        """SELECT w.id, substr(w.computed_at, 1, 10) AS value_date,
                  w.asset, w.location, w.weather_var, w.window_days,
                  w.correlation, w.n_observations
           FROM weather_correlations w
           JOIN (
             SELECT asset, location, weather_var, window_days,
                    MAX(substr(computed_at, 1, 10)) AS max_date
             FROM weather_correlations
             WHERE substr(computed_at, 1, 10) <= ?
             GROUP BY asset, location, weather_var, window_days
           ) latest
             ON latest.asset = w.asset
            AND latest.location = w.location
            AND latest.weather_var = w.weather_var
            AND latest.window_days = w.window_days
            AND latest.max_date = substr(w.computed_at, 1, 10)
           ORDER BY ABS(w.correlation) DESC
           LIMIT 80""",
        (as_of.isoformat(),),
    ).fetchall()


def _latest_social_mentions(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "social_mentions"):
        return []
    return conn.execute(
        """SELECT s.id, s.date, s.source, s.ticker, s.mention_count,
                  s.sentiment_score, s.top_post_title, s.top_post_score
           FROM social_mentions s
           JOIN (
             SELECT source, ticker, MAX(date) AS max_date
             FROM social_mentions
             WHERE date <= ?
             GROUP BY source, ticker
           ) latest
             ON latest.source = s.source
            AND latest.ticker = s.ticker
            AND latest.max_date = s.date
           ORDER BY s.mention_count DESC, COALESCE(s.top_post_score, 0) DESC
           LIMIT 80""",
        (as_of.isoformat(),),
    ).fetchall()


def _latest_rumour_signals(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "rumour_signals"):
        return []
    return conn.execute(
        """SELECT r.id, r.date, r.ticker, r.mentions_today,
                  r.baseline_mean, r.z_score, r.sentiment_today
           FROM rumour_signals r
           JOIN (
             SELECT ticker, MAX(date) AS max_date
             FROM rumour_signals
             WHERE date <= ?
             GROUP BY ticker
           ) latest
             ON latest.ticker = r.ticker AND latest.max_date = r.date
           ORDER BY COALESCE(r.z_score, 0) DESC, r.mentions_today DESC
           LIMIT 80""",
        (as_of.isoformat(),),
    ).fetchall()


def _recent_news_items(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "news_items"):
        return []
    return conn.execute(
        """SELECT id, substr(COALESCE(published_at, fetched_at), 1, 10) AS value_date,
                  source, title, region, category, used_for_predictions, url
           FROM news_items
           WHERE substr(COALESCE(published_at, fetched_at), 1, 10) <= ?
             AND substr(COALESCE(published_at, fetched_at), 1, 10) >= date(?, '-7 days')
           ORDER BY value_date DESC, used_for_predictions DESC, source, title
           LIMIT 80""",
        (as_of.isoformat(), as_of.isoformat()),
    ).fetchall()


def _insider_activity(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "insider_trades"):
        return []
    return conn.execute(
        """SELECT MIN(id) AS source_id, MAX(filing_date) AS value_date, ticker,
                  COUNT(*) AS trades,
                  SUM(CASE WHEN value_usd > 0 THEN value_usd ELSE 0 END) AS buys,
                  SUM(CASE WHEN value_usd < 0 THEN value_usd ELSE 0 END) AS sells
           FROM insider_trades
           WHERE filing_date <= ? AND filing_date >= date(?, '-30 days')
             AND ticker IS NOT NULL
           GROUP BY ticker
           ORDER BY ABS(COALESCE(buys, 0)) + ABS(COALESCE(sells, 0)) DESC
           LIMIT 80""",
        (as_of.isoformat(), as_of.isoformat()),
    ).fetchall()


def _latest_fundamentals(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "company_fundamentals"):
        return []
    concepts = sorted(FUNDAMENTAL_CONCEPTS)
    placeholders = ",".join("?" for _ in concepts)
    return conn.execute(
        f"""WITH ranked AS (
              SELECT f.id, COALESCE(f.filed, f.period_end) AS value_date,
                     f.ticker, f.period_end, f.period_type, f.concept,
                     f.value, f.unit,
                     ROW_NUMBER() OVER (
                       PARTITION BY f.ticker, f.concept
                       ORDER BY COALESCE(f.filed, f.period_end) DESC,
                                f.period_end DESC,
                                CASE COALESCE(f.period_type, '')
                                  WHEN 'FY' THEN 0 WHEN 'Q' THEN 1 ELSE 2
                                END,
                                f.id DESC
                     ) AS rn
              FROM company_fundamentals f
              WHERE COALESCE(f.filed, f.period_end) <= ?
                AND f.concept IN ({placeholders})
            )
            SELECT id, value_date, ticker, period_end, period_type, concept,
                   value, unit
            FROM ranked
            WHERE rn = 1
            ORDER BY value_date DESC, ticker, concept
            LIMIT 120""",
        (as_of.isoformat(), *concepts),
    ).fetchall()


def _same_day_events(conn: sqlite3.Connection, as_of: dt.date) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if table_exists(conn, "calendar_events"):
        for row in conn.execute(
            """SELECT id, date, region, category, importance, title, source
               FROM calendar_events
               WHERE date = ?
               ORDER BY importance DESC, region, title""",
            (as_of.isoformat(),),
        ):
            events.append({
                "source_table": "calendar_events", "source_id": row[0],
                "date": row[1], "region": row[2], "category": row[3],
                "importance": row[4], "title": row[5], "source": row[6],
            })
    if table_exists(conn, "events"):
        for row in conn.execute(
            """SELECT id, date, country, type, title, source
               FROM events
               WHERE date = ?
               ORDER BY country, type, title""",
            (as_of.isoformat(),),
        ):
            events.append({
                "source_table": "events", "source_id": row[0],
                "date": row[1], "country": row[2], "category": row[3],
                "importance": None, "title": row[4], "source": row[5],
            })
    return events


def _latest_oracle_snapshots(conn: sqlite3.Connection, as_of: dt.date) -> list[tuple]:
    if not table_exists(conn, "oracle_index_snapshots"):
        return []
    latest = conn.execute(
        "SELECT MAX(as_of) FROM oracle_index_snapshots WHERE as_of <= ?",
        (as_of.isoformat(),),
    ).fetchone()[0]
    if not latest:
        return []
    return conn.execute(
        """SELECT id, as_of, entity_id, entity_label, entity_type, theme,
                  direction, horizon, score, market_bias, plain_read
           FROM oracle_index_snapshots
           WHERE as_of = ?
           ORDER BY score DESC
           LIMIT 80""",
        (latest,),
    ).fetchall()


def state_values_for_date(
    conn: sqlite3.Connection,
    as_of: dt.date,
    symbols: Iterable[str] | None = None,
) -> tuple[list[tuple], list[dict[str, Any]]]:
    selected_symbols = _target_symbols(conn, symbols)
    rows: list[tuple] = []

    for source_id, value_date, symbol, asset_class, name, price, currency in _latest_prices(conn, as_of, selected_symbols):
        labels = _combined_labels(
            _source_labels(conn, "prices", "symbol", symbol),
            _label_ids(conn, "symbol", "targets", "symbol", symbol),
        )
        rows.append(_state_row(
            as_of, f"price:{symbol}", "prices", source_id, symbol, name,
            None, None, asset_class, labels, float(price),
            f"{price:g} {currency or ''}".strip(), currency, value_date,
        ))

    for source_id, value_date, country, category, name, value, unit in _latest_indicators(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "indicators", "category", category),
            _label_ids(conn, "country", "indicators", "country", country),
        )
        rows.append(_state_row(
            as_of, f"indicator:{country}:{_slug(name)}", "indicators",
            source_id, None, name, None, country, category, labels,
            float(value) if value is not None else None,
            f"{value:g} {unit or ''}".strip() if value is not None else None,
            unit, value_date,
        ))

    for source_id, value_date, symbol, signal_name, value in _latest_signals(conn, as_of, selected_symbols):
        labels = _combined_labels(
            _source_labels(conn, "signals", "signal_name", signal_name),
            _label_ids(conn, "symbol", "prices", "symbol", symbol),
            _label_ids(conn, "symbol", "targets", "symbol", symbol),
        )
        rows.append(_state_row(
            as_of, f"signal:{symbol}:{signal_name}", "signals", source_id,
            symbol, signal_name, None, None, "signal", labels,
            float(value) if value is not None else None,
            f"{value:g}" if value is not None else None, None, value_date, 0.9,
        ))

    for row in _recent_gdelt_streams(conn, as_of):
        source_id, value_date, stream, region, country, count, share, severity, impact, labels_json = row
        try:
            labels = json.loads(labels_json or "[]")
        except json.JSONDecodeError:
            labels = []
        key_tail = f"{stream}:{region}:{country or 'all'}"
        rows.append(_state_row(
            as_of, f"gdelt_stream:{key_tail}:severity", "gdelt_streams",
            source_id, None, stream, region, country or None, stream,
            labels, float(severity or 0), f"severity {severity:g}", None,
            value_date, 0.8,
        ))
        rows.append(_state_row(
            as_of, f"gdelt_stream:{key_tail}:share", "gdelt_streams",
            source_id, None, stream, region, country or None, stream,
            labels, float(share or 0), f"{float(share or 0) * 100:.3f}%", "share",
            value_date, 0.8,
        ))
        rows.append(_state_row(
            as_of, f"gdelt_stream:{key_tail}:count", "gdelt_streams",
            source_id, None, stream, region, country or None, stream,
            labels, float(count or 0), f"{int(count or 0):,} articles", "articles",
            value_date, 0.8,
        ))
        rows.append(_state_row(
            as_of, f"gdelt_stream:{key_tail}:societal_impact", "gdelt_streams",
            source_id, None, stream, region, country or None, stream,
            labels, float(impact or 0), f"impact {impact:g}", None,
            value_date, 0.8,
        ))

    for source_id, value_date, country, program, product, count in _sanction_clusters(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "sanctions", "program", program, "category"),
            _label_ids(conn, "country", "sanctions", "country", country),
        )
        label = " / ".join(x for x in [country or "country n/a", program, product] if x)
        rows.append(_state_row(
            as_of, f"sanctions:{_slug(country or 'global')}:{_slug(program)}:{_slug(product or 'all')}",
            "sanctions", source_id, None, label, None, country or None,
            "sanctions", labels, float(count or 0), f"{int(count or 0):,} sanctions rows",
            "rows", value_date, 0.8,
        ))

    for source_id, value_date, name, region, country, category, severity, summary in _active_risk_hotspots(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "risk_hotspots", "category", category),
            _label_ids(conn, "country", "risk_hotspots", "country", country),
            _label_ids(conn, "region", "risk_hotspots", "region", region),
        )
        rows.append(_state_row(
            as_of, f"risk_hotspot:{source_id}", "risk_hotspots", source_id,
            None, name, region, country, category, labels, float(severity or 0),
            summary or name, "severity", value_date, 0.9,
        ))

    for source_id, value_date, location, temp, precip, wind in _latest_weather_obs(conn, as_of):
        for weather_var, value, unit in (
            ("temp_mean_c", temp, "C"),
            ("precipitation_mm", precip, "mm"),
            ("wind_max_kmh", wind, "kmh"),
        ):
            if value is None:
                continue
            labels = _source_labels(conn, "weather_obs", "location", location)
            rows.append(_state_row(
                as_of, f"weather_obs:{_slug(location)}:{weather_var}",
                "weather_obs", source_id, None, location, None, None,
                weather_var, labels, float(value), f"{value:g} {unit}", unit,
                value_date, 0.7,
            ))

    for source_id, value_date, asset, location, weather_var, window, corr, n_obs in _latest_weather_correlations(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "weather_correlations", "weather_var", weather_var),
            _label_ids(conn, "symbol", "prices", "symbol", asset),
            _label_ids(conn, "symbol", "targets", "symbol", asset),
        )
        rows.append(_state_row(
            as_of, f"weather_corr:{asset}:{_slug(location)}:{weather_var}:{window}",
            "weather_correlations", source_id, asset, location, None, None,
            weather_var, labels, float(corr), f"corr {corr:g} ({int(n_obs or 0)} obs)",
            "correlation", value_date, 0.7,
        ))

    for source_id, value_date, source, ticker, mentions, sentiment, title, score in _latest_social_mentions(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "social_mentions", "ticker", ticker, "category"),
            _label_ids(conn, "symbol", "prices", "symbol", ticker),
            _label_ids(conn, "symbol", "targets", "symbol", ticker),
        )
        rows.append(_state_row(
            as_of, f"social_mentions:{source}:{ticker}", "social_mentions",
            source_id, ticker, title or f"{source} mentions", None, None,
            "social_heat", labels, float(mentions or 0),
            f"{int(mentions or 0)} mentions", "mentions", value_date, 0.65,
        ))
        if sentiment is not None:
            rows.append(_state_row(
                as_of, f"social_sentiment:{source}:{ticker}", "social_mentions",
                source_id, ticker, title or f"{source} sentiment", None, None,
                "sentiment", labels, float(sentiment), f"sentiment {sentiment:g}",
                "sentiment", value_date, 0.65,
            ))

    for source_id, value_date, ticker, mentions, baseline, z_score, sentiment in _latest_rumour_signals(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "rumour_signals", "ticker", ticker, "category"),
            _label_ids(conn, "symbol", "prices", "symbol", ticker),
            _label_ids(conn, "symbol", "targets", "symbol", ticker),
        )
        rows.append(_state_row(
            as_of, f"rumour_spike:{ticker}", "rumour_signals", source_id,
            ticker, "Reddit/social rumour spike", None, None, "social_heat",
            labels, float(z_score) if z_score is not None else None,
            f"z {z_score:g}" if z_score is not None else f"{int(mentions or 0)} mentions",
            "z_score", value_date, 0.7,
        ))

    for source_id, value_date, source, title, region, category, used, url in _recent_news_items(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "news_items", "category", category),
            _label_ids(conn, "region", "news_items", "region", region),
        )
        rows.append(_state_row(
            as_of, f"news_item:{source_id}", "news_items", source_id,
            None, title, region, None, category, labels,
            float(used or 0), title, "used_for_predictions", value_date, 0.65,
        ))

    for source_id, value_date, ticker, trades, buys, sells in _insider_activity(conn, as_of):
        labels = _combined_labels(
            _table_labels(conn, "insider_trades"),
            _label_ids(conn, "symbol", "prices", "symbol", ticker),
            _label_ids(conn, "symbol", "targets", "symbol", ticker),
        )
        net = float(buys or 0) + float(sells or 0)
        rows.append(_state_row(
            as_of, f"insider_net:{ticker}", "insider_trades", source_id,
            ticker, "30d insider net value", None, None, "insider",
            labels, net, f"net {net:,.0f} USD across {int(trades or 0)} trades",
            "USD", value_date, 0.75,
        ))

    for source_id, value_date, ticker, period_end, period_type, concept, value, unit in _latest_fundamentals(conn, as_of):
        labels = _combined_labels(
            _source_labels(conn, "company_fundamentals", "concept", concept),
            _label_ids(conn, "symbol", "prices", "symbol", ticker),
            _label_ids(conn, "symbol", "targets", "symbol", ticker),
        )
        rows.append(_state_row(
            as_of, f"fundamental:{ticker}:{_slug(concept)}", "company_fundamentals",
            source_id, ticker, concept, None, None, "fundamentals", labels,
            float(value) if value is not None else None,
            f"{concept}: {value:g} {unit or ''}".strip() if value is not None else concept,
            unit, value_date, 0.75,
        ))

    for source_id, snap_date, entity_id, label, entity_type, theme, direction, horizon, score, market_bias, plain_read in _latest_oracle_snapshots(conn, as_of):
        labels = _label_ids(conn, "theme", "oracle_index_snapshots", "theme", theme)
        rows.append(_state_row(
            as_of, f"oracle_index:{entity_id}:{theme}:{_slug(horizon or 'near_term')}",
            "oracle_index_snapshots",
            source_id, entity_id, label, None, None, theme, labels,
            float(score or 0), plain_read or market_bias or direction, "score",
            snap_date, 0.85,
        ))

    events = _same_day_events(conn, as_of)
    for event in events:
        category_column = "type" if event["source_table"] == "events" else "category"
        labels = _source_labels(
            conn, event["source_table"], category_column,
            event.get("category"), "category",
        )
        rows.append(_state_row(
            as_of,
            f"event:{event['source_table']}:{event['source_id']}",
            event["source_table"], event["source_id"], None, event.get("title"),
            event.get("region"), event.get("country"), event.get("category"),
            labels, float(event["importance"]) if event.get("importance") else None,
            event.get("title"), None, event["date"], 0.9,
        ))

    return rows, events


def _forward_return_rows(
    conn: sqlite3.Connection,
    as_of: dt.date,
    symbols: list[str],
    horizons: Iterable[int],
) -> list[tuple]:
    if not table_exists(conn, "prices"):
        return []
    rows: list[tuple] = []
    for symbol in symbols:
        start = conn.execute(
            """SELECT date, price FROM prices
               WHERE symbol = ? AND date <= ?
               ORDER BY date DESC LIMIT 1""",
            (symbol, as_of.isoformat()),
        ).fetchone()
        if not start:
            continue
        start_date, start_price = start
        if not start_price:
            continue
        for horizon in horizons:
            target_date = (as_of + dt.timedelta(days=int(horizon))).isoformat()
            end = conn.execute(
                """SELECT date, price FROM prices
                   WHERE symbol = ? AND date >= ?
                   ORDER BY date ASC LIMIT 1""",
                (symbol, target_date),
            ).fetchone()
            end_date, end_price, fwd = None, None, None
            if end and end[1] is not None:
                end_date, end_price = end
                fwd = (float(end_price) / float(start_price)) - 1.0
            rows.append((
                as_of.isoformat(), symbol, int(horizon), float(start_price),
                float(end_price) if end_price is not None else None, end_date, fwd,
            ))
    return rows


def _daily_summary(
    as_of: dt.date,
    rows: list[tuple],
    events: list[dict[str, Any]],
    forward_rows: list[tuple],
    expected_forward_rows: int,
    required_keys: tuple[str, ...],
    training_cutoff: dt.date,
    min_coverage: float,
) -> tuple:
    value_keys = {r[1] for r in rows}
    coverage = len(value_keys.intersection(required_keys)) / len(required_keys) if required_keys else 0.0
    training_eligible = int(as_of >= training_cutoff and coverage >= min_coverage)
    completed_forward = sum(1 for row in forward_rows if row[6] is not None)
    evaluation_eligible = int(training_eligible and expected_forward_rows > 0 and completed_forward >= expected_forward_rows)
    labels = sorted({
        label
        for row in rows
        for label in json.loads(row[9] or "[]")
    })
    labeled_rows = sum(1 for row in rows if json.loads(row[9] or "[]"))
    freshness_counts: dict[str, int] = {}
    for row in rows:
        freshness_counts[row[15]] = freshness_counts.get(row[15], 0) + 1
    top_values = sorted(
        [
            {
                "key": row[1],
                "value": row[10],
                "text": row[11],
                "date": row[13],
                "freshness_days": row[14],
                "freshness_class": row[15],
            }
            for row in rows
            if row[10] is not None
        ],
        key=lambda item: (str(item["key"]).startswith("gdelt_stream"), abs(float(item["value"] or 0))),
        reverse=True,
    )[:40]
    event_tags = [
        f"{event.get('region') or event.get('country') or 'Global'}:{event.get('category') or 'event'}"
        for event in events
    ]
    if events:
        notable = events[0]["title"]
    else:
        gdelt = [row for row in rows if row[2] == "gdelt_streams" and row[1].endswith(":societal_impact")]
        notable = max(gdelt, key=lambda r: float(r[10] or 0))[11] if gdelt else "No same-day notable driver captured."
    return (
        as_of.isoformat(), round(coverage, 4), training_eligible, evaluation_eligible,
        json.dumps(event_tags), notable,
        json.dumps({
            "top_values": top_values,
            "value_count": len(rows),
            "labeled_rows": labeled_rows,
            "unlabeled_rows": len(rows) - labeled_rows,
            "label_coverage_pct": round(labeled_rows / len(rows) * 100, 2) if rows else 0.0,
            "freshness_counts": freshness_counts,
            "completed_forward_rows": completed_forward,
            "expected_forward_rows": expected_forward_rows,
        }),
        json.dumps(labels),
    )


def _write_historical_day(
    conn: sqlite3.Connection,
    as_of_iso: str,
    state_rows: list[tuple],
    returns: list[tuple],
    daily: tuple,
) -> dict[str, int]:
    conn.execute("DELETE FROM historical_state_values WHERE as_of = ?", (as_of_iso,))
    conn.execute("DELETE FROM historical_forward_returns WHERE as_of = ?", (as_of_iso,))
    if state_rows:
        conn.executemany(
            """INSERT INTO historical_state_values (
                 as_of, value_key, source_table, source_id, source_symbol,
                 source_name, region, country, category, label_ids_json,
                 value, value_text, unit, value_date, freshness_days,
                 freshness_class, confidence
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(as_of, value_key) DO UPDATE SET
                 source_table=excluded.source_table,
                 source_id=excluded.source_id,
                 source_symbol=excluded.source_symbol,
                 source_name=excluded.source_name,
                 region=excluded.region,
                 country=excluded.country,
                 category=excluded.category,
                 label_ids_json=excluded.label_ids_json,
                 value=excluded.value,
                 value_text=excluded.value_text,
                 unit=excluded.unit,
                 value_date=excluded.value_date,
                 freshness_days=excluded.freshness_days,
                 freshness_class=excluded.freshness_class,
                 confidence=excluded.confidence,
                 active=1,
                 created_at=datetime('now')""",
            state_rows,
        )
    if returns:
        conn.executemany(
            """INSERT INTO historical_forward_returns (
                 as_of, symbol, horizon_days, start_price, end_price,
                 end_date, forward_return
               ) VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(as_of, symbol, horizon_days) DO UPDATE SET
                 start_price=excluded.start_price,
                 end_price=excluded.end_price,
                 end_date=excluded.end_date,
                 forward_return=excluded.forward_return,
                 created_at=datetime('now')""",
            returns,
        )
    conn.execute(
        """INSERT INTO historical_state_daily (
             as_of, coverage_score, training_eligible, evaluation_eligible,
             event_tags_json, notable_driver_comment, state_json, labels_json
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(as_of) DO UPDATE SET
             coverage_score=excluded.coverage_score,
             training_eligible=excluded.training_eligible,
             evaluation_eligible=excluded.evaluation_eligible,
             event_tags_json=excluded.event_tags_json,
             notable_driver_comment=excluded.notable_driver_comment,
             state_json=excluded.state_json,
             labels_json=excluded.labels_json,
             updated_at=datetime('now')""",
        daily,
    )
    return {"values": len(state_rows), "daily": 1, "forward_returns": len(returns)}


def _readonly_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


def _materialize_day_task(args: tuple) -> tuple[str, list[tuple], list[tuple], tuple]:
    (
        db_path, as_of_iso, selected_symbols, horizon_days, required_keys,
        training_cutoff_iso, min_coverage,
    ) = args
    as_of = dt.date.fromisoformat(as_of_iso)
    conn = _readonly_conn(db_path)
    try:
        state_rows, events = state_values_for_date(conn, as_of, selected_symbols)
        returns = _forward_return_rows(conn, as_of, list(selected_symbols), horizon_days)
        daily = _daily_summary(
            as_of, state_rows, events, returns, len(returns),
            required_keys, dt.date.fromisoformat(training_cutoff_iso), min_coverage,
        )
        return as_of_iso, state_rows, returns, daily
    finally:
        _clear_label_assignment_conn_cache(conn)
        conn.close()


def build_historical_state(
    conn: sqlite3.Connection,
    start: dt.date,
    end: dt.date,
    symbols: Iterable[str] | None = None,
    horizons: Iterable[int] = DEFAULT_FORWARD_HORIZONS,
    training_cutoff: dt.date = TRAINING_CUTOFF,
    required_keys: tuple[str, ...] = CORE_REQUIRED_KEYS,
    min_coverage: float = 0.70,
) -> dict[str, int]:
    selected_symbols = _target_symbols(conn, symbols)
    horizon_days = tuple(int(h) for h in horizons)
    value_count = 0
    daily_count = 0
    forward_count = 0

    for as_of in _date_range(start, end):
        state_rows, events = state_values_for_date(conn, as_of, selected_symbols)
        returns = _forward_return_rows(conn, as_of, selected_symbols, horizon_days)
        daily = _daily_summary(
            as_of, state_rows, events, returns, len(returns),
            required_keys, training_cutoff, min_coverage,
        )
        counts = _write_historical_day(conn, as_of.isoformat(), state_rows, returns, daily)
        value_count += counts["values"]
        daily_count += counts["daily"]
        forward_count += counts["forward_returns"]

    return {"values": value_count, "daily": daily_count, "forward_returns": forward_count}


def build_historical_state_parallel(
    conn: sqlite3.Connection,
    start: dt.date,
    end: dt.date,
    symbols: Iterable[str] | None = None,
    horizons: Iterable[int] = DEFAULT_FORWARD_HORIZONS,
    training_cutoff: dt.date = TRAINING_CUTOFF,
    required_keys: tuple[str, ...] = CORE_REQUIRED_KEYS,
    min_coverage: float = 0.70,
    workers: int = DEFAULT_HISTORICAL_WORKERS,
    db_path: str = DB_PATH,
) -> dict[str, int]:
    worker_count = max(1, int(workers or 1))
    if worker_count <= 1 or db_path == ":memory:":
        return build_historical_state(
            conn, start, end, symbols=symbols, horizons=horizons,
            training_cutoff=training_cutoff, required_keys=required_keys,
            min_coverage=min_coverage,
        )

    selected_symbols = tuple(_target_symbols(conn, symbols))
    horizon_days = tuple(int(h) for h in horizons)
    dates = [day.isoformat() for day in _date_range(start, end)]
    if not dates:
        return {"values": 0, "daily": 0, "forward_returns": 0}

    tasks = [
        (
            db_path, as_of_iso, selected_symbols, horizon_days, required_keys,
            training_cutoff.isoformat(), min_coverage,
        )
        for as_of_iso in dates
    ]
    chunk_size = max(1, min(32, len(tasks) // (worker_count * 8) or 1))
    ctx = mp.get_context("fork") if "fork" in mp.get_all_start_methods() else mp.get_context()
    value_count = 0
    daily_count = 0
    forward_count = 0
    commit_every = max(32, worker_count * 8)
    conn.execute("PRAGMA busy_timeout=60000")
    with ctx.Pool(processes=worker_count) as pool:
        for as_of_iso, state_rows, returns, daily in pool.imap_unordered(
            _materialize_day_task, tasks, chunksize=chunk_size,
        ):
            counts = _write_historical_day(conn, as_of_iso, state_rows, returns, daily)
            value_count += counts["values"]
            daily_count += counts["daily"]
            forward_count += counts["forward_returns"]
            if daily_count % commit_every == 0:
                conn.commit()
    return {"values": value_count, "daily": daily_count, "forward_returns": forward_count}


def _delete_historical_rows_after(conn: sqlite3.Connection, end: dt.date) -> None:
    for table in ("historical_state_values", "historical_state_daily", "historical_forward_returns"):
        if table_exists(conn, table):
            conn.execute(f"DELETE FROM {table} WHERE as_of > ?", (end.isoformat(),))


def build_and_store(
    start: dt.date | None = None,
    end: dt.date | None = None,
    symbols: Iterable[str] | None = None,
    horizons: Iterable[int] = DEFAULT_FORWARD_HORIZONS,
    workers: int = DEFAULT_HISTORICAL_WORKERS,
) -> dict[str, int]:
    start = start or TRAINING_CUTOFF
    conn = connect_writable(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        end = end or _latest_complete_market_date(conn) or dt.date.today()
        _delete_historical_rows_after(conn, end)
        conn.commit()
        result = build_historical_state_parallel(
            conn, start, end, symbols=symbols, horizons=horizons,
            workers=workers, db_path=DB_PATH,
        )
        conn.commit()
        log(
            f"Built historical states {start.isoformat()}..{end.isoformat()}: "
            f"{result['daily']} day(s), {result['values']} value row(s), "
            f"{result['forward_returns']} forward-return row(s), workers={max(1, int(workers or 1))}.",
            module="historical_state",
        )
        return result
    finally:
        _clear_label_assignment_conn_cache(conn)
        conn.close()


def build_recent_and_store(
    recent_days: int = 120,
    symbols: Iterable[str] | None = None,
    horizons: Iterable[int] = DEFAULT_FORWARD_HORIZONS,
    workers: int = DEFAULT_HISTORICAL_WORKERS,
) -> dict[str, int]:
    conn = connect_writable(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        end = _latest_complete_market_date(conn) or dt.date.today()
        start = max(TRAINING_CUTOFF, end - dt.timedelta(days=max(1, int(recent_days)) - 1))
        _delete_historical_rows_after(conn, end)
        conn.commit()
        result = build_historical_state_parallel(
            conn, start, end, symbols=symbols, horizons=horizons,
            workers=workers, db_path=DB_PATH,
        )
        conn.commit()
        log(
            f"Built recent historical states {start.isoformat()}..{end.isoformat()}: "
            f"{result['daily']} day(s), {result['values']} value row(s), "
            f"{result['forward_returns']} forward-return row(s), workers={max(1, int(workers or 1))}.",
            module="historical_state",
        )
        return result
    finally:
        _clear_label_assignment_conn_cache(conn)
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=None, help="YYYY-MM-DD; defaults to 2009-11-30.")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD; defaults to today.")
    parser.add_argument("--recent-days", type=int, default=None, help="Refresh the latest N days through the latest complete market date.")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbol list; defaults to active targets plus core symbols.")
    parser.add_argument("--horizons", default="5,21,63", help="Comma-separated forward-return horizons in calendar days.")
    parser.add_argument("--workers", type=int, default=DEFAULT_HISTORICAL_WORKERS, help="Parallel day-materialization workers; defaults to all CPU cores.")
    args = parser.parse_args(argv)
    start = dt.date.fromisoformat(args.start) if args.start else None
    end = dt.date.fromisoformat(args.end) if args.end else None
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    horizons = tuple(int(x.strip()) for x in args.horizons.split(",") if x.strip())
    if args.recent_days is not None:
        build_recent_and_store(
            recent_days=args.recent_days,
            symbols=symbols,
            horizons=horizons,
            workers=args.workers,
        )
    else:
        build_and_store(start=start, end=end, symbols=symbols, horizons=horizons, workers=args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
