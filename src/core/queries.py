"""Read-only intelligence queries shared by frontend clients and future APIs."""
from __future__ import annotations

import sqlite3
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd

from src.core.db import connect_readonly, table_exists
from src.core.source_registry import registry_rows


SOURCE_HEALTH_CHECKS: tuple[dict[str, Any], ...] = (
    {"table": "prices", "label": "Market close prices", "col": "date", "max_hours": 96, "cadence": "daily close"},
    {"table": "signals", "label": "Computed signals", "col": "date", "where": "symbol != '_news'", "max_hours": 96, "cadence": "daily"},
    {"table": "signals", "label": "GDELT macro pulse", "col": "date", "where": "symbol = '_news' AND signal_name LIKE 'news_rate_%'", "max_hours": 36, "cadence": "hourly"},
    {"table": "events", "label": "Legacy event archive", "col": "date", "max_hours": None, "cadence": "archive"},
    {"table": "calendar_events", "label": "Macro calendar", "col": "date", "max_hours": None, "cadence": "manual/daily"},
    {"table": "macro_release_actuals", "label": "Macro release actuals", "col": "updated_at", "max_hours": 12, "cadence": "hourly/daily"},
    {"table": "indicators", "label": "Macro indicators", "col": "date", "max_hours": 90 * 24, "cadence": "daily/official"},
    {"table": "social_mentions", "label": "Reddit listener", "col": "date", "max_hours": 36, "cadence": "hourly"},
    {"table": "rumour_signals", "label": "Rumour synthesis", "col": "date", "max_hours": 48, "cadence": "daily"},
    {"table": "weather_obs", "label": "Weather observations", "col": "date", "max_hours": 7 * 24, "cadence": "daily archive"},
    {"table": "weather_correlations", "label": "Weather correlations", "col": "computed_at", "max_hours": 14 * 24, "cadence": "daily"},
    {"table": "company_fundamentals", "label": "Company fundamentals", "col": "filed", "max_hours": 120 * 24, "cadence": "daily"},
    {"table": "insider_trades", "label": "Insider Form 4", "col": "filing_date", "max_hours": 14 * 24, "cadence": "6h/daily"},
    {"table": "sanctions", "label": "Sanctions lists", "col": "fetched_at", "max_hours": 30, "cadence": "6h"},
    {"table": "news_items", "label": "RSS news tape", "col": "COALESCE(published_at, fetched_at)", "max_hours": 24, "cadence": "hourly"},
    {"table": "gdelt_disaster_signals", "label": "GDELT disasters", "col": "date", "max_hours": 36, "cadence": "hourly"},
    {"table": "gdelt_streams", "label": "GDELT streams", "col": "date", "max_hours": 36, "cadence": "hourly"},
    {"table": "twitter_macro_posts", "label": "X/Twitter macro listener", "col": "COALESCE(posted_at, fetched_at)", "max_hours": 6, "cadence": "hourly/token"},
    {"table": "intelligence_packages", "label": "World Pull intelligence", "col": "generated_at", "max_hours": 3, "cadence": "hourly"},
    {"table": "macro_event_predictions", "label": "Macro-event forecasts", "col": "generated_at", "max_hours": 12, "cadence": "daily/event"},
    {"table": "oracle_index_snapshots", "label": "Atlas impact graph", "col": "generated_at", "max_hours": 3, "cadence": "hourly"},
    {"table": "historical_state_daily", "label": "Historical comparison panel", "col": "as_of", "max_hours": None, "cadence": "manual/backfill"},
    {"table": "prediction_context_packs", "label": "Prediction context packs", "col": "updated_at", "max_hours": 24, "cadence": "per prediction"},
    {"table": "label_evaluations", "label": "Label evaluation", "col": "last_evaluated_at", "max_hours": 48, "cadence": "daily"},
    {"table": "data_objects", "label": "Semantic data contract", "col": "updated_at", "max_hours": None, "cadence": "schema seed"},
    {"table": "data_change_events", "label": "Data change events", "col": "created_at", "max_hours": 24, "cadence": "event-driven"},
    {"table": "current_events", "label": "Current events canary", "col": "updated_at", "max_hours": 1, "cadence": "15m"},
    {"table": "backtest_predictions", "label": "Forecast-lab backtests", "col": "generated_at", "max_hours": None, "cadence": "manual"},
    {"table": "source_runs", "label": "Ingestion run telemetry", "col": "started_at", "max_hours": 24, "cadence": "every pipeline"},
)


def source_health(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """Return source freshness checks for frontend clients and future APIs."""
    own_conn = conn is None
    conn = conn or connect_readonly()
    rows = []
    now = pd.Timestamp.now(tz="UTC")
    try:
        for check in SOURCE_HEALTH_CHECKS:
            table = check["table"]
            col = check["col"]
            where = check.get("where")
            max_hours = check["max_hours"]
            if not table_exists(conn, table):
                rows.append({
                    "source": check["label"], "cadence": check["cadence"],
                    "rows": 0, "latest": "table missing", "age": "n/a", "status": "empty",
                })
                continue
            if table == "calendar_events":
                count, latest = conn.execute(
                    f"SELECT COUNT(*), MIN({col}) FROM {table} WHERE {col} >= date('now')"
                ).fetchone()
                status = "ok" if count else "stale"
                age_label = "future"
            else:
                where_sql = f" AND {where}" if where else ""
                count, latest = conn.execute(
                    f"""SELECT COUNT(*), MAX({col}) FROM {table}
                        WHERE substr({col}, 1, 10) <= date('now'){where_sql}"""
                ).fetchone()
                age_label = "n/a"
                status = "empty" if not count else "ok"
                if latest:
                    status, age_label = _freshness_status(str(latest), now, max_hours, status)
            rows.append({
                "source": check["label"],
                "cadence": check["cadence"],
                "rows": int(count or 0),
                "latest": latest or "n/a",
                "age": age_label,
                "status": status,
            })
        return pd.DataFrame(rows)
    finally:
        if own_conn:
            conn.close()


def _freshness_status(latest: str, now: pd.Timestamp, max_hours: int | None, default_status: str) -> tuple[str, str]:
    try:
        if len(latest) <= 10 and "T" not in latest:
            latest_date = date.fromisoformat(latest[:10])
            age_days = max((date.today() - latest_date).days, 0)
            age_hours = age_days * 24
            age_label = "today" if age_days == 0 else f"{age_days}d"
        else:
            latest_ts = pd.to_datetime(latest, utc=True, errors="raise")
            age_hours = max((now - latest_ts).total_seconds() / 3600, 0)
            age_label = f"{age_hours:.0f}h" if age_hours < 48 else f"{age_hours / 24:.0f}d"
        status = "stale" if max_hours is not None and age_hours > max_hours else default_status
        return status, age_label
    except (ValueError, TypeError):
        return "unknown", "n/a"


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _safe_identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQLite identifier: {value!r}")
    return value


DATA_OBJECT_COLUMNS = [
    "object_id", "display_name", "prompt_name", "source_table", "description",
    "source_family", "freshness_sla_hours", "cadence", "frontend_group",
    "prompt_role", "active", "updated_at",
]


def data_catalog(conn: sqlite3.Connection | None = None, active_only: bool = True) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "data_objects"):
            return _empty(DATA_OBJECT_COLUMNS)
        where = "WHERE active = 1" if active_only else ""
        return pd.read_sql(
            f"""SELECT object_id, display_name, prompt_name, source_table,
                       description, source_family, freshness_sla_hours,
                       cadence, frontend_group, prompt_role, active, updated_at
                FROM data_objects
                {where}
                ORDER BY frontend_group, object_id""",
            conn,
            parse_dates=["updated_at"],
        )
    finally:
        if own_conn:
            conn.close()


def source_registry(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    rows = registry_rows()
    return pd.DataFrame(rows).sort_values(["heavy", "source"]).reset_index(drop=True)


def _source_table_count_latest(conn: sqlite3.Connection, table: str) -> tuple[int, str | None]:
    if not table or not table_exists(conn, table):
        return 0, None
    latest_columns = (
        "updated_at", "generated_at", "fetched_at", "created_at", "date",
        "as_of", "started_at", "filed", "filing_date", "published_at",
    )
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({_safe_identifier(table)})")}
    latest_col = next((col for col in latest_columns if col in existing), None)
    if latest_col:
        row = conn.execute(
            f"SELECT COUNT(*), MAX({_safe_identifier(latest_col)}) FROM {_safe_identifier(table)}"
        ).fetchone()
    else:
        row = conn.execute(f"SELECT COUNT(*), NULL FROM {_safe_identifier(table)}").fetchone()
    return int(row[0] or 0), row[1]


def named_data_overview(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    columns = DATA_OBJECT_COLUMNS + ["rows", "latest", "status"]
    try:
        catalog = data_catalog(conn)
        if catalog.empty:
            return _empty(columns)
        records = []
        now = pd.Timestamp.now(tz="UTC")
        for row in catalog.to_dict("records"):
            table = row.get("source_table") or ""
            count, latest = _source_table_count_latest(conn, table)
            status = "missing" if table and not table_exists(conn, table) else "empty"
            if count:
                status, _ = _freshness_status(
                    str(latest) if latest else "",
                    now,
                    int(row["freshness_sla_hours"]) if pd.notna(row.get("freshness_sla_hours")) else None,
                    "ok",
                )
            records.append({**row, "rows": count, "latest": latest, "status": status})
        return pd.DataFrame(records, columns=columns)
    finally:
        if own_conn:
            conn.close()



CURRENT_EVENT_COLUMNS = [
    "id", "event_key", "event_type", "title", "summary", "event_time",
    "region", "category", "priority", "status", "object_id",
    "source_table", "source_id", "labels_json", "affected_assets_json",
    "display_title", "display_summary", "why_text", "source_quality",
    "metadata_json", "oracle_review_required", "created_at", "updated_at",
    "expires_at",
]


def current_events(
    conn: sqlite3.Connection | None = None,
    hours_back: int = 48,
    days_forward: int = 14,
    limit: int = 100,
    include_expired: bool = False,
    now: datetime | None = None,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "current_events"):
            return _empty(CURRENT_EVENT_COLUMNS)
        ref = now or datetime.now(timezone.utc)
        if ref.tzinfo is not None:
            ref = ref.astimezone(timezone.utc).replace(tzinfo=None)
        ref_sql = ref.isoformat(sep=" ", timespec="seconds")
        clauses = ["datetime(event_time) BETWEEN datetime(?, ?) AND datetime(?, ?)"]
        params: list[Any] = [ref_sql, f"-{int(hours_back)} hours", ref_sql, f"+{int(days_forward)} days"]
        if not include_expired:
            clauses.append("status = 'active'")
            clauses.append("(expires_at IS NULL OR datetime(expires_at) >= datetime(?))")
            params.append(ref_sql)
        existing = {row[1] for row in conn.execute("PRAGMA table_info(current_events)")}
        display_title = "COALESCE(display_title, title)" if "display_title" in existing else "title"
        display_summary = "COALESCE(display_summary, summary)" if "display_summary" in existing else "summary"
        why_text = "why_text" if "why_text" in existing else "summary"
        source_quality = "source_quality" if "source_quality" in existing else "source_table"
        params.append(int(limit))
        return pd.read_sql(
            f"""SELECT id, event_key, event_type, title, summary, event_time,
                       region, category, priority, status, object_id,
                       source_table, source_id, labels_json, affected_assets_json,
                       {display_title} AS display_title,
                       {display_summary} AS display_summary,
                       {why_text} AS why_text,
                       {source_quality} AS source_quality,
                       metadata_json, oracle_review_required, created_at, updated_at,
                       expires_at
                FROM current_events
                WHERE {' AND '.join(clauses)}
                ORDER BY
                  CASE event_type
                    WHEN 'breaking_news' THEN 0
                    WHEN 'scheduled_catalyst' THEN 1
                    WHEN 'released_actual' THEN 2
                    WHEN 'source_alert' THEN 3
                    ELSE 4
                  END,
                  priority DESC,
                  datetime(event_time) ASC
                LIMIT ?""",
            conn,
            params=params,
            parse_dates=["event_time", "created_at", "updated_at", "expires_at"],
        )
    finally:
        if own_conn:
            conn.close()

CHANGE_EVENT_COLUMNS = [
    "id", "event_key", "object_id", "source_table", "source_id", "event_type",
    "priority", "labels_json", "metadata_json", "status",
    "oracle_review_required", "created_at", "updated_at",
]


def data_change_events(
    conn: sqlite3.Connection | None = None,
    status: str | None = None,
    min_priority: float | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "data_change_events"):
            return _empty(CHANGE_EVENT_COLUMNS)
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if min_priority is not None:
            clauses.append("priority >= ?")
            params.append(float(min_priority))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(int(limit))
        return pd.read_sql(
            f"""SELECT id, event_key, object_id, source_table, source_id,
                       event_type, priority, labels_json, metadata_json, status,
                       oracle_review_required, created_at, updated_at
                FROM data_change_events
                {where}
                ORDER BY priority DESC, created_at DESC
                LIMIT ?""",
            conn,
            params=params,
            parse_dates=["created_at", "updated_at"],
        )
    finally:
        if own_conn:
            conn.close()


CONTEXT_PACK_COLUMNS = [
    "id", "asset", "horizon", "as_of", "profile_id", "pack_json",
    "prompt_md", "historical_refs_json", "input_hash", "created_at", "updated_at",
]


def prediction_context_packs(
    conn: sqlite3.Connection | None = None,
    asset: str | None = None,
    horizon: str | None = None,
    limit: int = 100,
    include_prompt: bool = False,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "prediction_context_packs"):
            columns = CONTEXT_PACK_COLUMNS if include_prompt else [c for c in CONTEXT_PACK_COLUMNS if c != "prompt_md"]
            return _empty(columns)
        clauses: list[str] = []
        params: list[Any] = []
        if asset:
            clauses.append("asset = ?")
            params.append(asset)
        if horizon:
            clauses.append("horizon = ?")
            params.append(horizon)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(int(limit))
        select_cols = (
            "id, asset, horizon, as_of, profile_id, pack_json, prompt_md, "
            "historical_refs_json, input_hash, created_at, updated_at"
            if include_prompt
            else "id, asset, horizon, as_of, profile_id, pack_json, "
                 "historical_refs_json, input_hash, created_at, updated_at"
        )
        return pd.read_sql(
            f"""SELECT {select_cols}
                FROM prediction_context_packs
                {where}
                ORDER BY as_of DESC, updated_at DESC
                LIMIT ?""",
            conn,
            params=params,
            parse_dates=["as_of", "created_at", "updated_at"],
        )
    finally:
        if own_conn:
            conn.close()


JOURNAL_REVIEW_TYPES = (
    "current_event_journal",
    "macro_release_journal",
    "prediction_journal",
)

JOURNAL_NOTE_COLUMNS = [
    "id", "source_table", "source_id", "as_of", "review_type", "severity",
    "confidence", "comment", "model", "input_hash", "created_at",
]


def oracle_journal_notes(
    conn: sqlite3.Connection | None = None,
    *,
    review_type: str | None = None,
    days: int = 30,
    limit: int = 50,
) -> pd.DataFrame:
    """Read structured research notes from review annotations."""
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "oracle_review_annotations"):
            return _empty(JOURNAL_NOTE_COLUMNS)
        params: list[Any] = []
        clauses = ["date(created_at) >= date('now', ?)"]
        params.append(f"-{max(1, int(days))} days")
        if review_type:
            clauses.append("review_type = ?")
            params.append(review_type)
        else:
            placeholders = ",".join("?" for _ in JOURNAL_REVIEW_TYPES)
            clauses.append(f"review_type IN ({placeholders})")
            params.extend(JOURNAL_REVIEW_TYPES)
        params.append(int(limit))
        return pd.read_sql(
            f"""SELECT id, source_table, source_id, as_of, review_type, severity,
                       confidence, comment, model, input_hash, created_at
                FROM oracle_review_annotations
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at DESC, severity DESC
                LIMIT ?""",
            conn,
            params=params,
            parse_dates=["as_of", "created_at"],
        )
    finally:
        if own_conn:
            conn.close()


LABEL_EVALUATION_COLUMNS = [
    "profile_id", "label_id", "label_type", "label", "asset", "horizon",
    "horizon_days", "observations", "positive_count", "hit_rate",
    "avg_forward_return", "median_forward_return", "weighted_avg_forward_return",
    "avg_label_count", "effective_weight", "methodology_version", "scope",
    "first_as_of", "last_as_of", "last_evaluated_at",
]


def label_evaluation(
    conn: sqlite3.Connection | None = None,
    *,
    profile_id: str = "default",
    asset: str | None = None,
    horizon: str | None = None,
    min_observations: int = 20,
    limit: int = 100,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "label_evaluations"):
            return _empty(LABEL_EVALUATION_COLUMNS)
        clauses = ["le.profile_id = ?"]
        params: list[Any] = [profile_id]
        if asset:
            clauses.append("le.asset = ?")
            params.append(asset)
        if horizon:
            clauses.append("le.horizon = ?")
            params.append(horizon)
        if min_observations:
            clauses.append("COALESCE(le.observations, 0) >= ?")
            params.append(int(min_observations))
        existing = {row[1] for row in conn.execute("PRAGMA table_info(label_evaluations)")}
        methodology = "le.methodology_version" if "methodology_version" in existing else "'v1_broad_date_join'"
        scope = "le.scope" if "scope" in existing else "'broad_date'"
        params.append(int(limit))
        return pd.read_sql(
            f"""SELECT le.profile_id, le.label_id, dl.label_type, dl.label,
                      le.asset, le.horizon, le.horizon_days, le.observations,
                      le.positive_count, le.hit_rate, le.avg_forward_return,
                      le.median_forward_return, le.weighted_avg_forward_return,
                      le.avg_label_count, le.effective_weight,
                      {methodology} AS methodology_version,
                      {scope} AS scope,
                      le.first_as_of,
                      le.last_as_of, le.last_evaluated_at
               FROM label_evaluations le
               LEFT JOIN data_labels dl ON dl.label_id = le.label_id
               WHERE {' AND '.join(clauses)}
               ORDER BY le.observations DESC, ABS(COALESCE(le.avg_forward_return, 0)) DESC
               LIMIT ?""",
            conn,
            params=params,
            parse_dates=["first_as_of", "last_as_of", "last_evaluated_at"],
        )
    finally:
        if own_conn:
            conn.close()


def _assignment_labels(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    if not table_exists(conn, "data_label_assignments"):
        return []
    rows = conn.execute(
        """SELECT label_id, target_column, NULLIF(target_value, '') AS target_value,
                  weight_override, confidence
           FROM data_label_assignments
           WHERE active = 1 AND target_table = ?""",
        (table,),
    ).fetchall()
    return [
        {
            "label_id": r[0],
            "target_column": r[1] or "",
            "target_value": r[2],
            "weight_override": r[3],
            "confidence": r[4],
        }
        for r in rows
    ]


def label_enriched_source(
    conn: sqlite3.Connection | None = None,
    source: str = "prices",
    limit: int = 100,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    source = _safe_identifier(source)
    try:
        if not table_exists(conn, source):
            return _empty(["source_table", "labels_json"])
        labels = _assignment_labels(conn, source)
        df = pd.read_sql(f"SELECT * FROM {source} LIMIT ?", conn, params=(int(limit),))
        if df.empty:
            df["source_table"] = source
            df["labels_json"] = "[]"
            return df

        table_labels = [item["label_id"] for item in labels if not item["target_column"]]

        def row_labels(record: pd.Series) -> str:
            row_label_ids = list(table_labels)
            for item in labels:
                col = item["target_column"]
                value = item["target_value"]
                if col and col in record and (value is None or str(record[col]) == str(value)):
                    row_label_ids.append(item["label_id"])
            return json.dumps(sorted(set(row_label_ids)))

        df.insert(0, "source_table", source)
        df["labels_json"] = df.apply(row_labels, axis=1)
        return df
    finally:
        if own_conn:
            conn.close()


def target_assets(conn: sqlite3.Connection | None = None, active_only: bool = False) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    where = "WHERE active = 1" if active_only else ""
    try:
        if not table_exists(conn, "targets"):
            return _empty(["symbol", "name", "asset_class", "horizons", "source", "active", "added_at"])
        return pd.read_sql(
            f"""SELECT symbol, name, asset_class, horizons, source,
                       CAST(active AS INTEGER) AS active, added_at
                FROM targets {where}
                ORDER BY active DESC, symbol""",
            conn,
        )
    finally:
        if own_conn:
            conn.close()


def predictions(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "predictions"):
            return _empty(["id", "asset", "horizon", "as_of", "generated_at", "direction", "confidence", "expected_return_low", "expected_return_high", "rationale_md", "key_risks", "analogues_used", "model", "input_hash", "input_brief_md", "realized_return", "scored_at"])
        return pd.read_sql(
            """SELECT id, asset, horizon, as_of, generated_at, direction, confidence,
                      expected_return_low, expected_return_high,
                      rationale_md, key_risks, analogues_used,
                      model, input_hash, input_brief_md, realized_return, scored_at
               FROM predictions
               ORDER BY as_of DESC, asset, horizon""",
            conn, parse_dates=["as_of", "generated_at", "scored_at"],
        )
    finally:
        if own_conn:
            conn.close()


def prediction_summaries(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    """Latest public-safe prediction rows per asset/horizon.

    Full prompt text is deliberately excluded from this summary surface.
    """
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "predictions"):
            return _empty(["id", "asset", "horizon", "as_of", "generated_at", "direction", "confidence", "expected_return_low", "expected_return_high", "rationale_md", "key_risks", "model", "input_hash", "realized_return", "scored_at"])
        return pd.read_sql(
            """WITH latest AS (
                 SELECT asset, horizon, MAX(as_of) AS max_as_of
                 FROM predictions
                 GROUP BY asset, horizon
               )
               SELECT p.id, p.asset, p.horizon, p.as_of, p.generated_at,
                      p.direction, p.confidence,
                      p.expected_return_low, p.expected_return_high,
                      p.rationale_md, p.key_risks, p.model,
                      p.input_hash, p.realized_return, p.scored_at
               FROM predictions p
               JOIN latest l ON l.asset = p.asset
                            AND l.horizon = p.horizon
                            AND l.max_as_of = p.as_of
               ORDER BY p.asset, p.horizon""",
            conn, parse_dates=["as_of", "generated_at", "scored_at"],
        )
    finally:
        if own_conn:
            conn.close()


MARKET_TAPE_COLUMNS = [
    "symbol", "name", "asset_class", "latest_date", "last_price",
    "move_1d", "move_1w", "move_1m", "prediction_direction",
    "prediction_confidence", "prediction_horizon", "prediction_expected_low",
    "prediction_expected_high", "related_labels",
]


def market_tape(conn: sqlite3.Connection | None = None, limit: int = 40) -> pd.DataFrame:
    """Compact market tape with latest price move and current prediction direction."""
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "targets") or not table_exists(conn, "prices"):
            return _empty(MARKET_TAPE_COLUMNS)
        target_columns = {row[1] for row in conn.execute("PRAGMA table_info(targets)")}
        price_columns = {row[1] for row in conn.execute("PRAGMA table_info(prices)")}
        order_sql = "display_order, symbol" if "display_order" in target_columns else "symbol"
        price_col = "close" if "close" in price_columns else "price" if "price" in price_columns else None
        if price_col is None:
            return _empty(MARKET_TAPE_COLUMNS)
        targets = pd.read_sql(
            f"""SELECT symbol, COALESCE(name, symbol) AS name, COALESCE(asset_class, '') AS asset_class
               FROM targets
               WHERE active = 1
               ORDER BY {order_sql}
               LIMIT ?""",
            conn,
            params=(int(limit),),
        )
        if targets.empty:
            return _empty(MARKET_TAPE_COLUMNS)
        symbols = targets["symbol"].dropna().astype(str).tolist()
        placeholders = ",".join("?" for _ in symbols)
        prices = pd.read_sql(
            f"""SELECT symbol, date, {price_col} AS close
                FROM prices
                WHERE symbol IN ({placeholders})
                  AND date >= date('now', '-75 days')
                ORDER BY symbol, date""",
            conn,
            params=symbols,
            parse_dates=["date"],
        )
        preds = prediction_summaries(conn)
        pred_by_asset: dict[str, dict[str, Any]] = {}
        if not preds.empty:
            priority = {"1w": 0, "1m": 1, "3m": 2, "6m": 3}
            for asset, group in preds.groupby("asset"):
                ranked = group.assign(_priority=group["horizon"].map(priority).fillna(99)).sort_values(["_priority", "horizon"])
                pred_by_asset[str(asset)] = ranked.iloc[0].to_dict()
        records: list[dict[str, Any]] = []

        def _move(group: pd.DataFrame, latest_date: pd.Timestamp, latest_close: float, days: int) -> float | None:
            cutoff = latest_date - pd.Timedelta(days=days)
            prior = group[group["date"] <= cutoff]
            if prior.empty:
                prior = group[group["date"] < latest_date]
            if prior.empty:
                return None
            base = float(prior.iloc[-1]["close"] or 0)
            if not base:
                return None
            return (latest_close / base - 1.0) * 100.0

        for target in targets.to_dict("records"):
            symbol = str(target["symbol"])
            group = prices[prices["symbol"] == symbol].dropna(subset=["close"])
            if group.empty:
                latest_date = None
                latest_close = None
                moves = {"move_1d": None, "move_1w": None, "move_1m": None}
            else:
                latest = group.iloc[-1]
                latest_date = latest["date"]
                latest_close = float(latest["close"])
                moves = {
                    "move_1d": _move(group, latest_date, latest_close, 1),
                    "move_1w": _move(group, latest_date, latest_close, 7),
                    "move_1m": _move(group, latest_date, latest_close, 30),
                }
            pred = pred_by_asset.get(symbol, {})
            asset_class = str(target.get("asset_class") or "market")
            labels = [f"asset_group:{asset_class.lower().replace(' ', '_')}"]
            if pred.get("direction"):
                labels.append(f"prediction:{str(pred['direction']).lower()}")
            records.append({
                "symbol": symbol,
                "name": target.get("name") or symbol,
                "asset_class": target.get("asset_class") or "",
                "latest_date": latest_date,
                "last_price": latest_close,
                **moves,
                "prediction_direction": pred.get("direction"),
                "prediction_confidence": pred.get("confidence"),
                "prediction_horizon": pred.get("horizon"),
                "prediction_expected_low": pred.get("expected_return_low"),
                "prediction_expected_high": pred.get("expected_return_high"),
                "related_labels": ", ".join(labels),
            })
        return pd.DataFrame(records, columns=MARKET_TAPE_COLUMNS)
    finally:
        if own_conn:
            conn.close()


def prediction_detail(conn: sqlite3.Connection | None, prediction_id: int, include_private_input: bool = False) -> dict[str, Any] | None:
    own_conn = conn is None
    conn = conn or connect_readonly()
    columns = (
        "id, asset, horizon, as_of, generated_at, direction, confidence, "
        "expected_return_low, expected_return_high, rationale_md, key_risks, "
        "analogues_used, model, input_hash, realized_return, scored_at"
    )
    if include_private_input:
        columns += ", input_brief_md"
    try:
        if not table_exists(conn, "predictions"):
            return None
        row = conn.execute(
            f"SELECT {columns} FROM predictions WHERE id = ?",
            (prediction_id,),
        ).fetchone()
        if not row:
            return None
        names = [d[0] for d in conn.execute(f"SELECT {columns} FROM predictions WHERE id = ? LIMIT 0", (prediction_id,)).description]
        return dict(zip(names, row, strict=False))
    finally:
        if own_conn:
            conn.close()


def recent_events(conn: sqlite3.Connection | None = None, days: int = 90, types: tuple[str, ...] | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    try:
        if not table_exists(conn, "events"):
            return _empty(["date", "title", "country", "type", "source"])
        if types:
            ph = ",".join("?" for _ in types)
            return pd.read_sql(
                f"""SELECT date, title, country, type, source FROM events
                    WHERE date >= ? AND type IN ({ph}) ORDER BY date DESC""",
                conn, params=(cutoff, *types), parse_dates=["date"],
            )
        return pd.read_sql(
            """SELECT date, title, country, type, source FROM events
               WHERE date >= ? ORDER BY date DESC""",
            conn, params=(cutoff,), parse_dates=["date"],
        )
    finally:
        if own_conn:
            conn.close()


def calendar_events(conn: sqlite3.Connection | None = None, days_forward: int = 45) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    end = (date.today() + timedelta(days=days_forward)).isoformat()
    columns = [
        "date", "time_local", "region", "category", "importance", "title",
        "expected", "market_note", "source", "url", "event_key",
        "release_family", "scheduled_at_utc", "source_uid", "labels_json", "status",
    ]
    try:
        if not table_exists(conn, "calendar_events"):
            return _empty(columns)
        existing = {row[1] for row in conn.execute("PRAGMA table_info(calendar_events)")}
        select_cols = [
            col if col in existing else f"NULL AS {col}"
            for col in columns
        ]
        return pd.read_sql(
            f"""SELECT {', '.join(select_cols)}
               FROM calendar_events
               WHERE date BETWEEN date('now') AND ?
               ORDER BY date ASC, importance DESC, region""",
            conn, params=(end,), parse_dates=["date"],
        )
    finally:
        if own_conn:
            conn.close()


SOURCE_RUN_COLUMNS = [
    "run_id", "pipeline", "source", "started_at", "finished_at", "status",
    "duration_sec", "rows_seen", "rows_inserted", "rows_updated",
    "latest_source_ts", "error_message", "metadata_json",
]


def source_run_history(
    conn: sqlite3.Connection | None = None,
    source: str | None = None,
    days: int = 14,
    limit: int = 200,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "source_runs"):
            return _empty(SOURCE_RUN_COLUMNS)
        clauses = ["started_at >= datetime('now', ?)"]
        params: list[Any] = [f"-{int(days)} days"]
        if source:
            clauses.append("source = ?")
            params.append(source)
        params.append(int(limit))
        return pd.read_sql(
            f"""SELECT run_id, pipeline, source, started_at, finished_at, status,
                       duration_sec, rows_seen, rows_inserted, rows_updated,
                       latest_source_ts, error_message, metadata_json
                FROM source_runs
                WHERE {' AND '.join(clauses)}
                ORDER BY started_at DESC, run_id DESC
                LIMIT ?""",
            conn, params=params, parse_dates=["started_at", "finished_at"],
        )
    finally:
        if own_conn:
            conn.close()


def ingestion_health(conn: sqlite3.Connection | None = None, days: int = 7) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    columns = [
        "source", "pipeline", "runs", "last_status", "last_started_at",
        "last_success_at", "last_failure_at", "failure_streak",
        "avg_duration_sec", "rows_seen", "rows_inserted", "rows_updated",
        "latest_source_ts", "last_error",
    ]
    try:
        runs = source_run_history(conn, days=days, limit=10_000)
        if runs.empty:
            return _empty(columns)
        records = []
        for source, group in runs.sort_values("started_at", ascending=False).groupby("source", sort=False):
            latest = group.iloc[0]
            successes = group[group["status"] == "success"]
            failures = group[group["status"] != "success"]
            streak = 0
            for status in group["status"].tolist():
                if status == "success":
                    break
                streak += 1
            records.append({
                "source": source,
                "pipeline": latest["pipeline"],
                "runs": int(len(group)),
                "last_status": latest["status"],
                "last_started_at": latest["started_at"],
                "last_success_at": successes["started_at"].max() if not successes.empty else None,
                "last_failure_at": failures["started_at"].max() if not failures.empty else None,
                "failure_streak": int(streak),
                "avg_duration_sec": float(group["duration_sec"].dropna().mean()) if group["duration_sec"].notna().any() else None,
                "rows_seen": int(pd.to_numeric(group["rows_seen"], errors="coerce").fillna(0).sum()),
                "rows_inserted": int(pd.to_numeric(group["rows_inserted"], errors="coerce").fillna(0).sum()),
                "rows_updated": int(pd.to_numeric(group["rows_updated"], errors="coerce").fillna(0).sum()),
                "latest_source_ts": group["latest_source_ts"].dropna().max() if group["latest_source_ts"].notna().any() else None,
                "last_error": latest["error_message"],
            })
        return pd.DataFrame(records, columns=columns).sort_values(["failure_streak", "last_status", "source"], ascending=[False, True, True])
    finally:
        if own_conn:
            conn.close()


MACRO_RELEASE_COLUMNS = [
    "id", "calendar_event_id", "release_key", "region", "category", "title",
    "scheduled_date", "scheduled_time_local", "importance", "actual_value",
    "expected_value", "expected_text", "expected_unit", "previous_value",
    "surprise_value", "surprise_text", "unit", "source_table", "source_id", "source_indicator_name",
    "value_date", "status", "metadata_json", "updated_at",
]


def macro_release_actuals(
    conn: sqlite3.Connection | None = None,
    status: str | None = None,
    days_back: int = 14,
    days_forward: int = 45,
    limit: int = 200,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "macro_release_actuals"):
            return _empty(MACRO_RELEASE_COLUMNS)
        clauses = ["scheduled_date BETWEEN date('now', ?) AND date('now', ?)"]
        params: list[Any] = [f"-{int(days_back)} days", f"+{int(days_forward)} days"]
        if status:
            clauses.append("status = ?")
            params.append(status)
        params.append(int(limit))
        return pd.read_sql(
            f"""SELECT id, calendar_event_id, release_key, region, category, title,
                       scheduled_date, scheduled_time_local, importance,
                       actual_value, expected_value, expected_text,
                       expected_unit, previous_value, surprise_value,
                       surprise_text, unit, source_table,
                       source_id, source_indicator_name, value_date, status,
                       metadata_json, updated_at
                FROM macro_release_actuals
                WHERE {' AND '.join(clauses)}
                ORDER BY scheduled_date DESC, importance DESC, region, title
                LIMIT ?""",
            conn,
            params=params,
            parse_dates=["scheduled_date", "value_date", "updated_at"],
        )
    finally:
        if own_conn:
            conn.close()


def latest_macro_releases(
    conn: sqlite3.Connection | None = None,
    hours: int = 72,
    limit: int = 10,
) -> pd.DataFrame:
    days_back = max(1, int((hours + 23) // 24))
    df = macro_release_actuals(
        conn,
        status="released",
        days_back=days_back,
        days_forward=0,
        limit=limit,
    )
    return df.sort_values(["scheduled_date", "importance"], ascending=[False, False]) if not df.empty else df


def next_macro_catalysts(
    conn: sqlite3.Connection | None = None,
    days: int = 45,
    limit: int = 20,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if table_exists(conn, "macro_release_actuals"):
            return pd.read_sql(
                """SELECT id, calendar_event_id, release_key, region, category, title,
                          scheduled_date, scheduled_time_local, importance,
                          expected_value, expected_text, expected_unit,
                          previous_value, status, metadata_json, updated_at
                   FROM macro_release_actuals
                   WHERE scheduled_date BETWEEN date('now') AND date('now', ?)
                     AND status != 'released'
                   ORDER BY scheduled_date ASC, importance DESC, region, title
                   LIMIT ?""",
                conn,
                params=(f"+{int(days)} days", int(limit)),
                parse_dates=["scheduled_date", "updated_at"],
            )
        return calendar_events(conn, days_forward=days).head(limit)
    finally:
        if own_conn:
            conn.close()


MACRO_ACTUAL_COVERAGE_COLUMNS = [
    "region", "category", "total_events", "released_count", "waiting_count",
    "unmatched_pending_count", "unmatched_source_missing_count",
    "unmatched_rule_gap_count", "latest_scheduled_date", "latest_update",
    "status_summary",
]


def macro_actual_match_coverage(
    conn: sqlite3.Connection | None = None,
    days_back: int = 30,
    days_forward: int = 60,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "macro_release_actuals"):
            return _empty(MACRO_ACTUAL_COVERAGE_COLUMNS)
        return pd.read_sql(
            """SELECT region,
                      category,
                      COUNT(*) AS total_events,
                      SUM(CASE WHEN status = 'released' THEN 1 ELSE 0 END) AS released_count,
                      SUM(CASE WHEN status = 'waiting' THEN 1 ELSE 0 END) AS waiting_count,
                      SUM(CASE WHEN status = 'unmatched_pending' THEN 1 ELSE 0 END) AS unmatched_pending_count,
                      SUM(CASE WHEN status = 'unmatched_source_missing' THEN 1 ELSE 0 END) AS unmatched_source_missing_count,
                      SUM(CASE WHEN status = 'unmatched_rule_gap' THEN 1 ELSE 0 END) AS unmatched_rule_gap_count,
                      MAX(scheduled_date) AS latest_scheduled_date,
                      MAX(updated_at) AS latest_update,
                      GROUP_CONCAT(DISTINCT status) AS status_summary
               FROM macro_release_actuals
               WHERE scheduled_date BETWEEN date('now', ?) AND date('now', ?)
               GROUP BY region, category
               ORDER BY latest_scheduled_date DESC, region, category""",
            conn,
            params=(f"-{int(days_back)} days", f"+{int(days_forward)} days"),
            parse_dates=["latest_scheduled_date", "latest_update"],
        )
    finally:
        if own_conn:
            conn.close()


MACRO_EVENT_FLAG_COLUMNS = [
    "event_key", "source_table", "source_id", "release_key", "region",
    "category", "release_family", "title", "scheduled_date",
    "scheduled_time_local", "importance", "status", "expected_value",
    "expected_text", "expected_unit", "actual_value", "previous_value",
    "surprise_value", "surprise_text", "unit", "value_date",
    "days_to_release", "flag_phase", "flag_color", "flag_opacity",
    "marker_size", "tooltip", "current_event_type", "current_event_priority",
]

_RELEASE_FAMILY_LABELS = {
    "cpi": "Inflation",
    "cpif": "Inflation",
    "hicp": "Inflation",
    "ppi": "Producer prices",
    "pce": "PCE inflation",
    "payrolls": "Employment",
    "unemployment": "Employment",
    "jolts": "Employment",
    "gdp": "GDP",
    "pmi": "PMI",
    "retail_sales": "Retail sales",
    "confidence": "Confidence",
    "fomc": "Central bank",
    "ecb": "Central bank",
    "riksbank": "Central bank",
    "boe": "Central bank",
    "boj": "Central bank",
    "eia_oil": "Energy inventories",
}


def _release_family_label(raw: Any) -> str:
    value = str(raw or "").strip().lower().replace("-", "_")
    if not value:
        return "Macro release"
    if ":" in value:
        value = value.split(":", 1)[0]
    value = value.removeprefix("release_").removeprefix("macro_")
    return _RELEASE_FAMILY_LABELS.get(value, value.replace("_", " ").title())


def _blend_hex(start: str, end: str, t: float) -> str:
    t = max(0.0, min(1.0, t))
    s = tuple(int(start[i:i + 2], 16) for i in (1, 3, 5))
    e = tuple(int(end[i:i + 2], 16) for i in (1, 3, 5))
    return "#" + "".join(f"{round(a + (b - a) * t):02x}" for a, b in zip(s, e))


def _macro_flag_style(
    scheduled_date: pd.Timestamp,
    status: str,
    importance: float,
    as_of_date: date,
    days_before: int,
    days_after: int,
) -> dict[str, Any] | None:
    sched = scheduled_date.date()
    days_to = (sched - as_of_date).days
    if days_to > days_before or days_to < -days_after:
        return None
    released = status == "released"
    if released and 0 <= -days_to <= 3:
        phase = "released"
        color = "#00ff9f"
        opacity = 0.94
    elif days_to < 0:
        phase = "fading" if released else "waiting"
        progress = min(max((-days_to - 3) / max(days_after - 3, 1), 0.0), 1.0)
        color = _blend_hex("#00ff9f" if released else "#f4c542", "#8f63ff", progress)
        opacity = max(0.38, 0.76 - progress * 0.34)
    else:
        phase = "upcoming"
        progress = 1.0 - min(days_to / max(days_before, 1), 1.0)
        color = _blend_hex("#f4c542", "#00ff9f", progress)
        opacity = 0.68 + progress * 0.24
    size = 11.0 + min(max(float(importance or 1.0), 1.0), 5.0) * 2.6
    return {
        "days_to_release": days_to,
        "flag_phase": phase,
        "flag_color": color,
        "flag_opacity": opacity,
        "marker_size": size,
    }


def macro_event_flags(
    conn: sqlite3.Connection | None = None,
    *,
    days_before: int = 14,
    days_after: int = 14,
    as_of: str | date | None = None,
) -> pd.DataFrame:
    """Official macro release flags for the map, with lifecycle styling."""
    own_conn = conn is None
    conn = conn or connect_readonly()
    as_of_date = pd.Timestamp(as_of).date() if as_of is not None else date.today()
    start = (as_of_date - timedelta(days=int(days_after))).isoformat()
    end = (as_of_date + timedelta(days=int(days_before))).isoformat()
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    current_events_by_source: dict[str, dict[str, Any]] = {}
    try:
        if table_exists(conn, "current_events"):
            for ev in conn.execute(
                """SELECT event_type, source_table, source_id, priority
                   FROM current_events
                   WHERE event_type IN ('scheduled_catalyst', 'released_actual')
                     AND date(event_time) BETWEEN date(?) AND date(?)
                     AND status = 'active'""",
                (start, end),
            ).fetchall():
                current_events_by_source[f"{ev[1]}:{ev[2]}"] = {
                    "current_event_type": ev[0],
                    "current_event_priority": ev[3],
                }
        if table_exists(conn, "macro_release_actuals"):
            actuals = pd.read_sql(
                """SELECT id, calendar_event_id, release_key, region, category, title,
                          scheduled_date, scheduled_time_local, importance,
                          expected_value, expected_text, expected_unit,
                          actual_value, previous_value, surprise_value,
                          surprise_text, unit, value_date, status
                   FROM macro_release_actuals
                   WHERE scheduled_date BETWEEN date(?) AND date(?)""",
                conn,
                params=(start, end),
                parse_dates=["scheduled_date", "value_date"],
            )
            for row in actuals.to_dict("records"):
                key = (str(row.get("release_key") or row.get("title") or ""), str(row.get("scheduled_date"))[:10])
                if key in seen:
                    continue
                seen.add(key)
                row.update({
                    "event_key": f"macro:{key[0]}:{key[1]}",
                    "source_table": "macro_release_actuals",
                    "source_id": row.get("id"),
                    "release_family": _release_family_label(row.get("release_key") or row.get("category")),
                })
                rows.append(row)
        if table_exists(conn, "calendar_events"):
            calendar = pd.read_sql(
                """SELECT id, COALESCE(event_key, title || ':' || date) AS event_key,
                          release_family, region, category, title, date AS scheduled_date,
                          time_local AS scheduled_time_local, importance,
                          expected AS expected_text, status
                   FROM calendar_events
                   WHERE date BETWEEN date(?) AND date(?)""",
                conn,
                params=(start, end),
                parse_dates=["scheduled_date"],
            )
            for row in calendar.to_dict("records"):
                key = (str(row.get("event_key") or row.get("title") or ""), str(row.get("scheduled_date"))[:10])
                if key in seen:
                    continue
                seen.add(key)
                row.update({
                    "source_table": "calendar_events",
                    "source_id": row.get("id"),
                    "release_key": row.get("event_key"),
                    "release_family": _release_family_label(row.get("release_family") or row.get("category")),
                    "expected_value": None,
                    "expected_unit": None,
                    "actual_value": None,
                    "previous_value": None,
                    "surprise_value": None,
                    "surprise_text": None,
                    "unit": None,
                    "value_date": None,
                    "status": row.get("status") or "scheduled",
                })
                rows.append(row)
        styled_rows: list[dict[str, Any]] = []
        for row in rows:
            scheduled = pd.Timestamp(row.get("scheduled_date"))
            if pd.isna(scheduled):
                continue
            style = _macro_flag_style(
                scheduled,
                str(row.get("status") or "").lower(),
                float(row.get("importance") or 1.0),
                as_of_date,
                int(days_before),
                int(days_after),
            )
            if not style:
                continue
            source_key = f"{row.get('source_table')}:{row.get('source_id')}"
            source_event = current_events_by_source.get(source_key, {})
            title = str(row.get("title") or "Macro release")
            region = str(row.get("region") or "Global")
            status = str(row.get("status") or "scheduled")
            expected = row.get("expected_text") or row.get("expected_value")
            actual = row.get("actual_value")
            surprise = row.get("surprise_text") or row.get("surprise_value")
            tooltip = f"{title}<br>{region} - {row.get('release_family')}<br>{scheduled.date()} {row.get('scheduled_time_local') or ''}<br>Status: {status}"
            if expected not in (None, ""):
                tooltip += f"<br>Expected: {expected}"
            if actual not in (None, ""):
                tooltip += f"<br>Actual: {actual} {row.get('unit') or ''}"
            if surprise not in (None, ""):
                tooltip += f"<br>Surprise: {surprise}"
            styled_rows.append({
                **{col: row.get(col) for col in MACRO_EVENT_FLAG_COLUMNS},
                "scheduled_date": scheduled,
                "tooltip": tooltip,
                **style,
                **source_event,
            })
        if not styled_rows:
            return _empty(MACRO_EVENT_FLAG_COLUMNS)
        df = pd.DataFrame(styled_rows, columns=MACRO_EVENT_FLAG_COLUMNS)
        return df.sort_values(["scheduled_date", "importance"], ascending=[True, False]).reset_index(drop=True)
    finally:
        if own_conn:
            conn.close()


def risk_hotspots(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "risk_hotspots"):
            return _empty(["name", "region", "country", "category", "severity", "lat", "lon", "summary", "source", "updated_at"])
        return pd.read_sql(
            """SELECT name, region, country, category, severity, lat, lon,
                      summary, source, updated_at
               FROM risk_hotspots
               WHERE active = 1
               ORDER BY severity DESC, region, name""",
            conn,
        )
    finally:
        if own_conn:
            conn.close()


def official_macro_indicators(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "indicators"):
            return _empty(["source", "country", "category", "indicator_name", "date", "value", "unit"])
        return pd.read_sql(
            """SELECT
                    CASE
                      WHEN indicator_name LIKE 'SCB %' THEN 'SCB'
                      WHEN indicator_name LIKE 'Riksbank %' THEN 'Riksbank'
                      WHEN indicator_name LIKE 'Eurostat %' THEN 'Eurostat'
                      WHEN indicator_name LIKE 'IMF %' THEN 'IMF'
                      WHEN indicator_name LIKE 'CFTC %' THEN 'CFTC'
                      WHEN indicator_name LIKE 'EIA %' THEN 'EIA'
                      ELSE 'Other'
                    END AS source,
                    country, category, indicator_name, date, value, unit
               FROM indicators
               WHERE (
                    indicator_name LIKE 'SCB %'
                 OR indicator_name LIKE 'Riksbank %'
                 OR indicator_name LIKE 'Eurostat %'
                 OR indicator_name LIKE 'IMF %'
                 OR indicator_name LIKE 'CFTC %'
                 OR indicator_name LIKE 'EIA %'
               )
                 AND date = (
                   SELECT MAX(date) FROM indicators i2
                   WHERE i2.country = indicators.country
                     AND i2.indicator_name = indicators.indicator_name
                     AND i2.date <= date('now')
                 )
               ORDER BY source, country, category, indicator_name""",
            conn, parse_dates=["date"],
        )
    finally:
        if own_conn:
            conn.close()


def news_items(conn: sqlite3.Connection | None = None, days: int = 14, limit: int = 200) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "news_items"):
            return _empty(["published_at", "source", "region", "category", "title", "summary", "url", "used_for_predictions"])
        return pd.read_sql(
            """SELECT published_at, source, region, category, title, summary,
                      url, used_for_predictions
               FROM news_items
               WHERE substr(COALESCE(published_at, fetched_at), 1, 10) >= date('now', ?)
               ORDER BY COALESCE(published_at, fetched_at) DESC
               LIMIT ?""",
            conn, params=(f"-{days} days", limit), parse_dates=["published_at"],
        )
    finally:
        if own_conn:
            conn.close()


def trade_indicators(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "indicators"):
            return _empty(["country", "indicator_name", "date", "value", "unit", "impact"])
        return pd.read_sql(
            """SELECT country, indicator_name, date, value, unit, impact
               FROM indicators
               WHERE category = 'trade'
                 AND date = (
                   SELECT MAX(date) FROM indicators i2
                   WHERE i2.country = indicators.country
                     AND i2.indicator_name = indicators.indicator_name
                 )
               ORDER BY country, indicator_name""",
            conn, parse_dates=["date"],
        )
    finally:
        if own_conn:
            conn.close()


def sanctions(conn: sqlite3.Connection | None = None, limit: int = 200) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "sanctions"):
            return _empty(["source", "list_name", "program", "entity_name", "entity_type", "country", "target_type", "product", "measure", "fetched_at"])
        return pd.read_sql(
            """SELECT source, list_name, program, entity_name, entity_type,
                      country, target_type, product, measure, fetched_at
               FROM sanctions
               ORDER BY fetched_at DESC, country, program, entity_name
               LIMIT ?""",
            conn, params=(limit,), parse_dates=["fetched_at"],
        )
    finally:
        if own_conn:
            conn.close()


def sanction_clusters(conn: sqlite3.Connection | None = None, limit: int = 60) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "sanctions"):
            return _empty(["program", "country", "product", "rows", "entities", "lists", "latest"])
        return pd.read_sql(
            """SELECT COALESCE(program, 'program n/a') AS program,
                      COALESCE(country, 'country n/a') AS country,
                      COALESCE(product, '') AS product,
                      COUNT(*) AS rows,
                      COUNT(DISTINCT entity_name) AS entities,
                      GROUP_CONCAT(DISTINCT list_name) AS lists,
                      MAX(fetched_at) AS latest
               FROM sanctions
               GROUP BY program, country, product
               ORDER BY rows DESC, entities DESC
               LIMIT ?""",
            conn, params=(limit,),
        )
    finally:
        if own_conn:
            conn.close()


def sanction_program_options(conn: sqlite3.Connection | None = None) -> list[str]:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "sanctions"):
            return []
        rows = conn.execute(
            """SELECT DISTINCT COALESCE(program, 'program n/a') AS program
               FROM sanctions
               ORDER BY program"""
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        if own_conn:
            conn.close()


def sanction_program_stats(conn: sqlite3.Connection | None = None, program: str = "program n/a") -> dict[str, Any]:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "sanctions"):
            return {"rows": 0, "entities": 0, "countries": "", "products": ""}
        row = conn.execute(
            """SELECT COUNT(*) AS rows, COUNT(DISTINCT entity_name) AS entities,
                      GROUP_CONCAT(DISTINCT country) AS countries,
                      GROUP_CONCAT(DISTINCT product) AS products
               FROM sanctions
               WHERE COALESCE(program, 'program n/a') = ?""",
            (program,),
        ).fetchone()
        if not row:
            return {"rows": 0, "entities": 0, "countries": "", "products": ""}
        return {
            "rows": int(row[0] or 0),
            "entities": int(row[1] or 0),
            "countries": row[2] or "",
            "products": row[3] or "",
        }
    finally:
        if own_conn:
            conn.close()


def sanction_program_examples(conn: sqlite3.Connection | None = None,
                              program: str = "program n/a",
                              limit: int = 12) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "sanctions"):
            return _empty(["entity_name", "entity_type", "country", "product", "list_name", "fetched_at"])
        return pd.read_sql(
            """SELECT entity_name, entity_type, country, product,
                      list_name, fetched_at
               FROM sanctions
               WHERE COALESCE(program, 'program n/a') = ?
               ORDER BY fetched_at DESC, country, entity_name
               LIMIT ?""",
            conn, params=(program, limit), parse_dates=["fetched_at"],
        )
    finally:
        if own_conn:
            conn.close()


def reddit_context(conn: sqlite3.Connection | None = None, days: int = 7, limit: int = 80) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "social_mentions"):
            return _empty(["date", "source", "ticker", "mention_count", "sentiment_score", "top_post_title", "top_post_score"])
        return pd.read_sql(
            """SELECT date, source, ticker, mention_count, sentiment_score,
                      top_post_title, top_post_score
               FROM social_mentions
               WHERE date >= date('now', ?)
               ORDER BY mention_count DESC, top_post_score DESC
               LIMIT ?""",
            conn, params=(f"-{days} days", limit), parse_dates=["date"],
        )
    finally:
        if own_conn:
            conn.close()


def rumour_spikes(conn: sqlite3.Connection | None = None, limit: int = 30) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "rumour_signals"):
            return _empty(["date", "ticker", "mentions_today", "baseline_mean", "z_score", "sentiment_today", "realized_return_5d", "realized_return_21d"])
        return pd.read_sql(
            """SELECT date, ticker, mentions_today, baseline_mean, z_score,
                      sentiment_today, realized_return_5d, realized_return_21d
               FROM rumour_signals
               WHERE z_score IS NOT NULL
               ORDER BY date DESC, z_score DESC LIMIT ?""",
            conn, params=(limit,), parse_dates=["date"],
        )
    finally:
        if own_conn:
            conn.close()


def twitter_topic_heat(conn: sqlite3.Connection | None = None, days: int = 7, limit: int = 20) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "twitter_macro_posts"):
            return _empty(["topic", "posts", "likes", "reposts", "replies", "quotes", "heat", "latest"])
        return pd.read_sql(
            """SELECT COALESCE(topic, 'topic n/a') AS topic,
                      COUNT(*) AS posts,
                      SUM(COALESCE(like_count, 0)) AS likes,
                      SUM(COALESCE(repost_count, 0)) AS reposts,
                      SUM(COALESCE(reply_count, 0)) AS replies,
                      SUM(COALESCE(quote_count, 0)) AS quotes,
                      SUM(COALESCE(like_count, 0)
                          + COALESCE(repost_count, 0) * 2
                          + COALESCE(reply_count, 0)
                          + COALESCE(quote_count, 0) * 2) AS heat,
                      MAX(COALESCE(posted_at, fetched_at)) AS latest
               FROM twitter_macro_posts
               WHERE substr(COALESCE(posted_at, fetched_at), 1, 10) >= date('now', ?)
               GROUP BY topic
               ORDER BY heat DESC, posts DESC
               LIMIT ?""",
            conn, params=(f"-{days} days", limit),
        )
    finally:
        if own_conn:
            conn.close()


def twitter_macro_posts(conn: sqlite3.Connection | None = None, days: int = 7, limit: int = 100) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "twitter_macro_posts"):
            return _empty(["posted_at", "topic", "author_username", "text", "like_count", "repost_count", "reply_count", "quote_count", "url"])
        return pd.read_sql(
            """SELECT posted_at, topic, author_username, text,
                      like_count, repost_count, reply_count, quote_count, url
               FROM twitter_macro_posts
               WHERE substr(COALESCE(posted_at, fetched_at), 1, 10) >= date('now', ?)
               ORDER BY
                 COALESCE(like_count, 0) + COALESCE(repost_count, 0) * 2
                 + COALESCE(reply_count, 0) + COALESCE(quote_count, 0) * 2 DESC,
                 posted_at DESC
               LIMIT ?""",
            conn, params=(f"-{days} days", limit), parse_dates=["posted_at"],
        )
    finally:
        if own_conn:
            conn.close()


def insider_activity(conn: sqlite3.Connection | None = None, days: int = 30, limit: int = 50) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "insider_trades"):
            return _empty(["filing_date", "transaction_date", "ticker", "insider_name", "insider_role", "transaction_type", "shares", "price", "value_usd"])
        return pd.read_sql(
            """SELECT filing_date, transaction_date, ticker, insider_name,
                      insider_role, transaction_type, shares, price, value_usd
               FROM insider_trades
               WHERE filing_date >= date('now', ?) AND value_usd IS NOT NULL
               ORDER BY ABS(value_usd) DESC LIMIT ?""",
            conn, params=(f"-{days} days", limit), parse_dates=["filing_date", "transaction_date"],
        )
    finally:
        if own_conn:
            conn.close()


def weather_correlations(conn: sqlite3.Connection | None = None,
                         min_observations: int = 0,
                         limit: int = 30) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "weather_correlations"):
            return _empty(["asset", "location", "weather_var", "window_days", "correlation", "n_observations", "computed_at"])
        return pd.read_sql(
            """SELECT asset, location, weather_var, window_days,
                      correlation, n_observations, computed_at
               FROM weather_correlations
               WHERE n_observations >= ?
               ORDER BY ABS(correlation) DESC LIMIT ?""",
            conn, params=(min_observations, limit), parse_dates=["computed_at"],
        )
    finally:
        if own_conn:
            conn.close()


def company_fundamentals_summary(conn: sqlite3.Connection | None = None, limit: int = 50) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "company_fundamentals"):
            return _empty(["ticker", "rows", "latest_period", "latest_filed"])
        return pd.read_sql(
            """SELECT ticker, COUNT(*) AS rows, MAX(period_end) AS latest_period,
                      MAX(filed) AS latest_filed
               FROM company_fundamentals
               GROUP BY ticker
               ORDER BY latest_filed DESC, ticker
               LIMIT ?""",
            conn, params=(limit,),
        )
    finally:
        if own_conn:
            conn.close()


def data_source_summaries(conn: sqlite3.Connection | None = None) -> dict[str, pd.DataFrame]:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        return {
            "sanctions": sanction_clusters(conn, limit=12),
            "social": rumour_spikes(conn, limit=8),
            "weather": weather_correlations(conn, min_observations=30, limit=8),
            "fundamentals": company_fundamentals_summary(conn, limit=12),
            "insider": pd.read_sql(
                """SELECT ticker, COUNT(*) AS trades,
                          SUM(CASE WHEN value_usd > 0 THEN value_usd ELSE 0 END) AS buys,
                          SUM(CASE WHEN value_usd < 0 THEN value_usd ELSE 0 END) AS sells,
                          MAX(filing_date) AS latest
                   FROM insider_trades
                   WHERE filing_date >= date('now', '-30 days')
                   GROUP BY ticker
                   ORDER BY ABS(COALESCE(buys, 0)) + ABS(COALESCE(sells, 0)) DESC
                   LIMIT 8""",
                conn,
            ) if table_exists(conn, "insider_trades") else _empty(["ticker", "trades", "buys", "sells", "latest"]),
            "twitter": twitter_topic_heat(conn, days=7, limit=8),
            "gdelt_streams": gdelt_streams(conn, days=7, limit=12),
        }
    finally:
        if own_conn:
            conn.close()


def macro_regime(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "signals"):
            return _empty(["date", "signal_name", "value"])
        return pd.read_sql(
            """SELECT date, signal_name, value
               FROM signals
               WHERE symbol = '_macro'
                 AND date = (SELECT MAX(date) FROM signals WHERE symbol = '_macro')
               ORDER BY signal_name""",
            conn, parse_dates=["date"],
        )
    finally:
        if own_conn:
            conn.close()


def gdelt_pulse(conn: sqlite3.Connection | None = None, days: int = 30) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "signals"):
            return _empty(["date", "theme", "today_pct", "avg_pct", "ratio", "count"])
        latest = conn.execute(
            "SELECT MAX(date) FROM signals WHERE symbol='_news' AND signal_name LIKE 'news_rate_%'"
        ).fetchone()[0]
        if not latest:
            return pd.DataFrame()
        rates = pd.read_sql(
            """SELECT signal_name, value
               FROM signals
               WHERE symbol='_news' AND date=? AND signal_name LIKE 'news_rate_%'""",
            conn, params=(latest,),
        )
        counts = pd.read_sql(
            """SELECT signal_name, value
               FROM signals
               WHERE symbol='_news' AND date=? AND signal_name LIKE 'news_count_%'""",
            conn, params=(latest,),
        )
        rows = []
        for r in rates.itertuples():
            theme = r.signal_name.replace("news_rate_", "")
            avg = conn.execute(
                """SELECT AVG(value) FROM signals
                   WHERE symbol='_news' AND signal_name=?
                     AND date >= date(?, ?) AND date < ?""",
                (r.signal_name, latest, f"-{days} days", latest),
            ).fetchone()[0]
            count_name = f"news_count_{theme}"
            count_match = counts[counts["signal_name"] == count_name]
            count = float(count_match["value"].iloc[0]) if not count_match.empty else None
            today_pct = float(r.value) * 100
            avg_pct = float(avg) * 100 if avg is not None else None
            ratio = today_pct / avg_pct if avg_pct and avg_pct > 0 else None
            rows.append({
                "date": latest,
                "theme": theme.replace("_", " "),
                "today_pct": today_pct,
                "avg_pct": avg_pct,
                "ratio": ratio,
                "count": count,
            })
        df = pd.DataFrame(rows)
        return df.sort_values(["ratio", "today_pct"], ascending=False) if not df.empty else df
    finally:
        if own_conn:
            conn.close()


GDELT_STREAM_COLUMNS = [
    "id", "date", "stream", "region", "country", "article_count",
    "total_articles", "article_share", "baseline_30d", "z_score",
    "severity", "societal_impact_score", "labels_json",
    "top_theme_codes_json", "source", "fetched_at",
]

GDELT_STREAM_EXAMPLE_COLUMNS = [
    "id", "date", "stream", "region", "country", "example_rank", "title",
    "url", "source_domain", "location_name", "theme_codes_json",
    "labels_json", "tone", "source", "fetched_at",
]


def gdelt_streams(
    conn: sqlite3.Connection | None = None,
    days: int = 30,
    stream: str | None = None,
    region: str | None = None,
    limit: int = 200,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "gdelt_streams"):
            return _empty(GDELT_STREAM_COLUMNS)
        clauses = ["date >= date('now', ?)"]
        params: list[Any] = [f"-{int(days)} days"]
        if stream:
            clauses.append("stream = ?")
            params.append(stream)
        if region:
            clauses.append("region = ?")
            params.append(region)
        params.append(limit)
        return pd.read_sql(
            f"""SELECT id, date, stream, region, NULLIF(country, '') AS country,
                       article_count, total_articles, article_share,
                       baseline_30d, z_score, severity, societal_impact_score,
                       labels_json, top_theme_codes_json, source, fetched_at
                FROM gdelt_streams
                WHERE {' AND '.join(clauses)}
                ORDER BY date DESC, societal_impact_score DESC, severity DESC,
                         article_count DESC, stream, region, country
                LIMIT ?""",
            conn, params=params, parse_dates=["date", "fetched_at"],
        )
    finally:
        if own_conn:
            conn.close()


def gdelt_stream_examples(
    conn: sqlite3.Connection | None = None,
    stream: str | None = None,
    date: str | None = None,
    region: str | None = None,
    limit: int = 100,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "gdelt_stream_examples"):
            return _empty(GDELT_STREAM_EXAMPLE_COLUMNS)
        clauses: list[str] = []
        params: list[Any] = []
        if stream:
            clauses.append("stream = ?")
            params.append(stream)
        if date:
            clauses.append("date = ?")
            params.append(str(date)[:10])
        if region:
            clauses.append("region = ?")
            params.append(region)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(gdelt_stream_examples)")}
        labels_expr = "labels_json" if "labels_json" in existing_cols else "NULL AS labels_json"
        return pd.read_sql(
            f"""SELECT id, date, stream, region, NULLIF(country, '') AS country,
                       example_rank, title, url, source_domain, location_name,
                       theme_codes_json, {labels_expr}, tone, source, fetched_at
                FROM gdelt_stream_examples
                {where}
                ORDER BY date DESC, stream, region, country, example_rank
                LIMIT ?""",
            conn, params=params, parse_dates=["date", "fetched_at"],
        )
    finally:
        if own_conn:
            conn.close()


HISTORICAL_STATE_COLUMNS = [
    "as_of", "value_key", "source_table", "source_id", "source_symbol",
    "source_name", "region", "country", "category", "label_ids_json",
    "value", "value_text", "unit", "value_date", "freshness_days",
    "confidence", "freshness_class", "training_eligible",
    "evaluation_eligible", "coverage_score",
]

HISTORICAL_COMPARISON_COLUMNS = HISTORICAL_STATE_COLUMNS + [
    "event_tags_json", "notable_driver_comment", "horizon_days",
    "forward_return", "end_date",
]


def historical_state(
    conn: sqlite3.Connection | None = None,
    as_of: str | date | None = None,
    keys: list[str] | tuple[str, ...] | None = None,
    include_sparse: bool = True,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "historical_state_values"):
            return _empty(HISTORICAL_STATE_COLUMNS)
        as_of_str = str(as_of or conn.execute("SELECT MAX(as_of) FROM historical_state_values").fetchone()[0])[:10]
        if not as_of_str:
            return _empty(HISTORICAL_STATE_COLUMNS)
        clauses = ["v.as_of = ?", "v.active = 1"]
        params: list[Any] = [as_of_str]
        if keys:
            placeholders = ",".join("?" for _ in keys)
            clauses.append(f"v.value_key IN ({placeholders})")
            params.extend(keys)
        if not include_sparse and table_exists(conn, "historical_state_daily"):
            clauses.append("COALESCE(d.training_eligible, 0) = 1")
        daily_join = (
            "LEFT JOIN historical_state_daily d ON d.as_of = v.as_of"
            if table_exists(conn, "historical_state_daily")
            else "LEFT JOIN (SELECT NULL AS as_of, NULL AS training_eligible, NULL AS evaluation_eligible, NULL AS coverage_score) d ON 0"
        )
        has_freshness_class = "freshness_class" in {r[1] for r in conn.execute("PRAGMA table_info(historical_state_values)")}
        freshness_expr = "v.freshness_class" if has_freshness_class else "'archive'"
        return pd.read_sql(
            f"""SELECT v.as_of, v.value_key, v.source_table, v.source_id,
                       v.source_symbol, v.source_name, v.region, v.country,
                       v.category, v.label_ids_json, v.value, v.value_text,
                       v.unit, v.value_date, v.freshness_days, v.confidence,
                       {freshness_expr} AS freshness_class,
                       COALESCE(d.training_eligible, 0) AS training_eligible,
                       COALESCE(d.evaluation_eligible, 0) AS evaluation_eligible,
                       d.coverage_score
                FROM historical_state_values v
                {daily_join}
                WHERE {' AND '.join(clauses)}
                ORDER BY v.source_table, v.source_symbol, v.category, v.value_key""",
            conn, params=params, parse_dates=["as_of", "value_date"],
        )
    finally:
        if own_conn:
            conn.close()


def historical_comparison(
    conn: sqlite3.Connection | None = None,
    start: str | date | None = None,
    end: str | date | None = None,
    symbols: list[str] | tuple[str, ...] | None = None,
    labels: list[str] | tuple[str, ...] | None = None,
    streams: list[str] | tuple[str, ...] | None = None,
    limit: int = 2000,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "historical_state_values"):
            return _empty(HISTORICAL_COMPARISON_COLUMNS)
        end_str = str(end or conn.execute("SELECT MAX(as_of) FROM historical_state_values").fetchone()[0])[:10]
        start_str = str(start or end_str)[:10]
        clauses = ["v.as_of BETWEEN ? AND ?", "v.active = 1"]
        params: list[Any] = [start_str, end_str]
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            clauses.append(f"v.source_symbol IN ({placeholders})")
            params.extend(symbols)
        if labels:
            label_clauses = []
            for label in labels:
                label_clauses.append("v.label_ids_json LIKE ?")
                params.append(f'%"{label}"%')
            clauses.append(f"({' OR '.join(label_clauses)})")
        if streams:
            placeholders = ",".join("?" for _ in streams)
            clauses.append(f"(v.source_table = 'gdelt_streams' AND v.category IN ({placeholders}))")
            params.extend(streams)
        daily_join = (
            "LEFT JOIN historical_state_daily d ON d.as_of = v.as_of"
            if table_exists(conn, "historical_state_daily")
            else "LEFT JOIN (SELECT NULL AS as_of, NULL AS training_eligible, NULL AS evaluation_eligible, NULL AS coverage_score, NULL AS event_tags_json, NULL AS notable_driver_comment) d ON 0"
        )
        fwd_join = (
            """LEFT JOIN historical_forward_returns f
                      ON f.as_of = v.as_of AND f.symbol = v.source_symbol"""
            if table_exists(conn, "historical_forward_returns")
            else "LEFT JOIN (SELECT NULL AS as_of, NULL AS symbol, NULL AS horizon_days, NULL AS forward_return, NULL AS end_date) f ON 0"
        )
        has_freshness_class = "freshness_class" in {r[1] for r in conn.execute("PRAGMA table_info(historical_state_values)")}
        freshness_expr = "v.freshness_class" if has_freshness_class else "'archive'"
        params.append(limit)
        return pd.read_sql(
            f"""SELECT v.as_of, v.value_key, v.source_table, v.source_id,
                       v.source_symbol, v.source_name, v.region, v.country,
                       v.category, v.label_ids_json, v.value, v.value_text,
                       v.unit, v.value_date, v.freshness_days, v.confidence,
                       {freshness_expr} AS freshness_class,
                       COALESCE(d.training_eligible, 0) AS training_eligible,
                       COALESCE(d.evaluation_eligible, 0) AS evaluation_eligible,
                       d.coverage_score, d.event_tags_json,
                       d.notable_driver_comment, f.horizon_days,
                       f.forward_return, f.end_date
                FROM historical_state_values v
                {daily_join}
                {fwd_join}
                WHERE {' AND '.join(clauses)}
                ORDER BY v.as_of DESC, v.source_table, v.source_symbol,
                         v.category, v.value_key, f.horizon_days
                LIMIT ?""",
            conn, params=params, parse_dates=["as_of", "value_date", "end_date"],
        )
    finally:
        if own_conn:
            conn.close()


HISTORICAL_STATE_COVERAGE_COLUMNS = [
    "as_of", "source_table", "rows", "labeled_rows", "unlabeled_rows",
    "label_coverage_pct", "fresh", "aging", "stale", "archive",
    "training_eligible", "evaluation_eligible", "coverage_score",
]

GDELT_STREAM_COVERAGE_COLUMNS = [
    "stream", "rows", "days", "article_count", "examples", "reviews",
    "high_impact_rows", "latest", "avg_severity", "avg_societal_impact",
]


def historical_state_coverage(
    conn: sqlite3.Connection | None = None,
    as_of: str | date | None = None,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "historical_state_values"):
            return _empty(HISTORICAL_STATE_COVERAGE_COLUMNS)
        as_of_str = str(as_of or conn.execute("SELECT MAX(as_of) FROM historical_state_values").fetchone()[0])[:10]
        if not as_of_str:
            return _empty(HISTORICAL_STATE_COVERAGE_COLUMNS)
        has_freshness_class = "freshness_class" in {r[1] for r in conn.execute("PRAGMA table_info(historical_state_values)")}
        freshness_expr = "v.freshness_class" if has_freshness_class else "'archive'"
        daily_join = (
            "LEFT JOIN historical_state_daily d ON d.as_of = v.as_of"
            if table_exists(conn, "historical_state_daily")
            else "LEFT JOIN (SELECT NULL AS as_of, NULL AS training_eligible, NULL AS evaluation_eligible, NULL AS coverage_score) d ON 0"
        )
        return pd.read_sql(
            f"""WITH base AS (
                  SELECT v.as_of, v.source_table, v.label_ids_json,
                         {freshness_expr} AS freshness_class,
                         COALESCE(d.training_eligible, 0) AS training_eligible,
                         COALESCE(d.evaluation_eligible, 0) AS evaluation_eligible,
                         d.coverage_score
                  FROM historical_state_values v
                  {daily_join}
                  WHERE v.as_of = ? AND v.active = 1
                )
                SELECT as_of, source_table,
                       COUNT(*) AS rows,
                       SUM(CASE WHEN COALESCE(label_ids_json, '[]') NOT IN ('[]', '') THEN 1 ELSE 0 END) AS labeled_rows,
                       SUM(CASE WHEN COALESCE(label_ids_json, '[]') IN ('[]', '') THEN 1 ELSE 0 END) AS unlabeled_rows,
                       ROUND(100.0 * SUM(CASE WHEN COALESCE(label_ids_json, '[]') NOT IN ('[]', '') THEN 1 ELSE 0 END) / COUNT(*), 2) AS label_coverage_pct,
                       SUM(CASE WHEN freshness_class = 'fresh' THEN 1 ELSE 0 END) AS fresh,
                       SUM(CASE WHEN freshness_class = 'aging' THEN 1 ELSE 0 END) AS aging,
                       SUM(CASE WHEN freshness_class = 'stale' THEN 1 ELSE 0 END) AS stale,
                       SUM(CASE WHEN freshness_class = 'archive' THEN 1 ELSE 0 END) AS archive,
                       MAX(training_eligible) AS training_eligible,
                       MAX(evaluation_eligible) AS evaluation_eligible,
                       MAX(coverage_score) AS coverage_score
                FROM base
                GROUP BY as_of, source_table
                ORDER BY rows DESC, source_table""",
            conn, params=(as_of_str,), parse_dates=["as_of"],
        )
    finally:
        if own_conn:
            conn.close()


def gdelt_stream_coverage(
    conn: sqlite3.Connection | None = None,
    days: int = 35,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "gdelt_streams"):
            return _empty(GDELT_STREAM_COVERAGE_COLUMNS)
        examples_join = (
            """LEFT JOIN (
                   SELECT stream, COUNT(*) AS examples
                   FROM gdelt_stream_examples
                   WHERE date >= date('now', ?)
                   GROUP BY stream
                 ) e ON e.stream = s.stream"""
            if table_exists(conn, "gdelt_stream_examples")
            else "LEFT JOIN (SELECT NULL AS stream, 0 AS examples) e ON 0"
        )
        reviews_join = (
            """LEFT JOIN (
                   SELECT gs.stream, COUNT(*) AS reviews
                   FROM oracle_review_annotations r
                   JOIN gdelt_streams gs ON gs.id = r.source_id
                   WHERE r.source_table = 'gdelt_streams'
                     AND r.as_of >= date('now', ?)
                   GROUP BY gs.stream
                 ) rv ON rv.stream = s.stream"""
            if table_exists(conn, "oracle_review_annotations")
            else "LEFT JOIN (SELECT NULL AS stream, 0 AS reviews) rv ON 0"
        )
        params: list[Any] = []
        if table_exists(conn, "gdelt_stream_examples"):
            params.append(f"-{int(days)} days")
        if table_exists(conn, "oracle_review_annotations"):
            params.append(f"-{int(days)} days")
        params.append(f"-{int(days)} days")
        return pd.read_sql(
            f"""SELECT s.stream,
                       COUNT(*) AS rows,
                       COUNT(DISTINCT s.date) AS days,
                       SUM(s.article_count) AS article_count,
                       COALESCE(MAX(e.examples), 0) AS examples,
                       COALESCE(MAX(rv.reviews), 0) AS reviews,
                       SUM(CASE WHEN s.severity >= 4.0 OR s.societal_impact_score >= 4.0 THEN 1 ELSE 0 END) AS high_impact_rows,
                       MAX(s.date) AS latest,
                       AVG(s.severity) AS avg_severity,
                       AVG(s.societal_impact_score) AS avg_societal_impact
                FROM gdelt_streams s
                {examples_join}
                {reviews_join}
                WHERE s.date >= date('now', ?)
                GROUP BY s.stream
                ORDER BY high_impact_rows DESC, article_count DESC, s.stream""",
            conn, params=params, parse_dates=["latest"],
        )
    finally:
        if own_conn:
            conn.close()


def intelligence_packages(conn: sqlite3.Connection | None = None, as_of: str | None = None,
                          limit: int = 80) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "intelligence_packages"):
            return pd.DataFrame()
        date_filter = as_of or conn.execute("SELECT MAX(as_of) FROM intelligence_packages").fetchone()[0]
        if not date_filter:
            return pd.DataFrame()
        return pd.read_sql(
            """SELECT id, as_of, generated_at, scope_type, scope, parent_scope,
                      theme, direction, severity, confidence, freshness, horizon,
                      evidence_json, conclusion, affected_assets_json,
                      prediction_impact_json, next_watch, source_refs_json,
                      model, input_hash
               FROM intelligence_packages
               WHERE as_of = ?
               ORDER BY
                 CASE scope_type WHEN 'global' THEN 0 WHEN 'region' THEN 1 ELSE 2 END,
                 severity DESC, confidence DESC, scope, theme
               LIMIT ?""",
            conn, params=(date_filter, limit), parse_dates=["as_of", "generated_at"],
        )
    finally:
        if own_conn:
            conn.close()


def macro_event_predictions(conn: sqlite3.Connection | None = None,
                            days_forward: int = 45,
                            days_back: int = 7,
                            latest_only: bool = True) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "macro_event_predictions"):
            return pd.DataFrame()
        existing = {row[1] for row in conn.execute("PRAGMA table_info(macro_event_predictions)")}

        def col(name: str) -> str:
            return name if name in existing else f"NULL AS {name}"

        as_of_filter = ""
        params: list[Any] = [
            (date.today() - timedelta(days=days_back)).isoformat(),
            (date.today() + timedelta(days=days_forward)).isoformat(),
        ]
        if latest_only:
            latest = conn.execute("SELECT MAX(as_of) FROM macro_event_predictions").fetchone()[0]
            if not latest:
                return pd.DataFrame()
            as_of_filter = "AND as_of = ?"
            params.append(latest)
        return pd.read_sql(
            f"""SELECT id, calendar_event_id, event_key, as_of, generated_at,
                       release_date, release_time_local, region, country, category,
                       title, importance, expected, previous_value,
                       predicted_surprise_bucket, confidence, scenario_json,
                       affected_assets_json, rationale_md, key_risks_json,
                       source, url, model, input_hash, actual_value,
                       actual_surprise, actual_detail_json, actual_summary,
                       result_status, {col('forecast_direction')},
                       {col('forecast_confidence')}, {col('forecast_summary')},
                       {col('forecast_rationale_md')}, {col('historical_pattern_json')},
                       {col('claude_forecast_json')}, {col('claude_model')},
                       {col('claude_at')}, local_model_summary, local_model_model,
                       local_model_at, scored_at
                FROM macro_event_predictions
                WHERE release_date BETWEEN ? AND ?
                  {as_of_filter}
                ORDER BY release_date ASC, importance DESC, region""",
            conn,
            params=params,
            parse_dates=[
                "as_of", "generated_at", "release_date", "claude_at",
                "local_model_at", "scored_at",
            ],
        )
    finally:
        if own_conn:
            conn.close()

def jsonable_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame to records suitable for API responses."""
    if df.empty:
        return []
    clean = df.copy()
    for col in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[col]):
            clean[col] = clean[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
    clean = clean.astype(object).where(pd.notna(clean), None)
    return clean.to_dict(orient="records")


def oracle_entities(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "oracle_entities"):
            return pd.DataFrame()
        return pd.read_sql(
            """SELECT entity_id, label, entity_type, parent_id, region, nation,
                      sector, symbol, description, active
               FROM oracle_entities
               WHERE active = 1
               ORDER BY
                 CASE entity_type
                   WHEN 'global' THEN 0 WHEN 'region' THEN 1 WHEN 'nation' THEN 2
                   WHEN 'macro_indicator' THEN 3 WHEN 'commodity' THEN 4 WHEN 'sector' THEN 5
                   WHEN 'market' THEN 6 ELSE 7 END,
                 label""",
            conn,
        )
    finally:
        if own_conn:
            conn.close()


def oracle_index_snapshots(conn: sqlite3.Connection | None = None, as_of: str | None = None,
                           limit: int = 120) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "oracle_index_snapshots"):
            return pd.DataFrame()
        date_filter = as_of or conn.execute("SELECT MAX(as_of) FROM oracle_index_snapshots").fetchone()[0]
        if not date_filter:
            return pd.DataFrame()
        return pd.read_sql(
            """SELECT s.id, s.as_of, s.generated_at, s.entity_id, s.entity_label, s.entity_type,
                      s.parent_id, s.theme, s.direction, s.horizon, s.score, s.magnitude,
                      s.confidence, s.evidence_count, s.market_bias, s.plain_read,
                      s.top_evidence_json, s.affected_assets_json, s.model, s.input_hash
               FROM oracle_index_snapshots s
               LEFT JOIN oracle_entities e ON e.entity_id = s.entity_id
               WHERE s.as_of = ? AND COALESCE(e.active, 1) = 1
               ORDER BY
                 CASE s.entity_type
                   WHEN 'global' THEN 0 WHEN 'region' THEN 1 WHEN 'nation' THEN 2
                   WHEN 'macro_indicator' THEN 3 WHEN 'commodity' THEN 4
                   WHEN 'sector' THEN 5 WHEN 'market' THEN 6 ELSE 7 END,
                 s.score DESC, s.entity_label, s.theme
               LIMIT ?""",
            conn, params=(date_filter, limit), parse_dates=["as_of", "generated_at"],
        )
    finally:
        if own_conn:
            conn.close()


ORACLE_LAYER_COLUMNS = [
    "id", "as_of", "generated_at", "entity_id", "entity_label", "entity_type",
    "parent_id", "region", "nation", "sector", "symbol", "theme", "direction",
    "horizon", "score", "magnitude", "confidence", "evidence_count",
    "market_bias", "plain_read", "top_evidence_json", "affected_assets_json",
    "display_label", "label_weight", "layer", "map_zone",
]


def oracle_layer_map(conn: sqlite3.Connection | None = None,
                     as_of: str | None = None,
                     layer: str | None = None,
                     entity_type: str | None = None,
                     limit: int = 240) -> pd.DataFrame:
    """Latest atlas index rows enriched for map/layer UI grouping."""
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "oracle_index_snapshots"):
            return _empty(ORACLE_LAYER_COLUMNS)
        date_filter = as_of or conn.execute("SELECT MAX(as_of) FROM oracle_index_snapshots").fetchone()[0]
        if not date_filter:
            return _empty(ORACLE_LAYER_COLUMNS)

        has_labels = table_exists(conn, "data_labels") and table_exists(conn, "data_label_assignments")
        if has_labels:
            label_ctes = """
                display_direction AS (
                    SELECT a.target_value AS direction,
                           MAX(l.label) AS display_label,
                           MAX(COALESCE(a.weight_override, l.default_weight, 1.0)) AS display_weight
                    FROM data_label_assignments a
                    JOIN data_labels l ON l.label_id = a.label_id
                    WHERE a.active = 1 AND l.active = 1
                      AND l.label_type = 'display'
                      AND a.target_type = 'direction'
                      AND a.target_table = 'oracle_index_snapshots'
                      AND a.target_column = 'direction'
                    GROUP BY a.target_value
                ),
                display_bias AS (
                    SELECT a.target_value AS market_bias,
                           MAX(l.label) AS display_label,
                           MAX(COALESCE(a.weight_override, l.default_weight, 1.0)) AS display_weight
                    FROM data_label_assignments a
                    JOIN data_labels l ON l.label_id = a.label_id
                    WHERE a.active = 1 AND l.active = 1
                      AND l.label_type = 'display'
                      AND a.target_type = 'market_bias'
                      AND a.target_table = 'oracle_index_snapshots'
                      AND a.target_column = 'market_bias'
                    GROUP BY a.target_value
                ),
                theme_weight AS (
                    SELECT a.target_value AS theme,
                           MAX(COALESCE(a.weight_override, l.default_weight, 1.0)) AS theme_weight
                    FROM data_label_assignments a
                    JOIN data_labels l ON l.label_id = a.label_id
                    WHERE a.active = 1 AND l.active = 1
                      AND l.label_type = 'theme'
                      AND a.target_type = 'theme'
                      AND a.target_table = 'oracle_index_snapshots'
                      AND a.target_column = 'theme'
                    GROUP BY a.target_value
                ),
            """
        else:
            label_ctes = """
                display_direction(direction, display_label, display_weight) AS (
                    SELECT NULL, NULL, NULL WHERE 0
                ),
                display_bias(market_bias, display_label, display_weight) AS (
                    SELECT NULL, NULL, NULL WHERE 0
                ),
                theme_weight(theme, theme_weight) AS (
                    SELECT NULL, NULL WHERE 0
                ),
            """

        sql = f"""
            WITH
                {label_ctes}
                layered AS (
                    SELECT s.id, s.as_of, s.generated_at, s.entity_id,
                           s.entity_label, s.entity_type, s.parent_id,
                           e.region, e.nation, e.sector, e.symbol,
                           s.theme, s.direction, s.horizon, s.score,
                           s.magnitude, s.confidence, s.evidence_count,
                           s.market_bias, s.plain_read,
                           s.top_evidence_json, s.affected_assets_json,
                           COALESCE(
                               dd.display_label,
                               db.display_label,
                               CASE s.direction
                                 WHEN 'macro-pressure' THEN 'Macro pressure'
                                 WHEN 'rates-volatility' THEN 'Rates pressure'
                                 WHEN 'inflation-risk' THEN 'Inflation pressure'
                                 WHEN 'growth-watch' THEN 'Growth watch'
                                 WHEN 'risk-off' THEN 'Risk pressure'
                                 WHEN 'supply-risk' THEN 'Supply shock'
                                 WHEN 'trade-friction' THEN 'Trade friction'
                                 WHEN 'energy-upside-risk' THEN 'Energy squeeze'
                                 WHEN 'fx-volatility' THEN 'FX stress'
                                 WHEN 'credit-risk' THEN 'Credit stress'
                                 ELSE NULL
                               END,
                               NULLIF(s.market_bias, ''),
                               s.direction
                           ) AS display_label,
                           COALESCE(dd.display_weight, db.display_weight, tw.theme_weight, 1.0) AS label_weight,
                           CASE
                             WHEN s.theme IN ('conflict', 'sanctions', 'disaster') THEN 'risk_layer'
                             WHEN s.theme = 'weather' THEN 'weather_layer'
                             WHEN s.theme IN ('social_heat', 'sentiment') THEN 'social_layer'
                             WHEN s.theme IN ('fundamentals', 'insider', 'earnings_quality',
                                              'balance_sheet', 'leverage', 'cash') THEN 'fundamental_layer'
                             WHEN s.theme IN ('inflation', 'central_bank', 'interest', 'rates',
                                              'currency', 'labour', 'gdp', 'housing', 'debt',
                                              'stockmarket', 'positioning', 'capital_flows',
                                              'industry', 'trade') THEN 'macro_layer'
                             WHEN s.entity_type IN ('market', 'sector', 'commodity') THEN 'market_layer'
                             ELSE 'oracle_layer'
                           END AS layer,
                           CASE
                             WHEN s.entity_type = 'global' THEN 'world'
                             WHEN s.entity_type IN ('region', 'nation') THEN 'regional'
                             WHEN s.entity_type IN ('market', 'sector', 'commodity') THEN 'market'
                             WHEN s.entity_type = 'macro_indicator' THEN 'macro'
                             ELSE 'other'
                           END AS map_zone
                    FROM oracle_index_snapshots s
                    LEFT JOIN oracle_entities e ON e.entity_id = s.entity_id
                    LEFT JOIN display_direction dd ON dd.direction = s.direction
                    LEFT JOIN display_bias db ON db.market_bias = s.market_bias
                    LEFT JOIN theme_weight tw ON tw.theme = s.theme
                    WHERE s.as_of = ? AND COALESCE(e.active, 1) = 1
                )
            SELECT *
            FROM layered
            WHERE (? IS NULL OR layer = ?)
              AND (? IS NULL OR entity_type = ?)
            ORDER BY
              CASE map_zone
                WHEN 'world' THEN 0 WHEN 'regional' THEN 1 WHEN 'macro' THEN 2
                WHEN 'market' THEN 3 ELSE 4 END,
              score DESC, entity_label, theme
            LIMIT ?
        """
        return pd.read_sql(
            sql,
            conn,
            params=(date_filter, layer, layer, entity_type, entity_type, limit),
            parse_dates=["as_of", "generated_at"],
        )
    finally:
        if own_conn:
            conn.close()


def oracle_impacts(conn: sqlite3.Connection | None = None, entity_id: str | None = None,
                   limit: int = 120) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "oracle_impacts"):
            return pd.DataFrame()
        latest = conn.execute("SELECT MAX(as_of) FROM oracle_impacts").fetchone()[0]
        if not latest:
            return pd.DataFrame()
        where = "as_of = ?"
        params: list[Any] = [latest]
        if entity_id:
            where += " AND entity_id = ?"
            params.append(entity_id)
        params.append(limit)
        return pd.read_sql(
            f"""SELECT id, as_of, generated_at, source_table, source_id, evidence_key,
                       theme, entity_id, direction, magnitude, confidence, horizon,
                       freshness, summary, source_refs_json, model, input_hash
                FROM oracle_impacts
                WHERE {where}
                ORDER BY magnitude * confidence DESC, entity_id, theme
                LIMIT ?""",
            conn, params=params, parse_dates=["as_of", "generated_at"],
        )
    finally:
        if own_conn:
            conn.close()


def backtest_runs(conn: sqlite3.Connection | None = None, limit: int = 20) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "backtest_runs"):
            return pd.DataFrame()
        return pd.read_sql(
            """SELECT id, name, created_at, started_at, finished_at, status,
                      config_json, notes
               FROM backtest_runs
               ORDER BY id DESC
               LIMIT ?""",
            conn, params=(limit,), parse_dates=["created_at", "started_at", "finished_at"],
        )
    finally:
        if own_conn:
            conn.close()


def backtest_summary(conn: sqlite3.Connection | None = None, run_id: int | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "backtest_predictions"):
            return pd.DataFrame()
        params: list[Any] = []
        where = ""
        if run_id is not None:
            where = "WHERE run_id = ?"
            params.append(run_id)
        return pd.read_sql(
            f"""SELECT run_id, asset, horizon,
                       COUNT(*) AS predictions,
                       SUM(CASE WHEN dry_run = 1 THEN 1 ELSE 0 END) AS dry_rows,
                       SUM(CASE WHEN dry_run = 0 AND realized_return IS NOT NULL THEN 1 ELSE 0 END) AS scored,
                       AVG(CASE WHEN dry_run = 0 THEN direction_hit END) AS directional_accuracy,
                       AVG(CASE WHEN dry_run = 0 THEN range_hit END) AS range_accuracy,
                       AVG(CASE WHEN dry_run = 0 THEN confidence END) AS avg_confidence,
                       AVG(CASE WHEN dry_run = 0 THEN realized_return END) AS avg_realized_return
                FROM backtest_predictions
                {where}
                GROUP BY run_id, asset, horizon
                ORDER BY run_id DESC, asset, horizon""",
            conn, params=params,
        )
    finally:
        if own_conn:
            conn.close()


def backtest_predictions(conn: sqlite3.Connection | None = None, run_id: int | None = None,
                         limit: int = 200) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "backtest_predictions"):
            return pd.DataFrame()
        params: list[Any] = []
        where = ""
        if run_id is not None:
            where = "WHERE run_id = ?"
            params.append(run_id)
        params.append(limit)
        return pd.read_sql(
            f"""SELECT id, run_id, asset, asset_name, horizon, as_of, generated_at,
                       direction, confidence, expected_return_low,
                       expected_return_high, model, input_hash, dry_run,
                       realized_return, direction_hit, range_hit, scored_at, error
                FROM backtest_predictions
                {where}
                ORDER BY run_id DESC, as_of DESC, asset, horizon
                LIMIT ?""",
            conn, params=params, parse_dates=["as_of", "generated_at", "scored_at"],
        )
    finally:
        if own_conn:
            conn.close()
