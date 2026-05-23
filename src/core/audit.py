"""Read-only backend data audit helpers."""
from __future__ import annotations

import re
import sqlite3
from datetime import date
from typing import Any

import pandas as pd

from src.core.db import connect_readonly, table_exists


AUDIT_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "table": "prices", "label": "Market close prices", "latest_col": "date",
        "required": ("id", "date", "symbol", "price"), "max_hours": 96, "optional": False,
    },
    {
        "table": "signals", "label": "Computed signals", "latest_col": "date",
        "required": ("id", "date", "symbol", "signal_name", "value"), "max_hours": 96, "optional": False,
    },
    {
        "table": "targets", "label": "Prediction targets", "latest_col": "added_at",
        "required": ("symbol", "name", "horizons", "active"), "max_hours": None, "optional": False,
    },
    {
        "table": "indicators", "label": "Macro indicators", "latest_col": "date",
        "required": ("id", "date", "country", "category", "indicator_name", "value"), "max_hours": 90 * 24, "optional": False,
    },
    {
        "table": "calendar_events", "label": "Macro calendar", "latest_col": "date",
        "required": ("id", "date", "region", "category", "importance", "title", "event_key", "release_family", "status"), "max_hours": None, "optional": False,
    },
    {
        "table": "macro_release_actuals", "label": "Macro release actuals", "latest_col": "updated_at",
        "required": ("id", "release_key", "scheduled_date", "region", "category", "status"), "max_hours": 12, "optional": True,
    },
    {
        "table": "risk_hotspots", "label": "Risk hotspots", "latest_col": "updated_at",
        "required": ("id", "name", "category", "severity", "lat", "lon", "active"), "max_hours": 30 * 24, "optional": False,
    },
    {
        "table": "news_items", "label": "RSS news tape", "latest_col": "COALESCE(published_at, fetched_at)",
        "required": ("id", "published_at", "source", "title", "url", "fetched_at"), "max_hours": 24, "optional": True,
    },
    {
        "table": "gdelt_disaster_signals", "label": "GDELT disasters", "latest_col": "date",
        "required": ("id", "date", "country", "disaster_type", "article_count"), "max_hours": 36, "optional": True,
    },
    {
        "table": "gdelt_streams", "label": "GDELT streams", "latest_col": "date",
        "required": ("id", "date", "stream", "region", "country", "article_count", "severity", "societal_impact_score"),
        "max_hours": 36, "optional": True,
    },
    {
        "table": "gdelt_stream_theme_rules", "label": "GDELT stream theme rules", "latest_col": "updated_at",
        "required": ("id", "stream", "theme_code", "label_id", "active"),
        "max_hours": None, "optional": False,
    },
    {
        "table": "gdelt_stream_examples", "label": "GDELT stream examples", "latest_col": "date",
        "required": ("id", "date", "stream", "region", "example_rank", "title"),
        "max_hours": 36, "optional": True,
    },
    {
        "table": "sanctions", "label": "Sanctions", "latest_col": "fetched_at",
        "required": ("id", "source", "entity_name", "fetched_at"), "max_hours": 30, "optional": True,
    },
    {
        "table": "social_mentions", "label": "Reddit mentions", "latest_col": "date",
        "required": ("id", "date", "source", "ticker", "mention_count"), "max_hours": 36, "optional": True,
    },
    {
        "table": "rumour_signals", "label": "Rumour signals", "latest_col": "date",
        "required": ("id", "date", "ticker", "mentions_today"), "max_hours": 48, "optional": True,
    },
    {
        "table": "twitter_macro_posts", "label": "X/Twitter macro listener",
        "latest_col": "COALESCE(posted_at, fetched_at)",
        "required": ("id", "query", "text", "fetched_at"), "max_hours": 6, "optional": True,
    },
    {
        "table": "weather_obs", "label": "Weather observations", "latest_col": "date",
        "required": ("id", "date", "location"), "max_hours": 7 * 24, "optional": True,
    },
    {
        "table": "weather_correlations", "label": "Weather correlations", "latest_col": "computed_at",
        "required": ("id", "asset", "location", "weather_var", "correlation"), "max_hours": 14 * 24, "optional": True,
    },
    {
        "table": "company_fundamentals", "label": "Company fundamentals", "latest_col": "filed",
        "required": ("id", "ticker", "period_end", "concept", "value"), "max_hours": 120 * 24, "optional": True,
    },
    {
        "table": "insider_trades", "label": "Insider Form 4", "latest_col": "filing_date",
        "required": ("id", "filing_date", "accession_number"), "max_hours": 14 * 24, "optional": True,
    },
    {
        "table": "intelligence_packages", "label": "World Pull intelligence", "latest_col": "generated_at",
        "required": ("id", "as_of", "generated_at", "scope_type", "scope", "theme", "severity"), "max_hours": 3, "optional": False,
    },
    {
        "table": "macro_event_predictions", "label": "Macro-event forecasts", "latest_col": "generated_at",
        "required": ("id", "event_key", "as_of", "release_date", "category", "confidence", "result_status"), "max_hours": 12, "optional": False,
    },
    {
        "table": "oracle_entities", "label": "Atlas hierarchy", "latest_col": None,
        "required": ("entity_id", "label", "entity_type", "active"), "max_hours": None, "optional": False,
    },
    {
        "table": "oracle_impacts", "label": "Atlas impacts", "latest_col": "generated_at",
        "required": ("id", "as_of", "source_table", "evidence_key", "theme", "entity_id"), "max_hours": 3, "optional": False,
    },
    {
        "table": "oracle_index_snapshots", "label": "Atlas index snapshots", "latest_col": "generated_at",
        "required": ("id", "as_of", "entity_id", "entity_label", "theme", "score", "market_bias"), "max_hours": 3, "optional": False,
    },
    {
        "table": "data_labels", "label": "Data labels", "latest_col": "updated_at",
        "required": ("label_id", "label_type", "label", "default_weight", "active"), "max_hours": None, "optional": False,
    },
    {
        "table": "data_label_assignments", "label": "Data label assignments", "latest_col": "updated_at",
        "required": ("id", "label_id", "target_type", "confidence", "active"), "max_hours": None, "optional": False,
    },
    {
        "table": "label_weight_profiles", "label": "Label weight profiles", "latest_col": "updated_at",
        "required": ("profile_id", "name", "active"), "max_hours": None, "optional": False,
    },
    {
        "table": "label_weight_overrides", "label": "Label weight overrides", "latest_col": "updated_at",
        "required": ("id", "profile_id", "label_id", "weight", "active"), "max_hours": None, "optional": True,
    },
    {
        "table": "historical_state_values", "label": "Historical state values", "latest_col": "as_of",
        "required": ("id", "as_of", "value_key", "source_table", "value_date", "freshness_days", "freshness_class"), "max_hours": None, "optional": True,
    },
    {
        "table": "historical_state_daily", "label": "Historical state daily", "latest_col": "as_of",
        "required": ("as_of", "coverage_score", "training_eligible", "evaluation_eligible", "state_json"), "max_hours": None, "optional": True,
    },
    {
        "table": "historical_forward_returns", "label": "Historical forward returns", "latest_col": "as_of",
        "required": ("id", "as_of", "symbol", "horizon_days", "start_price"), "max_hours": None, "optional": True,
    },
    {
        "table": "oracle_review_annotations", "label": "Research review annotations", "latest_col": "as_of",
        "required": ("id", "source_table", "as_of", "review_type", "comment"), "max_hours": None, "optional": True,
    },
    {
        "table": "source_runs", "label": "Source run telemetry", "latest_col": "started_at",
        "required": ("run_id", "pipeline", "source", "started_at", "status"), "max_hours": 24, "optional": True,
    },
    {
        "table": "data_objects", "label": "Semantic data contract", "latest_col": "updated_at",
        "required": ("object_id", "display_name", "prompt_name", "source_table", "prompt_role", "active"), "max_hours": None, "optional": False,
    },
    {
        "table": "data_change_events", "label": "Data change events", "latest_col": "created_at",
        "required": ("id", "event_key", "object_id", "source_table", "priority", "status"), "max_hours": 24, "optional": True,
    },
    {
        "table": "prediction_context_packs", "label": "Prediction context packs", "latest_col": "updated_at",
        "required": ("id", "asset", "horizon", "as_of", "profile_id", "pack_json", "input_hash"), "max_hours": 24, "optional": True,
    },
)


SOURCE_COLUMNS = [
    "table", "label", "optional", "exists", "rows", "latest", "age",
    "status", "missing_columns", "message",
]

ORPHAN_COLUMNS = [
    "check_id", "assignment_id", "status", "label_id", "target_type",
    "target_table", "target_column", "target_value", "message",
]

SOURCE_RUN_COLUMNS = [
    "source", "pipeline", "runs", "last_status", "last_started_at",
    "last_success_at", "last_failure_at", "failure_streak",
    "latest_source_ts", "last_error", "status", "message",
]


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _expr_columns(expr: str | None) -> set[str]:
    if not expr:
        return set()
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)
    return {t for t in tokens if t.upper() not in {"COALESCE", "MAX", "MIN", "DATE", "DATETIME", "NOW"}}


def _ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name or "")):
        raise ValueError(f"Unsafe SQLite identifier: {name!r}")
    return f'"{name}"'


def _freshness(latest: str | None, max_hours: int | None) -> tuple[str, str]:
    if not latest:
        return "empty", "n/a"
    try:
        if len(str(latest)) <= 10 and "T" not in str(latest):
            latest_date = date.fromisoformat(str(latest)[:10])
            age_days = max((date.today() - latest_date).days, 0)
            age_hours = age_days * 24
            age_label = "today" if age_days == 0 else f"{age_days}d"
        else:
            latest_ts = pd.to_datetime(latest, utc=True, errors="raise")
            now = pd.Timestamp.now(tz="UTC")
            age_hours = max((now - latest_ts).total_seconds() / 3600, 0)
            age_label = f"{age_hours:.0f}h" if age_hours < 48 else f"{age_hours / 24:.0f}d"
    except (TypeError, ValueError):
        return "unknown", "n/a"
    if max_hours is not None and age_hours > max_hours:
        return "stale", age_label
    return "ok", age_label


def source_audit(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    rows: list[dict[str, Any]] = []
    try:
        for source in AUDIT_SOURCES:
            table = source["table"]
            optional = bool(source["optional"])
            if not table_exists(conn, table):
                rows.append({
                    "table": table,
                    "label": source["label"],
                    "optional": optional,
                    "exists": False,
                    "rows": 0,
                    "latest": None,
                    "age": "n/a",
                    "status": "optional_missing" if optional else "missing",
                    "missing_columns": ",".join(source["required"]),
                    "message": "Optional source table is not installed." if optional else "Required source table is missing.",
                })
                continue
            quoted_table = _ident(table)
            columns = {r[1] for r in conn.execute(f"PRAGMA table_info({quoted_table})")}
            required = set(source["required"]) | _expr_columns(source.get("latest_col"))
            missing = sorted(required - columns)
            count = int(conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0] or 0)
            latest = None
            if source.get("latest_col") and not (_expr_columns(source.get("latest_col")) - columns):
                latest = conn.execute(f"SELECT MAX({source['latest_col']}) FROM {quoted_table}").fetchone()[0]
            if source.get("latest_col"):
                status, age_label = _freshness(str(latest) if latest else None, source["max_hours"])
            else:
                status, age_label = ("ok" if count else "empty", "n/a")
            if missing:
                status = "schema_mismatch"
                message = f"Missing required column(s): {', '.join(missing)}."
            elif count == 0:
                status = "optional_empty" if optional else "empty"
                message = "Optional source has no rows yet." if optional else "Required source has no rows."
            elif status == "stale":
                message = "Latest data is older than expected cadence."
            elif status == "unknown":
                message = "Latest timestamp could not be parsed."
            else:
                message = "Source is present and structurally usable."
            rows.append({
                "table": table,
                "label": source["label"],
                "optional": optional,
                "exists": True,
                "rows": count,
                "latest": latest,
                "age": age_label,
                "status": status,
                "missing_columns": ",".join(missing),
                "message": message,
            })
        return pd.DataFrame(rows, columns=SOURCE_COLUMNS)
    finally:
        if own_conn:
            conn.close()


def label_orphan_audit(conn: sqlite3.Connection | None = None) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    rows: list[dict[str, Any]] = []
    try:
        if not table_exists(conn, "data_label_assignments"):
            return _empty(ORPHAN_COLUMNS)
        if table_exists(conn, "data_labels"):
            for row in conn.execute(
                """SELECT a.id, a.label_id, a.target_type, a.target_table,
                          a.target_column, a.target_value
                   FROM data_label_assignments a
                   LEFT JOIN data_labels l ON l.label_id = a.label_id
                   WHERE a.active = 1 AND l.label_id IS NULL"""
            ):
                rows.append({
                    "check_id": f"label:{row[0]}",
                    "assignment_id": row[0],
                    "status": "orphan_label",
                    "label_id": row[1],
                    "target_type": row[2],
                    "target_table": row[3] or None,
                    "target_column": row[4] or None,
                    "target_value": row[5] or None,
                    "message": "Assignment references a missing data_labels row.",
                })
        for row in conn.execute(
            """SELECT id, label_id, target_type, target_table, target_column, target_value
               FROM data_label_assignments
               WHERE active = 1 AND COALESCE(target_table, '') != ''"""
        ):
            assignment_id, label_id, target_type, target_table, target_column, target_value = row
            try:
                quoted_table = _ident(target_table)
            except ValueError:
                rows.append({
                    "check_id": f"table:{assignment_id}",
                    "assignment_id": assignment_id,
                    "status": "orphan_target_table",
                    "label_id": label_id,
                    "target_type": target_type,
                    "target_table": target_table,
                    "target_column": target_column or None,
                    "target_value": target_value or None,
                    "message": "Assignment target table is not a safe SQLite identifier.",
                })
                continue
            if not table_exists(conn, target_table):
                rows.append({
                    "check_id": f"table:{assignment_id}",
                    "assignment_id": assignment_id,
                    "status": "orphan_target_table",
                    "label_id": label_id,
                    "target_type": target_type,
                    "target_table": target_table,
                    "target_column": target_column or None,
                    "target_value": target_value or None,
                    "message": "Assignment references a missing target table.",
                })
                continue
            if target_column:
                columns = {r[1] for r in conn.execute(f"PRAGMA table_info({quoted_table})")}
                if target_column not in columns:
                    rows.append({
                        "check_id": f"column:{assignment_id}",
                        "assignment_id": assignment_id,
                        "status": "orphan_target_column",
                        "label_id": label_id,
                        "target_type": target_type,
                        "target_table": target_table,
                        "target_column": target_column,
                        "target_value": target_value or None,
                        "message": "Assignment references a missing target column.",
                    })
        if table_exists(conn, "label_weight_overrides"):
            for row in conn.execute(
                """SELECT o.id, o.profile_id, o.label_id
                   FROM label_weight_overrides o
                   LEFT JOIN data_labels l ON l.label_id = o.label_id
                   WHERE l.label_id IS NULL"""
            ):
                rows.append({
                    "check_id": f"profile_label:{row[0]}",
                    "assignment_id": row[0],
                    "status": "orphan_label",
                    "label_id": row[2],
                    "target_type": "profile_override",
                    "target_table": "label_weight_overrides",
                    "target_column": "profile_id",
                    "target_value": row[1],
                    "message": "Weight override references a missing data_labels row.",
                })
            if table_exists(conn, "label_weight_profiles"):
                for row in conn.execute(
                    """SELECT o.id, o.profile_id, o.label_id
                       FROM label_weight_overrides o
                       LEFT JOIN label_weight_profiles p ON p.profile_id = o.profile_id
                       WHERE p.profile_id IS NULL"""
                ):
                    rows.append({
                        "check_id": f"profile:{row[0]}",
                        "assignment_id": row[0],
                        "status": "orphan_weight_profile",
                        "label_id": row[2],
                        "target_type": "profile_override",
                        "target_table": "label_weight_overrides",
                        "target_column": "profile_id",
                        "target_value": row[1],
                        "message": "Weight override references a missing label_weight_profiles row.",
                    })
        return pd.DataFrame(rows, columns=ORPHAN_COLUMNS)
    finally:
        if own_conn:
            conn.close()


def source_run_audit(conn: sqlite3.Connection | None = None, days: int = 7) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "source_runs"):
            return _empty(SOURCE_RUN_COLUMNS)
        runs = pd.read_sql(
            """SELECT pipeline, source, started_at, finished_at, status,
                      latest_source_ts, error_message
               FROM source_runs
               WHERE started_at >= datetime('now', ?)
               ORDER BY started_at DESC""",
            conn,
            params=(f"-{int(days)} days",),
            parse_dates=["started_at", "finished_at"],
        )
        if runs.empty:
            return _empty(SOURCE_RUN_COLUMNS)
        records = []
        for source, group in runs.groupby("source", sort=False):
            latest = group.iloc[0]
            successes = group[group["status"] == "success"]
            failures = group[group["status"] != "success"]
            streak = 0
            for status in group["status"].tolist():
                if status == "success":
                    break
                streak += 1
            status = "ok" if latest["status"] == "success" else "failing"
            records.append({
                "source": source,
                "pipeline": latest["pipeline"],
                "runs": int(len(group)),
                "last_status": latest["status"],
                "last_started_at": latest["started_at"],
                "last_success_at": successes["started_at"].max() if not successes.empty else None,
                "last_failure_at": failures["started_at"].max() if not failures.empty else None,
                "failure_streak": int(streak),
                "latest_source_ts": group["latest_source_ts"].dropna().max() if group["latest_source_ts"].notna().any() else None,
                "last_error": latest["error_message"],
                "status": status,
                "message": "Latest run succeeded." if status == "ok" else "Latest run failed; table freshness may hide this.",
            })
        return pd.DataFrame(records, columns=SOURCE_RUN_COLUMNS)
    finally:
        if own_conn:
            conn.close()


def run_audit(conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        sources = source_audit(conn)
        run_health = source_run_audit(conn)
        orphans = label_orphan_audit(conn)
        blocking = {"missing", "schema_mismatch", "empty"}
        summary = {
            "sources_checked": int(len(sources)),
            "ok": int((sources["status"] == "ok").sum()) if not sources.empty else 0,
            "stale": int((sources["status"] == "stale").sum()) if not sources.empty else 0,
            "optional_empty": int((sources["status"] == "optional_empty").sum()) if not sources.empty else 0,
            "blocking_issues": int(sources["status"].isin(blocking).sum()) if not sources.empty else 0,
            "run_failures": int((run_health["status"] == "failing").sum()) if not run_health.empty else 0,
            "run_sources_checked": int(len(run_health)),
            "label_orphans": int(len(orphans)),
        }
        return {"summary": summary, "sources": sources, "source_runs": run_health, "label_orphans": orphans}
    finally:
        if own_conn:
            conn.close()
