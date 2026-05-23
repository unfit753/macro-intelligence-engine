"""High-impact data-change event queue for research-note wakeups."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from collections.abc import Mapping
from typing import Any


CHANGE_SCHEMA = """
CREATE TABLE IF NOT EXISTS data_change_events (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key              TEXT NOT NULL,
    object_id              TEXT NOT NULL,
    source_table           TEXT NOT NULL,
    source_id              TEXT,
    event_type             TEXT NOT NULL DEFAULT 'upsert',
    priority               REAL NOT NULL DEFAULT 1.0,
    labels_json            TEXT,
    metadata_json          TEXT,
    status                 TEXT NOT NULL DEFAULT 'queued',
    oracle_review_required INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at             TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_change_events_key
    ON data_change_events(event_key);
CREATE INDEX IF NOT EXISTS idx_data_change_events_status
    ON data_change_events(status, priority, created_at);
"""

HIGH_IMPACT_OBJECTS = {
    "macro.release.actual",
    "calendar.event",
    "gdelt.stream",
    "risk.hotspot",
    "source.run",
}


def ensure_change_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(CHANGE_SCHEMA)


def _stable_key(parts: tuple[Any, ...]) -> str:
    raw = json.dumps(parts, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, default=str)


def requires_oracle_review(
    object_id: str,
    *,
    priority: float = 1.0,
    labels: list[str] | tuple[str, ...] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> bool:
    labels = list(labels or [])
    metadata = dict(metadata or {})
    if object_id == "source.run" and metadata.get("status") == "failure":
        return True
    if object_id == "macro.release.actual" and metadata.get("status") == "released":
        return priority >= 4.0 or any(label.startswith("release_family:") for label in labels)
    if object_id == "calendar.event":
        return priority >= 4.5
    if object_id in {"gdelt.stream", "risk.hotspot"}:
        return priority >= 4.25 or bool(metadata.get("ambiguous"))
    return object_id in HIGH_IMPACT_OBJECTS and priority >= 4.5


def enqueue_change_event(
    conn: sqlite3.Connection,
    *,
    object_id: str,
    source_table: str,
    source_id: str | int | None = None,
    event_type: str = "upsert",
    priority: float = 1.0,
    labels: list[str] | tuple[str, ...] | None = None,
    metadata: Mapping[str, Any] | None = None,
    event_key: str | None = None,
    oracle_review_required: bool | None = None,
) -> int:
    ensure_change_schema(conn)
    labels = list(labels or [])
    metadata = dict(metadata or {})
    key = event_key or _stable_key((
        object_id,
        source_table,
        str(source_id or ""),
        event_type,
        metadata.get("as_of") or metadata.get("date") or metadata.get("scheduled_date") or dt.date.today().isoformat(),
    ))
    review_required = (
        requires_oracle_review(object_id, priority=priority, labels=labels, metadata=metadata)
        if oracle_review_required is None
        else bool(oracle_review_required)
    )
    before = conn.total_changes
    conn.execute(
        """INSERT INTO data_change_events (
             event_key, object_id, source_table, source_id, event_type,
             priority, labels_json, metadata_json, status,
             oracle_review_required
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)
           ON CONFLICT(event_key) DO UPDATE SET
             object_id=excluded.object_id,
             source_table=excluded.source_table,
             source_id=excluded.source_id,
             event_type=excluded.event_type,
             priority=excluded.priority,
             labels_json=excluded.labels_json,
             metadata_json=excluded.metadata_json,
             oracle_review_required=excluded.oracle_review_required,
             updated_at=datetime('now')""",
        (
            key,
            object_id,
            source_table,
            str(source_id) if source_id is not None else None,
            event_type,
            float(priority),
            _json(labels),
            _json(metadata),
            int(review_required),
        ),
    )
    return conn.total_changes - before


def queue_high_impact_macro_release(conn: sqlite3.Connection, row: sqlite3.Row | Mapping[str, Any]) -> int:
    getter = row.get if isinstance(row, Mapping) else lambda key, default=None: row[key] if key in row.keys() else default
    if getter("status") != "released":
        return 0
    labels = []
    category = getter("category")
    if category:
        labels.append(f"theme:{category}")
    metadata_raw = getter("metadata_json")
    try:
        metadata = json.loads(metadata_raw or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    rule = metadata.get("rule") or {}
    release_key = str(getter("release_key") or "")
    release_name = release_key.split(":", 1)[0]
    family_map = {
        "us_cpi": "cpi",
        "se_cpi_cpif": "cpif",
        "gb_cpi": "cpi",
        "eu_hicp": "hicp",
        "us_ppi": "ppi",
        "us_pce": "pce",
        "us_payrolls": "payrolls",
        "us_unemployment": "unemployment",
        "us_jolts": "jolts",
        "us_gdp": "gdp",
        "us_eia_oil": "eia",
        "fomc_rate": "fomc",
        "ecb_policy_rate": "ecb",
        "riksbank_policy_rate": "riksbank",
        "boe_policy_rate": "boe",
        "boj_policy_rate": "boj",
    }
    family = family_map.get(release_name, release_name.split("_", 1)[-1])
    if family:
        labels.append(f"release_family:{family}")
    metadata.update({
        "status": getter("status"),
        "title": getter("title"),
        "region": getter("region"),
        "scheduled_date": getter("scheduled_date"),
        "actual_value": getter("actual_value"),
        "expected_value": getter("expected_value"),
        "surprise_value": getter("surprise_value"),
        "rule": rule,
    })
    return enqueue_change_event(
        conn,
        object_id="macro.release.actual",
        source_table="macro_release_actuals",
        source_id=getter("id") or getter("release_key"),
        event_type="released",
        priority=float(getter("importance") or 4),
        labels=labels,
        metadata=metadata,
        event_key=f"macro.release.actual:{release_key}:{getter('scheduled_date')}",
    )
