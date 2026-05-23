"""Source-run telemetry for fetchers and pipeline jobs."""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from config.config_fetch import DB_PATH
from src.core.db import connect_writable, table_exists


SOURCE_RUNS_SCHEMA = """
CREATE TABLE IF NOT EXISTS source_runs (
    run_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline         TEXT NOT NULL,
    source           TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    status           TEXT NOT NULL DEFAULT 'running',
    duration_sec     REAL,
    rows_seen        INTEGER,
    rows_inserted    INTEGER,
    rows_updated     INTEGER,
    latest_source_ts TEXT,
    error_message    TEXT,
    metadata_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_source_runs_source_started
    ON source_runs(source, started_at);
CREATE INDEX IF NOT EXISTS idx_source_runs_status_started
    ON source_runs(status, started_at);
"""


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def _json(value: Mapping[str, Any] | None) -> str | None:
    if not value:
        return None
    return json.dumps(dict(value), sort_keys=True, default=str)


def ensure_source_runs(conn: sqlite3.Connection) -> None:
    conn.executescript(SOURCE_RUNS_SCHEMA)


def start_source_run(
    pipeline: str,
    source: str,
    *,
    metadata: Mapping[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> int | None:
    try:
        conn = connect_writable(db_path)
        try:
            ensure_source_runs(conn)
            cur = conn.execute(
                """INSERT INTO source_runs
                   (pipeline, source, started_at, status, metadata_json)
                   VALUES (?, ?, ?, 'running', ?)""",
                (pipeline, source, _now(), _json(metadata)),
            )
            conn.commit()
            return int(cur.lastrowid)
        finally:
            conn.close()
    except sqlite3.Error:
        return None


def finish_source_run(
    run_id: int | None,
    *,
    status: str,
    rows_seen: int | None = None,
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    latest_source_ts: str | None = None,
    error_message: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> None:
    if run_id is None:
        return
    try:
        conn = connect_writable(db_path)
        try:
            ensure_source_runs(conn)
            finished = _now()
            conn.execute(
                """UPDATE source_runs
                   SET finished_at = ?,
                       status = ?,
                       duration_sec = ROUND((julianday(?) - julianday(started_at)) * 86400, 3),
                       rows_seen = COALESCE(?, rows_seen),
                       rows_inserted = COALESCE(?, rows_inserted),
                       rows_updated = COALESCE(?, rows_updated),
                       latest_source_ts = COALESCE(?, latest_source_ts),
                       error_message = ?,
                       metadata_json = COALESCE(?, metadata_json)
                   WHERE run_id = ?""",
                (
                    finished, status, finished, rows_seen, rows_inserted,
                    rows_updated, latest_source_ts, error_message,
                    _json(metadata), run_id,
                ),
            )
            if status != "success":
                try:
                    from src.core.changes import enqueue_change_event

                    row = conn.execute(
                        "SELECT pipeline, source FROM source_runs WHERE run_id = ?",
                        (run_id,),
                    ).fetchone()
                    if row:
                        enqueue_change_event(
                            conn,
                            object_id="source.run",
                            source_table="source_runs",
                            source_id=run_id,
                            event_type="failure",
                            priority=4.5,
                            labels=["source_family:operations"],
                            metadata={
                                "pipeline": row[0],
                                "source": row[1],
                                "status": status,
                                "error_message": error_message,
                            },
                            event_key=f"source.run:{run_id}:{status}",
                            oracle_review_required=True,
                        )
                except sqlite3.Error:
                    pass
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error:
        return


def coerce_run_stats(result: Any) -> dict[str, Any]:
    if isinstance(result, int):
        return {"rows_inserted": result}
    if not isinstance(result, Mapping):
        return {}
    metadata = dict(result.get("metadata") or {})
    rows_skipped = result.get("rows_skipped") or result.get("skipped")
    if rows_skipped is not None:
        metadata["rows_skipped"] = rows_skipped
    return {
        "rows_seen": result.get("rows_seen") or result.get("fetched") or result.get("seen"),
        "rows_inserted": result.get("rows_inserted") or result.get("inserted") or result.get("rows"),
        "rows_updated": result.get("rows_updated") or result.get("updated"),
        "latest_source_ts": result.get("latest_source_ts") or result.get("latest"),
        "metadata": metadata or None,
    }


def has_source_runs(conn: sqlite3.Connection) -> bool:
    return table_exists(conn, "source_runs")
