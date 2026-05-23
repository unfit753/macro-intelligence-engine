"""Database connection helpers for reusable read/write boundaries."""
from __future__ import annotations

import sqlite3
from collections.abc import Sequence

from config.config_fetch import DB_PATH


SQLITE_BUSY_TIMEOUT_MS = 60_000


def configure_connection(conn: sqlite3.Connection, *, writable: bool = False) -> sqlite3.Connection:
    """Apply the project's common SQLite runtime pragmas."""
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys=ON")
    if writable:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _readonly_uri(db_path: str, *, immutable: bool = False) -> str:
    params = "mode=ro"
    if immutable:
        params += "&immutable=1"
    return f"file:{db_path}?{params}"


def _open_readonly(db_path: str, *, immutable: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(
        _readonly_uri(db_path, immutable=immutable),
        uri=True,
        timeout=60,
        check_same_thread=False,
    )
    try:
        configured = configure_connection(conn, writable=False)
        configured.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
        return configured
    except Exception:
        conn.close()
        raise


def connect_readonly(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Open a read-only SQLite connection.

    Public/commercial surfaces should use this by default so they cannot mutate
    private admin state by accident. Docker mounts the live DB as read-only; when
    SQLite cannot create WAL sidecar files there, retry with immutable mode.
    """
    try:
        return _open_readonly(db_path)
    except sqlite3.OperationalError as exc:
        if "unable to open database file" not in str(exc).lower():
            raise
        return _open_readonly(db_path, immutable=True)


def connect_writable(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Open a writable connection for private admin and pipeline code."""
    conn = sqlite3.connect(db_path, timeout=60, check_same_thread=False)
    return configure_connection(conn, writable=True)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _ident(name: str) -> str:
    if not name or not name.replace("_", "").isalnum() or name[0].isdigit():
        raise ValueError(f"Unsafe SQLite identifier: {name!r}")
    return name


def upsert_many(
    conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    conflict_columns: Sequence[str],
    rows: Sequence[Sequence[object]],
    *,
    update_columns: Sequence[str] | None = None,
) -> int:
    """Insert rows and update mutable columns on natural-key conflicts."""
    if not rows:
        return 0
    table_sql = _ident(table)
    column_sql = ", ".join(_ident(c) for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    conflict_sql = ", ".join(_ident(c) for c in conflict_columns)
    updates = list(update_columns or [c for c in columns if c not in conflict_columns])
    if updates:
        update_sql = ", ".join(f"{_ident(c)}=excluded.{_ident(c)}" for c in updates)
        sql = (
            f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict_sql}) DO UPDATE SET {update_sql}"
        )
    else:
        sql = (
            f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict_sql}) DO NOTHING"
        )
    before = conn.total_changes
    conn.executemany(sql, rows)
    return conn.total_changes - before
