"""US Senate stock trade disclosures.

Source: timothycarambat/senate-stock-watcher-data on GitHub. The
underlying data is the eFD search at efdsearch.senate.gov; the GitHub
repo aggregates it as a single JSON file. The repo's update cadence has
been spotty since mid-2025 — we ingest whatever is current and the
historical 8k+ transactions stay useful even if the live feed lags.

For fresher data later, candidates are: Quiver Quantitative API
(requires registration), Capitol Trades scraping, or direct eFD.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


SOURCE_URL = (
    "https://raw.githubusercontent.com/timothycarambat/"
    "senate-stock-watcher-data/master/aggregate/all_transactions.json"
)


def normalise_date(s: str) -> str | None:
    """Source emits dates as MM/DD/YYYY; convert to ISO."""
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def fetch_all() -> list[dict]:
    r = requests.get(SOURCE_URL, timeout=60)
    r.raise_for_status()
    return r.json()


def store(conn: sqlite3.Connection, transactions: list[dict]) -> int:
    rows = []
    for t in transactions:
        d = normalise_date(t.get("transaction_date") or "")
        if not d:
            continue
        rows.append((
            d,
            t.get("senator") or "",
            (t.get("ticker") or "").strip() or None,
            t.get("asset_description") or None,
            t.get("asset_type") or None,
            t.get("type") or None,
            t.get("amount") or None,
            t.get("owner") or None,
            t.get("ptr_link") or None,
        ))
    before = conn.total_changes
    conn.executemany(
        """INSERT OR IGNORE INTO senate_trades
           (transaction_date, senator, ticker, asset_description, asset_type,
            transaction_type, amount_range, owner, ptr_link)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return conn.total_changes - before


def main():
    log(f"Fetching senate trades feed ({SOURCE_URL[-60:]})...", module="senate")
    try:
        transactions = fetch_all()
    except requests.RequestException as e:
        log(f"Fetch failed: {e}", module="senate")
        return
    log(f"Source returned {len(transactions):,} transactions.", module="senate")

    conn = connect_writable(DB_PATH)
    try:
        inserted = store(conn, transactions)
        conn.commit()
        log(f"Done. +{inserted} new rows.", module="senate")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
