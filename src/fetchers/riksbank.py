"""Fetch Swedish rates and SEK exchange rates from Riksbank's REST API."""
from __future__ import annotations

import sqlite3
import time
from datetime import date, timedelta

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


BASE = "https://api.riksbank.se/swea/v1"

# (series_id, indicator_name, category, unit)
SERIES: list[tuple[str, str, str, str]] = [
    ("SECBREPOEFF", "Riksbank Policy Rate", "interest", "%"),
    ("SECBDEPOEFF", "Riksbank Deposit Rate", "interest", "%"),
    ("SECBLENDEFF", "Riksbank Lending Rate", "interest", "%"),
    ("SEKUSDPMI", "Riksbank USD/SEK", "currency", "SEK"),
    ("SEKEURPMI", "Riksbank EUR/SEK", "currency", "SEK"),
    ("SEKGBPPMI", "Riksbank GBP/SEK", "currency", "SEK"),
    ("SEKJPYPMI", "Riksbank JPY/SEK", "currency", "SEK"),
    ("SEKNOKPMI", "Riksbank NOK/SEK", "currency", "SEK"),
]


def fetch_series(series_id: str, start: date, end: date) -> list[dict]:
    url = f"{BASE}/Observations/{series_id.lower()}/{start.isoformat()}/{end.isoformat()}"
    r = requests.get(url, timeout=30)
    if r.status_code == 429:
        log("Riksbank rate limit hit; waiting 65s before retry.", module="riksbank")
        time.sleep(65)
        r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def store(conn: sqlite3.Connection, rows: list[dict], name: str, category: str, unit: str) -> int:
    parsed = [
        (row["date"], "SE", category, name, float(row["value"]), unit)
        for row in rows
        if row.get("date") and row.get("value") is not None
    ]
    return upsert_many(
        conn,
        "indicators",
        ("date", "country", "category", "indicator_name", "value", "unit", "impact"),
        ("date", "country", "indicator_name"),
        [(d, c, cat, n, v, u, None) for d, c, cat, n, v, u in parsed],
        update_columns=("category", "value", "unit", "impact"),
    )


def main(days: int = 3650):
    conn = connect_writable(DB_PATH)
    end = date.today()
    start = end - timedelta(days=days)
    total = 0
    for series_id, name, category, unit in SERIES:
        try:
            log(f"Fetching {name} ({series_id})...", module="riksbank")
            rows = fetch_series(series_id, start, end)
            inserted = store(conn, rows, name, category, unit)
            conn.commit()
            total += inserted
            log(f"{name}: +{inserted} rows ({len(rows)} fetched).", module="riksbank")
        except Exception as e:
            log(f"Failed {name} ({series_id}): {e}", module="riksbank")
        time.sleep(13)
    log(f"Done. Total new rows: {total}", module="riksbank")
    conn.close()


if __name__ == "__main__":
    main()
