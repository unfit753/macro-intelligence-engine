"""Fetch optional energy market fundamentals from the EIA API v2."""
from __future__ import annotations

import os
import sqlite3

import requests
from dotenv import load_dotenv

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

# (series facet, name, unit)
SERIES: list[tuple[str, str, str]] = [
    ("RWTC", "EIA WTI Spot Price", "USD/bbl"),
    ("RBRTE", "EIA Brent Spot Price", "USD/bbl"),
]


def fetch_series(api_key: str, series: str) -> list[dict]:
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": series,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": "5000",
    }
    r = requests.get(URL, params=params, timeout=45)
    r.raise_for_status()
    body = r.json()
    if "error" in body:
        raise RuntimeError(body["error"].get("message", body["error"]))
    return body.get("response", {}).get("data", [])


def store(conn: sqlite3.Connection, rows: list[dict], name: str, unit: str) -> int:
    parsed = []
    for row in rows:
        value = row.get("value")
        period = row.get("period")
        if value is None or not period:
            continue
        parsed.append((period, "US", "energy", name, float(value), unit))

    return upsert_many(
        conn,
        "indicators",
        ("date", "country", "category", "indicator_name", "value", "unit", "impact"),
        ("date", "country", "indicator_name"),
        [(d, c, cat, n, v, u, None) for d, c, cat, n, v, u in parsed],
        update_columns=("category", "value", "unit", "impact"),
    )


def main():
    load_dotenv()
    api_key = os.getenv("EIA_API_KEY", "").strip()
    if not api_key:
        log("EIA_API_KEY missing; skipping EIA energy fetcher.", module="eia")
        return

    conn = connect_writable(DB_PATH)
    total = 0
    for series, name, unit in SERIES:
        try:
            log(f"Fetching {name} ({series})...", module="eia")
            rows = fetch_series(api_key, series)
            inserted = store(conn, rows, name, unit)
            conn.commit()
            total += inserted
            log(f"{name}: +{inserted} rows ({len(rows)} fetched).", module="eia")
        except Exception as e:
            log(f"Failed {name} ({series}): {e}", module="eia")
    log(f"Done. Total new rows: {total}", module="eia")
    conn.close()


if __name__ == "__main__":
    main()
