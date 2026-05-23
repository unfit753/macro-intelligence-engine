"""Fetch selected European macro series from Eurostat's dissemination API."""
from __future__ import annotations

import sqlite3
from datetime import date

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

GEOS: dict[str, str] = {
    "EU27_2020": "EU",
    "SE": "SE",
    "DE": "DE",
    "FR": "FR",
}

# (dataset, params, label, category, unit, impact)
SERIES: list[tuple[str, dict[str, str], str, str, str, str | None]] = [
    (
        "prc_hicp_manr",
        {"coicop": "CP00", "unit": "RCH_A"},
        "Eurostat HICP YoY",
        "inflation",
        "%",
        "negative",
    ),
    (
        "une_rt_m",
        {"age": "TOTAL", "sex": "T", "unit": "PC_ACT", "s_adj": "SA"},
        "Eurostat Unemployment Rate",
        "labour",
        "%",
        "negative",
    ),
]


def _period_to_date(period: str) -> str:
    if len(period) == 7 and "-" in period:
        return f"{period}-01"
    return f"{period}-01-01"


def fetch_series(dataset: str, params: dict[str, str], geo: str, since: str) -> dict:
    q = {"lang": "en", "geo": geo, "sinceTimePeriod": since, **params}
    r = requests.get(f"{BASE}/{dataset}", params=q, timeout=45)
    r.raise_for_status()
    return r.json()


def parse_jsonstat(body: dict) -> list[tuple[str, float]]:
    time_index = body.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})
    values = body.get("value", {})
    by_idx = {int(idx): period for period, idx in time_index.items()}
    out = []
    for idx_text, value in values.items():
        period = by_idx.get(int(idx_text))
        if period is None or value is None:
            continue
        out.append((_period_to_date(period), float(value)))
    return sorted(out)


def store(
    conn: sqlite3.Connection,
    rows: list[tuple[str, float]],
    country: str,
    name: str,
    category: str,
    unit: str,
    impact: str | None,
) -> int:
    parsed = [(d, country, category, name, v, unit, impact) for d, v in rows]
    return upsert_many(
        conn,
        "indicators",
        ("date", "country", "category", "indicator_name", "value", "unit", "impact"),
        ("date", "country", "indicator_name"),
        parsed,
        update_columns=("category", "value", "unit", "impact"),
    )


def main(years_back: int = 10):
    conn = connect_writable(DB_PATH)
    since = f"{date.today().year - years_back}-01"
    total = 0
    for dataset, params, label, category, unit, impact in SERIES:
        for geo, country in GEOS.items():
            name = f"{label} ({country})"
            try:
                log(f"Fetching {name} ({dataset})...", module="eurostat")
                body = fetch_series(dataset, params, geo, since)
                rows = parse_jsonstat(body)
                inserted = store(conn, rows, country, name, category, unit, impact)
                conn.commit()
                total += inserted
                log(f"{name}: +{inserted} rows ({len(rows)} fetched).", module="eurostat")
            except Exception as e:
                log(f"Failed {name} ({dataset}): {e}", module="eurostat")
    log(f"Done. Total new rows: {total}", module="eurostat")
    conn.close()


if __name__ == "__main__":
    main()
