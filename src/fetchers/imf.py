"""Fetch IMF DataMapper/WEO macro outlook series."""
from __future__ import annotations

import sqlite3
from datetime import date

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


BASE = "https://www.imf.org/external/datamapper/api/v1"

# IMF country code -> internal country code
COUNTRIES: dict[str, str] = {
    "SWE": "SE",
    "USA": "US",
    "DEU": "DE",
    "FRA": "FR",
    "GBR": "GB",
    "JPN": "JP",
    "CHN": "CN",
}

# (indicator_id, name, category, unit, impact)
INDICATORS: list[tuple[str, str, str, str, str | None]] = [
    ("NGDP_RPCH", "IMF Real GDP Growth", "gdp", "%", "positive"),
    ("PCPIPCH", "IMF Consumer Price Inflation", "inflation", "%", "negative"),
    ("LUR", "IMF Unemployment Rate", "labour", "%", "negative"),
    ("GGXWDG_NGDP", "IMF Government Gross Debt", "debt", "% GDP", "negative"),
    ("BCA_NGDPD", "IMF Current Account Balance", "trade", "% GDP", "context"),
]


def fetch_indicator(indicator_id: str) -> dict[str, dict[str, float]]:
    r = requests.get(f"{BASE}/{indicator_id}", timeout=60)
    r.raise_for_status()
    body = r.json()
    return body.get("values", {}).get(indicator_id, {})


def store(
    conn: sqlite3.Connection,
    series: dict[str, dict[str, float]],
    indicator_name: str,
    category: str,
    unit: str,
    impact: str | None,
) -> int:
    current_year = date.today().year
    rows = []
    for imf_country, country in COUNTRIES.items():
        for year, value in series.get(imf_country, {}).items():
            if value is None:
                continue
            try:
                year_int = int(year)
            except ValueError:
                continue
            if year_int > current_year:
                continue
            rows.append((f"{year_int}-01-01", country, category, indicator_name, float(value), unit, impact))

    return upsert_many(
        conn,
        "indicators",
        ("date", "country", "category", "indicator_name", "value", "unit", "impact"),
        ("date", "country", "indicator_name"),
        rows,
        update_columns=("category", "value", "unit", "impact"),
    )


def main():
    conn = connect_writable(DB_PATH)
    total = 0
    for indicator_id, name, category, unit, impact in INDICATORS:
        indicator_name = f"{name} ({indicator_id})"
        try:
            log(f"Fetching {indicator_name}...", module="imf")
            series = fetch_indicator(indicator_id)
            inserted = store(conn, series, indicator_name, category, unit, impact)
            conn.commit()
            total += inserted
            log(f"{indicator_name}: +{inserted} rows.", module="imf")
        except Exception as e:
            log(f"Failed {indicator_name}: {e}", module="imf")
    log(f"Done. Total new rows: {total}", module="imf")
    conn.close()


if __name__ == "__main__":
    main()
