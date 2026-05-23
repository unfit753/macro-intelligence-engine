"""Daily weather for major economic centres via Open-Meteo (free, no auth).

Layer-1 collection only — we store, we don't yet correlate. Pulled into
Layer-2 analysis (weather/market correlation) once enough history
accumulates.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


# (location_label, lat, lon)
LOCATIONS: list[tuple[str, float, float]] = [
    ("New York",   40.7128, -74.0060),
    ("London",     51.5074,  -0.1278),
    ("Frankfurt",  50.1109,   8.6821),
    ("Stockholm",  59.3293,  18.0686),
    ("Tokyo",      35.6762, 139.6503),
    ("Hong Kong",  22.3193, 114.1694),
    ("Singapore",   1.3521, 103.8198),
    ("Shanghai",   31.2304, 121.4737),
    ("Sydney",    -33.8688, 151.2093),
    ("Sao Paulo", -23.5505, -46.6333),
    ("Mumbai",     19.0760,  72.8777),
    ("Dubai",      25.2048,  55.2708),
]

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
COLD_START_DAYS = 365


def fetch_one(lat: float, lon: float, start: date, end: date) -> dict:
    r = requests.get(
        ARCHIVE_URL,
        params={
            "latitude": lat, "longitude": lon,
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "daily": "temperature_2m_mean,precipitation_sum,wind_speed_10m_max",
            "timezone": "UTC",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("daily", {})


def last_obs_date(conn: sqlite3.Connection, location: str) -> date | None:
    row = conn.execute(
        "SELECT MAX(date) FROM weather_obs WHERE location = ?", (location,),
    ).fetchone()
    if row and row[0]:
        return datetime.strptime(row[0], "%Y-%m-%d").date()
    return None


def store(conn: sqlite3.Connection, location: str, daily: dict) -> int:
    times = daily.get("time", [])
    temps = daily.get("temperature_2m_mean", [])
    precs = daily.get("precipitation_sum", [])
    winds = daily.get("wind_speed_10m_max", [])
    rows = list(zip(times, [location] * len(times), temps, precs, winds))
    return upsert_many(
        conn,
        "weather_obs",
        ("date", "location", "temp_mean_c", "precipitation_mm", "wind_max_kmh"),
        ("date", "location"),
        rows,
        update_columns=("temp_mean_c", "precipitation_mm", "wind_max_kmh"),
    )


def main():
    conn = connect_writable(DB_PATH)
    today = date.today()
    end = today - timedelta(days=2)  # archive lags ~1-2 days
    total = 0
    for label, lat, lon in LOCATIONS:
        last = last_obs_date(conn, label)
        start = (last + timedelta(days=1)) if last else (today - timedelta(days=COLD_START_DAYS))
        if start > end:
            log(f"{label}: up to date.", module="weather")
            continue
        try:
            log(f"{label}: fetching {start} -> {end}", module="weather")
            daily = fetch_one(lat, lon, start, end)
            inserted = store(conn, label, daily)
            conn.commit()
            log(f"{label}: +{inserted} rows.", module="weather")
            total += inserted
        except Exception as e:
            log(f"{label}: failed {e}", module="weather")
    log(f"Done. +{total} rows.", module="weather")
    conn.close()


if __name__ == "__main__":
    main()
