"""Fetch high-value BLS macro releases.

V1 focuses on US CPI because FRED can lag intraday on release days. BLS exposes
the public CPI series directly, so this gives Macro Intelligence Engine same-day actuals for macro
event reconciliation without waking the main AI layer.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
from dataclasses import dataclass

import requests

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.core.db import connect_writable


BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


@dataclass(frozen=True)
class SeriesDef:
    series_id: str
    label: str
    seasonal: str
    compute_yoy: bool = False
    compute_mom: bool = False


SERIES: tuple[SeriesDef, ...] = (
    SeriesDef("CUUR0000SA0", "BLS CPI-U All Items NSA", "NSA", compute_yoy=True),
    SeriesDef("CUUR0000SA0L1E", "BLS Core CPI-U NSA", "NSA", compute_yoy=True),
    SeriesDef("CUSR0000SA0", "BLS CPI-U All Items SA", "SA", compute_mom=True),
    SeriesDef("CUSR0000SA0L1E", "BLS Core CPI-U SA", "SA", compute_mom=True),
)


def _period_date(year: str, period: str) -> str | None:
    if not period.startswith("M") or period == "M13":
        return None
    try:
        month = int(period[1:])
        return dt.date(int(year), month, 1).isoformat()
    except ValueError:
        return None


def _fetch() -> dict[str, list[tuple[str, float]]]:
    end_year = dt.date.today().year
    start_year = end_year - 2
    payload = {
        "seriesid": [s.series_id for s in SERIES],
        "startyear": str(start_year),
        "endyear": str(end_year),
    }
    key = os.getenv("BLS_API_KEY")
    if key:
        payload["registrationkey"] = key
    response = requests.post(BLS_URL, json=payload, timeout=45)
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "REQUEST_SUCCEEDED":
        raise RuntimeError(f"BLS API request failed: {data.get('message')}")
    out: dict[str, list[tuple[str, float]]] = {}
    for series in data.get("Results", {}).get("series", []):
        sid = series.get("seriesID")
        points: list[tuple[str, float]] = []
        for item in series.get("data", []):
            d = _period_date(str(item.get("year", "")), str(item.get("period", "")))
            if not d:
                continue
            try:
                value = float(item["value"])
            except (KeyError, TypeError, ValueError):
                continue
            points.append((d, value))
        out[str(sid)] = sorted(points)
    return out


def _insert_indicator(conn: sqlite3.Connection, date: str, name: str, value: float,
                      unit: str, impact: str = "negative") -> None:
    conn.execute(
        """INSERT INTO indicators
           (date, country, category, indicator_name, value, unit, impact)
           VALUES (?, 'US', 'inflation', ?, ?, ?, ?)
           ON CONFLICT(date, country, indicator_name) DO UPDATE SET
             category=excluded.category,
             value=excluded.value,
             unit=excluded.unit,
             impact=excluded.impact""",
        (date, name, value, unit, impact),
    )


def store(conn: sqlite3.Connection, raw: dict[str, list[tuple[str, float]]]) -> int:
    inserted = 0
    defs = {s.series_id: s for s in SERIES}
    for series_id, points in raw.items():
        spec = defs.get(series_id)
        if not spec:
            continue
        by_date = dict(points)
        for d, value in points:
            _insert_indicator(conn, d, f"{spec.label} Index", value, "Index", "neutral")
            inserted += 1
            current = dt.date.fromisoformat(d)
            if spec.compute_yoy:
                prev_date = current.replace(year=current.year - 1).isoformat()
                prev = by_date.get(prev_date)
                if prev:
                    yoy = (value / prev - 1.0) * 100.0
                    _insert_indicator(conn, d, f"{spec.label} YoY", yoy, "%")
                    inserted += 1
            if spec.compute_mom:
                month = current.month - 1
                year = current.year
                if month == 0:
                    month = 12
                    year -= 1
                prev_date = dt.date(year, month, 1).isoformat()
                prev = by_date.get(prev_date)
                if prev:
                    mom = (value / prev - 1.0) * 100.0
                    _insert_indicator(conn, d, f"{spec.label} MoM", mom, "%")
                    inserted += 1
    return inserted


def main() -> int:
    conn = connect_writable(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        raw = _fetch()
        n = store(conn, raw)
        conn.commit()
        log(f"Fetched/stored {n} BLS CPI indicator row(s).", module="bls")
        return n
    finally:
        conn.close()


if __name__ == "__main__":
    main()
