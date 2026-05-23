"""Fetch Swedish macro indicators from Statistics Sweden (SCB) PxWeb.

This intentionally uses SCB's maintained PxWeb JSON API instead of the
retired SOAP/WSDL route.
"""
from __future__ import annotations

import sqlite3

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


# (path under /START, category, impact)
TABLES: list[tuple[str, str, str | None]] = [
    ("PR/PR0101/PR0101A/KPI2020M", "inflation", "negative"),
    ("PR/PR0101/PR0101G/KPIF2020", "inflation", "negative"),
    ("PR/PR0101/PR0101J/KPIFXE2020", "inflation", "negative"),
    ("PR/PR0101/PR0101C/HIKP2025", "inflation", "negative"),
]

BASE = "https://api.scb.se/OV0104/v1/doris/en/ssd/START"


def _period_to_date(period: str) -> str:
    if "M" in period:
        year, month = period.split("M", 1)
        return f"{year}-{int(month):02d}-01"
    return f"{period}-01-01"


def _value_to_float(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(" ", "").replace(",", ".")
    if text in {"", "..", "."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def fetch_metadata(path: str) -> dict:
    r = requests.get(f"{BASE}/{path}", timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_content(path: str, content_code: str) -> list[dict]:
    payload = {
        "query": [
            {
                "code": "ContentsCode",
                "selection": {"filter": "item", "values": [content_code]},
            }
        ],
        "response": {"format": "json"},
    }
    r = requests.post(f"{BASE}/{path}", json=payload, timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])


def _content_labels(metadata: dict) -> dict[str, str]:
    for var in metadata.get("variables", []):
        if var.get("code") == "ContentsCode":
            return dict(zip(var.get("values", []), var.get("valueTexts", []), strict=False))
    return {}


def _unit_for(label: str) -> str:
    lower = label.lower()
    if "annual changes" in lower or "monthly changes" in lower:
        return "%"
    if "index" in lower:
        return "Index"
    return ""


def store(
    conn: sqlite3.Connection,
    rows: list[dict],
    indicator_name: str,
    category: str,
    unit: str,
    impact: str | None,
) -> int:
    parsed = []
    for row in rows:
        value = _value_to_float(row.get("values", [None])[0])
        if value is None:
            continue
        period = row.get("key", [])[-1]
        parsed.append((_period_to_date(period), "SE", category, indicator_name, value, unit, impact))

    return upsert_many(
        conn,
        "indicators",
        ("date", "country", "category", "indicator_name", "value", "unit", "impact"),
        ("date", "country", "indicator_name"),
        parsed,
        update_columns=("category", "value", "unit", "impact"),
    )


def main():
    conn = connect_writable(DB_PATH)
    total = 0
    for path, category, impact in TABLES:
        try:
            metadata = fetch_metadata(path)
            labels = _content_labels(metadata)
            if not labels:
                log(f"No ContentsCode values in {path}; skipping.", module="scb")
                continue
            for content_code, label in labels.items():
                name = f"SCB {label}"
                log(f"Fetching {name} ({path}/{content_code})...", module="scb")
                rows = fetch_content(path, content_code)
                inserted = store(conn, rows, name, category, _unit_for(label), impact)
                conn.commit()
                total += inserted
                log(f"{name}: +{inserted} rows ({len(rows)} fetched).", module="scb")
        except Exception as e:
            log(f"Failed {path}: {e}", module="scb")
    log(f"Done. Total new rows: {total}", module="scb")
    conn.close()


if __name__ == "__main__":
    main()
