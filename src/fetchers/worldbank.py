"""Annual macro indicators from the World Bank Indicators API."""
import sqlite3

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


# (id, name, unit, category, impact)
INDICATORS: list[tuple[str, str, str, str, str]] = [
    ("FP.CPI.TOTL.ZG",   "Inflation (CPI, annual %)",       "%",   "inflation",     "positive"),
    ("NY.GDP.MKTP.KD.ZG","GDP growth (annual %)",           "%",   "gdp",           "positive"),
    ("FR.INR.RINR",      "Interest rate (real)",            "%",   "rates",         "negative"),
    ("SL.UEM.TOTL.ZS",   "Unemployment (% of labor force)", "%",   "labour",        "negative"),
    ("BX.KLT.DINV.CD.WD","FDI inflows (current US$)",       "USD", "capital_flows", "positive"),
    ("NE.TRD.GNFS.ZS",   "Trade (% of GDP)",                "%",   "trade",         "context"),
    ("TX.VAL.MRCH.CD.WT","Merchandise exports",             "USD", "trade",         "positive"),
    ("TM.VAL.MRCH.CD.WT","Merchandise imports",             "USD", "trade",         "context"),
    ("BN.GSR.GNFS.CD",   "Net trade in goods and services", "USD", "trade",         "positive"),
]

COUNTRIES: dict[str, str] = {
    "US":  "United States",
    "EUU": "European Union",
    "CN":  "China",
    "JP":  "Japan",
    "KR":  "South Korea",
    "DE":  "Germany",
    "IN":  "India",
    "BR":  "Brazil",
    "SA":  "Saudi Arabia",
    "AE":  "United Arab Emirates",
}

URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=1000"


def fetch_one(country: str, indicator_id: str) -> list[dict]:
    r = requests.get(URL.format(country=country, indicator=indicator_id), timeout=30)
    r.raise_for_status()
    body = r.json()
    return body[1] if len(body) >= 2 and body[1] else []


def store(conn: sqlite3.Connection, country: str, name: str, category: str,
          unit: str, impact: str, entries: list[dict]) -> int:
    rows = [
        (e["date"] + "-01", country, category, name, float(e["value"]), unit, impact)
        for e in entries
        if e.get("value") is not None
    ]
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
    for code, country_name in COUNTRIES.items():
        for indicator_id, name, unit, category, impact in INDICATORS:
            try:
                log(f"Fetching {name} for {country_name}...", module="worldbank")
                entries = fetch_one(code, indicator_id)
                inserted = store(conn, code, name, category, unit, impact, entries)
                conn.commit()
                log(f"{name} ({code}): +{inserted} rows.", module="worldbank")
                total += inserted
            except Exception as e:
                log(f"Failed {name} for {country_name}: {e}", module="worldbank")
    log(f"Done. Total new rows: {total}", module="worldbank")
    conn.close()


if __name__ == "__main__":
    main()
