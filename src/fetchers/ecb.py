"""Fetch macro series from the ECB Data Portal (SDMX 2.1 generic XML)."""
import sqlite3
import xml.etree.ElementTree as ET

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


BASE = "https://data-api.ecb.europa.eu/service/data"
HEADERS = {"Accept": "application/vnd.sdmx.genericdata+xml;version=2.1"}
NS = {"generic": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"}

# (path under /service/data, indicator_name, country, category, unit)
SERIES: list[tuple[str, str, str, str, str]] = [
    ("FM/D.U2.EUR.4F.KR.MRR_FR.LEV",         "ECB Refi Rate",             "EU", "interest", "%"),
    ("FM/D.U2.EUR.4F.KR.DFR.LEV",            "ECB Deposit Rate",          "EU", "interest", "%"),
    ("EXR/D.USD.EUR.SP00.A",                 "EUR/USD",                   "EU", "currency", "Rate"),
    ("BSI/M.U2.Y.V.M30.X.I.U2.2300.Z01.A",   "Euro Area M3 YoY",          "EU", "monetary", "%"),
    ("STS/M.I9.Y.PROD.NS0020.4.000",         "Industrial Production YoY", "EU", "industry", "%"),
]


def fetch_series(path: str) -> list[tuple[str, float]]:
    r = requests.get(f"{BASE}/{path}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    out = []
    for series in root.findall(".//generic:Series", NS):
        for obs in series.findall("generic:Obs", NS):
            date = obs.find("generic:ObsDimension", NS).attrib["value"]
            value = obs.find("generic:ObsValue", NS).attrib["value"]
            out.append((date, float(value)))
    return out


def store(conn: sqlite3.Connection, obs: list[tuple[str, float]],
          name: str, country: str, category: str, unit: str) -> int:
    rows = [(d, country, category, name, v, unit) for d, v in obs]
    return upsert_many(
        conn,
        "indicators",
        ("date", "country", "category", "indicator_name", "value", "unit", "impact"),
        ("date", "country", "indicator_name"),
        [(d, c, cat, n, v, u, None) for d, c, cat, n, v, u in rows],
        update_columns=("category", "value", "unit", "impact"),
    )


def main():
    conn = connect_writable(DB_PATH)
    total = 0
    for path, name, country, category, unit in SERIES:
        try:
            log(f"Fetching {name} ({path})...", module="ecb")
            obs = fetch_series(path)
            inserted = store(conn, obs, name, country, category, unit)
            conn.commit()
            log(f"{name}: +{inserted} rows ({len(obs)} fetched).", module="ecb")
            total += inserted
        except Exception as e:
            log(f"Failed {name} ({path}): {e}", module="ecb")
    log(f"Done. Total new rows: {total}", module="ecb")
    conn.close()


if __name__ == "__main__":
    main()
