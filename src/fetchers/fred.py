"""Fetch macro series from FRED via the public CSV endpoint (no API key)."""
import sqlite3
from io import StringIO
from datetime import date

import pandas as pd
import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

# (series_id, indicator_name, country, category, unit)
SERIES: list[tuple[str, str, str, str, str]] = [
    # ── United States ────────────────────────────────────────────────
    ("CPIAUCNS",   "CPI",                          "US", "inflation", "Index"),
    ("CPILFESL",   "Core CPI",                     "US", "inflation", "Index"),
    ("PPIACO",     "PPI All Commodities",          "US", "inflation", "Index"),
    ("PCEPI",      "PCE Price Index",              "US", "inflation", "Index"),
    ("UNRATE",     "Unemployment Rate",            "US", "labour",    "%"),
    ("NROU",       "Natural Rate of Unemployment", "US", "labour",    "%"),
    ("PAYEMS",     "Nonfarm Payrolls",             "US", "labour",    "Thousands"),
    ("M2SL",       "M2 Money Stock",               "US", "monetary",  "Million USD"),
    ("FEDFUNDS",   "Fed Funds Rate",               "US", "interest",  "%"),
    ("DGS10",      "10Y Treasury Yield",           "US", "interest",  "%"),
    ("T10Y2Y",     "10Y-2Y Yield Spread",          "US", "interest",  "pp"),
    ("DTWEXAFEGS", "US Dollar Index (Broad)",      "US", "currency",  "Index"),
    ("INDPRO",     "Industrial Production",        "US", "industry",  "Index"),
    ("RSAFS",      "Advance Retail Sales",         "US", "retail_sales", "Million USD"),
    ("UMCSENT",    "Michigan Consumer Sentiment",  "US", "sentiment", "Index"),
    ("GDPC1",      "Real GDP",                     "US", "gdp",       "Million USD"),

    # ── Eurozone aggregate ───────────────────────────────────────────
    ("CP0000EZ19M086NEST", "Eurozone HICP",            "EU", "inflation", "Index"),
    ("LRHUTTTTEZM156S",    "Eurozone Unemployment",    "EU", "labour",    "%"),
    ("IR3TIB01EZM156N",    "Eurozone 3M Interbank",    "EU", "interest",  "%"),
    ("CCRETT01EZM661N",    "Eurozone Consumer Confidence","EU","sentiment","Index"),

    # ── Sweden ───────────────────────────────────────────────────────
    ("CPALTT01SEM659N",    "Sweden CPI YoY",           "SE", "inflation", "%"),
    ("LRHUTTTTSEM156S",    "Sweden Unemployment",      "SE", "labour",    "%"),
    ("IRSTCI01SEM156N",    "Sweden Short-term Rate",   "SE", "interest",  "%"),
    ("IRLTLT01SEM156N",    "Sweden 10Y Bond Yield",    "SE", "interest",  "%"),
    ("CLVMNACSCAB1GQSE",   "Sweden Real GDP",          "SE", "gdp",       "Million SEK"),
    ("LCEAMN01SEM659S",    "Sweden Wage Index",        "SE", "labour",    "Index"),

    # ── United Kingdom ───────────────────────────────────────────────
    ("GBRCPIALLMINMEI",    "UK CPI",                   "GB", "inflation", "Index"),
    ("IRSTCI01GBM156N",    "UK Short-term Rate",       "GB", "interest",  "%"),
    ("IRLTLT01GBM156N",    "UK 10Y Gilt Yield",        "GB", "interest",  "%"),

    # ── Germany ──────────────────────────────────────────────────────
    ("DEUCPIALLMINMEI",    "Germany CPI",              "DE", "inflation", "Index"),
    ("LRHUTTTTDEM156S",    "Germany Unemployment",     "DE", "labour",    "%"),
    ("IRLTLT01DEM156N",    "Germany 10Y Bund Yield",   "DE", "interest",  "%"),

    # ── Japan ────────────────────────────────────────────────────────
    ("JPNCPIALLMINMEI",    "Japan CPI",                "JP", "inflation", "Index"),
    ("LRHUTTTTJPM156S",    "Japan Unemployment",       "JP", "labour",    "%"),
    ("IRSTCB01JPM156N",    "BoJ Policy Rate",          "JP", "interest",  "%"),
    ("IRLTLT01JPM156N",    "Japan 10Y JGB Yield",      "JP", "interest",  "%"),

    # ── China ────────────────────────────────────────────────────────
    ("CHNCPIALLMINMEI",    "China CPI",                "CN", "inflation", "Index"),

    # ── Other rate-watch ────────────────────────────────────────────
    ("IRSTCI01CHM156N",    "Switzerland Short-term Rate","CH","interest", "%"),
    ("IRSTCI01AUM156N",    "Australia Short-term Rate","AU", "interest",  "%"),
    ("IRSTCI01NOM156N",    "Norway Short-term Rate",   "NO", "interest",  "%"),
    ("IRSTCI01KRM156N",    "Korea Short-term Rate",    "KR", "interest",  "%"),
]


def fetch_series(series_id: str) -> pd.DataFrame:
    r = requests.get(CSV_URL.format(series_id=series_id), timeout=30)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    df.columns = ["date", "value"]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"])


def store(conn: sqlite3.Connection, df: pd.DataFrame, name: str, country: str, category: str, unit: str) -> int:
    today = date.today().isoformat()
    rows = [
        (str(d)[:10], country, category, name, float(v), unit)
        for d, v in zip(df["date"], df["value"])
        if str(d)[:10] <= today
    ]
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
    series_names = [name for _, name, _, _, _ in SERIES]
    placeholders = ",".join("?" for _ in series_names)
    removed_future = conn.execute(
        f"DELETE FROM indicators WHERE date > date('now') AND indicator_name IN ({placeholders})",
        series_names,
    ).rowcount
    if removed_future:
        conn.commit()
        log(f"Removed {removed_future} future-dated FRED projection rows.", module="fred")
    total = 0
    for sid, name, country, category, unit in SERIES:
        try:
            log(f"Fetching {name} ({sid})...", module="fred")
            df = fetch_series(sid)
            inserted = store(conn, df, name, country, category, unit)
            conn.commit()
            log(f"{name}: +{inserted} rows ({len(df)} fetched).", module="fred")
            total += inserted
        except Exception as e:
            log(f"Failed {name} ({sid}): {e}", module="fred")
    log(f"Done. Total new rows: {total}", module="fred")
    conn.close()


if __name__ == "__main__":
    main()
