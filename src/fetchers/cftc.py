"""Fetch CFTC Commitments of Traders positioning for macro commodities."""
from __future__ import annotations

import sqlite3
from datetime import date
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


URL = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"

# name -> market-name substrings, checked in order
CONTRACTS: dict[str, list[str]] = {
    "Gold": ["GOLD - COMMODITY EXCHANGE INC."],
    "Silver": ["SILVER - COMMODITY EXCHANGE INC."],
    "Copper": ["COPPER- #1 - COMMODITY EXCHANGE INC."],
    "WTI Crude Oil": [
        "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
        "CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE",
    ],
}

METRICS: list[tuple[str, str, str | None, str]] = [
    ("Open Interest", "Open_Interest_All", None, "contracts"),
    ("Managed Money Net", "M_Money_Positions_Long_All", "M_Money_Positions_Short_All", "contracts"),
    ("Producer/Merchant Net", "Prod_Merc_Positions_Long_All", "Prod_Merc_Positions_Short_All", "contracts"),
    ("Swap Dealer Net", "Swap_Positions_Long_All", "Swap__Positions_Short_All", "contracts"),
]


def fetch_year(year: int) -> pd.DataFrame:
    r = requests.get(URL.format(year=year), timeout=60)
    r.raise_for_status()
    with ZipFile(BytesIO(r.content)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as fh:
            return pd.read_csv(fh, na_values=[".", " ."])


def _select_contract(df: pd.DataFrame, patterns: list[str]) -> pd.DataFrame:
    names = df["Market_and_Exchange_Names"].astype(str)
    usable = df[~names.str.contains("MICRO|MINI", case=False, regex=True, na=False)].copy()
    usable_names = usable["Market_and_Exchange_Names"].astype(str)
    for pattern in patterns:
        matched = usable[usable_names.str.contains(pattern, case=False, regex=False, na=False)].copy()
        if not matched.empty:
            return matched
    return pd.DataFrame()


def _metric_value(row: pd.Series, long_col: str, short_col: str | None) -> float | None:
    long_val = pd.to_numeric(row.get(long_col), errors="coerce")
    if pd.isna(long_val):
        return None
    if short_col is None:
        return float(long_val)
    short_val = pd.to_numeric(row.get(short_col), errors="coerce")
    if pd.isna(short_val):
        return None
    return float(long_val - short_val)


def store(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = []
    for contract_name, patterns in CONTRACTS.items():
        contract_df = _select_contract(df, patterns)
        if contract_df.empty:
            log(f"No CFTC rows matched {contract_name}.", module="cftc")
            continue
        for _idx, row_s in contract_df.iterrows():
            report_date = str(row_s["Report_Date_as_YYYY-MM-DD"])[:10]
            for metric_label, long_col, short_col, unit in METRICS:
                value = _metric_value(row_s, long_col, short_col)
                if value is None:
                    continue
                indicator_name = f"CFTC {contract_name} {metric_label}"
                rows.append((report_date, "US", "positioning", indicator_name, value, unit))

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
    for year in (date.today().year - 1, date.today().year):
        try:
            log(f"Fetching CFTC disaggregated COT {year}...", module="cftc")
            df = fetch_year(year)
            inserted = store(conn, df)
            conn.commit()
            total += inserted
            log(f"CFTC {year}: +{inserted} rows ({len(df)} fetched).", module="cftc")
        except Exception as e:
            log(f"Failed CFTC {year}: {e}", module="cftc")
    log(f"Done. Total new rows: {total}", module="cftc")
    conn.close()


if __name__ == "__main__":
    main()
