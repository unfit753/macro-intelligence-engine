"""Daily close prices for a curated symbol list via Yahoo Finance.

Covers commodities (gold, oil), SPDR sector ETFs, broad equity index, long bonds,
the dollar index, and VIX. One fetcher, idempotent, picks up where it left off.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta

import yfinance as yf

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


# (ticker, asset_class, name)
SYMBOLS: list[tuple[str, str, str]] = [
    # Commodities
    ("GC=F",   "commodity",   "Gold (COMEX front-month)"),
    ("CL=F",   "commodity",   "WTI Crude Oil (NYMEX front-month)"),
    ("BZ=F",   "commodity",   "Brent Crude Oil (ICE front-month)"),
    # Broad equity
    ("SPY",    "equity_etf",  "SPDR S&P 500 ETF"),
    # SPDR sector ETFs
    ("XLE",    "sector_etf",  "Energy"),
    ("XLF",    "sector_etf",  "Financials"),
    ("XLK",    "sector_etf",  "Technology"),
    ("XLV",    "sector_etf",  "Health Care"),
    ("XLI",    "sector_etf",  "Industrials"),
    ("XLP",    "sector_etf",  "Consumer Staples"),
    ("XLY",    "sector_etf",  "Consumer Discretionary"),
    ("XLB",    "sector_etf",  "Materials"),
    ("XLU",    "sector_etf",  "Utilities"),
    ("XLRE",   "sector_etf",  "Real Estate"),
    ("XLC",    "sector_etf",  "Communication Services"),
    # Bonds
    ("TLT",    "bond_etf",    "iShares 20+ Year Treasury Bond ETF"),
    # FX / vol
    ("DX-Y.NYB", "fx",        "US Dollar Index (DXY)"),
    ("^VIX",   "volatility",  "CBOE Volatility Index"),

    # ── Regional equity indices ──────────────────────────────────────
    # Europe
    ("^STOXX50E", "equity_index", "Euro Stoxx 50"),
    ("^GDAXI",    "equity_index", "DAX (Germany)"),
    ("^FCHI",     "equity_index", "CAC 40 (France)"),
    ("^FTSE",     "equity_index", "FTSE 100 (UK)"),
    ("^OMX",      "equity_index", "OMXS30 (Stockholm)"),
    # Asia (open before European cash sessions — early indicator)
    ("^N225",     "equity_index", "Nikkei 225 (Japan)"),
    ("^HSI",      "equity_index", "Hang Seng (Hong Kong)"),
    # Country / region ETFs (USD-denominated, easier to compare)
    ("EWJ",       "equity_etf",   "iShares MSCI Japan"),
    ("MCHI",      "equity_etf",   "iShares MSCI China"),
    ("EEM",       "equity_etf",   "iShares MSCI Emerging Markets"),
    ("INDA",      "equity_etf",   "iShares MSCI India"),
]

# How far back to fetch when a symbol has no rows yet.
COLD_START = timedelta(days=365 * 30)


def last_date(conn: sqlite3.Connection, symbol: str) -> date | None:
    row = conn.execute(
        "SELECT MAX(date) FROM prices WHERE symbol = ?", (symbol,)
    ).fetchone()
    if row and row[0]:
        return datetime.strptime(row[0], "%Y-%m-%d").date()
    return None


def fetch_one(ticker: str, start: date, end: date) -> dict[str, float]:
    df = yf.Ticker(ticker).history(
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
        interval="1d",
        actions=False,
    )
    out: dict[str, float] = {}
    for ts, row in df.iterrows():
        close = row.get("Close")
        if close is None or close != close:  # NaN
            continue
        out[ts.date().isoformat()] = float(close)
    return out


def store(conn: sqlite3.Connection, symbol: str, asset_class: str, name: str,
          prices: dict[str, float]) -> int:
    rows = [(d, symbol, asset_class, name, p, "USD") for d, p in prices.items()]
    return upsert_many(
        conn,
        "prices",
        ("date", "symbol", "asset_class", "name", "price", "currency"),
        ("date", "symbol"),
        rows,
        update_columns=("asset_class", "name", "price", "currency"),
    )


def main():
    conn = connect_writable(DB_PATH)
    today = date.today()
    total = 0
    for ticker, asset_class, name in SYMBOLS:
        try:
            prev = last_date(conn, ticker)
            start = (prev + timedelta(days=1)) if prev else (today - COLD_START)
            if start > today:
                log(f"{ticker}: up to date.", module="yahoo")
                continue
            log(f"Fetching {ticker} ({name}) from {start} to {today}...", module="yahoo")
            prices = fetch_one(ticker, start, today)
            inserted = store(conn, ticker, asset_class, name, prices)
            conn.commit()
            log(f"{ticker}: +{inserted} rows ({len(prices)} fetched).", module="yahoo")
            total += inserted
        except Exception as e:
            log(f"Failed {ticker}: {e}", module="yahoo")
    log(f"Done. Total new rows: {total}", module="yahoo")
    conn.close()


if __name__ == "__main__":
    main()
