"""Compute mechanical signals from prices + indicators.

These are the numerical features that get fed to the LLM in phase 4.
Long-form storage: (date, symbol, signal_name, value).

Per-symbol price-derived:
    ret_{1d,1w,1m,3m,1y,5y}     trailing returns at each horizon (decimal)
    ma_{50,200}                 simple moving averages
    px_vs_ma{50,200}            (price/MA - 1), as a decimal
    ma50_above_ma200            1/0 regime flag (golden/death cross)
    rsi_14                      Wilder-style RSI on 14d window
    drawdown_252d               (price / 252d high - 1), <= 0
    vol_30d_ann                 annualised realised vol over 30d (decimal)

Macro / cross-asset (stored under symbol='_macro'):
    yield_curve_10y_ff          DGS10 - FEDFUNDS, in pp
    vix_regime                  0=low <15, 1=normal 15-25, 2=elevated 25-35, 3=crisis >35
    dxy_above_ma200             1/0 flag

Idempotent: full recompute every run, INSERT OR IGNORE on UNIQUE(date, symbol, signal_name).
"""
from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


HORIZON_DAYS = {"1d": 1, "1w": 5, "1m": 21, "3m": 63, "1y": 252, "5y": 1260}


def rsi(price: pd.Series, n: int = 14) -> pd.Series:
    delta = price.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    # Wilder's smoothing: equivalent to ewm with alpha=1/n
    avg_gain = gain.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    avg_loss = loss.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def signals_for_series(price: pd.Series) -> pd.DataFrame:
    """Compute all per-symbol signals for one price time series indexed by date."""
    out = pd.DataFrame(index=price.index)

    for label, n in HORIZON_DAYS.items():
        out[f"ret_{label}"] = price.pct_change(n)

    ma50 = price.rolling(50).mean()
    ma200 = price.rolling(200).mean()
    out["ma_50"] = ma50
    out["ma_200"] = ma200
    out["px_vs_ma50"] = price / ma50 - 1
    out["px_vs_ma200"] = price / ma200 - 1
    out["ma50_above_ma200"] = (ma50 > ma200).astype(float)

    out["rsi_14"] = rsi(price, 14)

    rolling_max = price.rolling(252).max()
    out["drawdown_252d"] = price / rolling_max - 1

    log_ret = np.log(price).diff()
    out["vol_30d_ann"] = log_ret.rolling(30).std() * np.sqrt(252)

    return out


def load_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT date, symbol, price FROM prices",
        conn,
        parse_dates=["date"],
    )
    return df.pivot(index="date", columns="symbol", values="price").sort_index()


def load_indicator(conn: sqlite3.Connection, name: str) -> pd.Series:
    df = pd.read_sql(
        "SELECT date, value FROM indicators WHERE indicator_name = ? ORDER BY date",
        conn, params=(name,), parse_dates=["date"],
    )
    return df.set_index("date")["value"]


def long_form(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Wide -> long: (date, symbol, signal_name, value), drop NaN."""
    stacked = df.stack(future_stack=True).rename("value").reset_index()
    stacked.columns = ["date", "signal_name", "value"]
    stacked = stacked.dropna(subset=["value"])
    stacked["symbol"] = symbol
    return stacked[["date", "symbol", "signal_name", "value"]]


def compute_per_symbol(prices: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for symbol in prices.columns:
        series = prices[symbol].dropna()
        if len(series) < 30:
            continue
        sig = signals_for_series(series)
        parts.append(long_form(sig, symbol))
    return pd.concat(parts, ignore_index=True)


def compute_macro(conn: sqlite3.Connection, prices: pd.DataFrame) -> pd.DataFrame:
    """Cross-asset signals stored under symbol='_macro'."""
    out = pd.DataFrame(index=pd.date_range(prices.index.min(), prices.index.max(), freq="D"))

    # Yield curve slope: DGS10 (daily) - FEDFUNDS (monthly, ffill to daily)
    try:
        dgs10 = load_indicator(conn, "10Y Treasury Yield")
        fedfunds = load_indicator(conn, "Fed Funds Rate")
        if not dgs10.empty and not fedfunds.empty:
            ff_daily = fedfunds.reindex(out.index, method="ffill")
            dgs_daily = dgs10.reindex(out.index, method="ffill")
            out["yield_curve_10y_ff"] = dgs_daily - ff_daily
    except Exception as e:
        log(f"yield curve skipped: {e}", module="signals")

    # VIX regime
    if "^VIX" in prices.columns:
        vix = prices["^VIX"].reindex(out.index, method="ffill")
        out["vix_regime"] = pd.cut(
            vix, bins=[-1, 15, 25, 35, 1e9], labels=[0, 1, 2, 3]
        ).astype("float")

    # DXY trend
    if "DX-Y.NYB" in prices.columns:
        dxy = prices["DX-Y.NYB"].reindex(out.index, method="ffill")
        out["dxy_above_ma200"] = (dxy > dxy.rolling(200).mean()).astype("float")

    return long_form(out, "_macro")


def store(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = [
        (d.strftime("%Y-%m-%d"), s, n, float(v))
        for d, s, n, v in df.itertuples(index=False)
    ]
    before = conn.total_changes
    # batch in chunks to keep memory reasonable
    CHUNK = 50000
    for i in range(0, len(rows), CHUNK):
        conn.executemany(
            """INSERT OR IGNORE INTO signals
               (date, symbol, signal_name, value)
               VALUES (?, ?, ?, ?)""",
            rows[i : i + CHUNK],
        )
    conn.commit()
    return conn.total_changes - before


def main():
    conn = connect_writable(DB_PATH)
    log("Loading prices...", module="signals")
    prices = load_prices(conn)
    log(f"Computing per-symbol signals for {prices.shape[1]} symbols...", module="signals")
    per_sym = compute_per_symbol(prices)
    log(f"Computing macro signals...", module="signals")
    macro = compute_macro(conn, prices)
    all_signals = pd.concat([per_sym, macro], ignore_index=True)
    log(f"Writing {len(all_signals):,} signal rows...", module="signals")
    inserted = store(conn, all_signals)
    log(f"Done. +{inserted:,} new rows.", module="signals")
    conn.close()


if __name__ == "__main__":
    main()
