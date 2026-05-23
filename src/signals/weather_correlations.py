"""Compute Pearson correlations between weather observations and asset returns.

For a curated set of (asset, location) pairs that have plausible economic
links — refining-belt heat → WTI, Stockholm temperature → OMX seasonality
proxy, etc. — compute the rolling correlation between weather variables
(temperature anomaly, precipitation, wind) and asset returns over fixed
windows. Surface the strongest correlations in downstream clients.

This is exploratory: real signal in weather/markets is rare outside
extreme events, but cheap to collect and surface. If something jumps out
at us we can lift it into the prediction inputs later.
"""
from __future__ import annotations

import datetime as dt
import sqlite3

import pandas as pd

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


# (asset_symbol, location_label) — pairs where there's at least a hand-wavey
# economic link. Add more as patterns emerge.
PAIRS: list[tuple[str, str]] = [
    ("CL=F",      "New York"),     # heating-oil demand proxy
    ("CL=F",      "London"),
    ("BZ=F",      "Dubai"),        # OPEC region
    ("BZ=F",      "Singapore"),    # Asian refining hub
    ("GC=F",      "Mumbai"),       # India's gold demand seasonality
    ("GC=F",      "Shanghai"),
    ("^OMX",      "Stockholm"),    # local sentiment proxy
    ("^N225",     "Tokyo"),
    ("^HSI",      "Hong Kong"),
    ("^STOXX50E", "Frankfurt"),
    ("XLE",       "New York"),     # energy ETF
    ("XLU",       "New York"),     # utilities — heating/cooling demand
]

WEATHER_VARS = ["temp_mean_c", "precipitation_mm", "wind_max_kmh"]
WINDOWS = [30, 90, 180]


def load_weather(conn: sqlite3.Connection, location: str) -> pd.DataFrame:
    return pd.read_sql(
        """SELECT date, temp_mean_c, precipitation_mm, wind_max_kmh
           FROM weather_obs WHERE location = ? ORDER BY date""",
        conn, params=(location,), parse_dates=["date"],
    ).set_index("date")


def load_asset_returns(conn: sqlite3.Connection, symbol: str) -> pd.Series:
    df = pd.read_sql(
        "SELECT date, price FROM prices WHERE symbol = ? ORDER BY date",
        conn, params=(symbol,), parse_dates=["date"],
    ).set_index("date")
    return df["price"].pct_change()


def correlations_for(conn: sqlite3.Connection, asset: str, location: str
                     ) -> list[tuple[str, str, str, int, float, int]]:
    weather = load_weather(conn, location)
    returns = load_asset_returns(conn, asset)
    if weather.empty or returns.empty:
        return []
    joined = weather.join(returns.rename("ret"), how="inner")
    if joined.empty:
        return []
    out = []
    end = joined.index.max()
    for window in WINDOWS:
        slice_df = joined.loc[end - pd.Timedelta(days=window):end]
        if len(slice_df) < 10:
            continue
        for var in WEATHER_VARS:
            if var not in slice_df:
                continue
            s = slice_df[[var, "ret"]].dropna()
            if len(s) < 10:
                continue
            corr = s[var].corr(s["ret"])
            if pd.isna(corr):
                continue
            out.append((asset, location, var, window, float(corr), len(s)))
    return out


def upsert_correlations(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    now_iso = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    payload = [(*r, now_iso) for r in rows]
    before = conn.total_changes
    conn.executemany(
        """INSERT INTO weather_correlations
           (asset, location, weather_var, window_days,
            correlation, n_observations, computed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(asset, location, weather_var, window_days) DO UPDATE SET
             correlation     = excluded.correlation,
             n_observations  = excluded.n_observations,
             computed_at     = excluded.computed_at""",
        payload,
    )
    return conn.total_changes - before


def main():
    conn = connect_writable(DB_PATH)
    try:
        all_rows: list[tuple] = []
        for asset, location in PAIRS:
            try:
                pair_rows = correlations_for(conn, asset, location)
                all_rows.extend(pair_rows)
            except Exception as e:
                log(f"{asset}/{location}: failed {e}", module="wcorr")
        if not all_rows:
            log("No correlations computed (weather or price data missing).",
                module="wcorr")
            return
        upserted = upsert_correlations(conn, all_rows)
        conn.commit()
        # Log the strongest absolute correlations as a teaser.
        top = sorted(all_rows, key=lambda r: abs(r[4]), reverse=True)[:5]
        log(f"upserted {upserted} rows. Top by |corr|:", module="wcorr")
        for r in top:
            log(f"  {r[0]:<10} {r[1]:<12} {r[2]:<18} {r[3]}d  corr={r[4]:+.3f} (n={r[5]})",
                module="wcorr")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
