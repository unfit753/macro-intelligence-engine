"""Detect mention-count spikes in social_mentions and score them retroactively.

Two passes per run:
  1. For every (ticker, date) in social_mentions, compute a baseline from the
     trailing 14 days (excluding the day itself), the day's z-score, and
     write or upsert into rumour_signals.
  2. For any rumour_signals row whose 5-day or 21-day look-ahead window has
     elapsed and whose realized_return_* is still null, compute the realised
     forward return on the asset's price and fill it in.

This is a layer-2 job — pure analysis on top of layer-1 data, no Claude
calls. Daily cron is fine; reading the whole social_mentions table is
cheap at this volume.
"""
from __future__ import annotations

import datetime as dt
import sqlite3

import pandas as pd

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


BASELINE_WINDOW_DAYS = 14
LOOKAHEAD_5D = 5
LOOKAHEAD_21D = 21


def collapse_daily(conn: sqlite3.Connection) -> pd.DataFrame:
    """Sum mention counts across subreddits per (date, ticker), avg sentiment."""
    return pd.read_sql(
        """SELECT date, ticker,
                  SUM(mention_count)              AS mentions,
                  AVG(sentiment_score)            AS sentiment
           FROM social_mentions
           GROUP BY date, ticker
           ORDER BY ticker, date""",
        conn, parse_dates=["date"],
    )


def compute_zscores(daily: pd.DataFrame) -> pd.DataFrame:
    """For each (ticker, date), compute baseline mean and z-score over a
    trailing window. Excludes today from its own baseline."""
    out_rows: list[dict] = []
    for ticker, group in daily.groupby("ticker"):
        group = group.sort_values("date").reset_index(drop=True)
        roll = group["mentions"].shift(1).rolling(BASELINE_WINDOW_DAYS, min_periods=3)
        group["baseline_mean"] = roll.mean()
        group["baseline_std"] = roll.std()
        group["z_score"] = (group["mentions"] - group["baseline_mean"]) / group["baseline_std"].replace(0, float("nan"))
        out_rows.extend(group.to_dict("records"))
    return pd.DataFrame(out_rows)


def upsert_rumours(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = []
    for _, r in df.iterrows():
        rows.append((
            r["date"].strftime("%Y-%m-%d"),
            r["ticker"],
            int(r["mentions"]),
            float(r["baseline_mean"]) if pd.notna(r["baseline_mean"]) else None,
            float(r["z_score"]) if pd.notna(r["z_score"]) else None,
            float(r["sentiment"]) if pd.notna(r["sentiment"]) else None,
        ))
    before = conn.total_changes
    conn.executemany(
        """INSERT INTO rumour_signals
           (date, ticker, mentions_today, baseline_mean, z_score, sentiment_today)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(date, ticker) DO UPDATE SET
             mentions_today  = excluded.mentions_today,
             baseline_mean   = excluded.baseline_mean,
             z_score         = excluded.z_score,
             sentiment_today = excluded.sentiment_today""",
        rows,
    )
    return conn.total_changes - before


def realized_return(conn: sqlite3.Connection, ticker: str, from_date: str,
                    n_days: int) -> float | None:
    """Trading-day forward return on a US equity ticker."""
    start_row = conn.execute(
        "SELECT date, price FROM prices WHERE symbol = ? AND date <= ? "
        "ORDER BY date DESC LIMIT 1",
        (ticker, from_date),
    ).fetchone()
    if not start_row:
        return None
    end_row = conn.execute(
        "SELECT price FROM prices WHERE symbol = ? AND date > ? "
        "ORDER BY date ASC LIMIT 1 OFFSET ?",
        (ticker, start_row[0], n_days - 1),
    ).fetchone()
    if not end_row:
        return None
    return end_row[0] / start_row[1] - 1


def fill_lookahead_returns(conn: sqlite3.Connection) -> int:
    """For each rumour row whose horizon has passed but isn't scored yet,
    compute the realised return and write back."""
    today = dt.date.today()
    pending = conn.execute(
        """SELECT id, date, ticker, realized_return_5d, realized_return_21d
           FROM rumour_signals
           WHERE realized_return_5d IS NULL OR realized_return_21d IS NULL"""
    ).fetchall()
    scored = 0
    now_iso = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    for row_id, d, ticker, r5, r21 in pending:
        rumour_date = dt.date.fromisoformat(d)
        days_old = (today - rumour_date).days
        new_r5 = r5
        new_r21 = r21
        # 5d look-ahead with calendar-day slack (1.4x).
        if r5 is None and days_old >= int(LOOKAHEAD_5D * 1.4) + 1:
            new_r5 = realized_return(conn, ticker, d, LOOKAHEAD_5D)
        if r21 is None and days_old >= int(LOOKAHEAD_21D * 1.4) + 1:
            new_r21 = realized_return(conn, ticker, d, LOOKAHEAD_21D)
        if (new_r5, new_r21) != (r5, r21):
            conn.execute(
                """UPDATE rumour_signals
                   SET realized_return_5d = ?, realized_return_21d = ?, scored_at = ?
                   WHERE id = ?""",
                (new_r5, new_r21, now_iso, row_id),
            )
            scored += 1
    return scored


def main():
    conn = connect_writable(DB_PATH)
    try:
        daily = collapse_daily(conn)
        if daily.empty:
            log("No social_mentions yet; skipping.", module="rumours")
            return
        zs = compute_zscores(daily)
        upserted = upsert_rumours(conn, zs)
        conn.commit()
        log(f"upserted {upserted} (date,ticker) rows.", module="rumours")
        scored = fill_lookahead_returns(conn)
        conn.commit()
        log(f"scored {scored} matured rows.", module="rumours")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
