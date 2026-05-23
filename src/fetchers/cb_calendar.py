"""Central-bank rate-decision events derived from existing rate series.

Rather than scraping FOMC / ECB calendars (which have moved around and
require fragile HTML parsing), we extract decision events from the rate
data we already pull:

    Fed Funds Rate    (FRED, monthly) -> Fed events
    ECB Refi Rate     (ECB,   daily)  -> ECB Main Refinancing events
    ECB Deposit Rate  (ECB,   daily)  -> ECB Deposit Facility events

Any change >= MIN_BP_THRESHOLD (default 25bp) is emitted as a named
event. Smaller adjustments (technical operations, intra-month noise on
FedFunds monthly average) are filtered out.

The seed_events.py corpus still owns regime-defining moments (Powell's
'no longer transitory' speech, Draghi's 'whatever it takes', etc.) which
are about communication, not rate moves, and therefore can't be derived
from rate data alone.
"""
from __future__ import annotations

import sqlite3
from datetime import date

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


MIN_BP_THRESHOLD = 25  # only emit changes >= 25bp

# FRED's FEDFUNDS is the effective monthly average, not the FOMC target,
# so pre-1990 it has a lot of intra-month noise. Restrict to the modern
# policy era; the seed corpus already covers Volcker/Plaza-era moves.
MIN_DATE = "1990-01-01"


# (indicator_name, country_code, label)
SOURCES: list[tuple[str, str, str]] = [
    ("Fed Funds Rate",   "US", "Fed"),
    ("ECB Refi Rate",    "EU", "ECB Refi"),
    ("ECB Deposit Rate", "EU", "ECB Deposit"),
]


def detect_changes(conn: sqlite3.Connection, indicator_name: str,
                   country: str, label: str) -> list[tuple]:
    """Walk the indicator series; emit one event per >=threshold change."""
    rows = conn.execute(
        """SELECT date, value FROM indicators
           WHERE indicator_name = ? AND date >= ? ORDER BY date""",
        (indicator_name, MIN_DATE),
    ).fetchall()
    events: list[tuple] = []
    prev_value: float | None = None
    for d, v in rows:
        if prev_value is None:
            prev_value = v
            continue
        delta_bp = round((v - prev_value) * 100)  # 1.00 -> 100bp
        if abs(delta_bp) >= MIN_BP_THRESHOLD:
            action = "cuts" if delta_bp < 0 else "hikes"
            title = f"{label} {action} {abs(delta_bp)}bp to {v:.2f}%"
            events.append((d, title, country, "monetary", "cb_calendar", ""))
        prev_value = v
    return events


def main():
    conn = connect_writable(DB_PATH)
    total = 0
    for indicator_name, country, label in SOURCES:
        events = detect_changes(conn, indicator_name, country, label)
        if not events:
            log(f"{label}: no source data, skipping.", module="cb_calendar")
            continue
        before = conn.total_changes
        conn.executemany(
            """INSERT OR IGNORE INTO events
               (date, title, country, type, source, url)
               VALUES (?, ?, ?, ?, ?, ?)""",
            events,
        )
        conn.commit()
        inserted = conn.total_changes - before
        log(f"{label}: {len(events)} changes detected, +{inserted} new.",
            module="cb_calendar")
        total += inserted
    log(f"Done. +{total} new rows.", module="cb_calendar")
    conn.close()


if __name__ == "__main__":
    main()
