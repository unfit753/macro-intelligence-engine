"""ACLED (Armed Conflict Location & Event Data) fetcher.

Free with registration at https://acleddata.com/user/register (use an
institutional email if possible — confers higher rate limits). Then add
to .env:

    ACLED_USERNAME=
    ACLED_PASSWORD=

The fetcher gracefully skips with a log message if those env vars are
missing, so the daily cron stays green until you finish registering.

Filter: fatalities >= MIN_FATALITIES (default 25), event_date >= cutoff.
This keeps the events table focused on high-impact violence rather than
flooding it with thousands of small protests/clashes per day. Tune the
threshold downwards if you want broader coverage.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime, timedelta

import requests
from dotenv import load_dotenv

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


OAUTH_URL = "https://acleddata.com/oauth/token"
API_URL = "https://acleddata.com/api/acled/read"
PAGE_SIZE = 5000
MIN_FATALITIES = 25
COLD_START_DAYS = 365


def get_token() -> str | None:
    load_dotenv(override=True)
    user = os.getenv("ACLED_USERNAME")
    pwd = os.getenv("ACLED_PASSWORD")
    if not user or not pwd:
        log("ACLED_USERNAME / ACLED_PASSWORD not set in .env; skipping. "
            "Register at acleddata.com/user/register to enable this feed.",
            module="acled")
        return None
    r = requests.post(
        OAUTH_URL,
        data={
            "username": user, "password": pwd,
            "grant_type": "password",
            "client_id": "acled",
            "scope": "authenticated",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def fetch_since(token: str, since: date) -> list[dict]:
    """Pull all events with date >= since and fatalities >= MIN_FATALITIES."""
    headers = {"Authorization": f"Bearer {token}"}
    page = 1
    out: list[dict] = []
    while True:
        params = {
            "_format": "json",
            "limit": PAGE_SIZE,
            "page": page,
            "fatalities_where": "GTE",
            "fatalities": str(MIN_FATALITIES),
            "event_date_where": "GTE",
            "event_date": since.isoformat(),
        }
        r = requests.get(API_URL, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        body = r.json()
        batch = body.get("data") or []
        out.extend(batch)
        log(f"ACLED page {page}: +{len(batch)} (running total {len(out)})",
            module="acled")
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    return out


def title_for(ev: dict) -> str:
    actor = (ev.get("actor1") or "?")[:60]
    etype = ev.get("event_type") or "?"
    country = ev.get("country") or ""
    fatal = ev.get("fatalities")
    where = f" in {country}" if country else ""
    fatal_part = f" ({fatal} fatalities)" if fatal else ""
    return f"[ACLED] {etype}: {actor}{where}{fatal_part}"[:200]


def store(conn: sqlite3.Connection, events: list[dict]) -> int:
    rows = []
    for ev in events:
        d = ev.get("event_date")
        if not d:
            continue
        rows.append((
            d, title_for(ev),
            ev.get("country") or "",
            ev.get("event_type") or "",
            "acled",
            (ev.get("source") or "")[:512],
        ))
    before = conn.total_changes
    conn.executemany(
        """INSERT OR IGNORE INTO events
           (date, title, country, type, source, url)
           VALUES (?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return conn.total_changes - before


def last_acled_date(conn: sqlite3.Connection) -> date | None:
    row = conn.execute(
        "SELECT MAX(date) FROM events WHERE source = 'acled'"
    ).fetchone()
    if row and row[0]:
        return datetime.strptime(row[0], "%Y-%m-%d").date()
    return None


def main():
    token = get_token()
    if token is None:
        return  # creds missing, already logged

    conn = connect_writable(DB_PATH)
    last = last_acled_date(conn)
    since = (last + timedelta(days=1)) if last else (date.today() - timedelta(days=COLD_START_DAYS))
    log(f"ACLED fetching events with fatalities>={MIN_FATALITIES} since {since}...",
        module="acled")
    try:
        events = fetch_since(token, since)
    except requests.HTTPError as e:
        log(f"ACLED API error: {e}", module="acled")
        conn.close()
        return
    inserted = store(conn, events)
    conn.commit()
    log(f"Done. fetched={len(events)} +{inserted} new rows.", module="acled")
    conn.close()


if __name__ == "__main__":
    main()
