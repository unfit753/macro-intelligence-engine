"""X/Twitter macro chatter listener.

Uses the X API v2 recent-search endpoint when X_BEARER_TOKEN is present
in .env. The feed is intentionally passive: it stores popular economics
posts for inspection and later cleaning, but does not directly influence
predictions until enough history exists to separate signal from noise.
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
USER_AGENT = "makroanalys/0.1"
MAX_RESULTS = 50

TOPIC_QUERIES: dict[str, str] = {
    "inflation": '(inflation OR CPI OR "core CPI" OR disinflation) lang:en -is:retweet',
    "central_banks": '("central bank" OR Fed OR ECB OR Riksbank OR FOMC OR "rate cuts") lang:en -is:retweet',
    "rates_bonds": '("bond yields" OR treasury OR bund OR gilt OR "yield curve") lang:en -is:retweet',
    "commodities": '(gold OR oil OR Brent OR WTI OR copper OR uranium) (market OR price OR supply) lang:en -is:retweet',
    "europe_sweden": '(OMX OR Stockholm OR Sweden OR SEK OR Eurozone OR DAX OR STOXX) (market OR inflation OR rates) lang:en -is:retweet',
    "risk": '(recession OR sanctions OR tariff OR war OR shipping OR "supply chain") (market OR economy) lang:en -is:retweet',
}


def bearer_token() -> str | None:
    load_dotenv(override=True)
    token = os.getenv("X_BEARER_TOKEN") or os.getenv("TWITTER_BEARER_TOKEN")
    if not token:
        log(
            "X_BEARER_TOKEN not set in .env; skipping X/Twitter macro listener.",
            module="twitter",
        )
        return None
    return token


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS twitter_macro_posts (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            posted_at          TEXT,
            query              TEXT NOT NULL,
            topic              TEXT,
            author_username    TEXT,
            text               TEXT NOT NULL,
            like_count         INTEGER,
            repost_count       INTEGER,
            reply_count        INTEGER,
            quote_count        INTEGER,
            url                TEXT,
            fetched_at         TEXT NOT NULL DEFAULT (datetime('now'))
        )"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_twitter_macro_url ON twitter_macro_posts(url)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_twitter_macro_posted ON twitter_macro_posts(posted_at, topic)"
    )


def fetch_topic(token: str, topic: str, query: str) -> list[dict]:
    params = {
        "query": query,
        "max_results": MAX_RESULTS,
        "tweet.fields": "created_at,public_metrics,author_id,lang",
        "expansions": "author_id",
        "user.fields": "username",
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
    }
    r = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30)
    if r.status_code == 429:
        log("X/Twitter API rate limit reached; stopping this run.", module="twitter")
        return []
    r.raise_for_status()
    body = r.json()
    users = {
        u.get("id"): u.get("username")
        for u in body.get("includes", {}).get("users", [])
    }
    rows = []
    for item in body.get("data", []) or []:
        metrics = item.get("public_metrics") or {}
        username = users.get(item.get("author_id"))
        url = f"https://x.com/{username}/status/{item.get('id')}" if username else f"https://x.com/i/web/status/{item.get('id')}"
        rows.append({
            "posted_at": item.get("created_at"),
            "query": query,
            "topic": topic,
            "author_username": username,
            "text": (item.get("text") or "").replace("\x00", "").strip(),
            "like_count": metrics.get("like_count"),
            "repost_count": metrics.get("retweet_count"),
            "reply_count": metrics.get("reply_count"),
            "quote_count": metrics.get("quote_count"),
            "url": url,
        })
    return rows


def store(conn: sqlite3.Connection, rows: list[dict]) -> int:
    before = conn.total_changes
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.executemany(
        """INSERT OR IGNORE INTO twitter_macro_posts
           (posted_at, query, topic, author_username, text,
            like_count, repost_count, reply_count, quote_count, url, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                r["posted_at"], r["query"], r["topic"], r["author_username"], r["text"],
                r["like_count"], r["repost_count"], r["reply_count"], r["quote_count"],
                r["url"], fetched_at,
            )
            for r in rows
            if r.get("text")
        ],
    )
    return conn.total_changes - before


def main() -> None:
    token = bearer_token()
    if token is None:
        return

    conn = connect_writable(DB_PATH)
    try:
        ensure_table(conn)
        total = 0
        for topic, query in TOPIC_QUERIES.items():
            try:
                rows = fetch_topic(token, topic, query)
                inserted = store(conn, rows)
                conn.commit()
                total += inserted
                log(f"{topic}: fetched={len(rows)} +{inserted} new rows", module="twitter")
                time.sleep(1.0)
            except requests.HTTPError as e:
                log(f"{topic}: X/Twitter API error {e}", module="twitter")
            except Exception as e:
                log(f"{topic}: failed {e}", module="twitter")
        log(f"Done. +{total} rows.", module="twitter")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
