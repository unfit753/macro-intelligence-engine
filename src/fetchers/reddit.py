"""Reddit ticker mentions across investing subreddits.

Pulls the recent posts from a curated set of subreddits via the public
JSON endpoints (no OAuth, just a polite User-Agent and rate limiting).
Counts $TICKER and bare-ticker mentions per day, plus a tiny VADER-style
sentiment proxy from the score and a small bullish/bearish word list.

Layer-1 only: writes to social_mentions; correlation analysis vs price
moves and rumour/fact matching live in Layer 2.
"""
from __future__ import annotations

import re
import sqlite3
import time
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


SUBREDDITS = ["wallstreetbets", "stocks", "investing", "options", "StockMarket"]
LIMIT_PER_SUB = 100  # posts per fetch
USER_AGENT = os.getenv("REDDIT_USER_AGENT", "macro-intelligence-engine/0.1")

# Tickers we care about — pulled from the targets table at runtime, plus
# this list of common-but-controversial mega-caps to keep the social
# panel useful even when no one is talking about XLK.
ALWAYS_TRACK = {
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA",
    "AMD", "NFLX", "BA", "PLTR", "GME", "AMC", "COIN", "RIVN",
    "DIS", "JPM", "BAC", "WFC", "GS",
}

# Crude lexicon for sentiment proxy. Real NLP is layer 2.
BULLISH = {"buy", "long", "calls", "moon", "bullish", "pump", "rally", "rip", "rocket", "yolo"}
BEARISH = {"sell", "short", "puts", "tank", "bearish", "dump", "crash", "drop", "fade", "rug"}

TICKER_RE = re.compile(r"\$([A-Z]{1,5})\b|\b([A-Z]{2,5})\b")


def fetch_subreddit(sub: str) -> list[dict]:
    url = f"https://www.reddit.com/r/{sub}/new.json?limit={LIMIT_PER_SUB}"
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
    r.raise_for_status()
    return [c["data"] for c in r.json().get("data", {}).get("children", [])]


def extract_tickers(text: str, valid: set[str]) -> list[str]:
    out = []
    for m in TICKER_RE.finditer(text or ""):
        sym = m.group(1) or m.group(2)
        if sym in valid:
            out.append(sym)
    return out


def crude_sentiment(text: str) -> float | None:
    """Returns +1 for bullish-leaning, -1 for bearish, ~0 for neutral, None for empty."""
    words = re.findall(r"[A-Za-z]+", (text or "").lower())
    if not words:
        return None
    bull = sum(1 for w in words if w in BULLISH)
    bear = sum(1 for w in words if w in BEARISH)
    if bull == bear == 0:
        return 0.0
    return (bull - bear) / max(bull + bear, 1)


def load_tracked_tickers(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT DISTINCT symbol FROM targets WHERE active = 1").fetchall()
    out = set(ALWAYS_TRACK)
    for (s,) in rows:
        # Strip yfinance suffixes / prefixes that don't appear in casual text
        if s.startswith("^") or "=" in s or "-" in s:
            continue
        out.add(s.upper())
    return out


def aggregate(posts: list[dict], source: str, valid: set[str]) -> dict[tuple[str, str], dict]:
    """key = (date, ticker), value = {count, sent_sum, top_score, top_title}"""
    out: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"count": 0, "sent_sum": 0.0, "sent_n": 0,
                 "top_score": -1, "top_title": ""}
    )
    for p in posts:
        ts = p.get("created_utc")
        if not ts:
            continue
        d = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        text = (p.get("title") or "") + " " + (p.get("selftext") or "")
        tickers = extract_tickers(text, valid)
        if not tickers:
            continue
        sentiment = crude_sentiment(text)
        score = int(p.get("score") or 0)
        title = p.get("title") or ""
        for tk in set(tickers):  # one per post per ticker
            agg = out[(d, tk)]
            agg["count"] += 1
            if sentiment is not None:
                agg["sent_sum"] += sentiment
                agg["sent_n"] += 1
            if score > agg["top_score"]:
                agg["top_score"] = score
                agg["top_title"] = title[:240]
    return out


def store(conn: sqlite3.Connection, source: str, agg: dict) -> int:
    rows = []
    for (d, tk), v in agg.items():
        sent = v["sent_sum"] / v["sent_n"] if v["sent_n"] else None
        rows.append((d, source, tk, v["count"], sent,
                     v["top_title"] or None,
                     v["top_score"] if v["top_score"] >= 0 else None))
    before = conn.total_changes
    # Upsert: re-running same day folds new mentions into the existing row.
    conn.executemany(
        """INSERT INTO social_mentions
           (date, source, ticker, mention_count, sentiment_score,
            top_post_title, top_post_score)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(date, source, ticker) DO UPDATE SET
             mention_count   = excluded.mention_count,
             sentiment_score = excluded.sentiment_score,
             top_post_title  = excluded.top_post_title,
             top_post_score  = excluded.top_post_score""",
        rows,
    )
    return conn.total_changes - before


def main():
    conn = connect_writable(DB_PATH)
    try:
        valid = load_tracked_tickers(conn)
        log(f"Tracking {len(valid)} tickers across r/{', r/'.join(SUBREDDITS)}",
            module="reddit")
        total = 0
        for sub in SUBREDDITS:
            try:
                posts = fetch_subreddit(sub)
                agg = aggregate(posts, f"reddit:{sub}", valid)
                inserted = store(conn, f"reddit:{sub}", agg)
                conn.commit()
                log(f"r/{sub}: {len(posts)} posts -> {len(agg)} (date,ticker) pairs "
                    f"-> +{inserted} rows", module="reddit")
                total += inserted
                time.sleep(1.5)  # polite pacing — Reddit's public JSON is rate-limited
            except Exception as e:
                log(f"r/{sub}: failed {e}", module="reddit")
        log(f"Done. +{total} rows.", module="reddit")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
