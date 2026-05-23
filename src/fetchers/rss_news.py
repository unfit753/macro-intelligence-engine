"""Fetch a simple macro-news RSS tape for downstream reading.

Layer-1 only. These items are explicitly marked `used_for_predictions=0`; the
prediction path continues to use cleaner structured feeds unless we promote a
source later.
"""
from __future__ import annotations

import email.utils
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, UTC

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


FEEDS: tuple[tuple[str, str, str, str], ...] = (
    ("Federal Reserve", "US", "central_bank", "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("ECB", "EU", "central_bank", "https://www.ecb.europa.eu/rss/press.html"),
    ("IMF", "Global", "macro", "https://www.imf.org/en/News/RSS?language=eng"),
    ("WTO", "Global", "trade", "https://www.wto.org/library/rss/latest_news_e.xml"),
)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _child_text(node: ET.Element, name: str) -> str:
    for child in node:
        if _local(child.tag) == name and child.text:
            return child.text.strip()
    return ""


def _link(node: ET.Element) -> str:
    direct = _child_text(node, "link")
    if direct:
        return direct
    for child in node:
        if _local(child.tag) == "link":
            href = child.attrib.get("href")
            if href:
                return href
    return ""


def _published(node: ET.Element) -> str:
    raw = (
        _child_text(node, "pubDate")
        or _child_text(node, "published")
        or _child_text(node, "updated")
    )
    if not raw:
        return ""
    try:
        parsed = email.utils.parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat(timespec="seconds")
    except (TypeError, ValueError):
        return raw[:32]


def fetch_feed(source: str, region: str, category: str, url: str) -> list[tuple]:
    r = requests.get(url, timeout=30, headers={"User-Agent": "makroanalys/1.0"})
    r.raise_for_status()
    root = ET.fromstring(r.content)
    items = [node for node in root.iter() if _local(node.tag) in ("item", "entry")]
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    rows = []
    for item in items[:50]:
        title = _child_text(item, "title")
        link = _link(item)
        if not title or not link:
            continue
        summary = _child_text(item, "description") or _child_text(item, "summary")
        rows.append((
            _published(item), source, title, summary, link,
            region, category, 0, fetched_at,
        ))
    return rows


def store(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    return upsert_many(
        conn,
        "news_items",
        (
            "published_at", "source", "title", "summary", "url", "region",
            "category", "used_for_predictions", "fetched_at",
        ),
        ("url",),
        rows,
        update_columns=(
            "published_at", "source", "title", "summary", "region",
            "category", "used_for_predictions", "fetched_at",
        ),
    )


def main() -> None:
    conn = connect_writable(DB_PATH)
    total = 0
    try:
        for source, region, category, url in FEEDS:
            try:
                rows = fetch_feed(source, region, category, url)
                inserted = store(conn, rows)
                conn.commit()
                total += inserted
                log(f"{source}: +{inserted} news items ({len(rows)} fetched).", module="rss_news")
            except Exception as e:
                log(f"{source}: failed: {e}", module="rss_news")
        log(f"Done. Total new rows: {total}", module="rss_news")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
