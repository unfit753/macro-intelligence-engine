"""Fetch OFAC sanctions lists into a lightweight macro watch table.

This is not a compliance screening engine. It keeps a macro-oriented view of
who/where is sanctioned so frontend clients and later prompt layers can reason
about country/program pressure. OFAC list rows usually do not specify a traded
product directly, so `product` is best-effort and often blank.
"""
from __future__ import annotations

import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, UTC

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


SOURCES = (
    ("OFAC", "SDN", "https://www.treasury.gov/ofac/downloads/sdn.xml"),
    ("OFAC", "Consolidated non-SDN", "https://www.treasury.gov/ofac/downloads/consolidated/consolidated.xml"),
)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _children(node: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in node if _local(child.tag) == name]


def _child_text(node: ET.Element, name: str) -> str:
    for child in _children(node, name):
        if child.text:
            return child.text.strip()
    return ""


def _desc_text(node: ET.Element, path: tuple[str, ...]) -> list[str]:
    current = [node]
    for part in path:
        nxt: list[ET.Element] = []
        for item in current:
            nxt.extend(_children(item, part))
        current = nxt
    return [item.text.strip() for item in current if item.text and item.text.strip()]


def _entity_name(entry: ET.Element) -> str:
    last = _child_text(entry, "lastName")
    first = _child_text(entry, "firstName")
    name = " ".join(part for part in (first, last) if part)
    return name or last or first or "unknown"


def _countries(entry: ET.Element) -> list[str]:
    countries = set(_desc_text(entry, ("addressList", "address", "country")))
    countries.update(_desc_text(entry, ("nationalityList", "nationality", "country")))
    countries.update(_desc_text(entry, ("citizenshipList", "citizenship", "country")))
    return sorted(countries) or [""]


def _product_hint(program: str) -> str:
    p = program.upper()
    if any(token in p for token in ("OIL", "PETRO", "ENERGY")):
        return "energy/oil"
    if any(token in p for token in ("DIAMOND", "GOLD", "MINING", "METALS")):
        return "commodities"
    if any(token in p for token in ("BANK", "FINANC", "DEBT")):
        return "financial services"
    if any(token in p for token in ("CYBER", "TECH", "SEMICONDUCTOR")):
        return "technology"
    if any(token in p for token in ("ARMS", "WEAPON", "MILITARY")):
        return "arms/defense"
    return ""


def fetch_xml(url: str) -> ET.Element:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return ET.fromstring(r.content)


def parse(source: str, list_name: str, root: ET.Element, url: str) -> list[tuple]:
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    rows = []
    for entry in root.iter():
        if _local(entry.tag) != "sdnEntry":
            continue
        name = _entity_name(entry)
        entity_type = _child_text(entry, "sdnType") or _child_text(entry, "type")
        programs = _desc_text(entry, ("programList", "program")) or [""]
        countries = _countries(entry)
        for program in programs:
            product = _product_hint(program)
            for country in countries:
                rows.append((
                    source, list_name, program, name, entity_type, country,
                    "person/entity/vessel", product,
                    "listing/blocking or restriction; product detail often not specified by source",
                    url, fetched_at,
                ))
    return rows


def store(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    before = conn.total_changes
    conn.executemany(
        """INSERT OR REPLACE INTO sanctions
           (source, list_name, program, entity_name, entity_type, country,
            target_type, product, measure, url, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return conn.total_changes - before


def main() -> None:
    conn = connect_writable(DB_PATH)
    total = 0
    try:
        for source, list_name, url in SOURCES:
            try:
                log(f"Fetching {source} {list_name} sanctions XML...", module="sanctions")
                rows = parse(source, list_name, fetch_xml(url), url)
                inserted = store(conn, rows)
                conn.commit()
                total += inserted
                log(f"{list_name}: upserted {inserted} rows ({len(rows)} parsed).", module="sanctions")
            except Exception as e:
                log(f"Failed {source} {list_name}: {e}", module="sanctions")
        log(f"Done. Total upserts: {total}", module="sanctions")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
