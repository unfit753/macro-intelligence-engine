"""SEC Form 4 (insider transactions) for a curated set of tickers.

Uses EDGAR's per-company Atom feed to find recent Form 4 filings, then
fetches each filing's XBRL XML and pulls out the non-derivative
transactions (codes P=open-market purchase, S=open-market sale, etc.).

SEC's polite-use rules: identify yourself in the User-Agent and stay
under 10 requests/second. We sleep 0.15s between calls.

Layer-1 only: writes to insider_trades. Higher-level "Pelosi-style
unusual activity" detection is layer 2.
"""
from __future__ import annotations

import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
import os
from urllib.parse import urljoin

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many


SEC_UA = os.getenv("SEC_USER_AGENT", "macro-intelligence-engine/0.1 (contact@example.com)")
SEC_BASE = "https://www.sec.gov"

# (ticker, CIK as zero-padded 10-digit string). CIKs cross-checked against
# https://www.sec.gov/files/company_tickers.json as of 2026.
WATCH: list[tuple[str, str]] = [
    ("AAPL",  "0000320193"),
    ("MSFT",  "0000789019"),
    ("GOOGL", "0001652044"),
    ("AMZN",  "0001018724"),
    ("META",  "0001326801"),
    ("NVDA",  "0001045810"),
    ("TSLA",  "0001318605"),
    ("AMD",   "0000002488"),
    ("NFLX",  "0001065280"),
    ("BA",    "0000012927"),
    ("PLTR",  "0001321655"),
    ("GME",   "0001326380"),
    ("AMC",   "0001411579"),
    ("COIN",  "0001679788"),
    ("DIS",   "0001744489"),
    ("JPM",   "0000019617"),
    ("BAC",   "0000070858"),
    ("WFC",   "0000072971"),
    ("GS",    "0000886982"),
    ("XOM",   "0000034088"),
    ("CVX",   "0000093410"),
    ("INTC",  "0000050863"),
    ("CSCO",  "0000858877"),
    ("ORCL",  "0001341439"),
    ("WMT",   "0000104169"),
    ("HD",    "0000354950"),
    ("PG",    "0000080424"),
    ("KO",    "0000021344"),
    ("PEP",   "0000077476"),
    ("MA",    "0001141391"),
    ("V",     "0001403161"),
]

ATOM_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}"
    "&type=4&dateb=&owner=include&count=40&output=atom"
)
NS_ATOM = {"a": "http://www.w3.org/2005/Atom"}


def _get(url: str) -> requests.Response:
    r = requests.get(url, headers={"User-Agent": SEC_UA, "Accept-Encoding": "gzip"}, timeout=30)
    r.raise_for_status()
    time.sleep(0.15)  # SEC: <= 10 req/sec
    return r


def list_recent_filings(cik: str) -> list[dict]:
    """Return [{accession, filed, index_url}] for recent Form 4 filings."""
    r = _get(ATOM_URL.format(cik=cik))
    root = ET.fromstring(r.content)
    out = []
    for entry in root.findall("a:entry", NS_ATOM):
        acc_el = entry.find("a:content/a:accession-number", NS_ATOM)
        if acc_el is None:
            # Newer feed format: accession in <id> URL
            link = entry.find("a:link", NS_ATOM)
            href = link.attrib.get("href") if link is not None else ""
            m = re.search(r"(\d{10}-\d{2}-\d{6})", href)
            acc = m.group(1) if m else None
        else:
            acc = acc_el.text
        filed_el = entry.find("a:updated", NS_ATOM)
        link = entry.find("a:link", NS_ATOM)
        if not acc or filed_el is None or link is None:
            continue
        out.append({
            "accession": acc,
            "filed": filed_el.text[:10],
            "index_url": link.attrib["href"],
        })
    return out


def find_xml_url(index_url: str) -> str | None:
    """Index page lists files; find the raw Form 4 XML.

    EDGAR exposes the same document under two URLs:
      .../000.../xslF345X06/form4.xml  (rendered HTML via XSLT)
      .../000.../wf-form4_<ts>.xml     (raw XML, what we want)
    Skip anything under /xsl/ paths.
    """
    r = _get(index_url)
    candidates: list[str] = []
    for m in re.finditer(r'href="(/Archives/edgar/data/[^"]+\.xml)"', r.text):
        url = urljoin(SEC_BASE, m.group(1))
        low = url.lower()
        if "/xsl" in low:
            continue  # rendered view, not raw XML
        if low.endswith("/feed.xml") or "/feed-" in low:
            continue  # listing feeds aren't filings
        candidates.append(url)
    # Prefer ones with form4/doc4/primary_doc in the name.
    for url in candidates:
        low = url.lower()
        if "form4" in low or "doc4" in low or "primary_doc" in low:
            return url
    return candidates[0] if candidates else None


def parse_form4(xml_bytes: bytes, ticker: str, accession: str) -> list[tuple]:
    """Return rows for insider_trades. Skips derivative table."""
    rows: list[tuple] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return rows

    issuer = root.find("issuer")
    company = (issuer.findtext("issuerName") or "") if issuer is not None else ""

    owner = root.find("reportingOwner")
    insider_name = ""
    insider_role = ""
    if owner is not None:
        oid = owner.find("reportingOwnerId")
        if oid is not None:
            insider_name = oid.findtext("rptOwnerName") or ""
        rel = owner.find("reportingOwnerRelationship")
        if rel is not None:
            roles = []
            if (rel.findtext("isOfficer") or "0") == "1":
                title = rel.findtext("officerTitle") or "Officer"
                roles.append(title)
            if (rel.findtext("isDirector") or "0") == "1":
                roles.append("Director")
            if (rel.findtext("isTenPercentOwner") or "0") == "1":
                roles.append("10% Owner")
            insider_role = ", ".join(roles)

    table = root.find("nonDerivativeTable")
    if table is None:
        return rows
    for tx in table.findall("nonDerivativeTransaction"):
        tx_date = tx.findtext("transactionDate/value") or None
        coding = tx.find("transactionCoding")
        code = coding.findtext("transactionCode") if coding is not None else None
        amounts = tx.find("transactionAmounts")
        shares = price = value_usd = None
        if amounts is not None:
            try:
                shares = float(amounts.findtext("transactionShares/value") or "")
            except (ValueError, TypeError):
                shares = None
            try:
                price = float(amounts.findtext("transactionPricePerShare/value") or "")
            except (ValueError, TypeError):
                price = None
            if shares is not None and price is not None:
                value_usd = shares * price
        rows.append((
            tx_date,           # transaction_date
            ticker,            # ticker
            company,           # company
            insider_name,      # insider_name
            insider_role,      # insider_role
            code,              # transaction_type
            shares,            # shares
            price,             # price
            value_usd,         # value_usd
            accession,         # accession_number
        ))
    return rows


def already_seen(conn: sqlite3.Connection, accession: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM insider_trades WHERE accession_number = ? LIMIT 1",
        (accession,),
    ).fetchone() is not None


def store(conn: sqlite3.Connection, filing_date: str, url: str,
          rows: list[tuple]) -> int:
    if not rows:
        return 0
    full = [
        (filing_date, *r, url)  # prepend filing_date, append url
        for r in rows
    ]
    return upsert_many(
        conn,
        "insider_trades",
        (
            "filing_date", "transaction_date", "ticker", "company",
            "insider_name", "insider_role", "transaction_type", "shares",
            "price", "value_usd", "accession_number", "url",
        ),
        ("accession_number", "insider_name", "ticker", "transaction_type"),
        full,
        update_columns=(
            "filing_date", "transaction_date", "company", "insider_role",
            "shares", "price", "value_usd", "url",
        ),
    )


def main(per_ticker_limit: int = 10):
    conn = connect_writable(DB_PATH)
    total = 0
    try:
        for ticker, cik in WATCH:
            try:
                filings = list_recent_filings(cik)[:per_ticker_limit]
            except Exception as e:
                log(f"{ticker}: list failed {e}", module="edgar")
                continue
            new_filings = [f for f in filings if not already_seen(conn, f["accession"])]
            if not new_filings:
                log(f"{ticker}: up to date.", module="edgar")
                continue
            log(f"{ticker}: {len(new_filings)} new filing(s)", module="edgar")
            for f in new_filings:
                try:
                    xml_url = find_xml_url(f["index_url"])
                    if not xml_url:
                        continue
                    xml = _get(xml_url).content
                    rows = parse_form4(xml, ticker, f["accession"])
                    inserted = store(conn, f["filed"], xml_url, rows)
                    conn.commit()
                    total += inserted
                except Exception as e:
                    log(f"{ticker} {f['accession']}: parse failed {e}", module="edgar")
        log(f"Done. +{total} insider transaction rows.", module="edgar")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
