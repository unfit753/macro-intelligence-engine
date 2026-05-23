"""Quarterly + annual financial facts from SEC EDGAR companyfacts XBRL.

For each tracked company (the WATCH list shared with edgar_form4), fetch
the consolidated XBRL companyfacts JSON, extract a curated set of
us-gaap concepts (revenue, net income, operating income, gross profit,
assets, liabilities, equity, cash, debt), and write them to the
company_fundamentals table.

Each concept observation arrives with start/end dates, the source form
(10-K / 10-Q), accession number, and the filing date — we keep all of
that for traceability and so frontend clients can render trend lines per
period.

The companyfacts endpoint serves the entire history in one JSON file
(typically 1-5 MB per company), so cold-start and incremental are the
same operation: re-fetch and INSERT OR IGNORE on the natural key.
"""
from __future__ import annotations

import sqlite3
import time
from typing import Iterable

import requests

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, upsert_many
from src.fetchers.edgar_form4 import WATCH, SEC_UA


COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Concepts we extract. Multiple US-GAAP concepts can refer to the same
# real-world line item across different accounting periods or industries.
# We capture all of them and let clients pick the most-populated.
CONCEPTS_BY_LABEL: dict[str, list[str]] = {
    "Revenue": [
        "RevenueFromContractWithCustomerExcludedAssessedTax",  # ASC 606, modern
        "Revenues",                                            # legacy
        "SalesRevenueNet",                                     # older legacy
    ],
    "NetIncome":         ["NetIncomeLoss"],
    "OperatingIncome":   ["OperatingIncomeLoss"],
    "GrossProfit":       ["GrossProfit"],
    "Assets":            ["Assets"],
    "Liabilities":       ["Liabilities"],
    "StockholdersEquity":["StockholdersEquity"],
    "Cash":              ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"],
    "LongTermDebt":      ["LongTermDebt", "LongTermDebtNoncurrent"],
    "EPS":               ["EarningsPerShareDiluted", "EarningsPerShareBasic"],
}


def fetch_companyfacts(cik: str) -> dict | None:
    r = requests.get(
        COMPANYFACTS_URL.format(cik=cik),
        headers={"User-Agent": SEC_UA, "Accept-Encoding": "gzip"},
        timeout=60,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    time.sleep(0.15)
    return r.json()


def period_type(form: str | None, fp: str | None) -> str:
    if not form:
        return "unknown"
    if form == "10-K":
        return "annual"
    if form == "10-Q":
        return "quarterly"
    return form.lower()


def iter_observations(facts: dict, ticker: str) -> Iterable[tuple]:
    """Yield rows for company_fundamentals: only 10-K / 10-Q observations,
    only the unit (USD or shares) with the most observations per concept."""
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    for label, concept_candidates in CONCEPTS_BY_LABEL.items():
        for concept in concept_candidates:
            entry = us_gaap.get(concept)
            if not entry:
                continue
            units = entry.get("units", {})
            # Pick the unit with the most observations (USD usually wins;
            # EPS uses USD/shares).
            best_unit = max(units, key=lambda u: len(units[u]), default=None)
            if best_unit is None:
                continue
            for obs in units[best_unit]:
                form = obs.get("form")
                if form not in ("10-K", "10-Q"):
                    continue
                end = obs.get("end")
                if not end:
                    continue
                yield (
                    ticker, end, period_type(form, obs.get("fp")),
                    label,
                    float(obs["val"]) if obs.get("val") is not None else None,
                    best_unit, form, obs.get("accn"), obs.get("filed"),
                )
            break  # took the first concept that had data; don't double-count


def store(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    return upsert_many(
        conn,
        "company_fundamentals",
        (
            "ticker", "period_end", "period_type", "concept", "value", "unit",
            "form", "accession_number", "filed",
        ),
        ("ticker", "period_end", "concept", "form"),
        rows,
        update_columns=("period_type", "value", "unit", "accession_number", "filed"),
    )


def main():
    conn = connect_writable(DB_PATH)
    total = 0
    try:
        for ticker, cik in WATCH:
            try:
                facts = fetch_companyfacts(cik)
                if facts is None:
                    log(f"{ticker}: no companyfacts (404).", module="edgar_facts")
                    continue
                rows = [r for r in iter_observations(facts, ticker)
                        if r[4] is not None]
                inserted = store(conn, rows)
                conn.commit()
                log(f"{ticker}: {len(rows)} observations -> +{inserted} rows.",
                    module="edgar_facts")
                total += inserted
            except Exception as e:
                log(f"{ticker}: failed {e}", module="edgar_facts")
        log(f"Done. +{total} fundamentals rows.", module="edgar_facts")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
