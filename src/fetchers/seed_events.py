"""Hand-curated regime-defining events spanning 1971–2024.

This is the corpus the analogue-retrieval layer uses to find historical
parallels. Quality > quantity — every entry should have moved at least
one of {gold, oil, equities, bonds} in a way that would still be
informative as a comparison point today.

Re-runnable; UNIQUE(date, title) prevents duplicates.
"""
import sqlite3

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable


# (date, title, country, type, source, url)
EVENTS: list[tuple[str, str, str, str, str, str]] = [
    # ── Bretton Woods + 70s stagflation ──────────────────────────────
    ("1971-08-15", "Nixon ends gold convertibility (Nixon Shock)",          "US", "monetary",   "seed", ""),
    ("1973-10-06", "Yom Kippur war begins; OPEC oil embargo follows",       "WW", "conflict",   "seed", ""),
    ("1979-08-06", "Paul Volcker becomes Fed chair",                        "US", "monetary",   "seed", ""),
    ("1979-11-04", "Iran hostage crisis begins",                            "IR", "conflict",   "seed", ""),
    ("1980-03-01", "Fed Funds peaks near 20% in Volcker disinflation",      "US", "monetary",   "seed", ""),

    # ── 80s ──────────────────────────────────────────────────────────
    ("1982-08-12", "Mexican debt default; LDC debt crisis erupts",          "MX", "crisis",     "seed", ""),
    ("1985-09-22", "Plaza Accord signed (USD coordinated devaluation)",     "WW", "monetary",   "seed", ""),
    ("1987-10-19", "Black Monday crash (-22.6% Dow)",                       "US", "crisis",     "seed", ""),
    ("1989-11-09", "Berlin Wall falls",                                     "DE", "political",  "seed", ""),
    ("1990-08-02", "Iraq invades Kuwait; oil spikes",                       "IQ", "conflict",   "seed", ""),

    # ── 90s ──────────────────────────────────────────────────────────
    ("1994-02-04", "Greenspan begins surprise hiking cycle; bond rout",     "US", "monetary",   "seed", ""),
    ("1997-07-02", "Thai baht devalued; Asian financial crisis begins",     "TH", "crisis",     "seed", ""),
    ("1998-08-17", "Russian sovereign default",                             "RU", "crisis",     "seed", ""),
    ("1998-09-23", "LTCM bailout coordinated by NY Fed",                    "US", "crisis",     "seed", ""),
    ("2000-03-10", "Nasdaq dot-com peak",                                   "US", "regime",     "seed", ""),

    # ── 2000s ────────────────────────────────────────────────────────
    ("2001-09-11", "9/11 attacks",                                          "US", "conflict",   "seed", ""),
    ("2003-03-20", "US-led invasion of Iraq begins",                        "IQ", "conflict",   "seed", ""),
    ("2007-08-09", "BNP Paribas freezes funds; subprime crisis goes global","FR", "crisis",     "seed", ""),
    ("2008-03-16", "Bear Stearns sold to JPMorgan at $2/share",             "US", "crisis",     "seed", ""),
    ("2008-09-15", "Lehman Brothers files Chapter 11",                      "US", "crisis",     "seed", ""),
    ("2008-10-03", "TARP signed into law ($700B bank bailout)",             "US", "monetary",   "seed", ""),
    ("2008-11-25", "Fed announces QE1",                                     "US", "monetary",   "seed", ""),

    # ── 2010s ────────────────────────────────────────────────────────
    ("2010-05-09", "EU/IMF Greek bailout; EFSF created",                    "GR", "crisis",     "seed", ""),
    ("2011-08-05", "S&P downgrades US sovereign rating",                    "US", "monetary",   "seed", ""),
    ("2012-07-26", "Draghi 'whatever it takes' speech ends EU debt crisis", "EU", "monetary",   "seed", ""),
    ("2013-05-22", "Bernanke taper tantrum",                                "US", "monetary",   "seed", ""),
    ("2014-06-19", "OPEC stands pat; oil collapse begins",                  "WW", "regime",     "seed", ""),
    ("2015-08-11", "China devalues RMB; global risk-off",                   "CN", "monetary",   "seed", ""),
    ("2015-12-16", "Fed hikes for first time since 2006",                   "US", "monetary",   "seed", ""),
    ("2016-06-23", "UK Brexit referendum",                                  "GB", "political",  "seed", ""),
    ("2016-11-08", "Trump elected US president",                            "US", "political",  "seed", ""),
    ("2018-02-05", "Volmageddon (XIV blowup)",                              "US", "crisis",     "seed", ""),
    ("2018-12-24", "Christmas Eve equity drawdown bottom",                  "US", "regime",     "seed", ""),

    # ── 2020s ────────────────────────────────────────────────────────
    ("2020-03-09", "COVID crash begins; circuit breakers triggered",        "WW", "crisis",     "seed", ""),
    ("2020-03-23", "Fed announces unlimited QE",                            "US", "monetary",   "seed", ""),
    ("2021-11-30", "Powell retires 'transitory'; hawkish pivot",            "US", "monetary",   "seed", ""),
    ("2022-02-24", "Russia invades Ukraine",                                "UA", "conflict",   "seed", ""),
    ("2022-03-16", "Fed begins fastest hiking cycle in 40 years",           "US", "monetary",   "seed", ""),
    ("2022-09-23", "UK gilt crisis (Truss budget)",                         "GB", "crisis",     "seed", ""),
    ("2023-03-10", "Silicon Valley Bank collapses",                         "US", "crisis",     "seed", ""),
    ("2023-03-19", "Credit Suisse rescued by UBS",                          "CH", "crisis",     "seed", ""),
    ("2023-10-07", "Israel-Hamas war begins",                               "IL", "conflict",   "seed", ""),
    ("2024-09-18", "Fed begins cutting cycle (50bp)",                       "US", "monetary",   "seed", ""),
]


def main():
    conn = connect_writable(DB_PATH)
    before = conn.total_changes
    conn.executemany(
        """INSERT OR IGNORE INTO events
           (date, title, country, type, source, url)
           VALUES (?, ?, ?, ?, ?, ?)""",
        EVENTS,
    )
    conn.commit()
    inserted = conn.total_changes - before
    log(f"Seed events: +{inserted} (of {len(EVENTS)} total).", module="events")
    conn.close()


if __name__ == "__main__":
    main()
