# archive/

Fetchers parked during phase 1 triage (2026-05-08).

- `fetch_scb.py` — recursive crawl over 6 SCB subject areas with hardcoded `Region=00`/`Kon=1+2`/`Alder=20-74` dimensions; every leaf returns 400. Needs a redesign with curated tables and per-table queries before it can come back.
- `historic_gold.py` — pulled the LBMA AM fix series from FRED; series ID `GOLDAMGBD228NLBM` was discontinued (404). Replace with WGC, LBMA direct, or Bundesbank if pre-2000 gold history becomes important.
