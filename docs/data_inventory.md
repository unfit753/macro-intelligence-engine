# Data Inventory

Snapshot date: 2026-05-22.

Macro Intelligence Engine is built around public and optional-public data feeds. The live
runtime database is intentionally not committed to the repository.

## Core Stores

- `prices`: asset price history.
- `indicators`: macro indicators from official and public providers.
- `signals`: derived market, trend, volatility, and macro-regime values.
- `calendar_events`: scheduled macro catalysts.
- `macro_release_actuals`: official release outcomes linked to scheduled events.
- `current_events`: current catalyst, release, breaking-news, and source-health
  surface for downstream clients.
- `risk_hotspots`: persistent geopolitical and supply-chain watchpoints.
- `gdelt_streams` and `gdelt_stream_examples`: GDELT stream aggregates and
  representative examples.
- `news_items`: RSS/news context.
- `sanctions`: sanctions context for trade and policy overlays.
- `intelligence_packages`: compiled macro/risk packages.
- `oracle_entities`, `oracle_impacts`, `oracle_index_snapshots`: legacy-named
  map hierarchy and pressure-index tables.
- `prediction_context_packs`: bounded forecast input packs.
- `predictions`: forecast records and later scoring fields.
- `backtest_runs` and `backtest_predictions`: isolated forecast-lab runs.
- `source_runs`: ingestion and pipeline telemetry.

## Public Data Families

- Markets: ETFs, indexes, commodities, rates, FX proxies.
- Macro: inflation, labour, GDP, production, retail, trade, rates, currencies,
  debt, monetary aggregates, sentiment, and positioning.
- Events: macro calendar, actual releases, central bank decisions, energy
  inventories, and relevant high-impact news.
- Risk: GDELT streams, sanctions, disasters, conflict/security, trade friction,
  energy and commodity pressure, weather observations.
- Company context: SEC fundamentals and Form 4 insider transactions.

## Optional Feeds

Optional feeds must skip cleanly when credentials are absent:

- Anthropic API for live forecast generation.
- ACLED for conflict/event data.
- X/Twitter recent search.
- EIA API key for richer energy data.
- BLS API key for higher-volume BLS access.

## Public Demo Requirement

Before publishing a hosted demo, add one of:

- a small sanitized SQLite demo database,
- deterministic fixture loading into `data/macro_engine.db`, or
- a documented read-only sample export.

The public demo should not require access to a private database path.
