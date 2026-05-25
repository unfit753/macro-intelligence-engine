# Macro Intelligence Engine

Macro Intelligence Engine is an open-source backend for gathering, normalizing,
scoring, and evaluating macro and geopolitical risk intelligence.

The project is intentionally backend-first. It focuses on public data fetchers,
idempotent pipelines, source health, current-event synthesis, historical as-of
state, context packs, forecast logs, and backtest/evaluation tools. A frontend
can consume sanitized exports later, but this repository should be useful on its
own through its API boundary and terminal explorer.

**Research only. Not investment advice. Forecasts are probabilistic and may be
wrong. No suitability assessment is performed. Users are responsible for their
own decisions.**

## What This Repo Does

- Collects public macro, market, policy, news, sanctions, weather, SEC, and
  optional social/conflict feeds.
- Stores them in SQLite with idempotent fetchers and explicit source telemetry.
- Builds current-event rows for scheduled catalysts, released actuals,
  breaking market-moving news, and source warnings.
- Materializes historical as-of state separately from forward-return outcomes
  to reduce lookahead leakage.
- Builds compact context packs for forecast generation and later audit.
- Stores forecast records with input hashes, rationale, risks, confidence, and
  eventual scoring.
- Provides read-only query/API functions and a terminal explorer for inspecting
  the public data boundary.

## Terminal Explorer

The terminal explorer is a dependency-free TUI-style browser for the engine's
read-only data surfaces.

Run it against the bundled synthetic demo snapshot:

```bash
PYTHONPATH=. python3 -m src.cli.explorer --demo
```

Render one view and exit:

```bash
PYTHONPATH=. python3 -m src.cli.explorer --demo --view overview
PYTHONPATH=. python3 -m src.cli.explorer --demo --view current-events --detail 1
PYTHONPATH=. python3 -m src.cli.explorer --demo --view market-tape --json
```

Run it against a real local SQLite database:

```bash
MACRO_ENGINE_DB_PATH=data/macro_engine.db \
PYTHONPATH=. python3 -m src.cli.explorer --view source-health
```

Available views:

- `overview`
- `source-health`
- `current-events`
- `market-tape`
- `catalysts`
- `data-catalog`

The demo snapshot under `examples/demo_snapshot.json` is synthetic. It is not
fetched data, not a private database export, and not live research output.

## Repository Layout

```text
config/             DB path, logging, schema setup
examples/           small synthetic demo fixtures
src/cli/            terminal explorer and command-line surfaces
src/fetchers/       public data connectors
src/core/           read-only queries, API facade, labels, source registry
src/intelligence/   current events, context packs, historical state, pressure graph
src/signals/        market and macro signal computation
src/retrieval/      analogue index helpers
src/inference/      prompt, prediction, briefing, scoring
src/local_model/    optional local model experiments
src/backtest/       walk-forward forecast lab
data/               local DB helpers and ignored runtime data
tests/              unit tests for queries, events, scoring, compliance, CLI
```

Some internal schema identifiers are legacy names from an earlier private
prototype. Treat `src.core.api` and `src.core.queries` as the public boundary.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
chmod 600 .env
PYTHONPATH=. .venv/bin/python config/db_setup.py
```

By default the engine uses:

- SQLite DB: `data/macro_engine.db`
- Logs: `data/logs/`

Override paths with `MACRO_ENGINE_DB_PATH` and `MACRO_ENGINE_LOG_DIR`.

## Run Data Fetchers

Each fetcher is designed to be safe to rerun. Optional feeds skip cleanly when
their credentials are missing.

```bash
PYTHONPATH=. .venv/bin/python -m src.fetchers.yahoo
PYTHONPATH=. .venv/bin/python -m src.fetchers.fred
PYTHONPATH=. .venv/bin/python -m src.fetchers.bls
PYTHONPATH=. .venv/bin/python -m src.fetchers.ecb
PYTHONPATH=. .venv/bin/python -m src.fetchers.worldbank
PYTHONPATH=. .venv/bin/python -m src.fetchers.scb
PYTHONPATH=. .venv/bin/python -m src.fetchers.riksbank
PYTHONPATH=. .venv/bin/python -m src.fetchers.eurostat
PYTHONPATH=. .venv/bin/python -m src.fetchers.imf
PYTHONPATH=. .venv/bin/python -m src.fetchers.cftc
PYTHONPATH=. .venv/bin/python -m src.fetchers.eia
PYTHONPATH=. .venv/bin/python -m src.fetchers.gdelt
PYTHONPATH=. .venv/bin/python -m src.fetchers.rss_news
PYTHONPATH=. .venv/bin/python -m src.fetchers.sanctions
PYTHONPATH=. .venv/bin/python -m src.fetchers.weather
PYTHONPATH=. .venv/bin/python -m src.fetchers.edgar_form4
PYTHONPATH=. .venv/bin/python -m src.fetchers.edgar_facts
```

## Compile Intelligence Layers

```bash
PYTHONPATH=. .venv/bin/python -m src.intelligence.current_events_canary
PYTHONPATH=. .venv/bin/python -m src.intelligence.world_pull
PYTHONPATH=. .venv/bin/python -m src.intelligence.macro_actuals
PYTHONPATH=. .venv/bin/python -m src.intelligence.macro_events
PYTHONPATH=. .venv/bin/python -m src.intelligence.impact_graph
PYTHONPATH=. .venv/bin/python -m src.signals.compute
```

## Forecast Lab

Dry-run backtests do not call external AI providers:

```bash
PYTHONPATH=. .venv/bin/python -m src.backtest.run
```

Live forecast generation requires `ANTHROPIC_API_KEY`:

```bash
PYTHONPATH=. .venv/bin/python -m src.inference.predict \
    --asset GC=F --horizon 1m --asset-name "Gold (COMEX)"
PYTHONPATH=. .venv/bin/python -m src.inference.score
```

The default Claude model is configured through `MACRO_ENGINE_CLAUDE_MODEL`.

## Refresh Loops

Frequent refresh, intended for current events, source health, and lightweight
intelligence surfaces:

```bash
PYTHONPATH=. .venv/bin/python -m src.cron_frequent
```

Daily research refresh, intended for heavier fetchers, context packs, forecasts,
analogue index rebuilds, and scoring:

```bash
PYTHONPATH=. .venv/bin/python -m src.cron_daily
```

## Public Data Boundary

The repository should contain code, schema, tests, docs, and small synthetic
fixtures only. It should not contain:

- `.env` files or credentials,
- API keys or bearer tokens,
- fetched SQLite databases,
- CSV/parquet exports,
- logs,
- personal journals,
- local deployment paths,
- private prompt dumps.

Runtime data is ignored under `data/`, and private local notes are ignored under
`.private/`.

## Public Safety Boundary

Macro Intelligence Engine is a research backend. Public-facing output should:

- Explain data sources, timestamps, assumptions, and uncertainty.
- Show forecast scoring only when enough predictions have matured.
- Avoid buy/sell commands, personalized advice, suitability language, or claims
  of guaranteed performance.
- Keep private prompts, local notes, credentials, and personal deployment
  details out of the repository.

## License

Macro Intelligence Engine is licensed under the GNU Affero General Public
License v3.0 or later. See `LICENSE` and `NOTICE.md`.

AGPL is a strong copyleft license suited to network software: if someone runs a
modified public service based on this code, users of that service must be able
to receive the corresponding source code under the same license. Preserve
copyright and license notices, and clearly mark modified versions.
