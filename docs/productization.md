# Public Boundary

Macro Intelligence Engine is the public backend and open-source portfolio
project. It should stay focused on data collection, source telemetry, macro
intelligence synthesis, context packs, and forecast evaluation.

The private research system that preceded it should not leak into the public
repo through names, domains, personal paths, local journals, prompt dumps, or
deployment secrets.

## Public Product Positioning

Use this language:

- Macro intelligence and scenario research.
- Data engine for a geopolitical and macro-risk atlas.
- Forecast lab and outcome evaluation.
- Research only, not investment advice.
- Forecasts are probabilistic and may be wrong.

Avoid this language:

- Personalized investment advice.
- Buy/sell commands.
- Guaranteed returns.
- Copy-trading or automated execution.
- Suitability for a specific user or portfolio.

## Backend Boundary

The reusable boundary is `src/core`:

- `src/core/db.py` opens read-only or writable SQLite connections.
- `src/core/queries.py` exposes source health, events, releases, map layers,
  forecasts, historical state, and public-safe summaries.
- `src/core/api.py` returns JSON-friendly records for a later web API.
- `src/core/compliance.py` keeps public copy in research-only territory.

Frontend work should live in a sibling repository and consume the core/query or
API layer instead of writing directly to tables.

## Public Demo Checklist

- Add a small demo database or deterministic fixtures.
- Keep runtime data under ignored `data/` paths.
- Remove personal journals from git history before making the repository public
  if they contain private research.
- Replace private domains and emails with environment variables.
- Show source timestamps and health in frontend clients.
- Include a visible AGPL source/license link in any hosted public demo.
- Keep calibration muted until enough predictions have matured.
