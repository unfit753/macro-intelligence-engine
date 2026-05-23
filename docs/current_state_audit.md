# Public Readiness Audit

Snapshot date: 2026-05-22.

## Short Verdict

The repository already has a strong data and intelligence backend. The first
public-readiness job is not inventing a new system; it is narrowing the story
and removing private context.

Macro Intelligence Engine should stay backend-first. The future frontend should
be a separate one-screen map-first demo with explainable overlays.

## What Is Good

- The ingestion layer is broad and mostly idempotent.
- `source_runs` gives operational telemetry instead of relying on row counts.
- Current events, macro releases, GDELT streams, sanctions, risk hotspots, and
  market signals already exist as separate surfaces.
- Historical state and forward returns are separated, which is useful for
  honest forecast evaluation.
- Compliance checks already push public copy away from advice language.

## Public Risks

- Old product names still exist in internal table names and some code paths.
- Private deployment details and journals must stay out of the public repo.
- Frontend concerns should not be mixed into the backend repo.
- Forecast accuracy should not be implied until enough outcomes have matured.
- GDELT/theme-code noise needs tighter presentation before it becomes a public
  experience.

## Recommended Order

1. Sanitize README, docs, defaults, deployment examples, user agents, and
   tracked journals.
2. Add AGPL license and notice files.
3. Create a small demo database or fixture loader.
4. Create a sibling frontend repo that consumes exported JSON or `src.core.api`.
5. Add a short architecture diagram.
6. Add a repeatable `make demo` or equivalent command.
