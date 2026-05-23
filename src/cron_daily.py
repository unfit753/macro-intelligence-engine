"""Daily pipeline: fetch -> compute signals -> rebuild index -> predict grid -> score.

For hourly client freshness without waking Claude, use src.cron_frequent.

Run as a host cron. The venv must be activated or invoked directly:

    0 22 * * *  cd /path/to/macro-intelligence-engine && PYTHONPATH=. \\
                .venv/bin/python -m src.cron_daily \\
                >> data/logs/cron_daily.log 2>&1

Each step logs to its own module log under the configured log directory.
A failure in one step doesn't stop the rest — partial progress is better
than no progress.
"""
from __future__ import annotations

import datetime as dt
import sys
import time
import traceback

from config.config_fetch import log


def load_predict_grid() -> list[tuple[str, str, str]]:
    """Read the active prediction grid from the `targets` table.

    Each target carries a comma-separated `horizons` list; we expand it
    into one (asset, horizon, name) tuple per horizon. Inactive rows are
    skipped. Tweak the grid via the targets table or by
    editing the targets table directly.
    """
    from config.config_fetch import DB_PATH
    from src.core.db import connect_readonly
    conn = connect_readonly(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT symbol, name, horizons FROM targets WHERE active = 1"
        ).fetchall()
    finally:
        conn.close()
    grid: list[tuple[str, str, str]] = []
    for symbol, name, horizons in rows:
        for h in (horizons or "").split(","):
            h = h.strip()
            if h:
                grid.append((symbol, h, name))
    return grid


def step(name: str, fn, *args, **kwargs) -> bool:
    """Run one pipeline step; never raise. Returns True on success."""
    from src.core.runs import coerce_run_stats, finish_source_run, start_source_run
    from src.core.source_registry import source_enabled, source_spec

    if not source_enabled(name):
        log(f"--- {name} skipped: inactive registry source ---", module="cron")
        return True
    log(f"--- {name} ---", module="cron")
    t0 = time.time()
    spec = source_spec(name)
    run_id = start_source_run("daily", name, metadata=spec.to_record() if spec else None)
    try:
        result = fn(*args, **kwargs)
        stats = coerce_run_stats(result)
        finish_source_run(run_id, status="success", **stats)
        log(f"--- {name} OK ({time.time() - t0:.1f}s) ---", module="cron")
        return True
    except Exception as e:
        finish_source_run(run_id, status="failure", error_message=str(e))
        log(f"--- {name} FAILED: {e} ---", module="cron")
        log(traceback.format_exc(), module="cron")
        return False


def predict_one(asset: str, horizon: str, asset_name: str):
    """Invoke predict.main() with synthesised argv so we re-use the CLI parser."""
    from src.inference import predict
    rc = predict.main([
        "--asset", asset,
        "--horizon", horizon,
        "--asset-name", asset_name,
    ])
    if rc != 0:
        raise RuntimeError(f"predict returned {rc}")


def main():
    started = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    log(f"Daily pipeline starting at {started}", module="cron")

    from src.fetchers import (
        yahoo, fred, bls, ecb, worldbank, scb, riksbank, eurostat, imf, eia, cftc, gdelt,
        seed_events, cb_calendar, acled,
        weather, reddit, edgar_form4, edgar_facts,
        sanctions, rss_news, twitter,
    )
    from src.signals import compute as signals_compute
    from src.signals import rumours, weather_correlations
    from src.retrieval import build_index
    from src.inference import score
    from src.intelligence import world_pull, macro_events, impact_graph, historical_state, macro_actuals, label_evaluation
    from src.local_model import update_event_results

    step("seed_events",    seed_events.main)
    step("yahoo",          yahoo.main)
    step("fred",           fred.main)
    step("bls",            bls.main)
    step("ecb",            ecb.main)
    step("worldbank",      worldbank.main)
    step("scb",            scb.main)
    step("riksbank",       riksbank.main)
    step("eurostat",       eurostat.main)
    step("imf",            imf.main)
    step("eia",            eia.main)
    step("cftc",           cftc.main)
    step("gdelt",          gdelt.main, backfill_days=2)
    step("sanctions",      sanctions.main)
    step("acled",          acled.main)
    step("cb_calendar",    cb_calendar.main)
    step("macro_actuals",  macro_actuals.generate_and_store)
    step("rss_news",       rss_news.main)
    step("weather",        weather.main)
    step("reddit",         reddit.main)
    step("twitter",        twitter.main)
    step("edgar_form4",    edgar_form4.main)
    step("edgar_facts",    edgar_facts.main)
    step("signals",        signals_compute.main)
    step("rumours",        rumours.main)
    step("weather_corr",   weather_correlations.main)
    step("world_pull",     world_pull.compile_and_store)
    step("macro_events",   macro_events.generate_and_store)
    step("event_results",  update_event_results.update_notes, unload=True)
    step("impact_graph",   impact_graph.build_and_store)
    step("historical_state_recent", historical_state.build_recent_and_store)
    step("build_index",    build_index.main)

    grid = load_predict_grid()
    successes = 0
    for asset, horizon, name in grid:
        if step(f"predict {asset}/{horizon}", predict_one, asset, horizon, name):
            successes += 1
    log(f"predictions: {successes}/{len(grid)} succeeded", module="cron")

    step("score",          score.main)
    step("label_evaluation", label_evaluation.generate_and_store)

    from src.inference import briefing
    for region, session in [("global", "global_close"), ("us", "us_postclose")]:
        step(f"briefing {region}:{session}", briefing.generate_one, region, None, session)

    finished = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    log(f"Daily pipeline finished at {finished}", module="cron")


if __name__ == "__main__":
    sys.exit(main() or 0)
