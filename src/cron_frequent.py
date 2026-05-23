"""Frequent lightweight refresh pipeline for client-facing feeds.

This intentionally avoids Claude, Qdrant rebuilds, full signal recomputes and
other daily-heavy work. Run it every 15 minutes from host cron to keep Overview/News/Data
fresh while the prediction layer stays on the daily cadence.

Suggested crontab:

    */15 * * * *  cd /path/to/macro-intelligence-engine && PYTHONPATH=. \\
               .venv/bin/python -m src.cron_frequent \\
               >> data/logs/cron_frequent.log 2>&1
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
import traceback
from pathlib import Path
from typing import Callable

from config.config_fetch import LOG_DIR, log
from src.core.source_registry import in_event_window, source_cadence_hours, source_spec


STATE_PATH = Path(LOG_DIR) / "frequent_state.json"


def load_state() -> dict[str, str]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(state: dict[str, str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))


def should_run(state: dict[str, str], name: str, min_hours: float, now: dt.datetime) -> bool:
    raw = state.get(name)
    if not raw:
        return True
    try:
        previous = dt.datetime.fromisoformat(raw)
    except ValueError:
        return True
    if previous.tzinfo is None:
        previous = previous.replace(tzinfo=dt.UTC)
    return (now - previous).total_seconds() >= min_hours * 3600


def step(name: str, fn: Callable, *args, **kwargs) -> bool:
    from src.core.runs import coerce_run_stats, finish_source_run, start_source_run

    log(f"--- {name} ---", module="frequent")
    t0 = time.time()
    spec = source_spec(name)
    run_id = start_source_run("frequent", name, metadata=spec.to_record() if spec else None)
    try:
        result = fn(*args, **kwargs)
        stats = coerce_run_stats(result)
        finish_source_run(run_id, status="success", **stats)
        log(f"--- {name} OK ({time.time() - t0:.1f}s) ---", module="frequent")
        return True
    except Exception as e:
        finish_source_run(run_id, status="failure", error_message=str(e))
        log(f"--- {name} FAILED: {e} ---", module="frequent")
        log(traceback.format_exc(), module="frequent")
        return False


def maybe_step(
    state: dict[str, str],
    name: str,
    min_hours: float,
    now: dt.datetime,
    fn: Callable,
    *args,
    force: bool = False,
    **kwargs,
) -> bool:
    if not force and not should_run(state, name, min_hours, now):
        log(f"--- {name} skipped; cadence {min_hours:g}h not elapsed ---", module="frequent")
        return True
    ok = step(name, fn, *args, **kwargs)
    if ok:
        state[name] = now.isoformat(timespec="seconds")
        save_state(state)
    return ok


def release_window_events(now: dt.datetime) -> list[dict[str, str]]:
    from config.config_fetch import DB_PATH
    from src.core.db import connect_readonly, table_exists

    conn = connect_readonly(DB_PATH)
    try:
        if not table_exists(conn, "calendar_events"):
            return []
        rows = conn.execute(
            """SELECT scheduled_at_utc, date, importance, title
               FROM calendar_events
               WHERE date BETWEEN date('now', '-1 day') AND date('now', '+2 day')
                 AND importance >= 4"""
        ).fetchall()
        return [
            {"scheduled_at_utc": row[0], "date": row[1], "importance": row[2], "title": row[3]}
            for row in rows
        ]
    finally:
        conn.close()


def registry_cadence(name: str, event_window: bool) -> float:
    return source_cadence_hours(name, event_window=event_window)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Ignore cadence gates.")
    parser.add_argument("--gdelt-workers", type=int, default=0)
    args = parser.parse_args(argv)

    now = dt.datetime.now(dt.UTC)
    state = load_state()
    event_window = in_event_window(now, release_window_events(now))
    log(f"Frequent pipeline starting at {now.isoformat(timespec='seconds')}", module="frequent")
    if event_window:
        log("Release-window cadence active for official macro sources.", module="frequent")

    from src.fetchers import bls, eia, eurostat, fred, gdelt, reddit, riksbank, rss_news, sanctions, scb, twitter, edgar_form4, weather
    from src.intelligence import world_pull, macro_events, impact_graph, macro_actuals, current_events_canary
    from src.local_model import update_event_results, research_journal

    # Dashboard-live feeds.
    maybe_step(state, "rss_news", registry_cadence("rss_news", event_window), now, rss_news.main, force=args.force)
    maybe_step(
        state,
        "gdelt_today",
        registry_cadence("gdelt_today", event_window),
        now,
        gdelt.main,
        backfill_days=0,
        workers=args.gdelt_workers or None,
        include_today=True,
        force=args.force,
    )
    maybe_step(state, "reddit", registry_cadence("reddit", event_window), now, reddit.main, force=args.force)
    maybe_step(state, "twitter", registry_cadence("twitter", event_window), now, twitter.main, force=args.force)
    maybe_step(state, "bls", registry_cadence("bls", event_window), now, bls.main, force=args.force)
    maybe_step(state, "fred", registry_cadence("fred", event_window), now, fred.main, force=args.force)
    maybe_step(state, "scb", registry_cadence("scb", event_window), now, scb.main, force=args.force)
    maybe_step(state, "riksbank", registry_cadence("riksbank", event_window), now, riksbank.main, force=args.force)
    maybe_step(state, "eurostat", registry_cadence("eurostat", event_window), now, eurostat.main, force=args.force)
    maybe_step(state, "eia", registry_cadence("eia", event_window), now, eia.main, force=args.force)
    maybe_step(state, "weather", registry_cadence("weather", event_window), now, weather.main, force=args.force)
    maybe_step(state, "macro_actuals", registry_cadence("macro_actuals", event_window), now, macro_actuals.generate_and_store, force=args.force)
    maybe_step(state, "current_events_canary", registry_cadence("current_events_canary", event_window), now, current_events_canary.generate_and_store, force=args.force)
    maybe_step(state, "research_journal", registry_cadence("research_journal", event_window), now, research_journal.generate_and_store, force=args.force)

    # Slower but still useful to refresh more than daily.
    maybe_step(state, "sanctions", registry_cadence("sanctions", event_window), now, sanctions.main, force=args.force)
    maybe_step(state, "edgar_form4", registry_cadence("edgar_form4", event_window), now, edgar_form4.main, force=args.force)

    # Product layer: cheap deterministic compilation used by Overview and prompts.
    maybe_step(state, "world_pull", registry_cadence("world_pull", event_window), now, world_pull.compile_and_store, force=args.force)
    maybe_step(state, "macro_event_scenarios", registry_cadence("macro_event_scenarios", event_window), now, macro_events.generate_and_store, force=args.force)
    maybe_step(state, "local_event_result_notes", registry_cadence("local_event_result_notes", event_window), now, update_event_results.update_notes, unload=True, force=args.force)
    maybe_step(state, "impact_graph", registry_cadence("impact_graph", event_window), now, impact_graph.build_and_store, force=args.force)

    finished = dt.datetime.now(dt.UTC)
    log(f"Frequent pipeline finished at {finished.isoformat(timespec='seconds')}", module="frequent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
