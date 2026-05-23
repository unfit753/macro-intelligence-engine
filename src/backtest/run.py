"""Small walk-forward backtest harness for Macro Intelligence Engine asset predictions.

The first version is deliberately modest:
  - monthly as-of dates
  - a small default asset set
  - 1m/3m horizons
  - isolated backtest tables, never the live predictions table
  - dry-run mode by default so jobs/prompts can be inspected before spending API

Backtests are point-in-time only to the degree the underlying prompt renderer is:
it asks for an historical as_of date and most source blocks filter by that date.
Some newer feeds do not have deep point-in-time history, so the prompt appends
an explicit source-coverage warning for every backtest row.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
from dataclasses import dataclass

from dotenv import load_dotenv

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.inference import predict
from src.inference.prompt import HORIZON_DESC, render_brief
from src.retrieval.query import Analogue, fetch_analogues, realised_return
from src.signals.compute import HORIZON_DAYS


DEFAULT_ASSETS: tuple[tuple[str, str], ...] = (
    ("GC=F", "Gold (COMEX)"),
    ("CL=F", "WTI Crude Oil"),
    ("SPY", "S&P 500 ETF"),
    ("TLT", "20Y+ Treasury ETF"),
    ("^OMX", "OMXS30 (Stockholm)"),
    ("^STOXX50E", "Euro Stoxx 50"),
    ("^N225", "Nikkei 225"),
)
DEFAULT_HORIZONS = ("1m", "3m")
DEFAULT_MODEL_DRY = "oracle_backtest_dry_v1"


@dataclass(frozen=True)
class Job:
    asset: str
    asset_name: str
    horizon: str
    as_of: dt.date


def month_ends(start: dt.date, end: dt.date, step_months: int) -> list[dt.date]:
    dates: list[dt.date] = []
    year, month = start.year, start.month
    while dt.date(year, month, 1) <= end:
        if month == 12:
            next_month = dt.date(year + 1, 1, 1)
        else:
            next_month = dt.date(year, month + 1, 1)
        candidate = next_month - dt.timedelta(days=1)
        if start <= candidate <= end:
            dates.append(candidate)
        month += step_months
        while month > 12:
            year += 1
            month -= 12
    return dates


def parse_assets(raw: str | None) -> list[tuple[str, str]]:
    if not raw:
        return list(DEFAULT_ASSETS)
    symbols = [x.strip() for x in raw.split(",") if x.strip()]
    default_names = dict(DEFAULT_ASSETS)
    return [(symbol, default_names.get(symbol, symbol)) for symbol in symbols]


def make_jobs(start: dt.date, end: dt.date, assets: list[tuple[str, str]],
              horizons: list[str], step_months: int, limit: int | None) -> list[Job]:
    jobs = [
        Job(asset, name, horizon, as_of)
        for as_of in month_ends(start, end, step_months)
        for asset, name in assets
        for horizon in horizons
    ]
    return jobs[:limit] if limit else jobs


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    add_missing_columns(conn)


def create_run(conn: sqlite3.Connection, name: str, config: dict, notes: str = "") -> int:
    cur = conn.execute(
        """INSERT INTO backtest_runs (name, created_at, started_at, status, config_json, notes)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            name,
            dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "running",
            json.dumps(config, sort_keys=True),
            notes,
        ),
    )
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str) -> None:
    conn.execute(
        "UPDATE backtest_runs SET finished_at = ?, status = ? WHERE id = ?",
        (dt.datetime.now(dt.UTC).isoformat(timespec="seconds"), status, run_id),
    )


def source_coverage_block(as_of: dt.date) -> str:
    return (
        "\n\n## Backtest source-coverage warning\n"
        f"This is a historical walk-forward test as of **{as_of.isoformat()}**. "
        "Use only information dated on or before this as-of date. Several modern "
        "Atlas feeds have shallow or non-point-in-time history: sanctions snapshots, "
        "risk_hotspots, RSS, social/X, recent GDELT disaster markers and current "
        "Atlas impact graph rows may be unavailable or incomplete for older dates. "
        "Discount missing or obviously stale sources. Do not infer future data.\n"
    )


def fallback_analogues(conn: sqlite3.Connection, asset: str, horizon: str,
                       as_of: dt.date, k: int) -> list[Analogue]:
    rows = conn.execute(
        """SELECT DISTINCT date FROM prices
           WHERE symbol = ?
             AND date <= date(?, '-180 days')
           ORDER BY date DESC
           LIMIT ?""",
        (asset, as_of.isoformat(), k * 12),
    ).fetchall()
    analogues: list[Analogue] = []
    seen_months: set[str] = set()
    for (raw_date,) in rows:
        month_key = raw_date[:7]
        if month_key in seen_months:
            continue
        seen_months.add(month_key)
        ret = realised_return(conn, asset, raw_date, horizon)
        if ret is None:
            continue
        analogues.append(Analogue(raw_date, 0.0, ret, {"fallback": True}))
        if len(analogues) >= k:
            break
    return analogues


def get_analogues(conn: sqlite3.Connection, asset: str, horizon: str,
                  as_of: dt.date, k: int) -> list[Analogue]:
    try:
        analogues = fetch_analogues(as_of, asset=asset, horizon=horizon, k=k)
        if analogues:
            return analogues
    except Exception as exc:
        log(f"analogue retrieval failed for {asset}/{horizon}/{as_of}: {exc}", module="backtest")
    return fallback_analogues(conn, asset, horizon, as_of, k)


def dry_forecast(conn: sqlite3.Connection, job: Job) -> dict:
    return {
        "direction": "flat",
        "confidence_0_1": 0.0,
        "expected_return_low": -0.03,
        "expected_return_high": 0.03,
        "rationale_md": (
            "Dry-run placeholder. This row verifies the walk-forward harness "
            "and prompt storage without using future returns or calling Claude."
        ),
        "key_risks": ["Dry-run row; not an AI forecast."],
        "analogues_used": [],
    }


def write_backtest_prediction(conn: sqlite3.Connection, run_id: int, job: Job,
                              forecast: dict, brief: str, model: str,
                              dry_run: bool, error: str | None = None) -> int:
    cur = conn.execute(
        """INSERT OR REPLACE INTO backtest_predictions (
             run_id, asset, asset_name, horizon, as_of, generated_at,
             direction, confidence, expected_return_low, expected_return_high,
             rationale_md, key_risks, analogues_used, model, input_hash,
             input_brief_md, dry_run, error
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            job.asset,
            job.asset_name,
            job.horizon,
            job.as_of.isoformat(),
            dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            forecast["direction"],
            float(forecast["confidence_0_1"]),
            float(forecast["expected_return_low"]),
            float(forecast["expected_return_high"]),
            forecast["rationale_md"],
            json.dumps(forecast["key_risks"]),
            json.dumps(forecast["analogues_used"]),
            model,
            predict.hash_input(brief),
            brief,
            int(dry_run),
            error,
        ),
    )
    return int(cur.lastrowid)


def score_run(conn: sqlite3.Connection, run_id: int) -> int:
    rows = conn.execute(
        """SELECT id, asset, horizon, as_of, direction, expected_return_low, expected_return_high
           FROM backtest_predictions
           WHERE run_id = ? AND realized_return IS NULL AND dry_run = 0""",
        (run_id,),
    ).fetchall()
    scored = 0
    for pred_id, asset, horizon, as_of, direction, low, high in rows:
        if horizon not in HORIZON_DAYS:
            continue
        ret = realised_return(conn, asset, as_of, horizon)
        if ret is None:
            continue
        actual_dir = "up" if ret > 0.0025 else "down" if ret < -0.0025 else "flat"
        hit = int(actual_dir == direction or (direction == "flat" and abs(ret) <= 0.01))
        range_hit = int((low is None or ret >= low) and (high is None or ret <= high))
        conn.execute(
            """UPDATE backtest_predictions
               SET realized_return = ?, direction_hit = ?, range_hit = ?, scored_at = ?
               WHERE id = ?""",
            (
                float(ret),
                hit,
                range_hit,
                dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
                pred_id,
            ),
        )
        scored += 1
    return scored


def run_backtest(args: argparse.Namespace) -> int:
    load_dotenv(override=True)
    dry_run = not args.live
    if not dry_run and not os.getenv("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set. Use --dry-run or configure .env.", file=sys.stderr)
        return 2

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    assets = parse_assets(args.assets)
    horizons = [h.strip() for h in args.horizons.split(",") if h.strip()]
    for horizon in horizons:
        if horizon not in HORIZON_DESC:
            raise ValueError(f"Unsupported horizon: {horizon}")
    jobs = make_jobs(start, end, assets, horizons, args.step_months, args.limit)

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_schema(conn)
        config = {
            "start": args.start,
            "end": args.end,
            "assets": assets,
            "horizons": horizons,
            "step_months": args.step_months,
            "limit": args.limit,
            "dry_run": dry_run,
            "k": args.k,
        }
        run_id = create_run(conn, args.name, config, args.notes)
        conn.commit()
        log(f"Backtest run #{run_id} starting with {len(jobs)} job(s). dry_run={dry_run}", module="backtest")

        failures = 0
        for idx, job in enumerate(jobs, start=1):
            log(f"[{idx}/{len(jobs)}] {job.asset}/{job.horizon} as_of={job.as_of}", module="backtest")
            try:
                analogues = get_analogues(conn, job.asset, job.horizon, job.as_of, args.k)
                brief = render_brief(conn, job.asset, job.horizon, job.as_of, analogues, job.asset_name)
                brief += source_coverage_block(job.as_of)
                if dry_run:
                    forecast = dry_forecast(conn, job)
                    model = DEFAULT_MODEL_DRY
                else:
                    forecast = predict.call_claude(brief)
                    model = predict.MODEL
                predict._validate(forecast)
                write_backtest_prediction(conn, run_id, job, forecast, brief, model, dry_run)
                conn.commit()
            except Exception as exc:
                failures += 1
                log(f"backtest job failed: {job} {exc}", module="backtest")
                placeholder = {
                    "direction": "flat",
                    "confidence_0_1": 0.0,
                    "expected_return_low": 0.0,
                    "expected_return_high": 0.0,
                    "rationale_md": f"Job failed: {exc}",
                    "key_risks": ["Backtest job failed."],
                    "analogues_used": [],
                }
                write_backtest_prediction(conn, run_id, job, placeholder, "", DEFAULT_MODEL_DRY, True, str(exc))
                conn.commit()

        scored = score_run(conn, run_id)
        finish_run(conn, run_id, "failed" if failures == len(jobs) else "completed")
        conn.commit()
        print(f"Backtest run #{run_id}: jobs={len(jobs)}, failures={failures}, scored={scored}")
        return 0 if failures < len(jobs) else 1
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=f"small_backtest_{dt.date.today().isoformat()}")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2021-06-30")
    parser.add_argument("--assets", default="GC=F,CL=F,SPY,TLT,^OMX")
    parser.add_argument("--horizons", default="1m")
    parser.add_argument("--step-months", type=int, default=1)
    parser.add_argument("--limit", type=int, default=10, help="Max jobs for first small run.")
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--live", action="store_true", help="Call Claude. Default is dry-run.")
    parser.add_argument("--notes", default="")
    return run_backtest(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
