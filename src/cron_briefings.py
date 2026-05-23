"""Session-aware briefing runner.

Use this for market-timed Claude calls without running the full daily
prediction pipeline. Intended host cron examples, Europe/Stockholm time:

    35 7  * * 1-5  cd /path/to/macro-intelligence-engine && PYTHONPATH=. .venv/bin/python -m src.cron_briefings --slot eu_premarket
    35 1  * * 1-5  cd /path/to/macro-intelligence-engine && PYTHONPATH=. .venv/bin/python -m src.cron_briefings --slot asia_preopen
    40 22 * * 1-5  cd /path/to/macro-intelligence-engine && PYTHONPATH=. .venv/bin/python -m src.cron_briefings --slot us_close
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import traceback

from dotenv import load_dotenv

from config.config_fetch import log
from src.inference import briefing


SLOTS: dict[str, list[tuple[str, str]]] = {
    "eu_premarket": [("eu_se", "eu_premarket")],
    "asia_preopen": [("asia", "asia_preopen")],
    "us_close": [("us", "us_postclose"), ("global", "global_close")],
}


def main(argv: list[str] | None = None) -> int:
    load_dotenv(override=True)
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set", file=sys.stderr)
        return 2

    parser = argparse.ArgumentParser()
    parser.add_argument("--slot", required=True, choices=sorted(SLOTS))
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to today")
    args = parser.parse_args(argv)

    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else dt.date.today()
    ok = 0
    for region, session in SLOTS[args.slot]:
        try:
            briefing.generate_one(region, as_of=as_of, session=session)
            ok += 1
        except Exception as e:
            log(f"{args.slot}/{region}:{session} failed: {e}", module="briefing")
            log(traceback.format_exc(), module="briefing")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
