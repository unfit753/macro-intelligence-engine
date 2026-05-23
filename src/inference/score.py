"""Score past predictions against realised prices.

Walks the predictions table, finds rows whose horizon has now elapsed and
that don't yet have a realized_return, computes the actual return on the
asset, and writes it back. Run regularly (daily cron is fine).

Out of scope here: directional accuracy / Brier / calibration metrics —
those belong in downstream clients.
"""
from __future__ import annotations

import datetime as dt
import sqlite3

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable
from src.retrieval.query import realised_return
from src.signals.compute import HORIZON_DAYS


def score_due() -> int:
    conn = connect_writable(DB_PATH)
    try:
        rows = conn.execute(
            """SELECT id, asset, horizon, as_of FROM predictions
               WHERE realized_return IS NULL"""
        ).fetchall()
        scored = 0
        for pred_id, asset, horizon, as_of in rows:
            n = HORIZON_DAYS[horizon]
            # Need at least n trading days of price data after as_of for the
            # horizon to be elapsed. We approximate trading days with
            # 1 trading day ≈ 1.4 calendar days as a slack.
            min_calendar = int(n * 1.4) + 1
            elapsed_cutoff = (dt.date.fromisoformat(as_of) +
                              dt.timedelta(days=min_calendar))
            if elapsed_cutoff > dt.date.today():
                continue
            ret = realised_return(conn, asset, as_of, horizon)
            if ret is None:
                continue
            conn.execute(
                """UPDATE predictions SET realized_return = ?, scored_at = ?
                   WHERE id = ?""",
                (float(ret), dt.datetime.now(dt.UTC).isoformat(timespec="seconds"), pred_id),
            )
            scored += 1
            log(f"#{pred_id} {asset}/{horizon} as_of={as_of}: realised {ret*100:+.2f}%", module="score")
        conn.commit()
        return scored
    finally:
        conn.close()


def main():
    n = score_due()
    log(f"Done. Scored {n} prediction(s).", module="score")


if __name__ == "__main__":
    main()
