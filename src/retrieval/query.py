"""Find historical month-ends most similar to a target date and report what
happened next at the requested horizon.

Returns a list of analogues, each augmented with the realised forward return
on the asset of interest — the concrete prior the LLM gets to reason against.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from qdrant_client import QdrantClient

from config.config_fetch import DB_PATH
from src.core.db import connect_readonly
from src.retrieval.build_index import (
    COLLECTION, DIM, FEATURE_KEYS, NORM_PATH,
    QDRANT_HOST, QDRANT_PORT, build_feature_frame,
)
from src.signals.compute import HORIZON_DAYS


@dataclass
class Analogue:
    date: str             # YYYY-MM-DD
    cosine: float         # similarity score
    realised_return: float | None  # forward return on the asset at the horizon
    payload: dict         # raw feature values at that historical date


def load_norm() -> tuple[list[str], dict, dict]:
    n = json.loads(NORM_PATH.read_text())
    return n["feature_keys"], n["mean"], n["std"]


def vector_for_date(target: date, conn: sqlite3.Connection) -> tuple[np.ndarray, dict]:
    """Build the same 10-dim feature vector for a target date (uses month-end state)."""
    df = build_feature_frame(conn)
    target_ts = pd.Timestamp(target)
    if target_ts not in df.index:
        # use the most recent month-end on or before target
        before = df.index[df.index <= target_ts]
        if len(before) == 0:
            raise ValueError(f"No feature data on or before {target}")
        target_ts = before.max()
    raw = df.loc[target_ts]
    keys, mu, sd = load_norm()
    z = np.array([(raw[k] - mu[k]) / sd[k] for k in keys], dtype=np.float32)
    return z, raw.to_dict()


def realised_return(conn: sqlite3.Connection, asset: str, from_date: str, horizon: str) -> float | None:
    """Return on `asset` over `horizon` trading days starting on/after `from_date`."""
    n = HORIZON_DAYS[horizon]
    start_row = conn.execute(
        "SELECT date, price FROM prices WHERE symbol = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (asset, from_date),
    ).fetchone()
    if not start_row:
        return None
    end_row = conn.execute(
        """SELECT price FROM prices WHERE symbol = ? AND date > ?
           ORDER BY date ASC LIMIT 1 OFFSET ?""",
        (asset, start_row[0], n - 1),
    ).fetchone()
    if not end_row:
        return None
    return end_row[0] / start_row[1] - 1


def fetch_analogues(target: date, asset: str, horizon: str, k: int = 5,
                    exclude_recent_days: int = 180) -> list[Analogue]:
    conn = connect_readonly(DB_PATH)
    try:
        vec, _raw = vector_for_date(target, conn)
        client = QdrantClient(QDRANT_HOST, port=QDRANT_PORT, check_compatibility=False)
        # Pull a few extras to allow filtering recent neighbours out.
        results = client.query_points(
            collection_name=COLLECTION,
            query=vec.tolist(),
            limit=k + 10,
        ).points
        cutoff = (target - timedelta(days=exclude_recent_days)).isoformat()
        analogues: list[Analogue] = []
        for r in results:
            d = r.payload["date"]
            if d > cutoff:
                continue  # skip our own neighbourhood
            ret = realised_return(conn, asset, d, horizon)
            analogues.append(Analogue(
                date=d, cosine=float(r.score),
                realised_return=ret, payload=r.payload,
            ))
            if len(analogues) >= k:
                break
        return analogues
    finally:
        conn.close()


if __name__ == "__main__":
    # Quick sanity check
    from datetime import date
    res = fetch_analogues(date.today(), asset="GC=F", horizon="1m", k=5)
    print(f"Top {len(res)} analogues for today on GC=F / 1m:")
    for a in res:
        ret = f"{a.realised_return:+.1%}" if a.realised_return is not None else "n/a"
        print(f"  {a.date}  cosine={a.cosine:.3f}  GC=F 1m ahead: {ret}")
