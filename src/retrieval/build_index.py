"""Encode each historical month-end macro state as a vector and push to Qdrant.

Feature vector (14 dims), all designed to capture *regime* not *level*,
with regional decomposition so analogues reflect the multi-region
universe we now trade (US, EU, Sweden, Asia):

     0  GC=F        ret_1y          gold momentum (commodity proxy)
     1  GC=F        drawdown_252d   gold stress
     2  SPY         ret_1y          US equity momentum
     3  SPY         drawdown_252d   US equity stress
     4  ^STOXX50E   ret_1y          EU equity momentum
     5  ^OMX        ret_1y          Sweden equity momentum
     6  ^N225       ret_1y          Japan/Asia equity momentum
     7  ^VIX        price           fear level
     8  _macro      yield_curve     US curve slope (10y - FedFunds, pp)
     9  DX-Y.NYB    px_vs_ma200     USD trend
    10  ___         us_cpi_yoy      US headline inflation YoY
    11  ___         eu_cpi_yoy      Eurozone HICP YoY
    12  ___         fed_funds       US policy rate (level)
    13  CL=F        ret_1y          oil momentum

Stored z-scored to make distances meaningful. Mean/std saved as a JSON
sidecar so query.py can normalise lookup vectors the same way.

Idempotent: drops and recreates the collection each run.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from config.config_fetch import DB_PATH, LOG_DIR, log
from src.core.db import connect_readonly


COLLECTION = "macro_states_monthly"
QDRANT_HOST = "localhost"
QDRANT_PORT = 6333
DIM = 14
NORM_PATH = Path(LOG_DIR).parent / "feature_norm.json"

FEATURE_KEYS = [
    "gold_ret_1y", "gold_drawdown",
    "spy_ret_1y", "spy_drawdown",
    "eu_equity_ret_1y", "se_equity_ret_1y", "asia_equity_ret_1y",
    "vix",
    "yield_curve", "dxy_trend",
    "us_cpi_yoy", "eu_cpi_yoy",
    "fed_funds",
    "oil_ret_1y",
]


def load_signals(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT date, symbol, signal_name, value FROM signals",
        conn, parse_dates=["date"],
    )
    return df.pivot_table(
        index="date", columns=["symbol", "signal_name"], values="value"
    ).sort_index()


def load_prices(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql(
        "SELECT date, symbol, price FROM prices",
        conn, parse_dates=["date"],
    )
    return df.pivot(index="date", columns="symbol", values="price").sort_index()


def load_indicator_series(conn: sqlite3.Connection, name: str) -> pd.Series:
    df = pd.read_sql(
        "SELECT date, value FROM indicators WHERE indicator_name = ? ORDER BY date",
        conn, params=(name,), parse_dates=["date"],
    )
    return df.set_index("date")["value"]


def load_cpi_yoy(conn: sqlite3.Connection) -> pd.Series:
    return load_indicator_series(conn, "CPI").pct_change(12) * 100


def load_eu_hicp_yoy(conn: sqlite3.Connection) -> pd.Series:
    """Eurozone HICP from FRED (CP0000EZ19M086NEST), converted to YoY %."""
    return load_indicator_series(conn, "Eurozone HICP").pct_change(12) * 100


def load_fed_funds(conn: sqlite3.Connection) -> pd.Series:
    return load_indicator_series(conn, "Fed Funds Rate")


def build_feature_frame(conn: sqlite3.Connection) -> pd.DataFrame:
    sig = load_signals(conn)
    prices = load_prices(conn)

    idx = pd.date_range(sig.index.min(), sig.index.max(), freq="D")
    out = pd.DataFrame(index=idx)

    def sig_or_nan(symbol: str, name: str) -> pd.Series:
        s = sig.get((symbol, name))
        return s.reindex(idx, method="ffill") if s is not None else pd.Series(index=idx, dtype=float)

    out["gold_ret_1y"]        = sig_or_nan("GC=F",      "ret_1y")
    out["gold_drawdown"]      = sig_or_nan("GC=F",      "drawdown_252d")
    out["spy_ret_1y"]         = sig_or_nan("SPY",       "ret_1y")
    out["spy_drawdown"]       = sig_or_nan("SPY",       "drawdown_252d")
    out["eu_equity_ret_1y"]   = sig_or_nan("^STOXX50E", "ret_1y")
    out["se_equity_ret_1y"]   = sig_or_nan("^OMX",      "ret_1y")
    out["asia_equity_ret_1y"] = sig_or_nan("^N225",     "ret_1y")
    out["vix"]                = (prices.get("^VIX") if "^VIX" in prices else pd.Series(dtype=float)).reindex(idx, method="ffill")
    out["yield_curve"]        = sig_or_nan("_macro",    "yield_curve_10y_ff")
    out["dxy_trend"]          = sig_or_nan("DX-Y.NYB",  "px_vs_ma200")
    out["us_cpi_yoy"]         = load_cpi_yoy(conn).reindex(idx, method="ffill")
    out["eu_cpi_yoy"]         = load_eu_hicp_yoy(conn).reindex(idx, method="ffill")
    out["fed_funds"]          = load_fed_funds(conn).reindex(idx, method="ffill")
    out["oil_ret_1y"]         = sig_or_nan("CL=F",      "ret_1y")

    monthly = out.resample("ME").last()
    return monthly.dropna()


def upsert_to_qdrant(client: QdrantClient, vectors: np.ndarray, payloads: list[dict]):
    if client.collection_exists(COLLECTION):
        client.delete_collection(COLLECTION)
    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=qm.VectorParams(size=DIM, distance=qm.Distance.COSINE),
    )
    points = [
        qm.PointStruct(id=i, vector=vec.tolist(), payload=pl)
        for i, (vec, pl) in enumerate(zip(vectors, payloads))
    ]
    client.upsert(collection_name=COLLECTION, points=points)


def main():
    conn = connect_readonly(DB_PATH)
    log("Loading features...", module="retrieval")
    df = build_feature_frame(conn)
    log(f"Built {len(df)} month-end states.", module="retrieval")

    # Z-score across the full corpus (mean/std saved for query-time use)
    mu = df.mean()
    sd = df.std().replace(0, 1.0)
    z = ((df - mu) / sd).values.astype(np.float32)

    NORM_PATH.parent.mkdir(parents=True, exist_ok=True)
    NORM_PATH.write_text(json.dumps({
        "feature_keys": FEATURE_KEYS,
        "mean": mu.to_dict(),
        "std": sd.to_dict(),
    }, indent=2))
    log(f"Saved normalisation to {NORM_PATH}", module="retrieval")

    payloads = [
        {"date": d.strftime("%Y-%m-%d"), **{k: float(v) for k, v in row.items()}}
        for d, row in df.iterrows()
    ]

    client = QdrantClient(QDRANT_HOST, port=QDRANT_PORT)
    log(f"Upserting {len(z)} points to Qdrant collection '{COLLECTION}'...", module="retrieval")
    upsert_to_qdrant(client, z, payloads)
    info = client.get_collection(COLLECTION)
    log(f"Done. Collection has {info.points_count} points.", module="retrieval")
    conn.close()


if __name__ == "__main__":
    main()
