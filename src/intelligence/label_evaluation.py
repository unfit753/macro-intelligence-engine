from __future__ import annotations

import argparse
import sqlite3
from typing import Any

from config.config_fetch import DB_PATH, log
from src.core.db import connect_writable, table_exists


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS label_evaluations (
    profile_id TEXT NOT NULL DEFAULT 'default',
    label_id TEXT NOT NULL,
    asset TEXT NOT NULL,
    horizon TEXT NOT NULL,
    horizon_days INTEGER,
    observations INTEGER DEFAULT 0,
    positive_count INTEGER DEFAULT 0,
    hit_rate REAL,
    avg_forward_return REAL,
    median_forward_return REAL,
    weighted_avg_forward_return REAL,
    avg_label_count REAL,
    effective_weight REAL,
    methodology_version TEXT NOT NULL DEFAULT 'v2_asset_scoped',
    scope TEXT NOT NULL DEFAULT 'asset',
    first_as_of TEXT,
    last_as_of TEXT,
    last_evaluated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, label_id, asset, horizon)
);
CREATE INDEX IF NOT EXISTS idx_label_evaluations_rank
    ON label_evaluations(profile_id, asset, horizon, observations DESC);
"""

REQUIRED_TABLES = (
    "historical_state_values",
    "historical_forward_returns",
    "data_labels",
    "label_weight_profiles",
    "label_weight_overrides",
    "data_label_assignments",
)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(label_evaluations)")}
    for name, sql_type in (
        ("methodology_version", "TEXT NOT NULL DEFAULT 'v2_asset_scoped'"),
        ("scope", "TEXT NOT NULL DEFAULT 'asset'"),
    ):
        if name not in existing:
            conn.execute(f"ALTER TABLE label_evaluations ADD COLUMN {name} {sql_type}")


def _has_required_tables(conn: sqlite3.Connection) -> bool:
    return all(table_exists(conn, table) for table in REQUIRED_TABLES)


def _daily_labels_available(conn: sqlite3.Connection) -> bool:
    if not table_exists(conn, "historical_state_daily"):
        return False
    row = conn.execute(
        """SELECT 1 FROM historical_state_daily
           WHERE labels_json IS NOT NULL AND labels_json != '' AND labels_json != '[]'
           LIMIT 1"""
    ).fetchone()
    return row is not None


ASSET_IMPACT_MAP = (
    ("asset_impact:sp500", "SPY"),
    ("asset_impact:gold", "GC=F"),
    ("asset_impact:oil", "CL=F"),
    ("asset_impact:oil", "BZ=F"),
    ("theme:oil_price", "CL=F"),
    ("theme:oil_price", "BZ=F"),
    ("theme:banking", "XLF"),
)


def build_rows(
    conn: sqlite3.Connection,
    *,
    profile_id: str | None = None,
    min_observations: int = 20,
    lookback_days: int = 1825,
) -> list[dict[str, Any]]:
    if not _has_required_tables(conn):
        return []
    profiles_cte = "SELECT ? AS profile_id" if profile_id else "SELECT profile_id FROM label_weight_profiles WHERE active = 1"
    params: list[Any] = []
    if profile_id:
        params.append(profile_id)
    params.append(f"-{int(lookback_days)} days")
    asset_values = " ".join("SELECT ? AS label_id, ? AS asset" if idx == 0 else "UNION ALL SELECT ?, ?" for idx, _ in enumerate(ASSET_IMPACT_MAP))
    for label_id, asset in ASSET_IMPACT_MAP:
        params.extend([label_id, asset])
    params.append(min_observations)
    sql = f"""
    WITH profiles AS ({profiles_cte}),
    mature_returns AS (
        SELECT as_of, symbol, horizon_days, forward_return
        FROM historical_forward_returns
        WHERE forward_return IS NOT NULL
          AND end_date IS NOT NULL
          AND date(end_date) <= date('now')
          AND date(as_of) >= date('now', ?)
    ),
    mature_dates AS (
        SELECT DISTINCT as_of FROM mature_returns
    ),
    assignment_weights AS (
        SELECT label_id, target_table, AVG(weight_override) AS weight_override
        FROM data_label_assignments
        WHERE active = 1 AND weight_override IS NOT NULL
        GROUP BY label_id, target_table
    ),
    asset_impact_map AS (
        {asset_values}
    ),
    assignment_asset_map AS (
        SELECT label_id, target_value AS asset
        FROM data_label_assignments
        WHERE active = 1
          AND target_table IN ('prices', 'targets', 'signals')
          AND target_column IN ('symbol', 'source_symbol')
          AND COALESCE(target_value, '') NOT IN ('', '_news', '_macro')
    ),
    raw_label_source AS (
        SELECT
            v.as_of,
            v.source_table,
            NULLIF(v.source_symbol, '') AS source_symbol,
            json_each.value AS label_id
        FROM mature_dates md
        JOIN historical_state_values v ON v.as_of = md.as_of
        JOIN json_each(CASE WHEN json_valid(v.label_ids_json) THEN v.label_ids_json ELSE '[]' END) ON 1=1
        WHERE COALESCE(v.active, 1) = 1
          AND COALESCE(v.label_ids_json, '') NOT IN ('', '[]')
    ),
    label_source AS (
        SELECT DISTINCT
            as_of,
            source_table,
            label_id,
            source_symbol AS asset,
            'direct_asset' AS scope
        FROM raw_label_source
        WHERE source_table IN ('prices', 'signals', 'targets')
          AND source_symbol IS NOT NULL
          AND source_symbol NOT IN ('_news', '_macro')
        UNION
        SELECT DISTINCT
            r.as_of,
            r.source_table,
            r.label_id,
            aam.asset,
            'assignment_asset' AS scope
        FROM raw_label_source r
        JOIN assignment_asset_map aam ON aam.label_id = r.label_id
        UNION
        SELECT DISTINCT
            r.as_of,
            r.source_table,
            r.label_id,
            aim.asset,
            'asset_impact' AS scope
        FROM raw_label_source r
        JOIN asset_impact_map aim ON aim.label_id = r.label_id
    ),
    label_daily AS (
        SELECT
            p.profile_id,
            ls.as_of,
            ls.asset,
            ls.label_id,
            COUNT(*) AS label_count,
            AVG(COALESCE(lwo.weight, aw.weight_override, dl.default_weight, 1.0)) AS effective_weight,
            GROUP_CONCAT(DISTINCT ls.scope) AS scope
        FROM label_source ls
        JOIN profiles p ON 1=1
        LEFT JOIN data_labels dl ON dl.label_id = ls.label_id
        LEFT JOIN assignment_weights aw
            ON aw.label_id = ls.label_id
           AND (aw.target_table = ls.source_table OR aw.target_table IS NULL)
        LEFT JOIN label_weight_overrides lwo
            ON lwo.profile_id = p.profile_id
           AND lwo.label_id = ls.label_id
           AND lwo.active = 1
        GROUP BY p.profile_id, ls.as_of, ls.asset, ls.label_id
    ),
    joined AS (
        SELECT
            ld.profile_id,
            ld.label_id,
            ld.asset AS asset,
            CASE f.horizon_days
                WHEN 1 THEN '1d'
                WHEN 5 THEN '1w'
                WHEN 21 THEN '1m'
                WHEN 63 THEN '3m'
                WHEN 252 THEN '1y'
                WHEN 1260 THEN '5y'
                ELSE CAST(f.horizon_days AS TEXT) || 'd'
            END AS horizon,
            f.horizon_days,
            ld.as_of,
            ld.label_count,
            ld.effective_weight,
            ld.scope,
            f.forward_return
        FROM label_daily ld
        JOIN mature_returns f ON f.as_of = ld.as_of AND f.symbol = ld.asset
    ),
    ranked AS (
        SELECT
            joined.*,
            ROW_NUMBER() OVER (
                PARTITION BY profile_id, label_id, asset, horizon_days
                ORDER BY forward_return
            ) AS rn,
            COUNT(*) OVER (
                PARTITION BY profile_id, label_id, asset, horizon_days
            ) AS cnt
        FROM joined
    )
    SELECT
        profile_id,
        label_id,
        asset,
        horizon,
        horizon_days,
        COUNT(*) AS observations,
        SUM(CASE WHEN forward_return > 0 THEN 1 ELSE 0 END) AS positive_count,
        AVG(CASE WHEN forward_return > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
        AVG(forward_return) AS avg_forward_return,
        AVG(CASE WHEN rn IN (CAST((cnt + 1) / 2 AS INTEGER), CAST((cnt + 2) / 2 AS INTEGER)) THEN forward_return END) AS median_forward_return,
        AVG(forward_return * effective_weight) AS weighted_avg_forward_return,
        AVG(label_count) AS avg_label_count,
        AVG(effective_weight) AS effective_weight,
        'v2_asset_scoped' AS methodology_version,
        GROUP_CONCAT(DISTINCT scope) AS scope,
        MIN(as_of) AS first_as_of,
        MAX(as_of) AS last_as_of
    FROM ranked
    GROUP BY profile_id, label_id, asset, horizon, horizon_days
    HAVING COUNT(*) >= ?
    ORDER BY observations DESC, ABS(avg_forward_return) DESC
    """
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def generate_and_store(
    conn: sqlite3.Connection | None = None,
    *,
    profile_id: str | None = None,
    min_observations: int = 20,
    lookback_days: int = 1825,
) -> dict[str, Any]:
    own_conn = conn is None
    conn = conn or connect_writable(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
        rows = build_rows(conn, profile_id=profile_id, min_observations=min_observations, lookback_days=lookback_days)
        if profile_id:
            conn.execute("DELETE FROM label_evaluations WHERE profile_id = ?", (profile_id,))
        else:
            conn.execute("DELETE FROM label_evaluations")
        conn.executemany(
            """
            INSERT INTO label_evaluations (
                profile_id, label_id, asset, horizon, horizon_days, observations, positive_count,
                hit_rate, avg_forward_return, median_forward_return, weighted_avg_forward_return,
                avg_label_count, effective_weight, methodology_version, scope,
                first_as_of, last_as_of, last_evaluated_at
            ) VALUES (
                :profile_id, :label_id, :asset, :horizon, :horizon_days, :observations, :positive_count,
                :hit_rate, :avg_forward_return, :median_forward_return, :weighted_avg_forward_return,
                :avg_label_count, :effective_weight, :methodology_version, :scope,
                :first_as_of, :last_as_of, CURRENT_TIMESTAMP
            )
            """,
            rows,
        )
        if own_conn:
            conn.commit()
        stats = {"profiles": profile_id or "active", "rows": len(rows), "min_observations": min_observations, "lookback_days": lookback_days}
        log(f"label_evaluation materialized rows={len(rows)}", module="label_evaluation")
        return stats
    finally:
        if own_conn:
            conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize label evaluation evidence from historical labels and forward returns.")
    parser.add_argument("--profile-id", default=None)
    parser.add_argument("--min-observations", type=int, default=20)
    parser.add_argument("--lookback-days", type=int, default=1825)
    args = parser.parse_args()
    generate_and_store(profile_id=args.profile_id, min_observations=args.min_observations, lookback_days=args.lookback_days)


if __name__ == "__main__":
    main()
