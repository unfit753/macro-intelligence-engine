"""Read-only label and weight resolution for backend data references."""
from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd

from src.core.db import connect_readonly, table_exists


LABEL_COLUMNS = [
    "label_id", "label_type", "label", "description",
    "default_weight", "active", "created_at", "updated_at",
]

PROFILE_COLUMNS = [
    "profile_id", "name", "description", "active", "created_at", "updated_at",
]

WEIGHTED_LABEL_COLUMNS = [
    "label_id", "label_type", "label", "description", "default_weight",
    "profile_id", "profile_weight", "effective_weight", "active",
    "created_at", "updated_at",
]

ASSIGNMENT_COLUMNS = [
    "id", "label_id", "label_type", "label", "description",
    "target_type", "target_table", "target_column", "target_value",
    "default_weight", "weight_override", "profile_id", "profile_weight",
    "effective_weight",
    "confidence", "notes", "active", "created_at", "updated_at",
]


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def label_catalog(conn: sqlite3.Connection | None = None, active_only: bool = True) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "data_labels"):
            return _empty(LABEL_COLUMNS)
        where = "WHERE active = 1" if active_only else ""
        return pd.read_sql(
            f"""SELECT label_id, label_type, label, description,
                       default_weight, active, created_at, updated_at
                FROM data_labels
                {where}
                ORDER BY label_type, label""",
            conn,
        )
    finally:
        if own_conn:
            conn.close()


def label_weight_profiles(conn: sqlite3.Connection | None = None, active_only: bool = True) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "label_weight_profiles"):
            return _empty(PROFILE_COLUMNS)
        where = "WHERE active = 1" if active_only else ""
        return pd.read_sql(
            f"""SELECT profile_id, name, description, active, created_at, updated_at
                FROM label_weight_profiles
                {where}
                ORDER BY CASE profile_id WHEN 'default' THEN 0 ELSE 1 END, name""",
            conn,
        )
    finally:
        if own_conn:
            conn.close()


def weighted_label_catalog(
    conn: sqlite3.Connection | None = None,
    profile_id: str | None = None,
    active_only: bool = True,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "data_labels"):
            return _empty(WEIGHTED_LABEL_COLUMNS)
        use_profile = bool(profile_id and table_exists(conn, "label_weight_overrides"))
        profile_join = ""
        profile_select = "NULL AS profile_id, NULL AS profile_weight"
        weight_expr = "COALESCE(l.default_weight, 1.0)"
        params: list[Any] = []
        if use_profile:
            profile_join = (
                "LEFT JOIN label_weight_overrides o "
                "ON o.label_id = l.label_id AND o.profile_id = ? AND o.active = 1"
            )
            profile_select = "? AS profile_id, o.weight AS profile_weight"
            weight_expr = "COALESCE(o.weight, l.default_weight, 1.0)"
            params.extend([profile_id, profile_id])
        where = "WHERE l.active = 1" if active_only else ""
        return pd.read_sql(
            f"""SELECT l.label_id, l.label_type, l.label, l.description,
                       l.default_weight, {profile_select},
                       {weight_expr} AS effective_weight,
                       l.active, l.created_at, l.updated_at
                FROM data_labels l
                {profile_join}
                {where}
                ORDER BY l.label_type, l.label""",
            conn, params=params,
        )
    finally:
        if own_conn:
            conn.close()


def label_assignments(
    conn: sqlite3.Connection | None = None,
    target_type: str | None = None,
    target_table: str | None = None,
    target_column: str | None = None,
    target_value: str | None = None,
    label_type: str | None = None,
    active_only: bool = True,
    profile_id: str | None = None,
) -> pd.DataFrame:
    own_conn = conn is None
    conn = conn or connect_readonly()
    try:
        if not table_exists(conn, "data_label_assignments") or not table_exists(conn, "data_labels"):
            return _empty(ASSIGNMENT_COLUMNS)
        use_profile = bool(profile_id and table_exists(conn, "label_weight_overrides"))
        profile_join = ""
        profile_select = "NULL AS profile_id, NULL AS profile_weight"
        weight_expr = "COALESCE(a.weight_override, l.default_weight, 1.0)"
        clauses: list[str] = []
        params: list[Any] = []
        if use_profile:
            profile_join = (
                "LEFT JOIN label_weight_overrides o "
                "ON o.label_id = a.label_id AND o.profile_id = ? AND o.active = 1"
            )
            profile_select = "? AS profile_id, o.weight AS profile_weight"
            weight_expr = "COALESCE(o.weight, a.weight_override, l.default_weight, 1.0)"
            params.extend([profile_id, profile_id])
        if active_only:
            clauses.append("a.active = 1")
            clauses.append("l.active = 1")
        for col, value in (
            ("a.target_type", target_type),
            ("a.target_table", target_table),
            ("a.target_column", target_column),
            ("a.target_value", target_value),
            ("l.label_type", label_type),
        ):
            if value is not None:
                clauses.append(f"COALESCE({col}, '') = ?")
                params.append(value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return pd.read_sql(
            f"""SELECT a.id, a.label_id, l.label_type, l.label, l.description,
                       a.target_type,
                       NULLIF(a.target_table, '') AS target_table,
                       NULLIF(a.target_column, '') AS target_column,
                       NULLIF(a.target_value, '') AS target_value,
                       l.default_weight, a.weight_override,
                       {profile_select},
                       {weight_expr} AS effective_weight,
                       a.confidence, a.notes, a.active, a.created_at, a.updated_at
                FROM data_label_assignments a
                JOIN data_labels l ON l.label_id = a.label_id
                {profile_join}
                {where}
                ORDER BY a.target_type, a.target_table, a.target_column,
                         a.target_value, l.label_type, l.label""",
            conn, params=params,
        )
    finally:
        if own_conn:
            conn.close()


def labels_for_target(
    conn: sqlite3.Connection | None,
    target_type: str,
    target_table: str | None = None,
    target_column: str | None = None,
    target_value: str | None = None,
    profile_id: str | None = None,
) -> list[dict[str, Any]]:
    rows = label_assignments(
        conn,
        target_type=target_type,
        target_table=target_table,
        target_column=target_column,
        target_value=target_value,
        profile_id=profile_id,
    )
    if rows.empty:
        return []
    return rows.where(pd.notna(rows), None).to_dict(orient="records")


def effective_weights_for_target(
    conn: sqlite3.Connection | None,
    target_type: str,
    target_table: str | None = None,
    target_column: str | None = None,
    target_value: str | None = None,
    profile_id: str | None = None,
) -> pd.DataFrame:
    rows = label_assignments(
        conn,
        target_type=target_type,
        target_table=target_table,
        target_column=target_column,
        target_value=target_value,
        profile_id=profile_id,
    )
    if rows.empty:
        return _empty(["label_id", "label_type", "label", "effective_weight", "confidence"])
    return rows[["label_id", "label_type", "label", "effective_weight", "confidence"]].copy()


def target_weight(
    conn: sqlite3.Connection | None,
    target_type: str,
    target_table: str | None = None,
    target_column: str | None = None,
    target_value: str | None = None,
    profile_id: str | None = None,
    fallback: float = 1.0,
) -> float:
    weights = effective_weights_for_target(conn, target_type, target_table, target_column, target_value, profile_id)
    if weights.empty:
        return fallback
    return float(weights["effective_weight"].fillna(fallback).max())
