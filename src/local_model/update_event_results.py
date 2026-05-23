"""Use a local model to add short reads to released macro-event results.

Numeric facts come from official/deterministic fetchers. The local model only
turns an already-matched result into a compact client note.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from typing import Any

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.core.db import connect_writable
from src.local_model.ollama_client import DEFAULT_HOST, DEFAULT_MODEL, generate_json, unload_model


SYSTEM_PROMPT = """You are Macro Intelligence Engine' local macro-result note writer.
You receive one macro event, its expected/context text and official actual
values that were already matched by deterministic code.
Do not invent numbers. Do not give personalized investment advice.
Do not say above expectations, below expectations, hot, cool, beat, miss or
surprise unless the input explicitly contains a numeric consensus/expected
value and an actual_surprise label. If consensus is not structured, state that
the surprise cannot be quantified from stored consensus.
Avoid portfolio action language such as buy, sell, overweight, underweight,
long or short.
Return strict JSON with a concise client note."""

RESULT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "result_read": {"type": "string"},
        "market_implication": {"type": "string"},
        "watch_next": {"type": "string"},
        "confidence_0_1": {"type": "number"},
    },
    "required": ["result_read", "market_implication", "watch_next", "confidence_0_1"],
}


def _prompt(row: sqlite3.Row) -> str:
    payload = {
        "title": row["title"],
        "region": row["region"],
        "category": row["category"],
        "release_date": row["release_date"],
        "expected_or_context": row["expected"],
        "actual_summary": row["actual_summary"],
        "actual_surprise": row["actual_surprise"],
        "scenario_ladder": json.loads(row["scenario_json"] or "[]"),
        "affected_assets": json.loads(row["affected_assets_json"] or "[]"),
    }
    return (
        "Write a compact result update for frontend clients.\n"
        "Use only the provided actual_summary for facts. The expected_or_context field is "
        "often prose, not consensus. If there is no explicit numeric consensus and no "
        "actual_surprise label beyond actual_available_consensus_unstructured, say the "
        "surprise cannot be quantified from stored consensus. Describe market pressure, "
        "not trade instructions.\n\n"
        f"{json.dumps(payload, indent=2, sort_keys=True)}"
    )


def _format_note(parsed: dict[str, Any]) -> str:
    read = str(parsed.get("result_read") or "").strip()
    implication = str(parsed.get("market_implication") or "").strip()
    watch = str(parsed.get("watch_next") or "").strip()
    parts = [x for x in [read, implication, f"Watch: {watch}" if watch else ""] if x]
    return " ".join(parts)[:900]


def update_notes(model: str = DEFAULT_MODEL, host: str = DEFAULT_HOST,
                 limit: int = 10, force: bool = False,
                 unload: bool = False) -> int:
    conn = connect_writable(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        extra = "" if force else "AND (local_model_summary IS NULL OR local_model_summary = '')"
        rows = conn.execute(
            f"""SELECT id, title, region, category, release_date, expected,
                       actual_summary, actual_surprise, scenario_json,
                       affected_assets_json
                FROM macro_event_predictions
                WHERE actual_summary IS NOT NULL
                  AND actual_summary != ''
                  {extra}
                ORDER BY release_date DESC, importance DESC
                LIMIT ?""",
            (limit,),
        ).fetchall()
        updated = 0
        for row in rows:
            try:
                result = generate_json(
                    _prompt(row),
                    system=SYSTEM_PROMPT,
                    model=model,
                    host=host,
                    schema=RESULT_SCHEMA,
                    temperature=0.1,
                    num_ctx=4096,
                    keep_alive="30s",
                )
                if not result.parsed:
                    raise RuntimeError("local model returned non-JSON result")
                note = _format_note(result.parsed)
                conn.execute(
                    """UPDATE macro_event_predictions
                       SET local_model_summary = ?,
                           local_model_model = ?,
                           local_model_at = ?
                       WHERE id = ?""",
                    (
                        note,
                        model,
                        dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
                        int(row["id"]),
                    ),
                )
                updated += 1
            except Exception as exc:
                log(f"local macro-result note failed for {row['id']}: {exc}", module="local_model")
        conn.commit()
        log(f"Updated {updated} local macro-result note(s).", module="local_model")
        return updated
    finally:
        conn.close()
        if unload:
            unload_model(model, host)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--unload", action="store_true")
    args = parser.parse_args(argv)
    update_notes(args.model, args.host, args.limit, args.force, args.unload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
