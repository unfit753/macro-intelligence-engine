"""Generate one prediction end-to-end.

Usage:
    PYTHONPATH=. .venv/bin/python -m src.inference.predict \\
        --asset GC=F --horizon 1m [--as-of YYYY-MM-DD] [--k 5]

What it does:
    1. Build the analogue set (Qdrant) for the target date.
    2. Render the Markdown brief with current signals + analogues + events.
    3. Call Claude with a forced `submit_forecast` tool that pins the JSON shape.
    4. Hash the input, write the row to `predictions`, print the rationale.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import sys

from anthropic import Anthropic
from dotenv import load_dotenv

from config.config_fetch import CLAUDE_MODEL, DB_PATH, log
from src.core.db import connect_writable
from src.retrieval.query import fetch_analogues
from src.inference.prompt import render_brief, HORIZON_DESC
from src.intelligence.context_packs import build_context_pack, render_context_prompt, store_context_pack


MODEL = CLAUDE_MODEL
SYSTEM_PROMPT = (
    "You are a macro analyst. You are given a named Macro Intelligence Engine prediction context "
    "pack, not a raw historical dump. The pack uses semantic data-contract "
    "names shared by the database, frontend, and prompts. Treat atlas map "
    "reads, latest macro releases, next catalysts, asset signals, relevant "
    "GDELT/risk streams, source-health exceptions, and historical analogue "
    "references as the full available context. Historical rows are reference "
    "handles only: do not invent values outside the provided analogue dates and "
    "keys. Reason explicitly from these named sections, discount stale sources, "
    "and lower confidence when analogue evidence disagrees.\n\n"
    "Important: the brief displays returns as percentages (e.g. '+2.18%') but "
    "the submit_forecast tool expects DECIMAL FRACTIONS in expected_return_low "
    "and expected_return_high. Convert: +2.18% → 0.0218; -5% → -0.05; +10% → 0.10. "
    "Both bounds must lie in [-1, 1]. Submit a single forecast via the tool."
)

FORECAST_TOOL = {
    "name": "submit_forecast",
    "description": "Submit your directional forecast for the asset/horizon.",
    "input_schema": {
        "type": "object",
        "properties": {
            "direction": {
                "type": "string", "enum": ["up", "down", "flat"],
                "description": "Net directional view over the horizon.",
            },
            "confidence_0_1": {
                "type": "number", "minimum": 0, "maximum": 1,
                "description": "How confident you are in the directional call.",
            },
            "expected_return_low": {
                "type": "number", "minimum": -1.0, "maximum": 1.0,
                "description": (
                    "Lower bound of plausible total return over the horizon, "
                    "as a DECIMAL FRACTION (not a percentage). "
                    "Examples: -0.05 means -5%, 0.10 means +10%. "
                    "Must be between -1.0 and +1.0."
                ),
            },
            "expected_return_high": {
                "type": "number", "minimum": -1.0, "maximum": 1.0,
                "description": (
                    "Upper bound of plausible total return over the horizon, "
                    "as a DECIMAL FRACTION (not a percentage). "
                    "Examples: 0.05 means +5%, 0.10 means +10%. "
                    "Must be between -1.0 and +1.0 and >= expected_return_low."
                ),
            },
            "rationale_md": {
                "type": "string",
                "description": "2-5 paragraphs of reasoning, in Markdown. Cite specific signals and analogues.",
            },
            "key_risks": {
                "type": "array", "items": {"type": "string"},
                "description": "3-6 bullet-style risks that would invalidate this view.",
            },
            "analogues_used": {
                "type": "array", "items": {"type": "string"},
                "description": "Subset of the analogue dates from the brief that you actually relied on.",
            },
        },
        "required": [
            "direction", "confidence_0_1", "expected_return_low",
            "expected_return_high", "rationale_md", "key_risks", "analogues_used",
        ],
    },
}


def hash_input(brief: str) -> str:
    return hashlib.sha256(brief.encode("utf-8")).hexdigest()[:16]


def _validate(forecast: dict) -> None:
    """Catch the kind of mistake the schema description is meant to prevent."""
    forecast["key_risks"] = _coerce_text_list(forecast.get("key_risks"), max_items=6)
    forecast["analogues_used"] = _coerce_text_list(forecast.get("analogues_used"), max_items=8)
    lo = float(forecast["expected_return_low"])
    hi = float(forecast["expected_return_high"])
    if not (-1.0 <= lo <= 1.0 and -1.0 <= hi <= 1.0):
        raise ValueError(
            f"expected_return out of decimal range "
            f"(low={lo}, high={hi}); model likely sent percent."
        )
    if lo > hi:
        raise ValueError(f"expected_return_low ({lo}) > high ({hi})")
    if not (0.0 <= float(forecast["confidence_0_1"]) <= 1.0):
        raise ValueError(f"confidence out of [0,1]: {forecast['confidence_0_1']}")
    if not forecast["key_risks"]:
        forecast["key_risks"] = ["No explicit key risks returned; review the rationale before relying on this call."]
    if not forecast["analogues_used"]:
        forecast["analogues_used"] = ["No explicit analogue dates returned."]


def _coerce_text_list(value, *, max_items: int) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        lines = []
        for line in raw.splitlines():
            clean = line.strip()
            clean = re.sub(r"^\d+[\.)]\s*", "", clean.lstrip("-*• \t").strip())
            if clean:
                lines.append(clean)
        items = lines or [raw]
    else:
        items = [str(value)]
    cleaned: list[str] = []
    for item in items:
        text = str(item).strip()
        text = re.sub(r"^\d+[\.)]\s*", "", text.lstrip("-*• \t").strip())
        if text and text not in cleaned:
            cleaned.append(text)
        if len(cleaned) >= max_items:
            break
    return cleaned


def _extract_tool_use(msg) -> tuple[str, dict] | None:
    for block in msg.content:
        if block.type == "tool_use" and block.name == "submit_forecast":
            return block.id, block.input
    return None


def prepare_prediction_prompt(
    conn: sqlite3.Connection,
    asset: str,
    horizon: str,
    as_of: dt.date,
    analogues,
    asset_name: str | None,
    prompt_mode: str,
) -> tuple[str, str, str, int]:
    legacy_brief = render_brief(conn, asset, horizon, as_of, analogues, asset_name or "")
    pack = build_context_pack(conn, asset, horizon, as_of, analogues, asset_name=asset_name or "")
    pack_prompt = render_context_prompt(pack)
    pack_id = store_context_pack(conn, pack, pack_prompt)
    selected_prompt = pack_prompt if prompt_mode == "pack" else legacy_brief
    return selected_prompt, legacy_brief, pack_prompt, pack_id


def call_claude(brief: str) -> dict:
    """Call Claude. If the model returns out-of-range values (a frequent
    percent-vs-decimal slip), reply with the validator error and retry once."""
    client = Anthropic()
    messages = [{"role": "user", "content": brief}]

    msg = client.messages.create(
        model=MODEL, max_tokens=4096, system=SYSTEM_PROMPT,
        tools=[FORECAST_TOOL],
        tool_choice={"type": "tool", "name": "submit_forecast"},
        messages=messages,
    )
    tool = _extract_tool_use(msg)
    if not tool:
        raise RuntimeError(f"No tool_use in response: {msg.stop_reason}")
    tool_id, forecast = tool

    try:
        _validate(forecast)
        return forecast
    except ValueError as err:
        log(f"validation failed, retrying once: {err}", module="predict")

    # Retry: hand the model its own previous tool_use and a corrective tool_result.
    messages += [
        {"role": "assistant", "content": msg.content},
        {"role": "user", "content": [{
            "type": "tool_result", "tool_use_id": tool_id, "is_error": True,
            "content": (
                f"Validation failed: {err}. "
                "Convert percentages to decimal fractions: e.g. -2% becomes "
                "-0.02, +10% becomes 0.10. Both bounds must be in [-1, 1]. "
                "Resubmit via submit_forecast."
            ),
        }]},
    ]
    msg2 = client.messages.create(
        model=MODEL, max_tokens=4096, system=SYSTEM_PROMPT,
        tools=[FORECAST_TOOL],
        tool_choice={"type": "tool", "name": "submit_forecast"},
        messages=messages,
    )
    tool2 = _extract_tool_use(msg2)
    if not tool2:
        raise RuntimeError(f"Retry: no tool_use in response: {msg2.stop_reason}")
    forecast2 = tool2[1]
    _validate(forecast2)
    return forecast2


def write_prediction(asset: str, horizon: str, as_of: dt.date,
                     forecast: dict, brief_hash: str, brief: str, model: str) -> int:
    conn = connect_writable(DB_PATH)
    try:
        cur = conn.execute(
            """INSERT OR REPLACE INTO predictions (
                 asset, horizon, as_of, generated_at,
                 direction, confidence,
                 expected_return_low, expected_return_high,
                 rationale_md, key_risks, analogues_used,
                 model, input_hash, input_brief_md
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                asset, horizon, as_of.isoformat(),
                dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
                forecast["direction"], float(forecast["confidence_0_1"]),
                float(forecast["expected_return_low"]),
                float(forecast["expected_return_high"]),
                forecast["rationale_md"],
                json.dumps(forecast["key_risks"]),
                json.dumps(forecast["analogues_used"]),
                model, brief_hash, brief,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    load_dotenv(override=True)
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set — load .env first.", file=sys.stderr)
        return 2

    p = argparse.ArgumentParser()
    p.add_argument("--asset", required=True, help="e.g. GC=F, CL=F, SPY")
    p.add_argument("--horizon", required=True, choices=list(HORIZON_DESC.keys()))
    p.add_argument("--as-of", default=None, help="YYYY-MM-DD; defaults to today")
    p.add_argument("--k", type=int, default=5, help="number of analogues")
    p.add_argument("--asset-name", default="", help="display name for the brief")
    p.add_argument("--prompt-mode", choices=("legacy", "shadow", "pack"), default="shadow", help="legacy/shadow send the legacy brief; shadow also stores a context pack. pack sends the rendered context-pack prompt.")
    args = p.parse_args(argv)

    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else dt.date.today()

    log(f"Fetching analogues for {args.asset} / {args.horizon} as of {as_of}...", module="predict")
    analogues = fetch_analogues(as_of, asset=args.asset, horizon=args.horizon, k=args.k)
    if not analogues:
        print("No analogues retrieved — aborting.", file=sys.stderr)
        return 1

    conn = connect_writable(DB_PATH)
    try:
        brief, legacy_brief, pack_prompt, pack_id = prepare_prediction_prompt(
            conn, args.asset, args.horizon, as_of, analogues, args.asset_name, args.prompt_mode
        )
        conn.commit()
    finally:
        conn.close()

    if args.prompt_mode == "pack":
        log(f"Context-pack prompt built ({len(pack_prompt):,} chars, pack #{pack_id or 'n/a'}). Calling {MODEL}...", module="predict")
    else:
        log(
            f"Legacy brief built ({len(legacy_brief):,} chars); shadow context pack #{pack_id or 'n/a'} stored ({len(pack_prompt):,} chars). Calling {MODEL}...",
            module="predict",
        )
    forecast = call_claude(brief)

    pred_id = write_prediction(
        args.asset, args.horizon, as_of, forecast,
        hash_input(brief), brief, MODEL,
    )
    log(f"Prediction #{pred_id} written.", module="predict")

    print()
    print("=" * 72)
    print(f"  {args.asset_name or args.asset}  /  {args.horizon}  /  as of {as_of}")
    print("=" * 72)
    print(f"  Direction:        {forecast['direction']}")
    print(f"  Confidence:       {forecast['confidence_0_1']:.2f}")
    print(f"  Expected range:   {forecast['expected_return_low']*100:+.2f}% .. "
          f"{forecast['expected_return_high']*100:+.2f}%")
    print(f"  Analogues used:   {', '.join(forecast['analogues_used'])}")
    print()
    print(forecast["rationale_md"])
    print()
    print("  Key risks:")
    for risk in forecast["key_risks"]:
        print(f"   • {risk}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
