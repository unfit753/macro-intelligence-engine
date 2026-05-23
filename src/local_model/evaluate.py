"""Evaluate a local small model as a Macro Intelligence Engine impact interpreter.

This is intentionally not a market predictor yet. The first job for a local
model is cheaper and safer: read cleaned atlas evidence, classify its macro
theme/direction, and identify affected assets. If it cannot do that reliably,
fine-tuning it for prediction would only teach it to sound confident.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from config.config_fetch import DB_PATH, log
from config.db_setup import SCHEMA, add_missing_columns
from src.local_model.ollama_client import DEFAULT_HOST, DEFAULT_MODEL, generate_json, unload_model


MODEL_ROLE = "oracle_impact_interpreter_v1"

THEME_OPTIONS = [
    "inflation", "central_bank", "interest", "monetary", "gdp", "growth",
    "labour", "trade", "currency", "energy", "oil_price", "conflict", "war",
    "sanctions", "disaster", "weather", "political", "politics", "banking",
    "debt", "stockmarket", "risk",
]

DIRECTION_OPTIONS = [
    "inflation-risk", "rates-volatility", "growth-watch", "trade-friction",
    "fx-volatility", "energy-upside-risk", "risk-off", "supply-risk",
    "policy-risk", "credit-risk", "risk-appetite-watch", "macro-pressure",
]

MARKET_BIAS_OPTIONS = [
    "bullish", "bearish", "risk-on", "risk-off", "inflation-up",
    "rates-up", "growth-down", "oil-up", "usd-up", "mixed", "neutral",
    "volatility-up",
]

ASSET_OPTIONS = [
    "SPY", "TLT", "GC=F", "CL=F", "BZ=F", "DX-Y.NYB", "^OMX",
    "^STOXX50E", "^GDAXI", "^FCHI", "^FTSE", "^N225", "^HSI",
    "EURUSD=X", "SEK=X",
]

IMPACT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "theme": {"type": "string", "enum": THEME_OPTIONS},
        "direction": {"type": "string", "enum": DIRECTION_OPTIONS},
        "market_bias": {"type": "string", "enum": MARKET_BIAS_OPTIONS},
        "horizon": {"type": "string"},
        "affected_regions": {"type": "array", "items": {"type": "string"}},
        "affected_sectors": {"type": "array", "items": {"type": "string"}},
        "affected_assets": {"type": "array", "items": {"type": "string", "enum": ASSET_OPTIONS}},
        "severity_0_5": {"type": "number"},
        "confidence_0_1": {"type": "number"},
        "source_quality": {"type": "string"},
        "rationale": {"type": "string"},
    },
    "required": [
        "theme",
        "direction",
        "market_bias",
        "horizon",
        "affected_regions",
        "affected_sectors",
        "affected_assets",
        "severity_0_5",
        "confidence_0_1",
        "source_quality",
        "rationale",
    ],
}

SYSTEM_PROMPT = """You are Macro Intelligence Engine' local macro-impact interpreter.
You receive cleaned evidence rows from a macro-intelligence system.
Return strict JSON only. Do not give investment advice.
Classify what the evidence most likely means for macro direction, regions,
sectors and broad market assets. Prefer plain market language over vague labels.
If evidence is weak, stale or ambiguous, lower confidence and say so in
source_quality.

You must use Macro Intelligence Engine canonical labels exactly:
- theme: one of {themes}
- direction: one of {directions}
- market_bias: one of {biases}
- affected_assets: ticker symbols only, chosen from {assets}

severity_0_5 uses a 0 to 5 scale, where 0 means negligible macro impact and
5 means severe cross-market impact. confidence_0_1 uses a 0 to 1 scale.""".format(
    themes=", ".join(THEME_OPTIONS),
    directions=", ".join(DIRECTION_OPTIONS),
    biases=", ".join(MARKET_BIAS_OPTIONS),
    assets=", ".join(ASSET_OPTIONS),
)


@dataclass(frozen=True)
class EvalCase:
    as_of: str
    source_table: str
    source_id: int
    prompt_input: dict[str, Any]
    expected: dict[str, Any]


def _hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _json_load(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return fallback


def _norm(value: str | None) -> str:
    return str(value or "").strip().lower().replace("_", "-").replace(" ", "-")


def _asset_overlap(predicted: Any, expected: Any) -> bool | None:
    if not isinstance(predicted, list) or not isinstance(expected, list):
        return None
    pred = {str(x).upper() for x in predicted if x}
    exp = {str(x).upper() for x in expected if x}
    if not exp:
        return None
    return bool(pred & exp)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    add_missing_columns(conn)


def load_world_pull_cases(conn: sqlite3.Connection, limit: int, as_of: str | None = None) -> list[EvalCase]:
    latest = as_of or conn.execute("SELECT MAX(as_of) FROM intelligence_packages").fetchone()[0]
    if not latest:
        return []
    rows = conn.execute(
        """SELECT id, as_of, scope_type, scope, parent_scope, theme, direction,
                  severity, confidence, freshness, horizon, evidence_json,
                  affected_assets_json, source_refs_json
           FROM intelligence_packages
           WHERE as_of = ?
           ORDER BY severity DESC, confidence DESC
           LIMIT ?""",
        (latest, limit),
    ).fetchall()
    cases: list[EvalCase] = []
    for row in rows:
        (
            source_id, row_as_of, scope_type, scope, parent_scope, theme,
            direction, severity, confidence, freshness, horizon, evidence_json,
            affected_assets_json, source_refs_json,
        ) = row
        evidence = _json_load(evidence_json, [])
        slim_evidence = [
            {
                "source": item.get("source"),
                "title": item.get("title"),
                "detail": item.get("detail"),
                "weight": item.get("weight"),
                "date": item.get("date"),
            }
            for item in evidence[:6]
            if isinstance(item, dict)
        ]
        prompt_input = {
            "scope": scope,
            "scope_type": scope_type,
            "parent_scope": parent_scope,
            "freshness": freshness,
            "evidence": slim_evidence,
            "source_refs": _json_load(source_refs_json, [])[:4],
        }
        expected = {
            "theme": theme,
            "direction": direction,
            "horizon": horizon,
            "affected_assets": _json_load(affected_assets_json, []),
            "severity_0_5": severity,
            "confidence_0_1": confidence,
        }
        cases.append(EvalCase(row_as_of, "intelligence_packages", int(source_id), prompt_input, expected))
    return cases


def render_prompt(case: EvalCase) -> str:
    return (
        "Analyze this Macro Intelligence Engine evidence package and classify its likely macro/market impact.\n"
        "Use the evidence only; do not assume hidden data.\n\n"
        "Return only canonical Macro Intelligence Engine labels and ticker symbols. If the source is too local "
        "for global tickers, use the closest broad proxy and lower confidence.\n\n"
        f"{json.dumps(case.prompt_input, indent=2, sort_keys=True)}"
    )


def create_run(conn: sqlite3.Connection, name: str, model: str, endpoint: str, config: dict[str, Any]) -> int:
    cur = conn.execute(
        """INSERT INTO local_model_runs
           (name, created_at, status, model, endpoint, config_json, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            name,
            dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "running",
            model,
            endpoint,
            json.dumps(config, sort_keys=True),
            "Local model impact-classification bakeoff. Not used for live predictions.",
        ),
    )
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str) -> None:
    conn.execute(
        "UPDATE local_model_runs SET finished_at = ?, status = ? WHERE id = ?",
        (dt.datetime.now(dt.UTC).isoformat(timespec="seconds"), status, run_id),
    )


def store_result(
    conn: sqlite3.Connection,
    run_id: int,
    case: EvalCase,
    prompt_hash: str,
    response_json: dict[str, Any] | None,
    latency_ms: int | None,
    error: str | None,
) -> None:
    parse_ok = response_json is not None
    theme_hit = direction_hit = affected_hit = None
    if response_json:
        theme_hit = int(_norm(response_json.get("theme")) == _norm(case.expected.get("theme")))
        direction_hit = int(_norm(response_json.get("direction")) == _norm(case.expected.get("direction")))
        overlap = _asset_overlap(response_json.get("affected_assets"), case.expected.get("affected_assets"))
        affected_hit = None if overlap is None else int(overlap)
    conn.execute(
        """INSERT INTO local_model_evaluations (
             run_id, created_at, as_of, source_table, source_id, prompt_hash,
             input_json, expected_json, response_json, parse_ok, theme_hit,
             direction_hit, affected_hit, latency_ms, error
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            case.as_of,
            case.source_table,
            case.source_id,
            prompt_hash,
            json.dumps(case.prompt_input, sort_keys=True),
            json.dumps(case.expected, sort_keys=True),
            json.dumps(response_json, sort_keys=True) if response_json else None,
            int(parse_ok),
            theme_hit,
            direction_hit,
            affected_hit,
            latency_ms,
            error,
        ),
    )


def summarize(conn: sqlite3.Connection, run_id: int) -> dict[str, Any]:
    row = conn.execute(
        """SELECT COUNT(*), AVG(parse_ok), AVG(theme_hit), AVG(direction_hit),
                  AVG(affected_hit), AVG(latency_ms)
           FROM local_model_evaluations
           WHERE run_id = ?""",
        (run_id,),
    ).fetchone()
    total, parse_ok, theme_hit, direction_hit, affected_hit, latency = row
    return {
        "run_id": run_id,
        "cases": int(total or 0),
        "parse_ok": float(parse_ok or 0),
        "theme_hit": float(theme_hit or 0),
        "direction_hit": float(direction_hit or 0),
        "affected_hit": float(affected_hit or 0),
        "avg_latency_ms": int(latency or 0),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--limit", type=int, default=12)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--num-ctx", type=int, default=4096)
    parser.add_argument("--keep-alive", default="30s")
    parser.add_argument("--unload", action="store_true", help="Unload model from Ollama when finished.")
    args = parser.parse_args(argv)

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_schema(conn)
        cases = load_world_pull_cases(conn, limit=args.limit, as_of=args.as_of)
        if not cases:
            print("No intelligence_packages rows available. Run src.intelligence.world_pull first.")
            return 2
        config = {
            "role": MODEL_ROLE,
            "limit": args.limit,
            "as_of": args.as_of,
            "num_ctx": args.num_ctx,
            "keep_alive": args.keep_alive,
            "schema": "impact_schema_v1",
        }
        run_id = create_run(conn, "local-qwen-impact-eval", args.model, args.host, config)
        conn.commit()
        status = "completed"
        try:
            for i, case in enumerate(cases, start=1):
                prompt = render_prompt(case)
                prompt_hash = _hash({"system": SYSTEM_PROMPT, "prompt": prompt, "schema": IMPACT_SCHEMA})
                try:
                    result = generate_json(
                        prompt,
                        system=SYSTEM_PROMPT,
                        model=args.model,
                        host=args.host,
                        schema=IMPACT_SCHEMA,
                        num_ctx=args.num_ctx,
                        keep_alive=args.keep_alive,
                    )
                    store_result(conn, run_id, case, prompt_hash, result.parsed, result.latency_ms, None)
                    print(f"[{i}/{len(cases)}] {case.prompt_input['scope']} parse={bool(result.parsed)}")
                except Exception as exc:
                    status = "completed_with_errors"
                    store_result(conn, run_id, case, prompt_hash, None, None, str(exc))
                    print(f"[{i}/{len(cases)}] {case.prompt_input['scope']} ERROR {exc}")
                conn.commit()
        finally:
            finish_run(conn, run_id, status)
            conn.commit()
            if args.unload:
                unload_model(args.model, args.host)
        summary = summarize(conn, run_id)
        log(f"Local model eval summary: {summary}", module="local_model")
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["cases"] else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
