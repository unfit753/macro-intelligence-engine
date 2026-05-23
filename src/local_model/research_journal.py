"""Macro Intelligence Engine research-journal annotations for high-impact data changes.

This worker is intentionally annotation-only. It creates compact notes for the
next human/Claude review loop and never updates predictions.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from config.config_fetch import log
from src.core.db import connect_writable, table_exists

MODEL_NAME = "deterministic_research_journal_v1"
MIN_PRIORITY = 4.6
REVIEW_TYPES_BY_OBJECT = {
    "current.event": "current_event_journal",
    "macro.release.actual": "macro_release_journal",
    "gdelt.stream": "current_event_journal",
    "source.run": "current_event_journal",
    "prediction.context": "prediction_journal",
}
REPO_ROOT = Path(__file__).resolve().parents[2]
STREAM_WORDS = {
    "economy_news": "Economy",
    "policy_rates": "Rates",
    "major_disaster": "Disaster",
    "political_risk": "Politics",
    "conflict_security": "Conflict / security",
    "trade_sanctions_supply": "Trade / sanctions",
    "energy_commodities": "Oil / energy",
    "market_stress": "Market stress",
}
ATLAS_JOURNAL = REPO_ROOT / "MacroAtlas_journal.md"
CLAUDE_JOURNAL = REPO_ROOT / "Claude_journal.md"


def _json_loads(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return fallback


def _compact_list(value: Any, limit: int = 4) -> list[str]:
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list):
        return []
    return [str(v) for v in value[:limit] if str(v).strip()]


def _labels_to_assets(labels: list[str], metadata: dict[str, Any]) -> list[str]:
    explicit = _compact_list(metadata.get("affected_assets") or metadata.get("assets"), limit=6)
    if explicit:
        return explicit
    label_text = " ".join(labels).lower()
    assets: list[str] = []
    if "sp500" in label_text or "stockmarket" in label_text:
        assets.extend(["SPY", "^VIX"])
    if "gold" in label_text:
        assets.append("GLD")
    if "oil" in label_text or "energy" in label_text:
        assets.extend(["CL=F", "XLE", "SPY"])
    if "bank" in label_text or "market_stress" in label_text:
        assets.extend(["XLF", "SPY", "^VIX"])
    if "inflation" in label_text or "macro" in label_text:
        assets.extend(["SPY", "TLT", "USD"])
    if "conflict" in label_text or "geopolitical" in label_text:
        assets.extend(["SPY", "GLD", "CL=F"])
    return list(dict.fromkeys(assets))[:6]


def _stream_word(value: Any) -> str:
    raw = str(value or "").strip()
    key = raw.lower().replace(" ", "_")
    return STREAM_WORDS.get(key, raw.replace("_", " ").title() if raw else "News")


def _friendly_title(raw: Any, metadata: dict[str, Any]) -> str:
    title = str(raw or "").strip()
    if " pulse: " in title.lower():
        left, right = title.split(":", 1)
        return f"{_stream_word(left.lower().replace(' pulse', '').strip().replace(' ', '_'))} {right.strip()}"
    if title in STREAM_WORDS or title.lower() in STREAM_WORDS:
        scope = metadata.get("region") or metadata.get("country") or "signal"
        return f"{_stream_word(title)} {scope}"
    return title


def _title(row: sqlite3.Row, metadata: dict[str, Any]) -> str:
    raw = (
        metadata.get("title")
        or metadata.get("event_title")
        or metadata.get("release_key")
        or metadata.get("stream")
        or row["event_key"]
    )
    return _friendly_title(raw, metadata)


def _scope(metadata: dict[str, Any]) -> str:
    for key in ("canonical_scope", "country", "region"):
        value = str(metadata.get(key) or "").strip()
        if value:
            return value
    return "Global"


def _article_count(metadata: dict[str, Any]) -> int | None:
    try:
        value = int(float(metadata.get("article_count") or metadata.get("count") or 0))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _what_happened(title: str, metadata: dict[str, Any], object_id: str) -> str:
    summary = str(metadata.get("summary") or metadata.get("comment") or "").strip()
    if summary and not summary.lower().endswith("needs a look."):
        return summary
    stream = metadata.get("stream")
    count = _article_count(metadata)
    scope = _scope(metadata)
    if stream and count:
        return f"{count} recent {_stream_word(stream).lower()} articles clustered in {scope}."
    if stream:
        return f"A high-priority {_stream_word(stream).lower()} cluster appeared in {scope}."
    if object_id == "macro.release.actual":
        actual = metadata.get("actual_value") or metadata.get("actual") or metadata.get("surprise_text")
        if actual:
            return f"{title} was released with actual/surprise value {actual}."
        return f"{title} was released and should be compared with expectations."
    return f"{title} crossed the high-impact review gate."


def _region_asset_line(assets: list[str], metadata: dict[str, Any]) -> str:
    scope = _scope(metadata)
    if assets:
        return f"{scope}; watch {', '.join(assets)}"
    return scope


def _why_it_matters(labels: list[str], metadata: dict[str, Any], object_id: str) -> str:
    label_text = " ".join(labels).lower()
    stream = str(metadata.get("stream") or "").lower()
    if object_id == "macro.release.actual":
        return "Official macro actuals can change the baseline used by prediction context packs."
    if "tariff" in label_text or "sanction" in label_text or "trade" in label_text or stream == "trade_sanctions_supply":
        return "Trade or sanctions news can change supply-chain assumptions, oil/gold risk premia and regional equity appetite."
    if "oil" in label_text or "energy" in label_text or stream == "energy_commodities":
        return "Oil and energy shocks often travel through inflation expectations, margins and risk appetite."
    if "bank" in label_text or "market_stress" in label_text or stream == "market_stress":
        return "Banking or market-stress signals can spill quickly into credit, volatility and index breadth."
    if "conflict" in label_text or "security" in label_text or stream == "conflict_security":
        return "Conflict and security clusters can reprice commodities, safe havens and regional exposure."
    if "political" in label_text or stream == "political_risk":
        return "Political-risk clusters can shift policy expectations, trade assumptions and risk appetite before markets have a clean macro release."
    if "macro" in label_text or stream in {"economy_news", "policy_rates"}:
        return "Macro news can shift the rate/growth mix that drives index calls and historical analogue matching."
    if metadata.get("source") == "rss_news":
        return "A high-priority news item entered the Current Events surface and should be considered against active forecasts."
    return "The event crossed the high-impact gate and deserves review before the next prediction run."


def _watch_next(labels: list[str], metadata: dict[str, Any], object_id: str) -> str:
    if object_id == "macro.release.actual":
        return "Check revisions, market reaction, and whether the next catalyst changes the path."
    label_text = " ".join(labels).lower()
    stream = str(metadata.get("stream") or "").lower()
    if "trade" in label_text or "sanction" in label_text or stream == "trade_sanctions_supply":
        return "Watch follow-up tariff/sanctions headlines, shipping costs, oil/gold reaction and affected regional indices."
    if "oil" in label_text or "energy" in label_text or stream == "energy_commodities":
        return "Watch crude inventories, shipping headlines, inflation breakevens and energy sector breadth."
    if "bank" in label_text or stream == "market_stress":
        return "Watch credit spreads, deposit/funding headlines, volatility and financial-sector relative strength."
    if "conflict" in label_text or stream == "conflict_security":
        return "Watch whether the cluster persists across regions and whether commodities or safe havens confirm it."
    if stream == "political_risk":
        return "Watch whether the story becomes policy action, a scheduled catalyst, or only headline noise."
    return "Watch follow-up sources, duplicate clusters, and whether affected assets confirm the signal."


def _note_for_row(row: sqlite3.Row) -> dict[str, Any]:
    labels = _compact_list(_json_loads(row["labels_json"], []), limit=12)
    metadata = _json_loads(row["metadata_json"], {})
    if not isinstance(metadata, dict):
        metadata = {}
    object_id = str(row["object_id"])
    title = _title(row, metadata)
    assets = _labels_to_assets(labels, metadata)
    priority = float(row["priority"] or 0.0)
    confidence = min(0.92, max(0.50, 0.42 + priority / 10.0))
    source_quality = "official release" if object_id == "macro.release.actual" else "clustered current-events signal"
    count = _article_count(metadata)
    if count:
        source_quality = f"{source_quality}; {count} matching articles"
    if object_id == "source.run":
        source_quality = "pipeline telemetry"
    lines = [
        f"What happened: {_what_happened(title, metadata, object_id)}",
        f"Why it matters: {_why_it_matters(labels, metadata, object_id)}",
        f"Affected regions/assets: {_region_asset_line(assets, metadata)}",
        f"Confidence/source quality: {confidence:.2f}; {source_quality}; priority {priority:.2f}",
        f"What to watch next: {_watch_next(labels, metadata, object_id)}",
    ]
    return {
        "title": title,
        "review_type": REVIEW_TYPES_BY_OBJECT.get(object_id, "current_event_journal"),
        "severity": priority,
        "confidence": confidence,
        "comment": "\n".join(lines),
        "labels": labels,
        "assets": assets,
    }


def _note_is_specific(note: dict[str, Any]) -> bool:
    comment = str(note.get("comment") or "").lower()
    if "needs a look" in comment or "signal needs" in comment:
        return False
    generic = "the event crossed the high-impact gate and deserves review"
    if generic in comment and not note.get("assets"):
        return False
    required = ("what happened:", "why it matters:", "affected regions/assets:", "what to watch next:")
    return all(part in comment for part in required)


def _input_hash(row: sqlite3.Row) -> str:
    raw = "|".join([
        str(row["event_key"]),
        str(row["object_id"]),
        str(row["source_table"]),
        str(row["source_id"]),
        str(row["metadata_json"] or ""),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _append_markdown(notes: list[dict[str, Any]], path: Path) -> None:
    if not notes:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        text = path.read_text()
    else:
        title = "Macro Intelligence Engine Journal" if "atlas" in path.name.lower() else "Claude Journal"
        text = f"# {title}\n\n## Entries\n"
    if "## Entries" not in text:
        text = text.rstrip() + "\n\n## Entries\n"
    today = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M UTC")
    block = [f"\n### {today} - Research notes"]
    grouped: dict[tuple[str, str, str, str, str], int] = {}
    for note in notes:
        fields: dict[str, str] = {}
        for line in note["comment"].splitlines():
            if ": " in line:
                key, value = line.split(": ", 1)
                fields[key] = value
        what = fields.get("What happened", "High-priority event crossed the review gate.")
        why = fields.get("Why it matters", "Review before the next prediction run.")
        watch = fields.get("What to watch next", "Watch follow-up sources and asset reaction.")
        key = (str(note["review_type"]), str(note["title"]), what, why, watch)
        grouped[key] = grouped.get(key, 0) + 1
    for (review_type, title, what, why, watch), count in grouped.items():
        suffix = f" ({count} signals)" if count > 1 else ""
        block.append(f"- {review_type}: {title}{suffix}")
        block.append(f"  - What: {what}")
        block.append(f"  - Why: {why}")
        block.append(f"  - Watch: {watch}")
    insert_at = text.index("## Entries") + len("## Entries")
    new_text = text[:insert_at] + "\n" + "\n".join(block) + "\n" + text[insert_at:]
    path.write_text(new_text)


def generate_and_store(
    *,
    limit: int = 8,
    min_priority: float = MIN_PRIORITY,
    append_markdown: bool = True,
    journal_path: str | Path | None = None,
    conn: sqlite3.Connection | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Create journal annotations for high-priority queued data changes."""
    own_conn = conn is None
    conn = conn or connect_writable()
    conn.row_factory = sqlite3.Row
    created = 0
    skipped = 0
    markdown_notes: list[dict[str, Any]] = []
    try:
        if not table_exists(conn, "data_change_events") or not table_exists(conn, "oracle_review_annotations"):
            return {"rows_seen": 0, "rows_inserted": 0, "rows_updated": 0, "rows_skipped": 0}
        rows = conn.execute(
            """SELECT id, event_key, object_id, source_table, source_id, event_type,
                      priority, labels_json, metadata_json, status,
                      oracle_review_required, created_at, updated_at
               FROM data_change_events
               WHERE status = 'queued'
                 AND oracle_review_required = 1
                 AND priority >= ?
               ORDER BY priority DESC, created_at DESC
               LIMIT ?""",
            (float(min_priority), int(limit)),
        ).fetchall()
        for row in rows:
            note = _note_for_row(row)
            if not _note_is_specific(note):
                conn.execute(
                    "UPDATE data_change_events SET status='superseded', updated_at=datetime('now') WHERE id=?",
                    (int(row["id"]),),
                )
                skipped += 1
                continue
            input_hash = _input_hash(row)
            now = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
            before = conn.total_changes
            conn.execute(
                """INSERT INTO oracle_review_annotations (
                         source_table, source_id, as_of, review_type, severity,
                         confidence, comment, model, input_hash, created_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                       ON CONFLICT(source_table, source_id, review_type, model)
                       DO UPDATE SET severity=excluded.severity,
                                     confidence=excluded.confidence,
                                     comment=excluded.comment,
                                     input_hash=excluded.input_hash,
                                     created_at=datetime('now')""",
                (
                    "data_change_events",
                    int(row["id"]),
                    now,
                    note["review_type"],
                    note["severity"],
                    note["confidence"],
                    note["comment"],
                    MODEL_NAME,
                    input_hash,
                ),
            )
            if conn.total_changes > before:
                created += 1
                markdown_notes.append(note)
            else:
                skipped += 1
            conn.execute(
                "UPDATE data_change_events SET status='reviewed', updated_at=datetime('now') WHERE id=?",
                (int(row["id"]),),
            )
        conn.commit()
        if append_markdown and markdown_notes:
            target = Path(journal_path) if journal_path else ATLAS_JOURNAL
            if any(n["review_type"] == "prediction_journal" for n in markdown_notes):
                target = Path(journal_path) if journal_path else CLAUDE_JOURNAL
            _append_markdown(markdown_notes, target)
        if created:
            log(f"Research journal wrote {created} annotation(s).", module="journal")
        return {
            "rows_seen": len(rows),
            "rows_inserted": created,
            "rows_updated": created,
            "rows_skipped": skipped,
        }
    finally:
        if own_conn:
            conn.close()


if __name__ == "__main__":
    print(generate_and_store())
