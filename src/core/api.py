"""Small read-only API surface for future commercial frontends.

These functions deliberately return plain records/dicts, not Streamlit objects
or SQLite cursors. A future FastAPI/Next.js bridge can wrap this module without
exposing private admin controls.
"""
from __future__ import annotations

from contextlib import closing
from typing import Any

from src.core import audit, labels, queries
from src.core.compliance import public_disclaimer
from src.core.db import connect_readonly


def source_freshness() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.source_health(conn))


def watchlist_assets(active_only: bool = True) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.target_assets(conn, active_only=active_only))


def latest_prediction_summaries() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.prediction_summaries(conn))


def latest_intelligence_packages() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.intelligence_packages(conn))


def latest_macro_event_scenarios() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.macro_event_predictions(conn, days_forward=45))


def latest_oracle_index() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.oracle_index_snapshots(conn))


def latest_oracle_layer_map(
    layer: str | None = None,
    entity_type: str | None = None,
    limit: int = 240,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.oracle_layer_map(
            conn, layer=layer, entity_type=entity_type, limit=limit,
        ))


def oracle_hierarchy() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.oracle_entities(conn))


def latest_backtest_summary(run_id: int | None = None) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.backtest_summary(conn, run_id=run_id))


def prediction_detail(prediction_id: int, include_private_input: bool = False) -> dict[str, Any] | None:
    with closing(connect_readonly()) as conn:
        return queries.prediction_detail(conn, prediction_id, include_private_input=include_private_input)


def macro_overview() -> dict[str, Any]:
    with closing(connect_readonly()) as conn:
        return {
            "positioning": "research_only",
            "disclaimer": public_disclaimer(),
            "source_health": queries.jsonable_records(queries.source_health(conn)),
            "macro_regime": queries.jsonable_records(queries.macro_regime(conn)),
            "official_macro": queries.jsonable_records(queries.official_macro_indicators(conn)),
            "calendar": queries.jsonable_records(queries.calendar_events(conn, days_forward=45)),
            "risk_hotspots": queries.jsonable_records(queries.risk_hotspots(conn)),
            "gdelt_pulse": queries.jsonable_records(queries.gdelt_pulse(conn)),
            "world_pull": queries.jsonable_records(queries.intelligence_packages(conn)),
            "oracle_index": queries.jsonable_records(queries.oracle_index_snapshots(conn)),
            "oracle_layer_map": queries.jsonable_records(queries.oracle_layer_map(conn)),
            "oracle_hierarchy": queries.jsonable_records(queries.oracle_entities(conn)),
            "macro_event_scenarios": queries.jsonable_records(queries.macro_event_predictions(conn)),
            "current_events": queries.jsonable_records(queries.current_events(conn, limit=100)),
            "macro_event_flags": queries.jsonable_records(queries.macro_event_flags(conn)),
            "oracle_journal_notes": queries.jsonable_records(queries.oracle_journal_notes(conn)),
            "market_tape": queries.jsonable_records(queries.market_tape(conn)),
            "predictions": queries.jsonable_records(queries.prediction_summaries(conn)),
        }


def _jsonable_frame_map(frames: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in frames.items():
        if hasattr(value, "empty"):
            out[key] = queries.jsonable_records(value)
        else:
            out[key] = value
    return out


def data_overview() -> dict[str, Any]:
    with closing(connect_readonly()) as conn:
        summaries = queries.data_source_summaries(conn)
        label_counts = labels.label_catalog(conn).groupby("label_type").size().to_dict()
        return {
            "positioning": "research_only",
            "disclaimer": public_disclaimer(),
            "source_health": queries.jsonable_records(queries.source_health(conn)),
            "data_catalog": queries.jsonable_records(queries.data_catalog(conn)),
            "named_data_overview": queries.jsonable_records(queries.named_data_overview(conn)),
            "data_change_events": queries.jsonable_records(queries.data_change_events(conn, limit=50)),
            "current_events": queries.jsonable_records(queries.current_events(conn, limit=100)),
            "source_registry": queries.jsonable_records(queries.source_registry(conn)),
            "targets": queries.jsonable_records(queries.target_assets(conn)),
            "compiled_intelligence": {
                "world_pull": len(queries.intelligence_packages(conn, limit=500)),
                "oracle_index": len(queries.oracle_index_snapshots(conn, limit=500)),
                "macro_events": len(queries.macro_event_predictions(conn, days_forward=45)),
                "macro_actuals": len(queries.macro_release_actuals(conn, limit=500)),
                "oracle_entities": len(queries.oracle_entities(conn)),
                "gdelt_streams": len(queries.gdelt_streams(conn, days=14, limit=500)),
                "historical_states": len(queries.historical_state(conn)),
                "prediction_context_packs": len(queries.prediction_context_packs(conn, limit=500)),
                "current_events": len(queries.current_events(conn, limit=500)),
                "macro_event_flags": len(queries.macro_event_flags(conn)),
                "oracle_journal_notes": len(queries.oracle_journal_notes(conn, limit=500)),
            },
            "oracle_layers": queries.jsonable_records(queries.oracle_layer_map(conn, limit=240)),
            "source_summaries": _jsonable_frame_map(summaries),
            "gdelt_stream_coverage": queries.jsonable_records(queries.gdelt_stream_coverage(conn, days=35)),
            "historical_state_coverage": queries.jsonable_records(queries.historical_state_coverage(conn)),
            "label_counts": {str(k): int(v) for k, v in label_counts.items()},
        }


def source_detail(source: str, limit: int = 100, days: int = 14) -> list[dict[str, Any]]:
    key = source.strip().lower().replace("-", "_").replace(" ", "_")
    with closing(connect_readonly()) as conn:
        detail_map = {
            "official_macro": lambda: queries.official_macro_indicators(conn),
            "trade": lambda: queries.trade_indicators(conn),
            "news": lambda: queries.news_items(conn, days=days, limit=limit),
            "sanctions": lambda: queries.sanctions(conn, limit=limit),
            "sanction_clusters": lambda: queries.sanction_clusters(conn, limit=limit),
            "reddit": lambda: queries.reddit_context(conn, days=days, limit=limit),
            "rumour_spikes": lambda: queries.rumour_spikes(conn, limit=limit),
            "twitter": lambda: queries.twitter_macro_posts(conn, days=days, limit=limit),
            "twitter_heat": lambda: queries.twitter_topic_heat(conn, days=days, limit=limit),
            "insider": lambda: queries.insider_activity(conn, days=days, limit=limit),
            "weather": lambda: queries.weather_correlations(conn, limit=limit),
            "fundamentals": lambda: queries.company_fundamentals_summary(conn, limit=limit),
            "targets": lambda: queries.target_assets(conn),
            "world_pull": lambda: queries.intelligence_packages(conn, limit=limit),
            "oracle_index": lambda: queries.oracle_index_snapshots(conn, limit=limit),
            "oracle_layers": lambda: queries.oracle_layer_map(conn, limit=limit),
            "oracle_impacts": lambda: queries.oracle_impacts(conn, limit=limit),
            "macro_events": lambda: queries.macro_event_predictions(conn, days_forward=45),
            "calendar": lambda: queries.calendar_events(conn, days_forward=45),
            "macro_actuals": lambda: queries.macro_release_actuals(conn, limit=limit),
            "latest_macro_releases": lambda: queries.latest_macro_releases(conn, limit=limit),
            "next_macro_catalysts": lambda: queries.next_macro_catalysts(conn, limit=limit),
            "ingestion_health": lambda: queries.ingestion_health(conn, days=days),
            "source_runs": lambda: queries.source_run_history(conn, days=days, limit=limit),
            "risk_hotspots": lambda: queries.risk_hotspots(conn),
            "gdelt_streams": lambda: queries.gdelt_streams(conn, days=days, limit=limit),
            "gdelt_stream_examples": lambda: queries.gdelt_stream_examples(conn, limit=limit),
            "historical_state": lambda: queries.historical_state(conn),
            "historical_comparison": lambda: queries.historical_comparison(conn, limit=limit),
            "historical_state_coverage": lambda: queries.historical_state_coverage(conn),
            "gdelt_stream_coverage": lambda: queries.gdelt_stream_coverage(conn, days=days),
            "data_catalog": lambda: queries.data_catalog(conn),
            "named_data_overview": lambda: queries.named_data_overview(conn),
            "data_change_events": lambda: queries.data_change_events(conn, limit=limit),
            "macro_event_flags": lambda: queries.macro_event_flags(conn),
            "oracle_journal_notes": lambda: queries.oracle_journal_notes(conn, limit=limit),
            "market_tape": lambda: queries.market_tape(conn, limit=limit),
            "prediction_context_packs": lambda: queries.prediction_context_packs(conn, limit=limit),
            "source_registry": lambda: queries.source_registry(conn),
            "label_evaluation": lambda: queries.label_evaluation(conn, limit=limit),
        }
        if key not in detail_map:
            raise ValueError(f"Unknown source detail key: {source}")
        return queries.jsonable_records(detail_map[key]())


def data_catalog(active_only: bool = True) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.data_catalog(conn, active_only=active_only))


def named_data_overview() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.named_data_overview(conn))


def current_events(hours_back: int = 48, days_forward: int = 14, limit: int = 100) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.current_events(
            conn, hours_back=hours_back, days_forward=days_forward, limit=limit,
        ))


def macro_event_flags(days_before: int = 14, days_after: int = 14) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.macro_event_flags(
            conn, days_before=days_before, days_after=days_after,
        ))


def oracle_journal_notes(
    review_type: str | None = None,
    days: int = 30,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.oracle_journal_notes(
            conn, review_type=review_type, days=days, limit=limit,
        ))


def market_tape(limit: int = 40) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.market_tape(conn, limit=limit))


def data_change_events(
    status: str | None = None,
    min_priority: float | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.data_change_events(
            conn,
            status=status,
            min_priority=min_priority,
            limit=limit,
        ))


def prediction_context_packs(
    asset: str | None = None,
    horizon: str | None = None,
    limit: int = 100,
    include_prompt: bool = False,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.prediction_context_packs(
            conn,
            asset=asset,
            horizon=horizon,
            limit=limit,
            include_prompt=include_prompt,
        ))


def source_registry() -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.source_registry(conn))


def label_enriched_source(source: str, limit: int = 100) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.label_enriched_source(conn, source=source, limit=limit))


def data_audit() -> dict[str, Any]:
    with closing(connect_readonly()) as conn:
        result = audit.run_audit(conn)
        return {
            "summary": result["summary"],
            "sources": queries.jsonable_records(result["sources"]),
            "source_runs": queries.jsonable_records(result["source_runs"]),
            "label_orphans": queries.jsonable_records(result["label_orphans"]),
        }


def label_catalog(active_only: bool = True) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(labels.label_catalog(conn, active_only=active_only))


def label_weight_profiles(active_only: bool = True) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(labels.label_weight_profiles(conn, active_only=active_only))


def weighted_label_catalog(
    profile_id: str | None = None,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(labels.weighted_label_catalog(
            conn, profile_id=profile_id, active_only=active_only,
        ))


def label_evaluation(
    profile_id: str = "default",
    asset: str | None = None,
    horizon: str | None = None,
    min_observations: int = 20,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.label_evaluation(
            conn, profile_id=profile_id, asset=asset, horizon=horizon,
            min_observations=min_observations, limit=limit,
        ))


def label_assignments(
    target_type: str | None = None,
    target_table: str | None = None,
    target_column: str | None = None,
    target_value: str | None = None,
    label_type: str | None = None,
    active_only: bool = True,
    profile_id: str | None = None,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(labels.label_assignments(
            conn,
            target_type=target_type,
            target_table=target_table,
            target_column=target_column,
            target_value=target_value,
            label_type=label_type,
            profile_id=profile_id,
            active_only=active_only,
        ))


def gdelt_streams(
    days: int = 30,
    stream: str | None = None,
    region: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.gdelt_streams(
            conn, days=days, stream=stream, region=region, limit=limit,
        ))


def gdelt_stream_examples(
    stream: str | None = None,
    date: str | None = None,
    region: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.gdelt_stream_examples(
            conn, stream=stream, date=date, region=region, limit=limit,
        ))


def gdelt_stream_coverage(days: int = 35) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.gdelt_stream_coverage(conn, days=days))


def historical_state(
    as_of: str | None = None,
    keys: list[str] | None = None,
    include_sparse: bool = True,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.historical_state(
            conn, as_of=as_of, keys=keys, include_sparse=include_sparse,
        ))


def historical_state_coverage(as_of: str | None = None) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.historical_state_coverage(conn, as_of=as_of))


def historical_comparison(
    start: str | None = None,
    end: str | None = None,
    symbols: list[str] | None = None,
    labels: list[str] | None = None,
    streams: list[str] | None = None,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.historical_comparison(
            conn,
            start=start,
            end=end,
            symbols=symbols,
            labels=labels,
            streams=streams,
            limit=limit,
        ))


def ingestion_health(days: int = 7) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.ingestion_health(conn, days=days))


def source_run_history(
    source: str | None = None,
    days: int = 14,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.source_run_history(
            conn, source=source, days=days, limit=limit,
        ))


def macro_release_actuals(
    status: str | None = None,
    days_back: int = 14,
    days_forward: int = 45,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.macro_release_actuals(
            conn, status=status, days_back=days_back,
            days_forward=days_forward, limit=limit,
        ))


def latest_macro_releases(hours: int = 72, limit: int = 10) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.latest_macro_releases(conn, hours=hours, limit=limit))


def next_macro_catalysts(days: int = 45, limit: int = 20) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.next_macro_catalysts(conn, days=days, limit=limit))


def macro_actual_match_coverage(days_back: int = 30, days_forward: int = 60) -> list[dict[str, Any]]:
    with closing(connect_readonly()) as conn:
        return queries.jsonable_records(queries.macro_actual_match_coverage(
            conn, days_back=days_back, days_forward=days_forward,
        ))
