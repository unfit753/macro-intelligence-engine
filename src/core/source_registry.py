"""Central source cadence registry for cron and API surfaces."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SourceSpec:
    source: str
    pipeline: str
    object_id: str
    cadence_hours: float
    event_window_hours: float | None = None
    heavy: bool = False
    dependencies: tuple[str, ...] = ()
    description: str = ""
    status: str = "active"
    status_reason: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "pipeline": self.pipeline,
            "object_id": self.object_id,
            "cadence_hours": self.cadence_hours,
            "event_window_hours": self.event_window_hours,
            "heavy": int(self.heavy),
            "dependencies": ",".join(self.dependencies),
            "description": self.description,
            "status": self.status,
            "status_reason": self.status_reason,
        }


SOURCE_REGISTRY: dict[str, SourceSpec] = {
    "rss_news": SourceSpec("rss_news", "frequent", "news.item", 1, description="RSS/official news tape."),
    "gdelt_today": SourceSpec("gdelt_today", "frequent", "gdelt.stream", 3, heavy=True, description="Optional intraday GDELT partial used by map only."),
    "reddit": SourceSpec("reddit", "frequent", "social.mention", 1, description="Reddit passive listener."),
    "twitter": SourceSpec("twitter", "frequent", "news.item", 1, description="X/Twitter macro listener when token is configured."),
    "bls": SourceSpec("bls", "frequent", "macro.indicator", 24, event_window_hours=1, description="Official US macro around scheduled release windows."),
    "fred": SourceSpec("fred", "frequent", "macro.indicator", 24, event_window_hours=1, description="FRED official macro/market series."),
    "scb": SourceSpec("scb", "frequent", "macro.indicator", 24, event_window_hours=1, description="Statistics Sweden official releases."),
    "riksbank": SourceSpec("riksbank", "frequent", "macro.indicator", 24, event_window_hours=1, description="Riksbank policy/rates data."),
    "eurostat": SourceSpec("eurostat", "frequent", "macro.indicator", 24, event_window_hours=1, description="Eurostat HICP and European macro."),
    "eia": SourceSpec("eia", "frequent", "macro.indicator", 24, event_window_hours=1, description="EIA energy releases."),
    "weather": SourceSpec("weather", "frequent", "weather.observation", 24, description="Weather observations."),
    "macro_actuals": SourceSpec("macro_actuals", "frequent", "macro.release.actual", 24, event_window_hours=1, dependencies=("calendar_events", "indicators"), description="Match scheduled catalysts to official actual rows."),
    "current_events_canary": SourceSpec("current_events_canary", "frequent", "current.event", 0.25, heavy=True, dependencies=("calendar_events", "macro_release_actuals", "gdelt_streams", "news_items"), description="15-minute Current Events canary for catalysts, releases and market-moving news."),
    "research_journal": SourceSpec("research_journal", "frequent", "oracle.journal", 6, dependencies=("data_change_events", "oracle_review_annotations"), description="Research-note digest for high-priority queued events; runs as a slower digest, not every canary tick."),
    "sanctions": SourceSpec("sanctions", "frequent", "sanctions.program", 6, description="Sanctions list refresh."),
    "edgar_form4": SourceSpec("edgar_form4", "frequent", "insider.trade", 6, description="SEC Form 4 insider trades."),
    "world_pull": SourceSpec("world_pull", "frequent", "oracle.world_pull", 1, dependencies=("oracle_impacts",), description="Compiled world/regional pull."),
    "macro_event_scenarios": SourceSpec("macro_event_scenarios", "frequent", "calendar.event", 6, event_window_hours=1, dependencies=("calendar_events",), description="Deterministic macro scenario ladder."),
    "local_event_result_notes": SourceSpec("local_event_result_notes", "frequent", "macro.release.actual", 1, dependencies=("macro_release_actuals",), description="Tiny local result notes for released macro events."),
    "event_results": SourceSpec("event_results", "daily", "macro.release.actual", 24, dependencies=("macro_release_actuals",), description="Daily local result notes for released macro events."),
    "impact_graph": SourceSpec("impact_graph", "frequent", "oracle.index", 1, dependencies=("oracle_impacts",), description="Atlas impact graph and map index."),
    "seed_events": SourceSpec("seed_events", "daily", "calendar.event", 24, description="Curated calendar/event seed fallback."),
    "cb_calendar": SourceSpec("cb_calendar", "daily", "calendar.event", 24, description="Central-bank and macro calendar refresh."),
    "ecb": SourceSpec("ecb", "daily", "macro.indicator", 24, description="ECB rates and euro-area official data."),
    "worldbank": SourceSpec("worldbank", "daily", "macro.indicator", 168, heavy=True, description="World Bank macro indicators."),
    "imf": SourceSpec("imf", "daily", "macro.indicator", 168, heavy=True, description="IMF macro indicators."),
    "cftc": SourceSpec("cftc", "daily", "market.positioning", 24, description="CFTC commitment-of-traders positioning."),
    "edgar_facts": SourceSpec("edgar_facts", "daily", "company.fundamental", 24, description="SEC EDGAR company facts."),
    "acled": SourceSpec(
        "acled",
        "daily",
        "risk.hotspot",
        24,
        description="ACLED conflict and political violence data.",
        status="needs_review",
        status_reason="Requires ACLED credentials; fetcher skips when credentials are missing.",
    ),
    "senate_trades": SourceSpec(
        "senate_trades",
        "daily",
        "insider.trade",
        24,
        description="US Senate trade disclosures.",
        status="inactive",
        status_reason="Unscheduled until feed reliability and prediction use case are confirmed.",
    ),
    "yahoo": SourceSpec("yahoo", "daily", "market.price", 24, heavy=True, description="Daily market close price refresh."),
    "signals": SourceSpec("signals", "daily", "market.signal", 24, heavy=True, dependencies=("prices", "indicators"), description="Daily computed signals."),
    "gdelt": SourceSpec("gdelt", "daily", "gdelt.stream", 24, heavy=True, description="Completed-day GDELT backfill/update."),
    "historical_state_recent": SourceSpec("historical_state_recent", "daily", "historical.state", 24, heavy=True, dependencies=("prices", "signals", "indicators"), description="Rolling historical-state and forward-return refresh."),
    "build_index": SourceSpec("build_index", "daily", "historical.state", 24, heavy=True, dependencies=("historical_state_values",), description="Analogue index rebuild."),
    "score": SourceSpec("score", "daily", "historical.forward_return", 24, dependencies=("predictions", "prices"), description="Prediction scoring once forward returns mature."),
    "label_evaluation": SourceSpec("label_evaluation", "daily", "label.evaluation", 24, dependencies=("score", "historical_state_recent"), description="Evaluate label evidence against matured forward returns."),
}


def source_spec(source: str) -> SourceSpec | None:
    return SOURCE_REGISTRY.get(source)


def source_cadence_hours(source: str, *, event_window: bool = False) -> float:
    spec = source_spec(source)
    if spec is None:
        return 24
    if event_window and spec.event_window_hours is not None:
        return spec.event_window_hours
    return spec.cadence_hours


def registry_rows() -> list[dict[str, Any]]:
    return [spec.to_record() for spec in sorted(SOURCE_REGISTRY.values(), key=lambda s: (s.pipeline, s.source))]


def in_event_window(
    now: dt.datetime,
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    before_hours: int = 24,
    after_hours: int = 12,
) -> bool:
    """Return True when any high-impact scheduled event is near `now`."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.UTC)
    for event in events:
        raw = event.get("scheduled_at_utc") or event.get("scheduled_at")
        if not raw:
            continue
        try:
            ts = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.UTC)
        start = ts - dt.timedelta(hours=before_hours)
        end = ts + dt.timedelta(hours=after_hours)
        if start <= now <= end:
            return True
    return False


def source_enabled(source: str) -> bool:
    spec = source_spec(source)
    return spec is None or spec.status != "inactive"
