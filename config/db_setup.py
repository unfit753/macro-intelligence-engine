"""Idempotent schema setup. Safe to run repeatedly."""
import datetime as dt
import json
import re
import sqlite3
from config.config_fetch import DB_PATH
from src.core.db import connect_writable


SCHEMA = """
CREATE TABLE IF NOT EXISTS indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL,
    country         TEXT NOT NULL,
    category        TEXT,
    indicator_name  TEXT NOT NULL,
    value           REAL,
    unit            TEXT,
    expected_value  REAL,
    impact          TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_indicators_unique
    ON indicators(date, country, indicator_name);

CREATE TABLE IF NOT EXISTS prices (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    asset_class  TEXT,
    name         TEXT,
    price        REAL NOT NULL,
    currency     TEXT DEFAULT 'USD'
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_unique
    ON prices(date, symbol);

CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    date     TEXT NOT NULL,
    title    TEXT NOT NULL,
    country  TEXT,
    type     TEXT,
    source   TEXT,
    url      TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_unique
    ON events(date, title);

CREATE TABLE IF NOT EXISTS signals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    signal_name  TEXT NOT NULL,
    value        REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_unique
    ON signals(date, symbol, signal_name);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_date
    ON signals(symbol, date);

CREATE TABLE IF NOT EXISTS predictions (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    asset                TEXT NOT NULL,
    horizon              TEXT NOT NULL,
    as_of                TEXT NOT NULL,
    generated_at         TEXT NOT NULL,
    direction            TEXT NOT NULL,
    confidence           REAL NOT NULL,
    expected_return_low  REAL,
    expected_return_high REAL,
    rationale_md         TEXT,
    key_risks            TEXT,
    analogues_used       TEXT,
    model                TEXT NOT NULL,
    input_hash           TEXT NOT NULL,
    input_brief_md       TEXT,
    realized_return      REAL,
    scored_at            TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_unique
    ON predictions(asset, horizon, as_of, model);
CREATE INDEX IF NOT EXISTS idx_predictions_asset_date
    ON predictions(asset, as_of);

CREATE TABLE IF NOT EXISTS prediction_context_packs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    asset             TEXT NOT NULL,
    horizon           TEXT NOT NULL,
    as_of             TEXT NOT NULL,
    profile_id        TEXT NOT NULL DEFAULT 'default',
    pack_json         TEXT NOT NULL,
    prompt_md         TEXT,
    historical_refs_json TEXT,
    input_hash        TEXT NOT NULL,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_context_packs_unique
    ON prediction_context_packs(asset, horizon, as_of, profile_id, input_hash);
CREATE INDEX IF NOT EXISTS idx_prediction_context_packs_lookup
    ON prediction_context_packs(asset, horizon, as_of);

CREATE TABLE IF NOT EXISTS targets (
    symbol       TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    asset_class  TEXT,
    horizons     TEXT NOT NULL,
    source       TEXT NOT NULL DEFAULT 'manual',
    active       INTEGER NOT NULL DEFAULT 1,
    added_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS briefings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    region          TEXT NOT NULL,
    as_of           TEXT NOT NULL,
    generated_at    TEXT NOT NULL,
    headline        TEXT,
    summary_md      TEXT,
    things_to_watch TEXT,
    model           TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_briefings_unique
    ON briefings(region, as_of, model);

CREATE TABLE IF NOT EXISTS source_runs (
    run_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline         TEXT NOT NULL,
    source           TEXT NOT NULL,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    status           TEXT NOT NULL DEFAULT 'running',
    duration_sec     REAL,
    rows_seen        INTEGER,
    rows_inserted    INTEGER,
    rows_updated     INTEGER,
    latest_source_ts TEXT,
    error_message    TEXT,
    metadata_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_source_runs_source_started
    ON source_runs(source, started_at);
CREATE INDEX IF NOT EXISTS idx_source_runs_status_started
    ON source_runs(status, started_at);

CREATE TABLE IF NOT EXISTS data_objects (
    object_id      TEXT PRIMARY KEY,
    display_name   TEXT NOT NULL,
    prompt_name    TEXT NOT NULL,
    source_table   TEXT,
    description    TEXT,
    source_family  TEXT,
    freshness_sla_hours REAL,
    cadence        TEXT,
    frontend_group TEXT,
    prompt_role    TEXT,
    active         INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_data_objects_group
    ON data_objects(frontend_group, active);

CREATE TABLE IF NOT EXISTS data_change_events (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key              TEXT NOT NULL,
    object_id              TEXT NOT NULL,
    source_table           TEXT NOT NULL,
    source_id              TEXT,
    event_type             TEXT NOT NULL DEFAULT 'upsert',
    priority               REAL NOT NULL DEFAULT 1.0,
    labels_json            TEXT,
    metadata_json          TEXT,
    status                 TEXT NOT NULL DEFAULT 'queued',
    oracle_review_required INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at             TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_change_events_key
    ON data_change_events(event_key);
CREATE INDEX IF NOT EXISTS idx_data_change_events_status
    ON data_change_events(status, priority, created_at);

CREATE TABLE IF NOT EXISTS current_events (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key              TEXT NOT NULL,
    event_type             TEXT NOT NULL,
    title                  TEXT NOT NULL,
    summary                TEXT,
    event_time             TEXT NOT NULL,
    region                 TEXT,
    category               TEXT,
    priority               REAL NOT NULL DEFAULT 1.0,
    status                 TEXT NOT NULL DEFAULT 'active',
    object_id              TEXT NOT NULL,
    source_table           TEXT NOT NULL,
    source_id              TEXT,
    labels_json            TEXT,
    affected_assets_json   TEXT,
    display_title          TEXT,
    display_summary        TEXT,
    why_text               TEXT,
    source_quality         TEXT,
    metadata_json          TEXT,
    oracle_review_required INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at             TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at             TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_current_events_key
    ON current_events(event_key);
CREATE INDEX IF NOT EXISTS idx_current_events_time
    ON current_events(status, event_time, priority);
CREATE INDEX IF NOT EXISTS idx_current_events_type
    ON current_events(event_type, status, priority);

CREATE TABLE IF NOT EXISTS gdelt_canary_files (
    file_stamp       TEXT PRIMARY KEY,
    url              TEXT NOT NULL,
    status           TEXT NOT NULL,
    rows_seen        INTEGER NOT NULL DEFAULT 0,
    error_message    TEXT,
    fetched_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Layer-1 passive ingestion tables (phase 9). All raw, deduped on natural
-- keys; analysis (rumour/fact matching, weather correlations) lives in
-- Layer 2 and reads these.

CREATE TABLE IF NOT EXISTS senate_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_date  TEXT NOT NULL,
    senator           TEXT NOT NULL,
    ticker            TEXT,
    asset_description TEXT,
    asset_type        TEXT,
    transaction_type  TEXT,
    amount_range      TEXT,
    owner             TEXT,
    ptr_link          TEXT,
    fetched_at        TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_senate_unique
    ON senate_trades(transaction_date, senator, ticker, transaction_type, amount_range);
CREATE INDEX IF NOT EXISTS idx_senate_ticker
    ON senate_trades(ticker, transaction_date);

CREATE TABLE IF NOT EXISTS insider_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_date       TEXT NOT NULL,
    transaction_date  TEXT,
    ticker            TEXT,
    company           TEXT,
    insider_name      TEXT,
    insider_role      TEXT,
    transaction_type  TEXT,
    shares            REAL,
    price             REAL,
    value_usd         REAL,
    accession_number  TEXT NOT NULL,
    url               TEXT,
    fetched_at        TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_insider_unique
    ON insider_trades(accession_number, insider_name, ticker, transaction_type);
CREATE INDEX IF NOT EXISTS idx_insider_ticker
    ON insider_trades(ticker, transaction_date);

CREATE TABLE IF NOT EXISTS social_mentions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL,
    source          TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    mention_count   INTEGER NOT NULL,
    sentiment_score REAL,
    top_post_title  TEXT,
    top_post_score  INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_social_unique
    ON social_mentions(date, source, ticker);

CREATE TABLE IF NOT EXISTS twitter_macro_posts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    posted_at          TEXT,
    query              TEXT NOT NULL,
    topic              TEXT,
    author_username    TEXT,
    text               TEXT NOT NULL,
    like_count         INTEGER,
    repost_count       INTEGER,
    reply_count        INTEGER,
    quote_count        INTEGER,
    url                TEXT,
    fetched_at         TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_twitter_macro_url
    ON twitter_macro_posts(url);
CREATE INDEX IF NOT EXISTS idx_twitter_macro_posted
    ON twitter_macro_posts(posted_at, topic);

CREATE TABLE IF NOT EXISTS weather_obs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    date             TEXT NOT NULL,
    location         TEXT NOT NULL,
    temp_mean_c      REAL,
    precipitation_mm REAL,
    wind_max_kmh     REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_unique
    ON weather_obs(date, location);

-- Layer-2 synthesis tables (phase 11+12)

CREATE TABLE IF NOT EXISTS rumour_signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    date                TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    mentions_today      INTEGER NOT NULL,
    baseline_mean       REAL,
    z_score             REAL,
    sentiment_today     REAL,
    realized_return_5d  REAL,
    realized_return_21d REAL,
    scored_at           TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rumour_unique
    ON rumour_signals(date, ticker);
CREATE INDEX IF NOT EXISTS idx_rumour_zscore
    ON rumour_signals(z_score);

CREATE TABLE IF NOT EXISTS weather_correlations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    asset           TEXT NOT NULL,
    location        TEXT NOT NULL,
    weather_var     TEXT NOT NULL,
    window_days     INTEGER NOT NULL,
    correlation     REAL NOT NULL,
    n_observations  INTEGER NOT NULL,
    computed_at     TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_wcorr_unique
    ON weather_correlations(asset, location, weather_var, window_days);

CREATE TABLE IF NOT EXISTS company_fundamentals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker           TEXT NOT NULL,
    period_end       TEXT NOT NULL,
    period_type      TEXT,
    concept          TEXT NOT NULL,
    value            REAL NOT NULL,
    unit             TEXT,
    form             TEXT,
    accession_number TEXT,
    filed            TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fundamentals_unique
    ON company_fundamentals(ticker, period_end, concept, form);
CREATE INDEX IF NOT EXISTS idx_fundamentals_ticker_period
    ON company_fundamentals(ticker, period_end);

CREATE TABLE IF NOT EXISTS calendar_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    date             TEXT NOT NULL,
    time_local       TEXT,
    region           TEXT NOT NULL,
    category         TEXT NOT NULL,
    importance       INTEGER NOT NULL DEFAULT 3,
    title            TEXT NOT NULL,
    expected         TEXT,
    market_note      TEXT,
    source           TEXT,
    url              TEXT,
    event_key        TEXT,
    release_family   TEXT,
    scheduled_at_utc TEXT,
    source_uid       TEXT,
    labels_json      TEXT,
    status           TEXT NOT NULL DEFAULT 'scheduled'
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_calendar_unique
    ON calendar_events(date, title, region);
CREATE INDEX IF NOT EXISTS idx_calendar_date
    ON calendar_events(date, importance);

CREATE TABLE IF NOT EXISTS macro_release_actuals (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    calendar_event_id      INTEGER,
    release_key            TEXT NOT NULL,
    region                 TEXT NOT NULL,
    category               TEXT NOT NULL,
    title                  TEXT NOT NULL,
    scheduled_date         TEXT NOT NULL,
    scheduled_time_local   TEXT,
    importance             INTEGER NOT NULL DEFAULT 3,
    actual_value           REAL,
    expected_value         REAL,
    expected_text          TEXT,
    expected_unit          TEXT,
    previous_value         REAL,
    surprise_value         REAL,
    surprise_text          TEXT,
    unit                   TEXT,
    source_table           TEXT,
    source_id              INTEGER,
    source_indicator_name  TEXT,
    value_date             TEXT,
    status                 TEXT NOT NULL DEFAULT 'waiting',
    metadata_json          TEXT,
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at             TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(calendar_event_id) REFERENCES calendar_events(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_macro_release_actuals_unique
    ON macro_release_actuals(release_key, scheduled_date);
CREATE INDEX IF NOT EXISTS idx_macro_release_actuals_schedule
    ON macro_release_actuals(scheduled_date, status, importance);

CREATE TABLE IF NOT EXISTS risk_hotspots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    region      TEXT,
    country     TEXT,
    category    TEXT NOT NULL,
    severity    INTEGER NOT NULL DEFAULT 3,
    lat         REAL NOT NULL,
    lon         REAL NOT NULL,
    summary     TEXT,
    source      TEXT,
    active      INTEGER NOT NULL DEFAULT 1,
    updated_at  TEXT NOT NULL DEFAULT (date('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_hotspot_unique
    ON risk_hotspots(name, category);

CREATE TABLE IF NOT EXISTS sanctions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    list_name     TEXT,
    program       TEXT,
    entity_name   TEXT NOT NULL,
    entity_type   TEXT,
    country       TEXT,
    target_type   TEXT,
    product       TEXT,
    measure       TEXT,
    url           TEXT,
    fetched_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sanctions_unique
    ON sanctions(source, list_name, entity_name, program, country);
CREATE INDEX IF NOT EXISTS idx_sanctions_country
    ON sanctions(country, program);

CREATE TABLE IF NOT EXISTS news_items (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    published_at         TEXT,
    source               TEXT NOT NULL,
    title                TEXT NOT NULL,
    summary              TEXT,
    url                  TEXT NOT NULL,
    region               TEXT,
    category             TEXT,
    used_for_predictions INTEGER NOT NULL DEFAULT 0,
    fetched_at           TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_news_items_url
    ON news_items(url);
CREATE INDEX IF NOT EXISTS idx_news_items_published
    ON news_items(published_at, source);

CREATE TABLE IF NOT EXISTS gdelt_disaster_signals (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    date           TEXT NOT NULL,
    country        TEXT NOT NULL,
    disaster_type  TEXT NOT NULL,
    article_count  INTEGER NOT NULL,
    total_articles INTEGER,
    lat            REAL,
    lon            REAL,
    examples       TEXT,
    source         TEXT NOT NULL DEFAULT 'gdelt_gkg',
    fetched_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_disaster_unique
    ON gdelt_disaster_signals(date, country, disaster_type);

CREATE TABLE IF NOT EXISTS gdelt_stream_theme_rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    stream      TEXT NOT NULL,
    theme_code  TEXT NOT NULL,
    label_id    TEXT,
    notes       TEXT,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_stream_theme_rules_unique
    ON gdelt_stream_theme_rules(stream, theme_code);

CREATE TABLE IF NOT EXISTS gdelt_streams (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    date                  TEXT NOT NULL,
    stream                TEXT NOT NULL,
    region                TEXT NOT NULL DEFAULT 'Global',
    country               TEXT NOT NULL DEFAULT '',
    article_count         INTEGER NOT NULL DEFAULT 0,
    total_articles        INTEGER,
    article_share         REAL,
    baseline_30d          REAL,
    z_score               REAL,
    severity              REAL NOT NULL DEFAULT 0,
    societal_impact_score REAL NOT NULL DEFAULT 0,
    labels_json           TEXT,
    top_theme_codes_json  TEXT,
    source                TEXT NOT NULL DEFAULT 'gdelt_gkg',
    fetched_at            TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_streams_unique
    ON gdelt_streams(date, stream, region, country);
CREATE INDEX IF NOT EXISTS idx_gdelt_streams_date
    ON gdelt_streams(date, stream, severity);

CREATE TABLE IF NOT EXISTS gdelt_stream_examples (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    date             TEXT NOT NULL,
    stream           TEXT NOT NULL,
    region           TEXT NOT NULL DEFAULT 'Global',
    country          TEXT NOT NULL DEFAULT '',
    example_rank     INTEGER NOT NULL DEFAULT 1,
    title            TEXT,
    url              TEXT,
    source_domain    TEXT,
    location_name    TEXT,
    theme_codes_json TEXT,
    labels_json      TEXT,
    tone             REAL,
    source           TEXT NOT NULL DEFAULT 'gdelt_gkg',
    fetched_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_stream_examples_unique
    ON gdelt_stream_examples(date, stream, region, country, example_rank);
CREATE INDEX IF NOT EXISTS idx_gdelt_stream_examples_lookup
    ON gdelt_stream_examples(stream, date, region);

CREATE TABLE IF NOT EXISTS oracle_review_annotations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source_table TEXT NOT NULL,
    source_id    INTEGER,
    as_of        TEXT NOT NULL,
    review_type  TEXT NOT NULL,
    severity     REAL,
    confidence   REAL,
    comment      TEXT,
    model        TEXT NOT NULL DEFAULT 'deterministic_oracle_review_v1',
    input_hash   TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_oracle_review_annotations_unique
    ON oracle_review_annotations(source_table, source_id, review_type, model);

CREATE TABLE IF NOT EXISTS intelligence_packages (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of                  TEXT NOT NULL,
    generated_at           TEXT NOT NULL,
    scope_type             TEXT NOT NULL,
    scope                  TEXT NOT NULL,
    parent_scope           TEXT,
    theme                  TEXT NOT NULL,
    direction              TEXT NOT NULL,
    severity               REAL NOT NULL,
    confidence             REAL NOT NULL,
    freshness              TEXT,
    horizon                TEXT NOT NULL DEFAULT 'near_term',
    evidence_json          TEXT,
    conclusion             TEXT,
    affected_assets_json   TEXT,
    prediction_impact_json TEXT,
    next_watch             TEXT,
    source_refs_json       TEXT,
    model                  TEXT NOT NULL DEFAULT 'deterministic_world_pull_v1',
    input_hash             TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_intelligence_packages_unique
    ON intelligence_packages(as_of, scope_type, scope, theme, horizon, model);
CREATE INDEX IF NOT EXISTS idx_intelligence_packages_scope
    ON intelligence_packages(as_of, scope_type, scope, severity);

CREATE TABLE IF NOT EXISTS macro_event_predictions (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    calendar_event_id           INTEGER,
    event_key                   TEXT NOT NULL,
    as_of                       TEXT NOT NULL,
    generated_at                TEXT NOT NULL,
    release_date                TEXT NOT NULL,
    release_time_local          TEXT,
    region                      TEXT NOT NULL,
    country                     TEXT,
    category                    TEXT NOT NULL,
    title                       TEXT NOT NULL,
    importance                  INTEGER NOT NULL DEFAULT 3,
    expected                    TEXT,
    previous_value              REAL,
    predicted_surprise_bucket   TEXT,
    confidence                  REAL NOT NULL,
    scenario_json               TEXT NOT NULL,
    affected_assets_json        TEXT,
    rationale_md                TEXT,
    key_risks_json              TEXT,
    source                      TEXT,
    url                         TEXT,
    model                       TEXT NOT NULL DEFAULT 'deterministic_macro_event_scenarios_v1',
    input_hash                  TEXT,
    actual_value                REAL,
    actual_surprise             TEXT,
    actual_detail_json          TEXT,
    actual_summary              TEXT,
    result_status               TEXT,
    forecast_direction          TEXT,
    forecast_confidence         REAL,
    forecast_summary            TEXT,
    forecast_rationale_md       TEXT,
    historical_pattern_json     TEXT,
    claude_forecast_json        TEXT,
    claude_model                TEXT,
    claude_at                   TEXT,
    local_model_summary         TEXT,
    local_model_model           TEXT,
    local_model_at              TEXT,
    scored_at                   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_macro_event_predictions_unique
    ON macro_event_predictions(event_key, as_of, model);
CREATE INDEX IF NOT EXISTS idx_macro_event_predictions_release
    ON macro_event_predictions(release_date, importance);

CREATE TABLE IF NOT EXISTS oracle_entities (
    entity_id    TEXT PRIMARY KEY,
    label        TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    parent_id    TEXT,
    region       TEXT,
    nation       TEXT,
    sector       TEXT,
    symbol       TEXT,
    description  TEXT,
    active       INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_oracle_entities_parent
    ON oracle_entities(parent_id, entity_type);

CREATE TABLE IF NOT EXISTS oracle_impacts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of            TEXT NOT NULL,
    generated_at     TEXT NOT NULL,
    source_table     TEXT NOT NULL,
    source_id        INTEGER,
    evidence_key     TEXT NOT NULL,
    theme            TEXT NOT NULL,
    entity_id        TEXT NOT NULL,
    direction        TEXT NOT NULL,
    magnitude        REAL NOT NULL,
    confidence       REAL NOT NULL,
    horizon          TEXT NOT NULL DEFAULT 'near_term',
    freshness        TEXT,
    summary          TEXT,
    source_refs_json TEXT,
    model            TEXT NOT NULL DEFAULT 'deterministic_impact_graph_v1',
    input_hash       TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_oracle_impacts_unique
    ON oracle_impacts(as_of, source_table, evidence_key, entity_id, theme, horizon, model);
CREATE INDEX IF NOT EXISTS idx_oracle_impacts_entity
    ON oracle_impacts(as_of, entity_id, magnitude);

CREATE TABLE IF NOT EXISTS oracle_index_snapshots (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of                TEXT NOT NULL,
    generated_at         TEXT NOT NULL,
    entity_id            TEXT NOT NULL,
    entity_label         TEXT NOT NULL,
    entity_type          TEXT NOT NULL,
    parent_id            TEXT,
    theme                TEXT NOT NULL,
    direction            TEXT NOT NULL,
    horizon              TEXT NOT NULL DEFAULT 'near_term',
    score                REAL NOT NULL,
    magnitude            REAL NOT NULL,
    confidence           REAL NOT NULL,
    evidence_count       INTEGER NOT NULL,
    market_bias          TEXT,
    plain_read           TEXT,
    top_evidence_json    TEXT,
    affected_assets_json TEXT,
    model                TEXT NOT NULL DEFAULT 'deterministic_impact_graph_v1',
    input_hash           TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_oracle_index_unique
    ON oracle_index_snapshots(as_of, entity_id, theme, horizon, model);
CREATE INDEX IF NOT EXISTS idx_oracle_index_entity
    ON oracle_index_snapshots(as_of, entity_type, score);

CREATE TABLE IF NOT EXISTS data_labels (
    label_id       TEXT PRIMARY KEY,
    label_type     TEXT NOT NULL,
    label          TEXT NOT NULL,
    description    TEXT,
    default_weight REAL NOT NULL DEFAULT 1.0,
    active         INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_labels_type_label
    ON data_labels(label_type, label);

CREATE TABLE IF NOT EXISTS data_label_assignments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    label_id        TEXT NOT NULL,
    target_type     TEXT NOT NULL,
    target_table    TEXT,
    target_column   TEXT,
    target_value    TEXT,
    weight_override REAL,
    confidence      REAL NOT NULL DEFAULT 1.0,
    notes           TEXT,
    active          INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(label_id) REFERENCES data_labels(label_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_data_label_assignments_unique
    ON data_label_assignments(label_id, target_type, target_table, target_column, target_value);
CREATE INDEX IF NOT EXISTS idx_data_label_assignments_target
    ON data_label_assignments(target_type, target_table, target_column, target_value);

CREATE TABLE IF NOT EXISTS label_weight_profiles (
    profile_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS label_weight_overrides (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id  TEXT NOT NULL,
    label_id    TEXT NOT NULL,
    weight      REAL NOT NULL,
    notes       TEXT,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(profile_id) REFERENCES label_weight_profiles(profile_id),
    FOREIGN KEY(label_id) REFERENCES data_labels(label_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_label_weight_overrides_unique
    ON label_weight_overrides(profile_id, label_id);

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

CREATE TABLE IF NOT EXISTS historical_state_values (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of           TEXT NOT NULL,
    value_key       TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    source_id       INTEGER,
    source_symbol   TEXT,
    source_name     TEXT,
    region          TEXT,
    country         TEXT,
    category        TEXT,
    label_ids_json  TEXT,
    value           REAL,
    value_text      TEXT,
    unit            TEXT,
    value_date      TEXT NOT NULL,
    freshness_days  INTEGER,
    freshness_class TEXT NOT NULL DEFAULT 'archive',
    confidence      REAL NOT NULL DEFAULT 1.0,
    active          INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_historical_state_values_unique
    ON historical_state_values(as_of, value_key);
CREATE INDEX IF NOT EXISTS idx_historical_state_values_lookup
    ON historical_state_values(as_of, source_table, source_symbol, category);

CREATE TABLE IF NOT EXISTS historical_state_daily (
    as_of                  TEXT PRIMARY KEY,
    coverage_score         REAL NOT NULL DEFAULT 0,
    training_eligible      INTEGER NOT NULL DEFAULT 0,
    evaluation_eligible    INTEGER NOT NULL DEFAULT 0,
    event_tags_json        TEXT,
    notable_driver_comment TEXT,
    state_json             TEXT,
    labels_json            TEXT,
    created_at             TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at             TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_historical_state_daily_training
    ON historical_state_daily(training_eligible, as_of);

CREATE TABLE IF NOT EXISTS historical_forward_returns (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of          TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    horizon_days   INTEGER NOT NULL,
    start_price    REAL NOT NULL,
    end_price      REAL,
    end_date       TEXT,
    forward_return REAL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_historical_forward_returns_unique
    ON historical_forward_returns(as_of, symbol, horizon_days);
CREATE INDEX IF NOT EXISTS idx_historical_forward_returns_lookup
    ON historical_forward_returns(symbol, horizon_days, as_of);

CREATE TABLE IF NOT EXISTS backtest_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'created',
    config_json     TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS backtest_predictions (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id               INTEGER NOT NULL,
    asset                TEXT NOT NULL,
    asset_name           TEXT,
    horizon              TEXT NOT NULL,
    as_of                TEXT NOT NULL,
    generated_at         TEXT NOT NULL,
    direction            TEXT NOT NULL,
    confidence           REAL NOT NULL,
    expected_return_low  REAL,
    expected_return_high REAL,
    rationale_md         TEXT,
    key_risks            TEXT,
    analogues_used       TEXT,
    model                TEXT NOT NULL,
    input_hash           TEXT NOT NULL,
    input_brief_md       TEXT,
    dry_run              INTEGER NOT NULL DEFAULT 0,
    realized_return      REAL,
    direction_hit        INTEGER,
    range_hit            INTEGER,
    scored_at            TEXT,
    error                TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_predictions_unique
    ON backtest_predictions(run_id, asset, horizon, as_of, model, dry_run);
CREATE INDEX IF NOT EXISTS idx_backtest_predictions_score
    ON backtest_predictions(run_id, asset, horizon, as_of);

CREATE TABLE IF NOT EXISTS local_model_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL DEFAULT 'created',
    model           TEXT NOT NULL,
    endpoint        TEXT NOT NULL,
    config_json     TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS local_model_evaluations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            INTEGER NOT NULL,
    created_at        TEXT NOT NULL,
    as_of             TEXT,
    source_table      TEXT NOT NULL,
    source_id         INTEGER,
    prompt_hash       TEXT NOT NULL,
    input_json        TEXT NOT NULL,
    expected_json     TEXT,
    response_json     TEXT,
    parse_ok          INTEGER NOT NULL DEFAULT 0,
    theme_hit         INTEGER,
    direction_hit     INTEGER,
    affected_hit      INTEGER,
    latency_ms        INTEGER,
    error             TEXT
);
CREATE INDEX IF NOT EXISTS idx_local_model_eval_run
    ON local_model_evaluations(run_id, source_table, source_id);
"""


# Initial set seeded into the targets table so the daily cron has work
# to do out of the box. Add/remove via the targets table or a future admin UI.
# raw SQL after this point.
DEFAULT_TARGETS: list[tuple[str, str, str, str, str]] = [
    # symbol, name, asset_class, horizons, source
    ("GC=F",      "Gold (COMEX)",            "commodity",    "1w,1m,3m",     "default"),
    ("CL=F",      "WTI Crude Oil",           "commodity",    "1w,1m,3m",     "default"),
    ("SPY",       "S&P 500 ETF",             "equity_etf",   "1w,1m,3m",     "default"),
    ("TLT",       "20Y+ Treasury ETF",       "bond_etf",     "1m,3m",        "default"),
    ("^OMX",      "OMXS30 (Stockholm)",      "equity_index", "1w,1m,3m",     "default"),
    ("^STOXX50E", "Euro Stoxx 50",           "equity_index", "1w,1m,3m",     "default"),
    ("^N225",     "Nikkei 225 (Japan)",      "equity_index", "1m,3m",        "default"),
]


DEFAULT_DATA_OBJECTS: list[tuple[str, str, str, str | None, str, str, float | None, str, str, str]] = [
    ("market.price", "Market price", "market price", "prices", "Daily close and market-price observations for tracked assets.", "market", 96, "daily close", "Markets", "current_state"),
    ("market.signal", "Market signal", "market signal", "signals", "Computed momentum, trend, volatility, drawdown, regime and macro-curve signals.", "market", 96, "daily", "Markets", "current_state"),
    ("macro.indicator", "Macro indicator", "official macro indicator", "indicators", "Official macroeconomic time series and revised values.", "official_macro", 2160, "official/daily", "Macro", "evidence"),
    ("calendar.event", "Calendar event", "scheduled macro catalyst", "calendar_events", "Scheduled macro, policy and energy catalysts with stable event keys.", "calendar", 720, "event-aware", "Macro", "catalyst"),
    ("macro.release.actual", "Macro release actual", "released macro actual", "macro_release_actuals", "Matched actual/expected/previous values for scheduled macro releases.", "calendar", 72, "hourly around release windows", "Macro", "fresh_result"),
    ("current.event", "Current event", "current event", "current_events", "Live current-events surface combining catalysts, releases and market-moving news.", "intelligence", 1, "15m canary", "Overview", "wake_up"),
    ("gdelt.stream", "GDELT stream", "GDELT stream aggregate", "gdelt_streams", "Daily stream/region/country news aggregates classified into market-useful streams.", "news", 36, "daily completed, optional intraday", "Map", "risk_context"),
    ("gdelt.example", "GDELT example", "GDELT stream example", "gdelt_stream_examples", "Top article/example rows that explain high-signal GDELT stream buckets.", "news", 36, "daily completed, retained bounded", "Map", "evidence_example"),
    ("risk.hotspot", "Risk hotspot", "persistent risk hotspot", "risk_hotspots", "Manual and fetched geopolitical, shipping, conflict and supply-chain watchpoints.", "risk", 720, "daily/manual", "Map", "risk_context"),
    ("sanctions.program", "Sanctions program", "sanctions program", "sanctions", "Restricted-party, program and sanctions-list context.", "sanctions", 30, "6h", "Map", "risk_context"),
    ("social.mention", "Social mention", "social mention", "social_mentions", "Passive Reddit/social ticker attention observations.", "social", 36, "hourly", "Data", "weak_signal"),
    ("social.rumour", "Rumour signal", "rumour signal", "rumour_signals", "Derived social attention spike and rumour checks.", "social", 48, "daily", "Data", "weak_signal"),
    ("news.item", "News item", "news item", "news_items", "RSS and official news items used for contextual review.", "news", 24, "hourly", "Data", "evidence"),
    ("weather.observation", "Weather observation", "weather observation", "weather_obs", "Weather archive rows used for commodity and supply-chain context.", "weather", 168, "daily", "Data", "weak_signal"),
    ("weather.correlation", "Weather correlation", "weather correlation", "weather_correlations", "Exploratory weather/market correlation materialization.", "weather", 336, "daily", "Data", "weak_signal"),
    ("company.fundamental", "Company fundamental", "company fundamental", "company_fundamentals", "EDGAR company fundamental facts.", "fundamental", 2880, "daily", "Data", "asset_context"),
    ("insider.trade", "Insider trade", "insider trade", "insider_trades", "SEC Form 4 insider transaction rows.", "insider", 336, "6h", "Data", "asset_context"),
    ("oracle.world_pull", "World Pull", "compiled world pull", "intelligence_packages", "Deterministic compiled macro/risk pull for world and regions.", "intelligence", 3, "hourly", "Overview", "summary"),
    ("oracle.impact", "Atlas impact", "atlas impact link", "oracle_impacts", "Evidence-to-entity impact links used by the map and hierarchy.", "intelligence", 3, "hourly", "Map", "summary"),
    ("oracle.index", "Atlas index", "atlas index snapshot", "oracle_index_snapshots", "Rolled-up world, regional, sector, market and asset reads.", "intelligence", 3, "hourly", "Overview", "summary"),
    ("historical.state", "Historical state", "historical state reference", "historical_state_values", "No-lookahead historical feature rows for comparison and analogue references.", "intelligence", None, "recent daily/full backfill", "Predictions", "historical_reference"),
    ("historical.forward_return", "Historical forward return", "historical forward return", "historical_forward_returns", "Forward return outcomes used for evaluation and tuning, never as live input.", "intelligence", None, "recent daily/full backfill", "Predictions", "evaluation"),
    ("label.evaluation", "Label evaluation", "label evaluation", "label_evaluations", "Evidence table comparing labels and weight profiles against matured forward returns.", "intelligence", None, "daily", "Predictions", "evaluation"),
    ("prediction.context", "Prediction context pack", "prediction context pack", "prediction_context_packs", "Compact named context packages passed to Claude instead of raw scattered blocks.", "intelligence", 24, "per prediction", "Predictions", "prompt_input"),
    ("source.run", "Source run", "ingestion run", "source_runs", "Fetcher and pipeline run telemetry.", "operations", 24, "every pipeline", "Runs", "source_health"),
    ("data.change", "Data change event", "high-impact data change", "data_change_events", "Queue of inserted or materially updated high-impact rows for research-note review.", "operations", 6, "event-driven", "Runs", "wake_up"),
]


DEFAULT_CALENDAR_EVENTS: list[tuple[str, str, str, str, int, str, str, str, str, str]] = [
    (
        "2026-05-12", "08:30 ET", "US", "inflation", 5,
        "US CPI for April 2026",
        "BLS release; headline/core CPI likely moves USD, rates, gold, SPY.",
        "High-volatility input for Fed path. Watch DXY, 10Y, gold, TLT, SPY.",
        "BLS", "https://www.bls.gov/schedule/news_release/cpi.htm?lv=true",
    ),
    (
        "2026-05-13", "08:00 CET", "SE", "inflation", 5,
        "Sweden CPI / CPIF April 2026",
        "Regular SCB CPI publication after flash CPI showed CPIF slowing to 0.8%.",
        "Direct input for OMX and SEK/Riksbank expectations.",
        "SCB", "https://www.scb.se/en/PR0101",
    ),
    (
        "2026-05-20", "07:00 UK", "UK", "inflation", 4,
        "UK CPI April 2026",
        "ONS confirmed release for Consumer price inflation, UK: April 2026.",
        "Watch FTSE, GBP, gilt proxies, and spillover into European rates.",
        "ONS", "https://www.gov.uk/government/statistics/announcements/consumer-price-inflation-uk-april-2026",
    ),
    (
        "2026-05-20", "12:00 CET", "EU", "inflation", 4,
        "Euro area HICP April 2026 final",
        "Full April HICP data for euro area.",
        "Important for ECB repricing and STOXX/OMX rate-sensitive sectors.",
        "ECB/Eurostat", "https://www.ecb.europa.eu/press/calendars/statscal/ges/html/sthicp.en.html",
    ),
    (
        "2026-06-02", "15:00 CET", "EU", "inflation", 5,
        "Euro area HICP flash estimate May 2026",
        "Flash estimate for May HICP.",
        "Fast ECB-policy input; watch EUR, STOXX, OMX, gold.",
        "ECB/Eurostat", "https://www.ecb.europa.eu/press/calendars/statscal/ges/html/sthicp.en.html",
    ),
    (
        "2026-06-11", "14:15 CET", "EU", "central_bank", 5,
        "ECB monetary policy decision",
        "Governing Council monetary-policy meeting.",
        "Potentially high impact for European equities, EUR, OMX and long duration.",
        "ECB", "https://www.ecb.europa.eu/press/calendars/html/index.en.html",
    ),
    (
        "2026-06-17", "14:00 ET", "US", "central_bank", 5,
        "FOMC rate decision",
        "Two-day FOMC meeting June 16-17; statement at 14:00 ET, press conference 14:30 ET.",
        "High impact for USD, yields, gold, TLT, SPY and global risk.",
        "Federal Reserve", "https://www.federalreserve.gov/newsevents/2026-june.htm",
    ),
    (
        "2026-05-28", "12:00 ET", "US", "energy", 4,
        "EIA weekly petroleum status report",
        "EIA WPSR release delayed by Memorial Day holiday; crude, gasoline and distillate stock changes.",
        "Energy catalyst for CL=F, inflation breakevens, transports and broad risk appetite.",
        "EIA", "https://www.eia.gov/petroleum/supply/weekly/schedule.php",
    ),
    (
        "2026-05-29", "08:30 ET", "US", "inflation", 5,
        "US PCE price index April 2026",
        "BEA Personal Income and Outlays release; headline/core PCE is the Fed's preferred inflation gauge.",
        "High-impact input for USD, rates, gold, TLT and SPY.",
        "BEA", "https://www.bea.gov/news/schedule",
    ),
    (
        "2026-06-02", "10:00 ET", "US", "labour", 3,
        "US JOLTS April 2026",
        "BLS Job Openings and Labor Turnover Survey for April 2026.",
        "Labour-market balance input; second-order for Fed path unless surprise is large.",
        "BLS", "https://www.bls.gov/schedule/2026/",
    ),
    (
        "2026-06-05", "08:30 ET", "US", "labour", 5,
        "US employment situation May 2026",
        "BLS payrolls, unemployment and wage release for May 2026.",
        "High-volatility catalyst for USD, yields, gold, TLT, SPY and global risk.",
        "BLS", "https://www.bls.gov/schedule/2026/",
    ),
    (
        "2026-06-10", "08:30 ET", "US", "inflation", 5,
        "US CPI May 2026",
        "BLS Consumer Price Index release for May 2026.",
        "High-volatility input for Fed path. Watch DXY, 10Y, gold, TLT, SPY.",
        "BLS", "https://www.bls.gov/schedule/2026/",
    ),
    (
        "2026-06-11", "08:30 ET", "US", "inflation", 4,
        "US PPI May 2026",
        "BLS Producer Price Index release for May 2026.",
        "Pipeline inflation read; strongest when it confirms CPI/PCE pressure.",
        "BLS", "https://www.bls.gov/schedule/2026/",
    ),
    (
        "2026-06-17", "09:30 CET", "SE", "central_bank", 5,
        "Riksbank monetary policy decision",
        "Riksbank monetary-policy decision and communication window.",
        "Direct catalyst for OMX, SEK and Nordic rates; watch European spillover.",
        "Riksbank", "https://www.riksbank.se/en-gb/press-and-published/calendar/calendar-2026/?month=6",
    ),
    (
        "2026-06-18", "12:00 UK", "UK", "central_bank", 4,
        "Bank of England MPC decision",
        "Bank of England Monetary Policy Committee Summary and minutes.",
        "Catalyst for GBP, gilts, FTSE and European rates.",
        "Bank of England", "https://www.bankofengland.co.uk/news/2025/september/monetary-policy-committee-dates-for-2026",
    ),
    (
        "2026-06-19", "", "JP", "central_bank", 4,
        "Bank of Japan monetary policy decision",
        "Bank of Japan June 2026 MPM policy decision release window.",
        "Catalyst for JPY, Japanese equities, global duration and carry/risk appetite.",
        "Bank of Japan", "https://www.boj.or.jp/en/mopo/mpmsche_minu/index.htm",
    ),
    (
        "2026-06-25", "08:30 ET", "US", "gdp", 4,
        "US GDP Q1 2026 third estimate",
        "BEA GDP third estimate, industries, corporate profits, state GDP and state personal income for Q1 2026.",
        "Growth and profit-mix catalyst for SPY, rates and USD if materially revised.",
        "BEA", "https://www.bea.gov/news/schedule",
    ),
    (
        "2026-06-25", "08:30 ET", "US", "inflation", 5,
        "US PCE price index May 2026",
        "BEA Personal Income and Outlays release for May 2026.",
        "High-impact Fed inflation input; watch rates, USD, gold and equities.",
        "BEA", "https://www.bea.gov/news/schedule",
    ),
]


DEFAULT_RISK_HOTSPOTS: list[tuple[str, str, str, str, int, float, float, str, str]] = [
    (
        "Russia-Ukraine war", "Europe", "UA", "conflict", 5, 49.0, 31.4,
        "Ongoing high-impact war zone; energy, grains, defense, sanctions and European risk premium.",
        "manual",
    ),
    (
        "Strait of Hormuz / Iran risk", "Middle East", "IR", "conflict", 5, 26.6, 56.2,
        "Hormuz disruption risk; direct read-through to oil, inflation expectations and risk assets.",
        "manual",
    ),
    (
        "Israel-Gaza / regional spillover", "Middle East", "IL", "conflict", 4, 31.5, 34.5,
        "Regional escalation risk; safe-haven, oil and defense-sector relevance.",
        "manual",
    ),
    (
        "Red Sea / Bab el-Mandeb shipping risk", "Middle East", "YM", "conflict", 4, 12.6, 43.4,
        "Shipping-lane risk for container freight, energy flows and European import inflation.",
        "manual",
    ),
    (
        "Taiwan Strait", "Asia", "CN", "conflict", 4, 24.0, 121.0,
        "Semiconductor and Asia-risk premium hotspot; watch tech supply chain and Asian equities.",
        "manual",
    ),
    (
        "Korean Peninsula", "Asia", "KR", "conflict", 3, 37.6, 127.0,
        "Recurring military escalation risk affecting Korea/Japan and Asian risk appetite.",
        "manual",
    ),
]


DEFAULT_DATA_LABELS: list[tuple[str, str, str, str, float]] = [
    # label_id, label_type, label, description, default_weight
    ("theme:inflation", "theme", "inflation", "CPI, HICP, CPIF and broad price-pressure evidence.", 1.15),
    ("theme:central_bank", "theme", "central_bank", "Policy-rate, central-bank and meeting-cycle evidence.", 1.15),
    ("theme:interest", "theme", "interest", "Yield, rates and duration-pressure evidence.", 1.10),
    ("theme:monetary", "theme", "monetary", "Money supply, balance-sheet and monetary aggregate evidence.", 1.00),
    ("theme:gdp", "theme", "gdp", "GDP and national-accounts growth evidence.", 0.95),
    ("theme:growth", "theme", "growth", "Growth pulse and demand-side evidence.", 1.00),
    ("theme:labour", "theme", "labour", "Employment, unemployment and wage-market evidence.", 0.95),
    ("theme:trade", "theme", "trade", "Trade, tariff, shipping and cross-border flow evidence.", 1.05),
    ("theme:currency", "theme", "currency", "FX and currency-pressure evidence.", 1.00),
    ("theme:energy", "theme", "energy", "Energy market and energy-cost evidence.", 1.15),
    ("theme:oil_price", "theme", "oil_price", "Oil-price and crude-market evidence.", 1.15),
    ("theme:conflict", "theme", "conflict", "War, military and geopolitical escalation evidence.", 1.25),
    ("theme:sanctions", "theme", "sanctions", "Sanctions and restricted-party evidence.", 1.10),
    ("theme:disaster", "theme", "disaster", "Natural disaster and disruption evidence.", 1.05),
    ("theme:weather", "theme", "weather", "Weather observation and weather-correlation evidence.", 0.90),
    ("theme:political", "theme", "political", "Policy and political-risk evidence.", 1.00),
    ("theme:banking", "theme", "banking", "Banking, funding and financial-stability evidence.", 1.10),
    ("theme:debt", "theme", "debt", "Debt, fiscal and credit-sustainability evidence.", 1.05),
    ("theme:industry", "theme", "industry", "Industrial production, manufacturing and real-economy cycle evidence.", 0.95),
    ("theme:housing", "theme", "housing", "Housing, construction and property-market evidence.", 0.95),
    ("theme:positioning", "theme", "positioning", "Investor positioning, speculative flow and crowded-trade evidence.", 0.90),
    ("theme:sentiment", "theme", "sentiment", "Survey, confidence and sentiment evidence.", 0.85),
    ("theme:retail_sales", "theme", "retail_sales", "Retail sales and household spending evidence.", 0.95),
    ("theme:capital_flows", "theme", "capital_flows", "Cross-border flow and portfolio-allocation evidence.", 1.00),
    ("theme:rates", "theme", "rates", "Rates-pressure alias used by source rows that distinguish rates from interest.", 1.10),
    ("theme:social_heat", "theme", "social_heat", "Retail/social attention and discussion-spike evidence.", 0.75),
    ("theme:fundamentals", "theme", "fundamentals", "Company fundamental and filing-derived evidence.", 0.85),
    ("theme:insider", "theme", "insider", "Insider activity and Form 4 transaction evidence.", 0.85),
    ("theme:earnings_quality", "theme", "earnings_quality", "Revenue, margin, EPS and income-statement evidence.", 0.90),
    ("theme:balance_sheet", "theme", "balance_sheet", "Assets, liabilities and equity evidence.", 0.90),
    ("theme:leverage", "theme", "leverage", "Debt and leverage evidence.", 0.95),
    ("theme:cash", "theme", "cash", "Cash, liquidity and cash-position evidence.", 0.90),
    ("theme:stockmarket", "theme", "stockmarket", "Broad equity-market evidence.", 0.95),
    ("theme:macro_news", "theme", "macro news", "News evidence about official macro releases, growth, inflation, labour, PMIs and policy rates.", 1.08),
    ("theme:market_moving_news", "theme", "market-moving news", "News buckets that typically move liquid index, rates, FX, oil or gold markets.", 1.16),
    ("asset_impact:sp500", "asset_impact", "S&P 500 impact", "News evidence commonly relevant to S&P 500 and SPY direction or volatility.", 1.10),
    ("asset_impact:gold", "asset_impact", "Gold impact", "News evidence commonly relevant to gold, safe-haven demand and real-rate sensitivity.", 1.10),
    ("asset_impact:oil", "asset_impact", "Oil impact", "News evidence commonly relevant to crude oil, energy supply and inflation-through-energy channels.", 1.15),
    ("theme:crisis", "theme", "crisis", "Historical market, financial or macro crisis event evidence.", 1.10),
    ("theme:regime", "theme", "regime", "Historical macro-regime shift event evidence.", 1.00),
    ("theme:risk", "theme", "risk", "General risk evidence that does not fit a narrower theme.", 1.00),
    ("theme:momentum", "theme", "momentum", "Return and momentum-derived market signals.", 0.90),
    ("theme:trend", "theme", "trend", "Moving-average and trend-following market signals.", 0.90),
    ("theme:volatility", "theme", "volatility", "Realized volatility and volatility-regime signals.", 1.00),
    ("theme:drawdown", "theme", "drawdown", "Drawdown and market-stress technical signals.", 0.95),
    ("theme:macro_curve", "theme", "macro_curve", "Yield-curve and cross-market macro regime signals.", 1.05),
    ("stream:economy_news", "stream", "Economy news", "GDELT stream for inflation, debt, housing, labour, growth and broad economy news.", 1.05),
    ("stream:policy_rates", "stream", "Policy and rates", "GDELT stream for central-bank, monetary-policy and rates news.", 1.15),
    ("stream:major_disaster", "stream", "Major disasters", "GDELT stream for natural disasters with potential societal or supply impact.", 1.10),
    ("stream:political_risk", "stream", "Political risk", "GDELT stream for elections, policy conflict and governance risk.", 1.00),
    ("stream:conflict_security", "stream", "Conflict and security", "GDELT stream for conflict, war, terrorism and security stress.", 1.20),
    ("stream:trade_sanctions_supply", "stream", "Trade and sanctions", "GDELT stream for sanctions, tariffs, shipping and supply-chain friction.", 1.10),
    ("stream:energy_commodities", "stream", "Energy and commodities", "GDELT stream for oil, gas, energy and commodity-market stress.", 1.15),
    ("stream:market_stress", "stream", "Market stress", "GDELT stream for equity, banking, credit, bankruptcy and FX stress.", 1.10),
    ("direction:inflation-risk", "direction", "inflation-risk", "Evidence points toward higher inflation or inflation sensitivity.", 1.10),
    ("direction:rates-volatility", "direction", "rates-volatility", "Evidence likely affects policy-rate or yield volatility.", 1.10),
    ("direction:growth-watch", "direction", "growth-watch", "Evidence is most useful as a growth-watch signal.", 0.95),
    ("direction:trade-friction", "direction", "trade-friction", "Evidence points toward trade or logistics friction.", 1.05),
    ("direction:fx-volatility", "direction", "fx-volatility", "Evidence likely affects FX volatility.", 1.00),
    ("direction:energy-upside-risk", "direction", "energy-upside-risk", "Evidence points toward energy upside or supply risk.", 1.15),
    ("direction:risk-off", "direction", "risk-off", "Evidence points toward risk-off market pressure.", 1.15),
    ("direction:supply-risk", "direction", "supply-risk", "Evidence points toward physical supply disruption.", 1.10),
    ("direction:policy-risk", "direction", "policy-risk", "Evidence points toward policy or political uncertainty.", 1.00),
    ("direction:credit-risk", "direction", "credit-risk", "Evidence points toward credit or liquidity risk.", 1.10),
    ("direction:risk-appetite-watch", "direction", "risk-appetite-watch", "Evidence is useful for risk-appetite monitoring.", 0.95),
    ("direction:macro-pressure", "direction", "macro-pressure", "General macro pressure without a narrower direction.", 1.00),
    ("market_bias:bullish", "market_bias", "bullish", "Generally supportive for the linked asset or entity.", 1.00),
    ("market_bias:bearish", "market_bias", "bearish", "Generally negative for the linked asset or entity.", 1.00),
    ("market_bias:risk-on", "market_bias", "risk-on", "Risk appetite supportive.", 1.00),
    ("market_bias:risk-off", "market_bias", "risk-off", "Risk appetite defensive.", 1.05),
    ("market_bias:inflation-up", "market_bias", "inflation-up", "Inflation pressure higher.", 1.05),
    ("market_bias:rates-up", "market_bias", "rates-up", "Rates pressure higher.", 1.05),
    ("market_bias:growth-down", "market_bias", "growth-down", "Growth pressure lower.", 1.05),
    ("market_bias:oil-up", "market_bias", "oil-up", "Oil or energy pressure higher.", 1.05),
    ("market_bias:usd-up", "market_bias", "usd-up", "USD pressure higher.", 1.00),
    ("market_bias:mixed", "market_bias", "mixed", "Mixed read requiring confirmation.", 0.90),
    ("market_bias:neutral", "market_bias", "neutral", "No strong directional read.", 0.75),
    ("market_bias:volatility-up", "market_bias", "volatility-up", "Volatility pressure higher.", 1.05),
    ("market_bias:watch", "market_bias", "watch", "Watchlist state without a strong directional read.", 0.85),
    ("market_bias:macro-headwind", "market_bias", "macro headwind", "Macro evidence is a headwind for the linked entity.", 1.00),
    ("market_bias:risk-headwind", "market_bias", "risk headwind", "Risk evidence is a headwind for the linked entity.", 1.05),
    ("market_bias:supportive-watch", "market_bias", "supportive/watch", "Evidence is partly supportive but needs confirmation.", 0.90),
    ("market_bias:bearish-risk-off", "market_bias", "bearish/risk-off", "Bearish and defensive risk-off read.", 1.05),
    ("market_bias:bullish-upside-risk", "market_bias", "bullish/upside risk", "Upside risk for the linked sector or asset.", 1.05),
    ("market_bias:growth-watch", "market_bias", "growth watch", "Growth-sensitive watch state.", 0.90),
    ("market_bias:bullish-safe-haven", "market_bias", "bullish/safe haven", "Safe-haven demand supportive.", 1.00),
    ("market_bias:bullish-volatile", "market_bias", "bullish/volatile", "Supportive but volatile market read.", 0.95),
    ("market_bias:mixed-sek-supportive", "market_bias", "mixed/SEK supportive", "Mixed read with a SEK-supportive component.", 0.90),
    ("market_bias:bearish-cost-risk", "market_bias", "bearish/cost risk", "Bearish read driven by cost or margin pressure.", 1.00),
    ("market_bias:mixed-supportive", "market_bias", "mixed/supportive", "Mixed read with a supportive component.", 0.90),
    ("display:macro-pressure", "display", "Macro pressure", "Plain-language UI label for broad macro headwinds.", 1.00),
    ("display:rates-pressure", "display", "Rates pressure", "Plain-language UI label for rate and yield pressure.", 1.05),
    ("display:inflation-pressure", "display", "Inflation pressure", "Plain-language UI label for inflation-sensitive evidence.", 1.10),
    ("display:growth-watch", "display", "Growth watch", "Plain-language UI label for growth-sensitive evidence.", 0.95),
    ("display:risk-pressure", "display", "Risk pressure", "Plain-language UI label for defensive risk pressure.", 1.10),
    ("display:supply-shock", "display", "Supply shock", "Plain-language UI label for physical supply disruption.", 1.10),
    ("display:trade-friction", "display", "Trade friction", "Plain-language UI label for sanctions, tariffs and logistics friction.", 1.05),
    ("display:energy-squeeze", "display", "Energy squeeze", "Plain-language UI label for energy upside or supply tightness.", 1.10),
    ("display:fx-stress", "display", "FX stress", "Plain-language UI label for currency-pressure evidence.", 1.00),
    ("display:credit-stress", "display", "Credit stress", "Plain-language UI label for credit and liquidity risk.", 1.05),
    ("display:safe-haven-bid", "display", "Safe-haven bid", "Plain-language UI label for defensive haven demand.", 1.00),
    ("display:social-heat", "display", "Social heat", "Plain-language UI label for social attention spikes.", 0.80),
    ("display:insider-signal", "display", "Insider signal", "Plain-language UI label for insider trading evidence.", 0.85),
    ("display:weather-drag", "display", "Weather drag", "Plain-language UI label for adverse weather-linked evidence.", 0.85),
    ("display:supportive-watch", "display", "Supportive watch", "Plain-language UI label for constructive but unconfirmed reads.", 0.90),
    ("display:mixed-read", "display", "Mixed read", "Plain-language UI label for conflicting evidence.", 0.85),
    ("display:momentum", "display", "Momentum", "Plain-language UI label for return and momentum signals.", 0.90),
    ("display:trend", "display", "Trend", "Plain-language UI label for trend and moving-average signals.", 0.90),
    ("display:volatility", "display", "Volatility", "Plain-language UI label for realized volatility and volatility regimes.", 1.00),
    ("display:drawdown", "display", "Drawdown", "Plain-language UI label for drawdown signals.", 0.95),
    ("display:macro-curve", "display", "Macro curve", "Plain-language UI label for yield-curve and macro-regime signals.", 1.05),
    ("source_family:market", "source_family", "market", "Market price and technical signal sources.", 1.00),
    ("source_family:official_macro", "source_family", "official_macro", "Official macroeconomic sources.", 1.15),
    ("source_family:calendar", "source_family", "calendar", "Scheduled macro calendar and event scenario sources.", 1.10),
    ("source_family:news", "source_family", "news", "News and GDELT intensity sources.", 0.95),
    ("source_family:risk", "source_family", "risk", "Persistent risk hotspot and geopolitical sources.", 1.15),
    ("source_family:sanctions", "source_family", "sanctions", "Sanctions list and program sources.", 1.10),
    ("source_family:social", "source_family", "social", "Passive social-listening sources.", 0.75),
    ("source_family:weather", "source_family", "weather", "Weather observations and correlation sources.", 0.80),
    ("source_family:fundamental", "source_family", "fundamental", "Company fundamental and insider sources.", 0.85),
    ("source_family:insider", "source_family", "insider", "Insider-transaction and ownership-change sources.", 0.85),
    ("source_family:disaster", "source_family", "disaster", "Disaster and physical disruption signal sources.", 0.95),
    ("source_family:intelligence", "source_family", "intelligence", "Compiled intelligence and Atlas impact sources.", 1.15),
    ("source_family:operations", "source_family", "operations", "Operational telemetry, data contracts and change-event queues.", 0.70),
    ("release_family:cpi", "release_family", "CPI", "Consumer price inflation release family.", 1.15),
    ("release_family:cpif", "release_family", "CPIF", "Swedish fixed-interest inflation release family.", 1.15),
    ("release_family:hicp", "release_family", "HICP", "Harmonised euro-area inflation release family.", 1.15),
    ("release_family:ppi", "release_family", "PPI", "Producer price inflation release family.", 1.05),
    ("release_family:pce", "release_family", "PCE", "US personal consumption expenditures inflation release family.", 1.15),
    ("release_family:payrolls", "release_family", "Payrolls", "Employment situation and payroll release family.", 1.10),
    ("release_family:unemployment", "release_family", "Unemployment", "Unemployment-rate release family.", 1.00),
    ("release_family:jolts", "release_family", "JOLTS", "US job openings release family.", 0.90),
    ("release_family:gdp", "release_family", "GDP", "Gross domestic product release family.", 1.00),
    ("release_family:pmi", "release_family", "PMI", "Purchasing managers' index release family.", 0.95),
    ("release_family:retail_sales", "release_family", "Retail sales", "Retail-sales release family.", 0.95),
    ("release_family:confidence", "release_family", "Confidence", "Consumer and business confidence release family.", 0.85),
    ("release_family:fomc", "release_family", "FOMC", "Federal Reserve policy decision family.", 1.20),
    ("release_family:ecb", "release_family", "ECB", "European Central Bank policy decision family.", 1.15),
    ("release_family:riksbank", "release_family", "Riksbank", "Riksbank policy decision family.", 1.15),
    ("release_family:boe", "release_family", "BoE", "Bank of England policy decision family.", 1.10),
    ("release_family:boj", "release_family", "BoJ", "Bank of Japan policy decision family.", 1.05),
    ("release_family:eia", "release_family", "EIA oil inventories", "EIA weekly petroleum status release family.", 1.05),
    ("release_family:trade", "release_family", "Trade", "Trade balance and trade-policy release family.", 1.00),
    ("release_family:sanctions", "release_family", "Sanctions", "Sanctions-window and program-update family.", 1.05),
    ("asset_group:commodity", "asset_group", "commodity", "Commodity targets and commodity-linked signals.", 1.00),
    ("asset_group:energy", "asset_group", "energy", "Oil, gas and energy-sensitive targets.", 1.10),
    ("asset_group:equity_index", "asset_group", "equity_index", "Broad equity index targets.", 1.00),
    ("asset_group:equity_sector", "asset_group", "equity_sector", "Equity-sector ETF and sector-index targets.", 0.95),
    ("asset_group:emerging_market", "asset_group", "emerging_market", "Emerging-market equity and regional targets.", 1.00),
    ("asset_group:volatility", "asset_group", "volatility", "Volatility-index and volatility proxy targets.", 1.05),
    ("asset_group:single_stock", "asset_group", "single_stock", "Single-company equity targets and source rows.", 0.90),
    ("asset_group:bond", "asset_group", "bond", "Duration and bond-proxy targets.", 1.00),
    ("asset_group:fx", "asset_group", "fx", "Currency and FX targets.", 1.00),
    ("region:global", "region", "Global", "Global aggregate region.", 1.00),
    ("region:us", "region", "US", "United States region.", 1.05),
    ("region:eu", "region", "EU", "European Union and euro-area region.", 1.00),
    ("region:europe_non_eu", "region", "Europe non-EU", "European regional context outside the EU/euro-area bucket.", 0.95),
    ("region:nordics", "region", "Nordics", "Nordic and Swedish regional context.", 1.00),
    ("region:asia", "region", "Asia", "Asian macro and market region.", 1.00),
    ("region:middle_east", "region", "Middle East", "Middle East geopolitical and energy-flow context.", 1.10),
    ("region:energy_commodities", "region", "Energy / Commodities", "Commodity and energy-flow pseudo-region used by Atlas entities.", 1.05),
    ("country:us", "country", "US", "United States country code.", 1.05),
    ("country:se", "country", "SE", "Sweden country code.", 1.00),
    ("country:de", "country", "DE", "Germany country code.", 1.00),
    ("country:fr", "country", "FR", "France country code.", 1.00),
    ("country:gb", "country", "GB", "United Kingdom country code.", 1.00),
    ("country:jp", "country", "JP", "Japan country code.", 1.00),
    ("country:cn", "country", "CN", "China country code.", 1.00),
    ("country:ca", "country", "CA", "Canada country code.", 1.00),
    ("country:in", "country", "IN", "India country code.", 1.00),
    ("country:ir", "country", "IR", "Iran country code.", 1.10),
    ("country:ua", "country", "UA", "Ukraine country code.", 1.10),
    ("country:kr", "country", "KR", "South Korea country code.", 1.00),
    ("country:ch", "country", "CH", "Switzerland country code.", 1.00),
    ("country:no", "country", "NO", "Norway country code.", 1.00),
    ("country:au", "country", "AU", "Australia country code.", 1.00),
    ("country:br", "country", "BR", "Brazil country code.", 1.00),
    ("country:ae", "country", "AE", "United Arab Emirates country code.", 1.00),
    ("country:mx", "country", "MX", "Mexico country code.", 1.00),
    ("country:tr", "country", "TR", "Turkey country code.", 1.00),
    ("country:il", "country", "IL", "Israel country code.", 1.05),
    ("country:ym", "country", "YM", "Yemen country code as used by GDELT/risk source rows.", 1.05),
]


DEFAULT_LABEL_WEIGHT_PROFILES: list[tuple[str, str, str]] = [
    ("default", "Default weights", "Canonical label weights without experiment overrides."),
    ("macro_clean", "Macro-clean emphasis", "Raises official macro and policy streams while damping noisy passive feeds."),
    ("risk_stress", "Risk-stress emphasis", "Raises conflict, disaster, sanctions and market-stress labels for stress testing."),
]


DEFAULT_LABEL_WEIGHT_OVERRIDES: list[tuple[str, str, float, str | None]] = [
    ("macro_clean", "source_family:official_macro", 1.25, "Prefer official macro in cleaner macro experiments."),
    ("macro_clean", "source_family:calendar", 1.18, "Scheduled catalysts receive extra weight."),
    ("macro_clean", "source_family:social", 0.55, "Dampen passive social noise."),
    ("macro_clean", "stream:policy_rates", 1.25, "Rates and policy are core macro streams."),
    ("macro_clean", "stream:economy_news", 1.15, "Economy stream remains relevant but below official releases."),
    ("risk_stress", "theme:conflict", 1.40, "Stress profile emphasizes conflict risk."),
    ("risk_stress", "theme:disaster", 1.30, "Stress profile emphasizes physical disruption."),
    ("risk_stress", "theme:sanctions", 1.30, "Stress profile emphasizes sanctions/trade risk."),
    ("risk_stress", "stream:major_disaster", 1.35, "Stress profile emphasizes disaster streams."),
    ("risk_stress", "stream:conflict_security", 1.40, "Stress profile emphasizes security streams."),
    ("risk_stress", "stream:trade_sanctions_supply", 1.30, "Stress profile emphasizes trade and sanctions streams."),
    ("risk_stress", "stream:market_stress", 1.25, "Stress profile emphasizes market stress streams."),
]


DEFAULT_DATA_LABEL_ASSIGNMENTS: list[tuple[str, str, str | None, str | None, str | None, float | None, float, str | None]] = [
    # label_id, target_type, target_table, target_column, target_value, weight_override, confidence, notes
    ("source_family:market", "table", "prices", None, None, None, 1.0, "Market close prices."),
    ("source_family:market", "table", "signals", None, None, None, 1.0, "Computed market and macro signals."),
    ("source_family:official_macro", "table", "indicators", None, None, None, 1.0, "Official and public macro indicator store."),
    ("source_family:calendar", "table", "events", None, None, None, 1.0, "Legacy curated historical event archive."),
    ("source_family:calendar", "table", "calendar_events", None, None, None, 1.0, "Scheduled macro catalysts."),
    ("source_family:risk", "table", "risk_hotspots", None, None, None, 1.0, "Persistent risk watchpoints."),
    ("source_family:sanctions", "table", "sanctions", None, None, None, 1.0, "Sanctions list rows."),
    ("source_family:news", "table", "news_items", None, None, None, 1.0, "RSS and official news items."),
    ("source_family:news", "table", "gdelt_disaster_signals", None, None, None, 1.0, "GDELT disaster markers."),
    ("source_family:news", "table", "gdelt_streams", None, None, None, 1.0, "GDELT stream aggregate rows."),
    ("source_family:news", "table", "gdelt_stream_examples", None, None, None, 1.0, "GDELT stream example rows."),
    ("source_family:disaster", "table", "gdelt_disaster_signals", None, None, None, 1.0, "Physical disaster marker rows."),
    ("source_family:disaster", "table", "gdelt_streams", "stream", "major_disaster", None, 1.0, "Disaster stream aggregates."),
    ("source_family:social", "table", "social_mentions", None, None, None, 1.0, "Reddit listener rows."),
    ("source_family:social", "table", "rumour_signals", None, None, None, 1.0, "Reddit spike synthesis rows."),
    ("source_family:social", "table", "twitter_macro_posts", None, None, None, 1.0, "X/Twitter macro listener rows."),
    ("source_family:weather", "table", "weather_obs", None, None, None, 1.0, "Weather observations."),
    ("source_family:weather", "table", "weather_correlations", None, None, None, 1.0, "Weather/market correlations."),
    ("source_family:fundamental", "table", "company_fundamentals", None, None, None, 1.0, "Company fundamental rows."),
    ("source_family:fundamental", "table", "insider_trades", None, None, None, 1.0, "SEC Form 4 insider rows."),
    ("source_family:insider", "table", "insider_trades", None, None, None, 1.0, "SEC Form 4 insider rows."),
    ("source_family:intelligence", "table", "intelligence_packages", None, None, None, 1.0, "Compiled World Pull packages."),
    ("source_family:intelligence", "table", "macro_event_predictions", None, None, None, 1.0, "Macro-event scenario rows."),
    ("source_family:intelligence", "table", "oracle_impacts", None, None, None, 1.0, "Atlas impact links."),
    ("source_family:intelligence", "table", "oracle_index_snapshots", None, None, None, 1.0, "Atlas index snapshots."),
    ("source_family:intelligence", "table", "oracle_review_annotations", None, None, None, 1.0, "Research review annotations."),
    ("source_family:intelligence", "table", "historical_state_values", None, None, None, 1.0, "Historical comparison state values."),
    ("source_family:intelligence", "table", "historical_state_daily", None, None, None, 1.0, "Historical comparison daily summaries."),
    ("source_family:intelligence", "table", "historical_forward_returns", None, None, None, 1.0, "Historical forward return evaluation rows."),
    ("source_family:intelligence", "table", "label_evaluations", None, None, None, 1.0, "Label evidence evaluation rows."),
    ("source_family:intelligence", "table", "prediction_context_packs", None, None, None, 1.0, "Named context packs for Claude predictions."),
    ("source_family:intelligence", "table", "current_events", None, None, None, 1.0, "Current events canary rows."),
    ("source_family:operations", "table", "data_objects", None, None, None, 1.0, "Semantic data contract rows."),
    ("source_family:operations", "table", "data_change_events", None, None, None, 1.0, "High-impact data-change queue."),
    ("source_family:operations", "table", "source_runs", None, None, None, 1.0, "Run telemetry rows."),
    ("theme:inflation", "category", "indicators", "category", "inflation", None, 1.0, "Inflation indicator rows."),
    ("theme:inflation", "category", "calendar_events", "category", "inflation", None, 1.0, "Inflation calendar rows."),
    ("theme:inflation", "category", "macro_event_predictions", "category", "inflation", None, 1.0, "Inflation macro-event rows."),
    ("theme:central_bank", "category", "calendar_events", "category", "central_bank", None, 1.0, "Central-bank calendar rows."),
    ("theme:central_bank", "category", "macro_event_predictions", "category", "central_bank", None, 1.0, "Central-bank macro-event rows."),
    ("theme:labour", "category", "calendar_events", "category", "labour", None, 1.0, "Labour calendar rows."),
    ("theme:gdp", "category", "calendar_events", "category", "gdp", None, 1.0, "GDP calendar rows."),
    ("theme:energy", "category", "calendar_events", "category", "energy", None, 1.0, "Energy calendar rows."),
    ("theme:retail_sales", "category", "indicators", "category", "retail_sales", None, 1.0, "Retail-sales indicator rows."),
    ("theme:retail_sales", "category", "calendar_events", "category", "retail_sales", None, 1.0, "Retail-sales calendar rows."),
    ("theme:monetary", "category", "events", "type", "monetary", None, 1.0, "Legacy monetary event rows."),
    ("theme:political", "category", "events", "type", "political", None, 1.0, "Legacy political event rows."),
    ("theme:conflict", "category", "events", "type", "conflict", None, 1.0, "Legacy conflict event rows."),
    ("theme:crisis", "category", "events", "type", "crisis", None, 1.0, "Legacy crisis event rows."),
    ("theme:regime", "category", "events", "type", "regime", None, 1.0, "Legacy regime-shift event rows."),
    ("theme:interest", "category", "indicators", "category", "interest", None, 1.0, "Interest-rate indicator rows."),
    ("theme:rates", "category", "indicators", "category", "rates", None, 1.0, "Rates category rows."),
    ("theme:monetary", "category", "indicators", "category", "monetary", None, 1.0, "Monetary aggregate rows."),
    ("theme:gdp", "category", "indicators", "category", "gdp", None, 1.0, "GDP indicator rows."),
    ("theme:labour", "category", "indicators", "category", "labour", None, 1.0, "Labour indicator rows."),
    ("theme:trade", "category", "indicators", "category", "trade", None, 1.0, "Trade indicator rows."),
    ("theme:trade", "category", "news_items", "category", "trade", None, 1.0, "Trade news rows."),
    ("theme:currency", "category", "indicators", "category", "currency", None, 1.0, "Currency indicator rows."),
    ("theme:debt", "category", "indicators", "category", "debt", None, 1.0, "Debt indicator rows."),
    ("theme:industry", "category", "indicators", "category", "industry", None, 1.0, "Industrial-cycle indicator rows."),
    ("theme:housing", "category", "indicators", "category", "housing", None, 1.0, "Housing indicator rows."),
    ("theme:positioning", "category", "indicators", "category", "positioning", None, 1.0, "Positioning indicator rows."),
    ("theme:sentiment", "category", "indicators", "category", "sentiment", None, 1.0, "Sentiment indicator rows."),
    ("theme:capital_flows", "category", "indicators", "category", "capital_flows", None, 1.0, "Capital-flow indicator rows."),
    ("theme:conflict", "category", "risk_hotspots", "category", "conflict", None, 1.0, "Conflict risk rows."),
    ("stream:economy_news", "stream", "gdelt_streams", "stream", "economy_news", None, 1.0, "GDELT economy stream rows."),
    ("stream:policy_rates", "stream", "gdelt_streams", "stream", "policy_rates", None, 1.0, "GDELT policy/rates stream rows."),
    ("stream:major_disaster", "stream", "gdelt_streams", "stream", "major_disaster", None, 1.0, "GDELT disaster stream rows."),
    ("stream:political_risk", "stream", "gdelt_streams", "stream", "political_risk", None, 1.0, "GDELT political-risk stream rows."),
    ("stream:conflict_security", "stream", "gdelt_streams", "stream", "conflict_security", None, 1.0, "GDELT conflict/security stream rows."),
    ("stream:trade_sanctions_supply", "stream", "gdelt_streams", "stream", "trade_sanctions_supply", None, 1.0, "GDELT trade/sanctions stream rows."),
    ("stream:energy_commodities", "stream", "gdelt_streams", "stream", "energy_commodities", None, 1.0, "GDELT energy/commodities stream rows."),
    ("stream:market_stress", "stream", "gdelt_streams", "stream", "market_stress", None, 1.0, "GDELT market-stress stream rows."),
    ("theme:growth", "stream", "gdelt_streams", "stream", "economy_news", None, 0.9, "Economy stream theme link."),
    ("theme:central_bank", "stream", "gdelt_streams", "stream", "policy_rates", None, 1.0, "Policy/rates stream theme link."),
    ("theme:disaster", "stream", "gdelt_streams", "stream", "major_disaster", None, 1.0, "Disaster stream theme link."),
    ("theme:political", "stream", "gdelt_streams", "stream", "political_risk", None, 1.0, "Political-risk stream theme link."),
    ("theme:conflict", "stream", "gdelt_streams", "stream", "conflict_security", None, 1.0, "Conflict/security stream theme link."),
    ("theme:sanctions", "stream", "gdelt_streams", "stream", "trade_sanctions_supply", None, 1.0, "Trade/sanctions stream theme link."),
    ("theme:energy", "stream", "gdelt_streams", "stream", "energy_commodities", None, 1.0, "Energy/commodities stream theme link."),
    ("theme:stockmarket", "stream", "gdelt_streams", "stream", "market_stress", None, 1.0, "Market-stress stream theme link."),
    ("theme:macro_news", "stream", "gdelt_streams", "stream", "economy_news", None, 1.0, "Macro-news stream link."),
    ("theme:macro_news", "stream", "gdelt_streams", "stream", "policy_rates", None, 1.0, "Macro-news policy/rates stream link."),
    ("theme:macro_news", "stream", "gdelt_stream_examples", "stream", "economy_news", None, 1.0, "Macro-news example rows."),
    ("theme:macro_news", "stream", "gdelt_stream_examples", "stream", "policy_rates", None, 1.0, "Macro-news policy/rates example rows."),
    ("theme:market_moving_news", "stream", "gdelt_streams", "stream", "policy_rates", None, 1.0, "Market-moving policy/rates stream link."),
    ("theme:market_moving_news", "stream", "gdelt_streams", "stream", "conflict_security", None, 1.0, "Market-moving conflict/security stream link."),
    ("theme:market_moving_news", "stream", "gdelt_streams", "stream", "trade_sanctions_supply", None, 1.0, "Market-moving trade/supply stream link."),
    ("theme:market_moving_news", "stream", "gdelt_streams", "stream", "energy_commodities", None, 1.0, "Market-moving energy/commodities stream link."),
    ("theme:market_moving_news", "stream", "gdelt_streams", "stream", "market_stress", None, 1.0, "Market-moving market-stress stream link."),
    ("theme:market_moving_news", "stream", "gdelt_stream_examples", "stream", "policy_rates", None, 1.0, "Market-moving policy/rates example rows."),
    ("theme:market_moving_news", "stream", "gdelt_stream_examples", "stream", "conflict_security", None, 1.0, "Market-moving conflict/security example rows."),
    ("theme:market_moving_news", "stream", "gdelt_stream_examples", "stream", "trade_sanctions_supply", None, 1.0, "Market-moving trade/supply example rows."),
    ("theme:market_moving_news", "stream", "gdelt_stream_examples", "stream", "energy_commodities", None, 1.0, "Market-moving energy/commodities example rows."),
    ("theme:market_moving_news", "stream", "gdelt_stream_examples", "stream", "market_stress", None, 1.0, "Market-moving market-stress example rows."),
    ("asset_impact:sp500", "stream", "gdelt_streams", "stream", "economy_news", None, 0.9, "S&P 500 macro/economy stream link."),
    ("asset_impact:sp500", "stream", "gdelt_streams", "stream", "policy_rates", None, 1.0, "S&P 500 policy/rates stream link."),
    ("asset_impact:sp500", "stream", "gdelt_streams", "stream", "market_stress", None, 1.0, "S&P 500 market-stress stream link."),
    ("asset_impact:gold", "stream", "gdelt_streams", "stream", "policy_rates", None, 1.0, "Gold rates/safe-haven stream link."),
    ("asset_impact:gold", "stream", "gdelt_streams", "stream", "conflict_security", None, 1.0, "Gold conflict/security stream link."),
    ("asset_impact:gold", "stream", "gdelt_streams", "stream", "market_stress", None, 1.0, "Gold market-stress stream link."),
    ("asset_impact:oil", "stream", "gdelt_streams", "stream", "energy_commodities", None, 1.0, "Oil energy/commodities stream link."),
    ("asset_impact:oil", "stream", "gdelt_streams", "stream", "trade_sanctions_supply", None, 0.95, "Oil trade/supply stream link."),
    ("asset_impact:oil", "stream", "gdelt_streams", "stream", "conflict_security", None, 0.9, "Oil conflict/security stream link."),
    ("theme:banking", "signal_name", "signals", "signal_name", "news_rate_banking", None, 1.0, "Banking news-rate signal."),
    ("theme:banking", "signal_name", "signals", "signal_name", "news_count_banking", None, 1.0, "Banking news-count signal."),
    ("theme:banking", "stream", "gdelt_streams", "stream", "market_stress", None, 1.0, "Market-stress stream includes banking stress when present."),
    ("theme:banking", "stream", "gdelt_stream_examples", "stream", "market_stress", None, 1.0, "Market-stress examples include banking stress when present."),
    ("theme:banking", "category", "current_events", "category", "market_stress", None, 1.0, "Banking-stress current events."),
    ("theme:banking", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", "WB_1234_BANKING_INSTITUTIONS", None, 1.0, "GDELT banking-institution theme."),
    ("theme:banking", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", "ECON_BANKRUPTCY", None, 1.0, "GDELT bankruptcy/credit-stress theme."),
    ("theme:banking", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", "FINANCIAL_MARKETS", None, 0.8, "Financial-market stress theme."),
    ("theme:oil_price", "signal_name", "signals", "signal_name", "news_rate_oil_price", None, 1.0, "Oil-price news-rate signal."),
    ("theme:oil_price", "signal_name", "signals", "signal_name", "news_count_oil_price", None, 1.0, "Oil-price news-count signal."),
    ("theme:oil_price", "stream", "gdelt_streams", "stream", "energy_commodities", None, 1.0, "Energy/commodities stream includes oil price shocks."),
    ("theme:oil_price", "stream", "gdelt_stream_examples", "stream", "energy_commodities", None, 1.0, "Energy/commodities examples include oil price shocks."),
    ("theme:oil_price", "category", "current_events", "category", "energy_commodities", None, 1.0, "Oil/energy current events."),
    ("theme:oil_price", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", "ECON_OILPRICE", None, 1.0, "GDELT oil-price theme."),
    ("theme:oil_price", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", "WB_1015_ENERGY", None, 0.9, "Energy theme as oil-price context."),
    ("theme:oil_price", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", "ENERGY", None, 0.9, "Energy theme as oil-price context."),
    ("theme:oil_price", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", "OIL", None, 1.0, "Oil theme."),
    ("theme:sanctions", "category", "sanctions", "program", None, None, 0.8, "Sanctions program field."),
    ("theme:social_heat", "category", "social_mentions", "ticker", None, None, 0.8, "Social mention ticker field."),
    ("theme:social_heat", "category", "rumour_signals", "ticker", None, None, 0.8, "Rumour spike ticker field."),
    ("theme:weather", "weather_var", "weather_correlations", "weather_var", "temp_mean_c", None, 1.0, "Temperature correlation rows."),
    ("theme:weather", "weather_var", "weather_correlations", "weather_var", "precipitation_mm", None, 1.0, "Precipitation correlation rows."),
    ("theme:weather", "weather_var", "weather_correlations", "weather_var", "wind_max_kmh", None, 1.0, "Wind correlation rows."),
    ("theme:momentum", "signal_name", "signals", "signal_name", "ret_1d", None, 1.0, "Daily return signal rows."),
    ("theme:momentum", "signal_name", "signals", "signal_name", "ret_1w", None, 1.0, "Weekly return signal rows."),
    ("theme:momentum", "signal_name", "signals", "signal_name", "ret_1m", None, 1.0, "Monthly return signal rows."),
    ("theme:momentum", "signal_name", "signals", "signal_name", "ret_3m", None, 1.0, "Quarterly return signal rows."),
    ("theme:momentum", "signal_name", "signals", "signal_name", "ret_1y", None, 1.0, "Annual return signal rows."),
    ("theme:momentum", "signal_name", "signals", "signal_name", "ret_5y", None, 0.8, "Long-run return signal rows."),
    ("theme:trend", "signal_name", "signals", "signal_name", "px_vs_ma50", None, 1.0, "Price versus 50-day moving average."),
    ("theme:trend", "signal_name", "signals", "signal_name", "px_vs_ma200", None, 1.0, "Price versus 200-day moving average."),
    ("theme:trend", "signal_name", "signals", "signal_name", "ma50_above_ma200", None, 1.0, "50-day above 200-day trend state."),
    ("theme:trend", "signal_name", "signals", "signal_name", "ma_50", None, 0.8, "50-day moving average level."),
    ("theme:trend", "signal_name", "signals", "signal_name", "ma_200", None, 0.8, "200-day moving average level."),
    ("theme:volatility", "signal_name", "signals", "signal_name", "vol_30d_ann", None, 1.0, "Realized volatility signal rows."),
    ("theme:volatility", "signal_name", "signals", "signal_name", "vix_regime", None, 1.0, "VIX regime signal rows."),
    ("theme:momentum", "signal_name", "signals", "signal_name", "rsi_14", None, 0.9, "RSI momentum signal rows."),
    ("theme:drawdown", "signal_name", "signals", "signal_name", "drawdown_252d", None, 1.0, "Trailing drawdown signal rows."),
    ("theme:macro_curve", "signal_name", "signals", "signal_name", "yield_curve_10y_ff", None, 1.0, "US yield curve regime rows."),
    ("theme:macro_curve", "signal_name", "signals", "signal_name", "dxy_above_ma200", None, 1.0, "US dollar macro trend regime rows."),
    ("display:momentum", "signal_name", "signals", "signal_name", "ret_1d", None, 0.9, "Display wording for return signals."),
    ("display:momentum", "signal_name", "signals", "signal_name", "ret_1w", None, 0.9, "Display wording for return signals."),
    ("display:momentum", "signal_name", "signals", "signal_name", "ret_1m", None, 0.9, "Display wording for return signals."),
    ("display:trend", "signal_name", "signals", "signal_name", "px_vs_ma200", None, 0.9, "Display wording for trend signals."),
    ("display:volatility", "signal_name", "signals", "signal_name", "vol_30d_ann", None, 0.9, "Display wording for volatility signals."),
    ("display:drawdown", "signal_name", "signals", "signal_name", "drawdown_252d", None, 0.9, "Display wording for drawdown signals."),
    ("display:macro-curve", "signal_name", "signals", "signal_name", "yield_curve_10y_ff", None, 1.0, "Display wording for yield curve signals."),
    ("theme:earnings_quality", "concept", "company_fundamentals", "concept", "Revenue", None, 1.0, "Revenue fundamental rows."),
    ("theme:earnings_quality", "concept", "company_fundamentals", "concept", "GrossProfit", None, 1.0, "Gross-profit fundamental rows."),
    ("theme:earnings_quality", "concept", "company_fundamentals", "concept", "OperatingIncome", None, 1.0, "Operating-income fundamental rows."),
    ("theme:earnings_quality", "concept", "company_fundamentals", "concept", "NetIncome", None, 1.0, "Net-income fundamental rows."),
    ("theme:earnings_quality", "concept", "company_fundamentals", "concept", "EPS", None, 1.0, "EPS fundamental rows."),
    ("theme:cash", "concept", "company_fundamentals", "concept", "Cash", None, 1.0, "Cash fundamental rows."),
    ("theme:balance_sheet", "concept", "company_fundamentals", "concept", "Assets", None, 1.0, "Asset fundamental rows."),
    ("theme:balance_sheet", "concept", "company_fundamentals", "concept", "Liabilities", None, 1.0, "Liability fundamental rows."),
    ("theme:balance_sheet", "concept", "company_fundamentals", "concept", "StockholdersEquity", None, 1.0, "Equity fundamental rows."),
    ("theme:leverage", "concept", "company_fundamentals", "concept", "LongTermDebt", None, 1.0, "Long-term debt fundamental rows."),
    ("theme:insider", "transaction_type", "insider_trades", "transaction_type", "P", None, 1.0, "Insider open-market purchase rows."),
    ("theme:insider", "transaction_type", "insider_trades", "transaction_type", "S", None, 1.0, "Insider sale rows."),
    ("theme:insider", "transaction_type", "insider_trades", "transaction_type", "M", None, 0.8, "Insider option exercise rows."),
    ("theme:insider", "transaction_type", "insider_trades", "transaction_type", "F", None, 0.8, "Insider tax/withholding disposition rows."),
    ("display:macro-pressure", "direction", "oracle_index_snapshots", "direction", "macro-pressure", None, 1.0, "Display wording for macro pressure."),
    ("display:rates-pressure", "direction", "oracle_index_snapshots", "direction", "rates-volatility", None, 1.0, "Display wording for rates pressure."),
    ("display:inflation-pressure", "direction", "oracle_index_snapshots", "direction", "inflation-risk", None, 1.0, "Display wording for inflation pressure."),
    ("display:growth-watch", "direction", "oracle_index_snapshots", "direction", "growth-watch", None, 1.0, "Display wording for growth watch."),
    ("display:risk-pressure", "direction", "oracle_index_snapshots", "direction", "risk-off", None, 1.0, "Display wording for risk pressure."),
    ("display:supply-shock", "direction", "oracle_index_snapshots", "direction", "supply-risk", None, 1.0, "Display wording for supply shocks."),
    ("display:trade-friction", "direction", "oracle_index_snapshots", "direction", "trade-friction", None, 1.0, "Display wording for trade friction."),
    ("display:energy-squeeze", "direction", "oracle_index_snapshots", "direction", "energy-upside-risk", None, 1.0, "Display wording for energy squeeze."),
    ("display:fx-stress", "direction", "oracle_index_snapshots", "direction", "fx-volatility", None, 1.0, "Display wording for FX stress."),
    ("display:credit-stress", "direction", "oracle_index_snapshots", "direction", "credit-risk", None, 1.0, "Display wording for credit stress."),
    ("display:supportive-watch", "direction", "oracle_index_snapshots", "direction", "risk-appetite-watch", None, 1.0, "Display wording for risk-appetite watch."),
    ("display:macro-pressure", "market_bias", "oracle_index_snapshots", "market_bias", "macro headwind", None, 1.0, "Display wording for macro headwind bias."),
    ("display:risk-pressure", "market_bias", "oracle_index_snapshots", "market_bias", "risk headwind", None, 1.0, "Display wording for risk headwind bias."),
    ("display:growth-watch", "market_bias", "oracle_index_snapshots", "market_bias", "growth watch", None, 1.0, "Display wording for growth-watch bias."),
    ("display:mixed-read", "market_bias", "oracle_index_snapshots", "market_bias", "mixed", None, 1.0, "Display wording for mixed bias."),
    ("display:supportive-watch", "market_bias", "oracle_index_snapshots", "market_bias", "supportive/watch", None, 1.0, "Display wording for supportive-watch bias."),
    ("display:risk-pressure", "market_bias", "oracle_index_snapshots", "market_bias", "bearish/risk-off", None, 1.0, "Display wording for bearish risk-off bias."),
    ("display:energy-squeeze", "market_bias", "oracle_index_snapshots", "market_bias", "bullish/upside risk", None, 1.0, "Display wording for upside-risk bias."),
    ("display:safe-haven-bid", "market_bias", "oracle_index_snapshots", "market_bias", "bullish/safe haven", None, 1.0, "Display wording for safe-haven bias."),
    ("display:rates-pressure", "market_bias", "oracle_index_snapshots", "market_bias", "bullish/volatile", None, 1.0, "Display wording for volatile rates bias."),
    ("display:fx-stress", "market_bias", "oracle_index_snapshots", "market_bias", "mixed/SEK supportive", None, 1.0, "Display wording for SEK-supportive mixed bias."),
    ("display:energy-squeeze", "market_bias", "oracle_index_snapshots", "market_bias", "bearish/cost risk", None, 1.0, "Display wording for cost-risk bias."),
    ("display:social-heat", "category", "social_mentions", "ticker", None, None, 0.8, "Display wording for social attention rows."),
    ("display:social-heat", "category", "rumour_signals", "ticker", None, None, 0.8, "Display wording for rumour spike rows."),
    ("display:weather-drag", "weather_var", "weather_correlations", "weather_var", "precipitation_mm", None, 0.8, "Display wording for weather drag."),
    ("display:weather-drag", "weather_var", "weather_correlations", "weather_var", "wind_max_kmh", None, 0.8, "Display wording for weather drag."),
    ("display:insider-signal", "transaction_type", "insider_trades", "transaction_type", "P", None, 0.9, "Display wording for insider purchases."),
    ("display:insider-signal", "transaction_type", "insider_trades", "transaction_type", "S", None, 0.9, "Display wording for insider sales."),
    ("asset_group:commodity", "symbol", "targets", "symbol", "GC=F", None, 1.0, "Gold target."),
    ("asset_group:energy", "symbol", "targets", "symbol", "CL=F", None, 1.0, "WTI target."),
    ("asset_group:equity_index", "symbol", "targets", "symbol", "^OMX", None, 1.0, "OMX target."),
    ("asset_group:equity_index", "symbol", "targets", "symbol", "^STOXX50E", None, 1.0, "Euro Stoxx target."),
    ("asset_group:equity_index", "symbol", "targets", "symbol", "^N225", None, 1.0, "Nikkei target."),
    ("asset_group:bond", "symbol", "targets", "symbol", "TLT", None, 1.0, "US duration proxy."),
    ("asset_group:equity_index", "symbol", "targets", "symbol", "SPY", None, 1.0, "S&P 500 ETF proxy."),
    ("asset_group:commodity", "symbol", "prices", "symbol", "BZ=F", None, 1.0, "Brent crude price rows."),
    ("asset_group:energy", "symbol", "prices", "symbol", "BZ=F", None, 1.0, "Brent crude price rows."),
    ("asset_group:energy", "symbol", "prices", "symbol", "XLE", None, 1.0, "US energy-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLE", None, 1.0, "US energy-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLF", None, 1.0, "US financial-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLK", None, 1.0, "US technology-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLI", None, 1.0, "US industrial-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLU", None, 1.0, "US utilities-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLB", None, 1.0, "US materials-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLC", None, 1.0, "US communication-services ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLP", None, 1.0, "US staples-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLRE", None, 1.0, "US real-estate-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLV", None, 1.0, "US healthcare-sector ETF rows."),
    ("asset_group:equity_sector", "symbol", "prices", "symbol", "XLY", None, 1.0, "US consumer-discretionary ETF rows."),
    ("asset_group:equity_index", "symbol", "prices", "symbol", "^GDAXI", None, 1.0, "DAX index rows."),
    ("asset_group:equity_index", "symbol", "prices", "symbol", "^FCHI", None, 1.0, "CAC 40 index rows."),
    ("asset_group:equity_index", "symbol", "prices", "symbol", "^FTSE", None, 1.0, "FTSE 100 index rows."),
    ("asset_group:equity_index", "symbol", "prices", "symbol", "^HSI", None, 1.0, "Hang Seng index rows."),
    ("asset_group:emerging_market", "symbol", "prices", "symbol", "EEM", None, 1.0, "Emerging-market ETF rows."),
    ("asset_group:emerging_market", "symbol", "prices", "symbol", "INDA", None, 1.0, "India ETF rows."),
    ("asset_group:emerging_market", "symbol", "prices", "symbol", "MCHI", None, 1.0, "China ETF rows."),
    ("asset_group:volatility", "symbol", "prices", "symbol", "^VIX", None, 1.0, "VIX volatility-index rows."),
    ("asset_group:fx", "symbol", "prices", "symbol", "DX-Y.NYB", None, 1.0, "US dollar index rows."),
    ("country:us", "symbol", "prices", "symbol", "SPY", None, 1.0, "US equity proxy rows."),
    ("country:us", "symbol", "prices", "symbol", "TLT", None, 1.0, "US duration proxy rows."),
    ("country:us", "symbol", "prices", "symbol", "DX-Y.NYB", None, 1.0, "US dollar index rows."),
    ("country:de", "symbol", "prices", "symbol", "^GDAXI", None, 1.0, "German equity index rows."),
    ("country:fr", "symbol", "prices", "symbol", "^FCHI", None, 1.0, "French equity index rows."),
    ("country:gb", "symbol", "prices", "symbol", "^FTSE", None, 1.0, "UK equity index rows."),
    ("country:jp", "symbol", "prices", "symbol", "^N225", None, 1.0, "Japanese equity index rows."),
    ("country:cn", "symbol", "prices", "symbol", "MCHI", None, 1.0, "China ETF rows."),
    ("country:in", "symbol", "prices", "symbol", "INDA", None, 1.0, "India ETF rows."),
    ("region:us", "region", "calendar_events", "region", "US", None, 1.0, "US calendar events."),
    ("region:eu", "region", "calendar_events", "region", "EU", None, 1.0, "EU calendar events."),
    ("region:nordics", "region", "calendar_events", "region", "SE", None, 1.0, "Swedish calendar events."),
    ("region:europe_non_eu", "region", "calendar_events", "region", "UK", None, 1.0, "UK calendar events."),
    ("region:asia", "region", "calendar_events", "region", "Asia", None, 1.0, "Asia calendar events."),
    ("region:us", "region", "news_items", "region", "US", None, 1.0, "US news rows."),
    ("region:eu", "region", "news_items", "region", "EU", None, 1.0, "EU news rows."),
    ("region:global", "region", "news_items", "region", "Global", None, 1.0, "Global news rows."),
    ("region:middle_east", "region", "risk_hotspots", "region", "Middle East", None, 1.0, "Middle East risk hotspot rows."),
    ("region:asia", "region", "risk_hotspots", "region", "Asia", None, 1.0, "Asia risk hotspot rows."),
    ("region:europe_non_eu", "region", "risk_hotspots", "region", "Europe", None, 1.0, "Europe non-EU risk hotspot rows."),
    ("country:us", "country", "indicators", "country", "US", None, 1.0, "US indicator rows."),
    ("country:se", "country", "indicators", "country", "SE", None, 1.0, "Swedish indicator rows."),
    ("country:de", "country", "indicators", "country", "DE", None, 1.0, "German indicator rows."),
    ("country:jp", "country", "indicators", "country", "JP", None, 1.0, "Japanese indicator rows."),
    ("country:gb", "country", "indicators", "country", "GB", None, 1.0, "UK indicator rows."),
    ("country:cn", "country", "indicators", "country", "CN", None, 1.0, "China indicator rows."),
    ("country:kr", "country", "indicators", "country", "KR", None, 1.0, "South Korea indicator rows."),
    ("country:ch", "country", "indicators", "country", "CH", None, 1.0, "Swiss indicator rows."),
    ("country:no", "country", "indicators", "country", "NO", None, 1.0, "Norwegian indicator rows."),
    ("country:in", "country", "indicators", "country", "IN", None, 1.0, "India indicator rows."),
    ("country:fr", "country", "indicators", "country", "FR", None, 1.0, "French indicator rows."),
    ("country:br", "country", "indicators", "country", "BR", None, 1.0, "Brazil indicator rows."),
    ("country:au", "country", "indicators", "country", "AU", None, 1.0, "Australian indicator rows."),
    ("country:ae", "country", "indicators", "country", "AE", None, 1.0, "UAE indicator rows."),
    ("country:cn", "country", "risk_hotspots", "country", "CN", None, 1.0, "China risk hotspot rows."),
    ("country:il", "country", "risk_hotspots", "country", "IL", None, 1.0, "Israel risk hotspot rows."),
    ("country:ir", "country", "risk_hotspots", "country", "IR", None, 1.0, "Iran risk hotspot rows."),
    ("country:kr", "country", "risk_hotspots", "country", "KR", None, 1.0, "Korea risk hotspot rows."),
    ("country:ua", "country", "risk_hotspots", "country", "UA", None, 1.0, "Ukraine risk hotspot rows."),
    ("country:ym", "country", "risk_hotspots", "country", "YM", None, 1.0, "Yemen risk hotspot rows."),
    ("country:us", "country", "gdelt_disaster_signals", "country", "US", None, 0.9, "US disaster signal rows."),
    ("country:gb", "country", "gdelt_disaster_signals", "country", "UK", None, 0.9, "UK disaster signal rows."),
    ("country:in", "country", "gdelt_disaster_signals", "country", "IN", None, 0.9, "India disaster signal rows."),
    ("country:ca", "country", "gdelt_disaster_signals", "country", "CA", None, 0.9, "Canada disaster signal rows."),
    ("country:ir", "country", "gdelt_disaster_signals", "country", "IR", None, 0.9, "Iran disaster signal rows."),
    ("country:fr", "country", "gdelt_disaster_signals", "country", "FR", None, 0.9, "France disaster signal rows."),
    ("region:global", "entity", "oracle_entities", "entity_id", "global", None, 1.0, "Global Atlas entity."),
    ("region:us", "entity", "oracle_entities", "entity_id", "region:us", None, 1.0, "US Atlas region."),
    ("region:eu", "entity", "oracle_entities", "entity_id", "region:eu", None, 1.0, "EU Atlas region."),
    ("region:nordics", "entity", "oracle_entities", "entity_id", "region:nordics", None, 1.0, "Nordics Atlas region."),
    ("region:asia", "entity", "oracle_entities", "entity_id", "region:asia", None, 1.0, "Asia Atlas region."),
    ("region:energy_commodities", "entity", "oracle_entities", "entity_id", "commodity:energy_commodities", None, 1.0, "Energy/commodities Atlas entity."),
    ("country:us", "entity", "oracle_entities", "entity_id", "nation:us", None, 1.0, "US Atlas nation."),
    ("country:se", "entity", "oracle_entities", "entity_id", "nation:se", None, 1.0, "Swedish Atlas nation."),
    ("country:de", "entity", "oracle_entities", "entity_id", "nation:de", None, 1.0, "German Atlas nation."),
    ("country:fr", "entity", "oracle_entities", "entity_id", "nation:fr", None, 1.0, "French Atlas nation."),
    ("country:gb", "entity", "oracle_entities", "entity_id", "nation:gb", None, 1.0, "UK Atlas nation."),
    ("country:jp", "entity", "oracle_entities", "entity_id", "nation:jp", None, 1.0, "Japanese Atlas nation."),
    ("country:cn", "entity", "oracle_entities", "entity_id", "nation:cn", None, 1.0, "China Atlas nation."),
    ("asset_group:equity_index", "entity", "oracle_entities", "entity_id", "market:sp500", None, 1.0, "S&P 500 Atlas market entity."),
    ("asset_group:bond", "entity", "oracle_entities", "entity_id", "market:bonds_us", None, 1.0, "US duration Atlas market entity."),
    ("asset_group:commodity", "entity", "oracle_entities", "entity_id", "market:gold", None, 1.0, "Gold Atlas market entity."),
    ("asset_group:energy", "entity", "oracle_entities", "entity_id", "market:oil", None, 1.0, "Oil Atlas market entity."),
    ("asset_group:equity_index", "entity", "oracle_entities", "entity_id", "market:omx", None, 1.0, "OMX Atlas market entity."),
    ("asset_group:equity_index", "entity", "oracle_entities", "entity_id", "market:stoxx50", None, 1.0, "Euro Stoxx Atlas market entity."),
    ("asset_group:equity_index", "entity", "oracle_entities", "entity_id", "market:nikkei", None, 1.0, "Nikkei Atlas market entity."),
    ("asset_group:fx", "entity", "oracle_entities", "entity_id", "market:usd", None, 1.0, "US dollar Atlas market entity."),
    ("asset_group:fx", "entity", "oracle_entities", "entity_id", "market:sek", None, 1.0, "SEK Atlas market entity."),
    ("asset_group:equity_sector", "entity", "oracle_entities", "entity_id", "sector:energy", None, 1.0, "Energy sector Atlas entity."),
    ("asset_group:equity_sector", "entity", "oracle_entities", "entity_id", "sector:banks", None, 1.0, "Financial sector Atlas entity."),
    ("asset_group:equity_sector", "entity", "oracle_entities", "entity_id", "sector:technology", None, 1.0, "Technology sector Atlas entity."),
    ("asset_group:equity_sector", "entity", "oracle_entities", "entity_id", "sector:shipping", None, 1.0, "Shipping sector Atlas entity."),
    ("asset_group:equity_sector", "entity", "oracle_entities", "entity_id", "sector:defense", None, 1.0, "Defense sector Atlas entity."),
]


_ORACLE_THEME_ASSIGNMENTS = [
    "inflation", "central_bank", "interest", "rates", "currency", "labour",
    "gdp", "housing", "debt", "stockmarket", "positioning", "sentiment",
    "capital_flows", "industry", "trade", "sanctions", "conflict",
    "disaster", "weather", "fundamentals", "insider",
]
_ORACLE_DIRECTION_ASSIGNMENTS = [
    "inflation-risk", "rates-volatility", "growth-watch", "trade-friction",
    "fx-volatility", "energy-upside-risk", "risk-off", "supply-risk",
    "policy-risk", "credit-risk", "risk-appetite-watch", "macro-pressure",
]
_ORACLE_MARKET_BIAS_ASSIGNMENTS = [
    "watch", "macro headwind", "risk headwind", "bearish", "supportive/watch",
    "bearish/risk-off", "bullish/upside risk", "growth watch", "mixed",
    "bullish/volatile", "bullish/safe haven", "mixed/SEK supportive",
    "bearish/cost risk", "mixed/supportive", "bullish",
]
_MARKET_BIAS_LABEL_IDS = {
    "risk-on": "market_bias:risk-on",
    "risk off": "market_bias:risk-off",
    "risk-off": "market_bias:risk-off",
    "inflation up": "market_bias:inflation-up",
    "inflation-up": "market_bias:inflation-up",
    "rates up": "market_bias:rates-up",
    "rates-up": "market_bias:rates-up",
    "growth down": "market_bias:growth-down",
    "growth-down": "market_bias:growth-down",
    "oil up": "market_bias:oil-up",
    "oil-up": "market_bias:oil-up",
    "usd up": "market_bias:usd-up",
    "usd-up": "market_bias:usd-up",
    "neutral": "market_bias:neutral",
    "volatility up": "market_bias:volatility-up",
    "volatility-up": "market_bias:volatility-up",
    "watch": "market_bias:watch",
    "macro headwind": "market_bias:macro-headwind",
    "risk headwind": "market_bias:risk-headwind",
    "bearish": "market_bias:bearish",
    "supportive/watch": "market_bias:supportive-watch",
    "bearish/risk-off": "market_bias:bearish-risk-off",
    "bullish/upside risk": "market_bias:bullish-upside-risk",
    "growth watch": "market_bias:growth-watch",
    "mixed": "market_bias:mixed",
    "bullish/volatile": "market_bias:bullish-volatile",
    "bullish/safe haven": "market_bias:bullish-safe-haven",
    "mixed/SEK supportive": "market_bias:mixed-sek-supportive",
    "bearish/cost risk": "market_bias:bearish-cost-risk",
    "mixed/supportive": "market_bias:mixed-supportive",
    "bullish": "market_bias:bullish",
}

for _theme in _ORACLE_THEME_ASSIGNMENTS:
    _label_id = f"theme:{_theme}"
    DEFAULT_DATA_LABEL_ASSIGNMENTS.extend([
        (_label_id, "theme", "intelligence_packages", "theme", _theme, None, 1.0, "World Pull theme rows."),
        (_label_id, "theme", "oracle_impacts", "theme", _theme, None, 1.0, "Atlas impact theme rows."),
        (_label_id, "theme", "oracle_index_snapshots", "theme", _theme, None, 1.0, "Atlas index theme rows."),
    ])

for _direction in _ORACLE_DIRECTION_ASSIGNMENTS:
    _label_id = f"direction:{_direction}"
    DEFAULT_DATA_LABEL_ASSIGNMENTS.extend([
        (_label_id, "direction", "intelligence_packages", "direction", _direction, None, 1.0, "World Pull direction rows."),
        (_label_id, "direction", "oracle_impacts", "direction", _direction, None, 1.0, "Atlas impact direction rows."),
        (_label_id, "direction", "oracle_index_snapshots", "direction", _direction, None, 1.0, "Atlas index direction rows."),
    ])

for _bias, _label_id in _MARKET_BIAS_LABEL_IDS.items():
    DEFAULT_DATA_LABEL_ASSIGNMENTS.append(
        (_label_id, "market_bias", "oracle_index_snapshots", "market_bias", _bias, None, 1.0, "Atlas index market-bias rows.")
    )

_GDELT_STREAM_THEME_ASSIGNMENTS = {
    "economy_news": [
        "ECON_INFLATION", "ECON_DEBT", "ECON_BANKRUPTCY",
        "ECON_HOUSING_PRICES", "ECON_UNEMPLOYMENT", "ECON_TAXATION",
        "WB_470_INFLATION", "WB_1101_MACROECONOMIC_VULNERABILITY_AND_DEBT",
    ],
    "policy_rates": [
        "ECON_INTEREST_RATES", "ECON_CENTRALBANK", "WB_444_MONETARY_POLICY",
        "WB_445_CENTRAL_BANKS", "WB_467_FINANCIAL_SECTOR_POLICY",
    ],
    "major_disaster": [
        "NATURAL_DISASTER", "NATURAL_DISASTER_EARTHQUAKE",
        "NATURAL_DISASTER_FLOOD", "NATURAL_DISASTER_STORM",
        "NATURAL_DISASTER_HURRICANE", "NATURAL_DISASTER_WILDFIRE",
        "NATURAL_DISASTER_DROUGHT",
    ],
    "political_risk": [
        "ELECTION", "TAX_FNCACT_POLITICIAN", "WB_696_PUBLIC_SECTOR_MANAGEMENT",
        "WB_845_POLITICAL_ECONOMY", "LEGISLATION", "POLITICAL_TURMOIL",
    ],
    "conflict_security": [
        "TERROR", "MILITARY", "WAR", "ARMEDCONFLICT", "SECURITY_SERVICES",
        "WB_2432_FRAGILITY_CONFLICT_AND_VIOLENCE",
    ],
    "trade_sanctions_supply": [
        "SANCTIONS", "ECON_SANCTIONS", "TRADE", "ECON_TRADE_DISPUTE",
        "WB_1332_TRADE", "SUPPLY_CHAIN", "MARITIME",
    ],
    "energy_commodities": [
        "ECON_OILPRICE", "ENERGY", "OIL", "GAS", "WB_1015_ENERGY",
        "WB_827_AGRICULTURE", "FOOD_SECURITY",
    ],
    "market_stress": [
        "ECON_STOCKMARKET", "WB_1234_BANKING_INSTITUTIONS",
        "ECON_BANKRUPTCY", "ECON_CURRENCY_EXCHANGE_RATE",
        "FINANCIAL_MARKETS", "ECON_CREDIT", "ECON_FORECLOSURE",
    ],
}

for _stream, _theme_codes in _GDELT_STREAM_THEME_ASSIGNMENTS.items():
    DEFAULT_DATA_LABEL_ASSIGNMENTS.extend([
        (f"stream:{_stream}", "gdelt_theme", "gdelt_stream_theme_rules", "theme_code", _theme_code, None, 1.0, "GDELT theme-code stream mapping.")
        for _theme_code in _theme_codes
    ])

for _label_id, _label_type, _label, _description, _weight in DEFAULT_DATA_LABELS:
    if _label_type == "release_family":
        family = _label_id.split(":", 1)[1]
        DEFAULT_DATA_LABEL_ASSIGNMENTS.extend([
            (_label_id, "release_family", "calendar_events", "release_family", family, None, 1.0, f"{_label} calendar events."),
            (_label_id, "release_family", "macro_release_actuals", "release_key", family, None, 0.8, f"{_label} matched release actuals."),
        ])

DEFAULT_GDELT_STREAM_THEME_RULES: list[tuple[str, str, str, str]] = [
    (_stream, _theme_code, f"stream:{_stream}", "Deterministic GDELT theme-code stream mapping.")
    for _stream, _theme_codes in _GDELT_STREAM_THEME_ASSIGNMENTS.items()
    for _theme_code in _theme_codes
]


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_") or "event"


def _release_family(category: str, title: str) -> str:
    text = f"{category} {title}".lower()
    checks = [
        ("cpif", "cpif"),
        ("cpi", "cpi"),
        ("hicp", "hicp"),
        ("ppi", "ppi"),
        ("pce", "pce"),
        ("payroll", "payrolls"),
        ("employment", "payrolls"),
        ("unemployment", "unemployment"),
        ("jolts", "jolts"),
        ("gdp", "gdp"),
        ("pmi", "pmi"),
        ("retail", "retail_sales"),
        ("confidence", "confidence"),
        ("fomc", "fomc"),
        ("ecb", "ecb"),
        ("riksbank", "riksbank"),
        ("bank of england", "boe"),
        ("boj", "boj"),
        ("bank of japan", "boj"),
        ("eia", "eia"),
        ("sanction", "sanctions"),
        ("trade", "trade"),
    ]
    for needle, family in checks:
        if needle in text:
            return family
    return _slug(category)


def _scheduled_at_utc(date: str, time_local: str) -> str | None:
    if not time_local:
        return None
    match = re.search(r"(\d{1,2}):(\d{2})\s*([A-Z]+)", time_local.strip())
    if not match:
        return None
    hour, minute, zone = int(match.group(1)), int(match.group(2)), match.group(3)
    offsets = {
        "ET": -4,
        "EDT": -4,
        "EST": -5,
        "UK": 1,
        "BST": 1,
        "GMT": 0,
        "CET": 2,
        "CEST": 2,
        "UTC": 0,
    }
    offset = offsets.get(zone)
    if offset is None:
        return None
    local_dt = dt.datetime.fromisoformat(f"{date}T{hour:02d}:{minute:02d}:00")
    return (local_dt - dt.timedelta(hours=offset)).replace(microsecond=0).isoformat() + "Z"


def _calendar_labels(region: str, category: str, release_family: str) -> str:
    labels = [
        "source_family:calendar",
        f"theme:{category}",
        f"release_family:{release_family}",
    ]
    region_labels = {
        "US": "region:us",
        "EU": "region:eu",
        "SE": "region:nordics",
        "UK": "region:europe_non_eu",
        "JP": "region:asia",
    }
    if region in region_labels:
        labels.append(region_labels[region])
    return json.dumps(sorted(set(labels)))


def _calendar_seed_rows() -> list[tuple]:
    rows = []
    for date, time_local, region, category, importance, title, expected, market_note, source, url in DEFAULT_CALENDAR_EVENTS:
        family = _release_family(category, title)
        event_key = f"{_slug(region)}:{family}:{date}:{_slug(title)}"
        rows.append((
            date,
            time_local,
            region,
            category,
            importance,
            title,
            expected,
            market_note,
            source,
            url,
            event_key,
            family,
            _scheduled_at_utc(date, time_local),
            f"{source}:{date}:{_slug(title)}",
            _calendar_labels(region, category, family),
            "scheduled",
        ))
    return rows


def add_missing_columns(conn):
    """Idempotent: add columns that newer schema versions introduced."""
    additions = {
        "events": [("source", "TEXT"), ("url", "TEXT")],
        "predictions": [("input_brief_md", "TEXT")],
        "oracle_index_snapshots": [("market_bias", "TEXT"), ("plain_read", "TEXT")],
        "macro_event_predictions": [
            ("actual_detail_json", "TEXT"),
            ("actual_summary", "TEXT"),
            ("result_status", "TEXT"),
            ("forecast_direction", "TEXT"),
            ("forecast_confidence", "REAL"),
            ("forecast_summary", "TEXT"),
            ("forecast_rationale_md", "TEXT"),
            ("historical_pattern_json", "TEXT"),
            ("claude_forecast_json", "TEXT"),
            ("claude_model", "TEXT"),
            ("claude_at", "TEXT"),
            ("local_model_summary", "TEXT"),
            ("local_model_model", "TEXT"),
            ("local_model_at", "TEXT"),
        ],
        "historical_state_values": [("freshness_class", "TEXT NOT NULL DEFAULT 'archive'")],
        "historical_state_daily": [("evaluation_eligible", "INTEGER NOT NULL DEFAULT 0")],
        "macro_release_actuals": [
            ("importance", "INTEGER NOT NULL DEFAULT 3"),
            ("expected_text", "TEXT"),
            ("expected_unit", "TEXT"),
        ],
        "current_events": [
            ("display_title", "TEXT"),
            ("display_summary", "TEXT"),
            ("why_text", "TEXT"),
            ("source_quality", "TEXT"),
        ],
        "label_evaluations": [
            ("methodology_version", "TEXT NOT NULL DEFAULT 'v2_asset_scoped'"),
            ("scope", "TEXT NOT NULL DEFAULT 'asset'"),
        ],
        "gdelt_stream_examples": [("labels_json", "TEXT")],
        "calendar_events": [
            ("event_key", "TEXT"),
            ("release_family", "TEXT"),
            ("scheduled_at_utc", "TEXT"),
            ("source_uid", "TEXT"),
            ("labels_json", "TEXT"),
            ("status", "TEXT NOT NULL DEFAULT 'scheduled'"),
        ],
    }
    for table, cols in additions.items():
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for name, sqltype in cols:
            if name not in existing:
                try:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sqltype}")
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='calendar_events'").fetchone():
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_calendar_event_key "
            "ON calendar_events(event_key) WHERE event_key IS NOT NULL"
        )


def migrate_gold_price_to_prices(conn):
    """One-shot migration: gold_price -> prices, then drop gold_price."""
    cur = conn.cursor()
    has_old = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='gold_price'"
    ).fetchone()
    if not has_old:
        return 0
    moved = cur.execute("""
        INSERT OR IGNORE INTO prices (date, symbol, asset_class, name, price, currency)
        SELECT date, 'GC=F', 'commodity', 'Gold (COMEX front-month)', price_usd, 'USD'
        FROM gold_price
    """).rowcount
    cur.execute("DROP TABLE gold_price")
    return moved


def seed_default_targets(conn):
    """Insert the default target set. Won't overwrite existing rows."""
    cur = conn.executemany(
        """INSERT OR IGNORE INTO targets
           (symbol, name, asset_class, horizons, source)
           VALUES (?, ?, ?, ?, ?)""",
        DEFAULT_TARGETS,
    )
    return cur.rowcount


def seed_calendar_events(conn):
    cur = conn.executemany(
        """INSERT INTO calendar_events
           (date, time_local, region, category, importance, title,
            expected, market_note, source, url, event_key, release_family,
            scheduled_at_utc, source_uid, labels_json, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(date, title, region) DO UPDATE SET
             time_local=excluded.time_local,
             category=excluded.category,
             importance=excluded.importance,
             expected=excluded.expected,
             market_note=excluded.market_note,
             source=excluded.source,
             url=excluded.url,
             event_key=COALESCE(calendar_events.event_key, excluded.event_key),
             release_family=COALESCE(calendar_events.release_family, excluded.release_family),
             scheduled_at_utc=COALESCE(calendar_events.scheduled_at_utc, excluded.scheduled_at_utc),
             source_uid=COALESCE(calendar_events.source_uid, excluded.source_uid),
             labels_json=COALESCE(calendar_events.labels_json, excluded.labels_json),
             status=COALESCE(calendar_events.status, excluded.status)""",
        _calendar_seed_rows(),
    )
    return cur.rowcount


def seed_data_objects(conn):
    cur = conn.executemany(
        """INSERT INTO data_objects
           (object_id, display_name, prompt_name, source_table, description,
            source_family, freshness_sla_hours, cadence, frontend_group, prompt_role)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(object_id) DO UPDATE SET
             display_name=excluded.display_name,
             prompt_name=excluded.prompt_name,
             source_table=excluded.source_table,
             description=excluded.description,
             source_family=excluded.source_family,
             freshness_sla_hours=excluded.freshness_sla_hours,
             cadence=excluded.cadence,
             frontend_group=excluded.frontend_group,
             prompt_role=excluded.prompt_role,
             active=1,
             updated_at=datetime('now')""",
        DEFAULT_DATA_OBJECTS,
    )
    return cur.rowcount


def seed_risk_hotspots(conn):
    cur = conn.executemany(
        """INSERT OR IGNORE INTO risk_hotspots
           (name, region, country, category, severity, lat, lon, summary, source)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        DEFAULT_RISK_HOTSPOTS,
    )
    return cur.rowcount


def backfill_gdelt_example_labels(conn):
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gdelt_stream_examples'").fetchone():
        return 0
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gdelt_streams'").fetchone():
        return 0
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gdelt_stream_examples)")}
    if "labels_json" not in columns:
        return 0
    cur = conn.execute(
        """UPDATE gdelt_stream_examples
           SET labels_json = (
             SELECT gs.labels_json
             FROM gdelt_streams gs
             WHERE gs.date = gdelt_stream_examples.date
               AND gs.stream = gdelt_stream_examples.stream
               AND gs.region = gdelt_stream_examples.region
               AND gs.country = gdelt_stream_examples.country
             LIMIT 1
           )
           WHERE (labels_json IS NULL OR labels_json = '')
             AND EXISTS (
               SELECT 1
               FROM gdelt_streams gs
               WHERE gs.date = gdelt_stream_examples.date
                 AND gs.stream = gdelt_stream_examples.stream
                 AND gs.region = gdelt_stream_examples.region
                 AND gs.country = gdelt_stream_examples.country
                 AND gs.labels_json IS NOT NULL
                 AND gs.labels_json != ''
             )"""
    )
    return cur.rowcount


def seed_data_labels(conn):
    assignments = [
        (
            label_id,
            target_type,
            target_table or "",
            target_column or "",
            target_value or "",
            weight_override,
            confidence,
            notes,
        )
        for (
            label_id,
            target_type,
            target_table,
            target_column,
            target_value,
            weight_override,
            confidence,
            notes,
        ) in DEFAULT_DATA_LABEL_ASSIGNMENTS
    ]
    label_cur = conn.executemany(
        """INSERT INTO data_labels
           (label_id, label_type, label, description, default_weight)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(label_id) DO UPDATE SET
             label_type=excluded.label_type,
             label=excluded.label,
             description=excluded.description,
             default_weight=excluded.default_weight,
             active=1,
             updated_at=datetime('now')""",
        DEFAULT_DATA_LABELS,
    )
    assignment_cur = conn.executemany(
        """INSERT INTO data_label_assignments
           (label_id, target_type, target_table, target_column, target_value,
            weight_override, confidence, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(label_id, target_type, target_table, target_column, target_value)
           DO UPDATE SET
             weight_override=excluded.weight_override,
             confidence=excluded.confidence,
             notes=excluded.notes,
             active=1,
             updated_at=datetime('now')""",
        assignments,
    )
    conn.execute(
        """UPDATE data_label_assignments
           SET active = 0, updated_at = datetime('now')
           WHERE target_type = 'gdelt_theme'
             AND target_table = 'gdelt_streams'
             AND target_column = 'theme_code'"""
    )
    conn.executemany(
        """INSERT INTO gdelt_stream_theme_rules
           (stream, theme_code, label_id, notes)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(stream, theme_code) DO UPDATE SET
             label_id=excluded.label_id,
             notes=excluded.notes,
             active=1,
             updated_at=datetime('now')""",
        DEFAULT_GDELT_STREAM_THEME_RULES,
    )
    conn.executemany(
        """INSERT INTO label_weight_profiles
           (profile_id, name, description)
           VALUES (?, ?, ?)
           ON CONFLICT(profile_id) DO UPDATE SET
             name=excluded.name,
             description=excluded.description,
             active=1,
             updated_at=datetime('now')""",
        DEFAULT_LABEL_WEIGHT_PROFILES,
    )
    conn.executemany(
        """INSERT INTO label_weight_overrides
           (profile_id, label_id, weight, notes)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(profile_id, label_id) DO UPDATE SET
             weight=excluded.weight,
             notes=excluded.notes,
             active=1,
             updated_at=datetime('now')""",
        DEFAULT_LABEL_WEIGHT_OVERRIDES,
    )
    return label_cur.rowcount, assignment_cur.rowcount


def main():
    conn = connect_writable(DB_PATH)
    conn.executescript(SCHEMA)
    add_missing_columns(conn)
    moved = migrate_gold_price_to_prices(conn)
    seeded = seed_default_targets(conn)
    object_seeded = seed_data_objects(conn)
    calendar_seeded = seed_calendar_events(conn)
    hotspot_seeded = seed_risk_hotspots(conn)
    label_seeded, assignment_seeded = seed_data_labels(conn)
    example_labels_backfilled = backfill_gdelt_example_labels(conn)
    conn.commit()
    if moved:
        print(f"Migrated {moved} rows from gold_price -> prices.")
    if seeded:
        print(f"Seeded {seeded} default targets.")
    if object_seeded:
        print(f"Seeded {object_seeded} data objects.")
    if calendar_seeded:
        print(f"Seeded {calendar_seeded} calendar events.")
    if hotspot_seeded:
        print(f"Seeded {hotspot_seeded} risk hotspots.")
    if label_seeded or assignment_seeded:
        print(f"Seeded {label_seeded} data labels and {assignment_seeded} label assignments.")
    if example_labels_backfilled:
        print(f"Backfilled {example_labels_backfilled} GDELT example label rows.")
    print("Schema ready.")
    conn.close()


if __name__ == "__main__":
    main()
