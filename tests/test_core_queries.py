import sqlite3
import tempfile
import unittest
import datetime as dt
import os
from unittest.mock import patch
from dataclasses import dataclass

from config.db_setup import SCHEMA, seed_data_labels, seed_data_objects
from src.core.changes import enqueue_change_event, requires_oracle_review
from src.core import audit, db as core_db, labels, queries
from src.core.db import connect_writable, upsert_many
from src.core.runs import finish_source_run, start_source_run
from src.core.source_registry import in_event_window, source_cadence_hours
from src.fetchers import gdelt
from src.intelligence import context_packs, historical_state, macro_actuals


@dataclass
class Analogue:
    date: str
    cosine: float
    realised_return: float | None
    payload: dict


class CoreQueryTests(unittest.TestCase):
    def test_source_health_reports_missing_tables_without_mutation(self):
        conn = sqlite3.connect(":memory:")
        health = queries.source_health(conn)
        self.assertFalse(health.empty)
        self.assertIn("source", health.columns)
        self.assertTrue((health["status"] == "empty").all())

    def test_readonly_connection_falls_back_to_immutable_uri(self):
        calls = []
        real_connect = sqlite3.connect
        real_configure = core_db.configure_connection

        def fake_connect(database, *args, **kwargs):
            calls.append(database)
            return real_connect(":memory:")

        def fake_configure(conn, *args, **kwargs):
            if calls[-1] == "file:/tmp/oracle.db?mode=ro":
                raise sqlite3.OperationalError("unable to open database file")
            return real_configure(conn, *args, **kwargs)

        with patch("src.core.db.sqlite3.connect", side_effect=fake_connect), patch(
            "src.core.db.configure_connection", side_effect=fake_configure
        ):
            conn = core_db.connect_readonly("/tmp/oracle.db")
        try:
            self.assertEqual(
                calls,
                [
                    "file:/tmp/oracle.db?mode=ro",
                    "file:/tmp/oracle.db?mode=ro&immutable=1",
                ],
            )
        finally:
            conn.close()

    def test_market_tape_includes_prediction_range(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE targets (
                symbol TEXT PRIMARY KEY, name TEXT, asset_class TEXT,
                active INTEGER
            );
            CREATE TABLE prices (symbol TEXT, date TEXT, close REAL);
            CREATE TABLE predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, horizon TEXT, as_of TEXT, generated_at TEXT,
                direction TEXT, confidence REAL,
                expected_return_low REAL, expected_return_high REAL,
                rationale_md TEXT, key_risks TEXT, analogues_used TEXT,
                model TEXT, input_hash TEXT, input_brief_md TEXT,
                realized_return REAL, scored_at TEXT
            );
            INSERT INTO targets VALUES ('SPY', 'S&P 500 ETF', 'equity_etf', 1);
            INSERT INTO prices VALUES ('SPY', date('now', '-2 day'), 100.0), ('SPY', date('now'), 102.0);
            INSERT INTO predictions (
                asset, horizon, as_of, generated_at, direction, confidence,
                expected_return_low, expected_return_high, model, input_hash
            ) VALUES (
                'SPY', '1w', date('now'), datetime('now'), 'up', 0.55,
                -0.02, 0.05, 'test', 'hash'
            );
            """
        )
        tape = queries.market_tape(conn)
        self.assertEqual(len(tape), 1)
        row = tape.iloc[0]
        self.assertEqual(row["prediction_direction"], "up")
        self.assertEqual(row["prediction_horizon"], "1w")
        self.assertAlmostEqual(float(row["prediction_expected_low"]), -0.02)
        self.assertAlmostEqual(float(row["prediction_expected_high"]), 0.05)

    def test_prediction_summary_excludes_private_prompt_text(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, horizon TEXT, as_of TEXT, generated_at TEXT,
                direction TEXT, confidence REAL,
                expected_return_low REAL, expected_return_high REAL,
                rationale_md TEXT, key_risks TEXT, analogues_used TEXT,
                model TEXT, input_hash TEXT, input_brief_md TEXT,
                realized_return REAL, scored_at TEXT
            );
            INSERT INTO predictions (
                asset, horizon, as_of, generated_at, direction, confidence,
                expected_return_low, expected_return_high, rationale_md,
                key_risks, analogues_used, model, input_hash, input_brief_md
            ) VALUES (
                'GC=F', '1m', '2026-05-11', '2026-05-11T20:00:00',
                'up', 0.62, 0.01, 0.04, 'research view', 'risk',
                '[]', 'claude', 'abc', 'private prompt'
            );
            """
        )
        summary = queries.prediction_summaries(conn)
        self.assertEqual(len(summary), 1)
        self.assertNotIn("input_brief_md", summary.columns)

    def test_intelligence_and_macro_event_queries_are_public_safe(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE intelligence_packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                as_of TEXT, generated_at TEXT, scope_type TEXT, scope TEXT,
                parent_scope TEXT, theme TEXT, direction TEXT, severity REAL,
                confidence REAL, freshness TEXT, horizon TEXT,
                evidence_json TEXT, conclusion TEXT, affected_assets_json TEXT,
                prediction_impact_json TEXT, next_watch TEXT,
                source_refs_json TEXT, model TEXT, input_hash TEXT
            );
            CREATE TABLE macro_event_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                calendar_event_id INTEGER, event_key TEXT, as_of TEXT,
                generated_at TEXT, release_date TEXT, release_time_local TEXT,
                region TEXT, country TEXT, category TEXT, title TEXT,
                importance INTEGER, expected TEXT, previous_value REAL,
                predicted_surprise_bucket TEXT, confidence REAL,
                scenario_json TEXT, affected_assets_json TEXT, rationale_md TEXT,
                key_risks_json TEXT, source TEXT, url TEXT, model TEXT,
                input_hash TEXT, actual_value REAL, actual_surprise TEXT,
                actual_detail_json TEXT, actual_summary TEXT,
                result_status TEXT, local_model_summary TEXT,
                local_model_model TEXT, local_model_at TEXT, scored_at TEXT
            );
            INSERT INTO intelligence_packages (
                as_of, generated_at, scope_type, scope, theme, direction,
                severity, confidence, freshness, horizon, conclusion, model
            ) VALUES (
                '2026-05-12', '2026-05-12T10:00:00', 'global', 'Global',
                'inflation', 'inflation-risk', 4.2, 0.74, 'today',
                'near_term', 'Global inflation pressure is rising.', 'test'
            );
            INSERT INTO macro_event_predictions (
                event_key, as_of, generated_at, release_date, region, category,
                title, importance, predicted_surprise_bucket, confidence,
                scenario_json, model
            ) VALUES (
                'event-1', '2026-05-12', '2026-05-12T10:00:00',
                date('now', '+1 day'), 'US', 'inflation', 'US CPI',
                5, 'inline', '0.72', '[]', 'test'
            );
            """
        )
        packages = queries.intelligence_packages(conn)
        scenarios = queries.macro_event_predictions(conn, days_forward=7)
        self.assertEqual(len(packages), 1)
        self.assertEqual(packages.iloc[0]["scope"], "Global")
        self.assertEqual(len(scenarios), 1)
        self.assertEqual(scenarios.iloc[0]["title"], "US CPI")

    def test_oracle_index_query(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE oracle_entities (
                entity_id TEXT PRIMARY KEY, label TEXT, entity_type TEXT,
                parent_id TEXT, region TEXT, nation TEXT, sector TEXT,
                symbol TEXT, description TEXT, active INTEGER
            );
            CREATE TABLE oracle_index_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                as_of TEXT, generated_at TEXT, entity_id TEXT,
                entity_label TEXT, entity_type TEXT, parent_id TEXT,
                theme TEXT, direction TEXT, horizon TEXT, score REAL,
                magnitude REAL, confidence REAL, evidence_count INTEGER,
                market_bias TEXT, plain_read TEXT,
                top_evidence_json TEXT, affected_assets_json TEXT,
                model TEXT, input_hash TEXT
            );
            CREATE TABLE oracle_impacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                as_of TEXT, generated_at TEXT, source_table TEXT,
                source_id INTEGER, evidence_key TEXT, theme TEXT,
                entity_id TEXT, direction TEXT, magnitude REAL,
                confidence REAL, horizon TEXT, freshness TEXT, summary TEXT,
                source_refs_json TEXT, model TEXT, input_hash TEXT
            );
            INSERT INTO oracle_entities VALUES (
                'global', 'Global Atlas Index', 'global', NULL, NULL, NULL,
                NULL, NULL, 'root', 1
            );
            INSERT INTO oracle_index_snapshots (
                as_of, generated_at, entity_id, entity_label, entity_type,
                theme, direction, horizon, score, magnitude, confidence,
                evidence_count, market_bias, plain_read, model
            ) VALUES (
                '2026-05-12', '2026-05-12T10:00:00', 'global',
                'Global Atlas Index', 'global', 'inflation',
                'inflation-risk', 'near_term', 77.5, 4.1, 0.72, 8, 'macro headwind', 'Inflation pressure is a headwind.', 'test'
            );
            INSERT INTO oracle_impacts (
                as_of, generated_at, source_table, evidence_key, theme,
                entity_id, direction, magnitude, confidence, horizon, summary,
                model
            ) VALUES (
                '2026-05-12', '2026-05-12T10:00:00', 'intelligence_packages',
                'x', 'inflation', 'global', 'inflation-risk', 4.0, 0.7,
                'near_term', 'test impact', 'test'
            );
            """
        )
        entities = queries.oracle_entities(conn)
        index = queries.oracle_index_snapshots(conn)
        impacts = queries.oracle_impacts(conn)
        self.assertEqual(len(entities), 1)
        self.assertEqual(index.iloc[0]["entity_label"], "Global Atlas Index")
        self.assertEqual(index.iloc[0]["market_bias"], "macro headwind")
        self.assertEqual(impacts.iloc[0]["entity_id"], "global")

    def test_oracle_layer_map_enriches_display_labels_and_layers(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO oracle_entities (
                entity_id, label, entity_type, parent_id, region, nation,
                sector, symbol, description, active
            ) VALUES (
                'region:us', 'United States', 'region', 'global', 'US', 'US',
                NULL, NULL, 'US region', 1
            ), (
                'market:oil', 'Oil', 'market', 'commodity:energy_commodities',
                NULL, NULL, 'Energy', 'CL=F', 'Oil market', 1
            );
            INSERT INTO oracle_index_snapshots (
                as_of, generated_at, entity_id, entity_label, entity_type,
                parent_id, theme, direction, horizon, score, magnitude,
                confidence, evidence_count, market_bias, plain_read, model
            ) VALUES (
                '2026-05-12', '2026-05-12T10:00:00', 'region:us',
                'United States', 'region', 'global', 'inflation',
                'inflation-risk', 'near_term', 55, 2.0, 0.8, 3,
                'macro headwind', 'Raw inflation read.', 'test'
            ), (
                '2026-05-12', '2026-05-12T10:00:00', 'market:oil',
                'Oil', 'market', 'commodity:energy_commodities', 'disaster',
                'supply-risk', 'near_term', 75, 3.0, 0.9, 4,
                'risk headwind', 'Raw supply read.', 'test'
            );
            """
        )

        rows = queries.oracle_layer_map(conn)
        self.assertEqual(len(rows), 2)
        by_entity = {row["entity_id"]: row for row in rows.to_dict(orient="records")}
        self.assertEqual(by_entity["region:us"]["display_label"], "Inflation pressure")
        self.assertEqual(by_entity["region:us"]["layer"], "macro_layer")
        self.assertEqual(by_entity["region:us"]["map_zone"], "regional")
        self.assertEqual(by_entity["market:oil"]["display_label"], "Supply shock")
        self.assertEqual(by_entity["market:oil"]["layer"], "risk_layer")
        self.assertEqual(by_entity["market:oil"]["map_zone"], "market")

        risk_rows = queries.oracle_layer_map(conn, layer="risk_layer")
        self.assertEqual(len(risk_rows), 1)
        self.assertEqual(risk_rows.iloc[0]["entity_id"], "market:oil")

    def test_backtest_summary_query(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE backtest_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT, created_at TEXT, started_at TEXT, finished_at TEXT,
                status TEXT, config_json TEXT, notes TEXT
            );
            CREATE TABLE backtest_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER, asset TEXT, asset_name TEXT, horizon TEXT,
                as_of TEXT, generated_at TEXT, direction TEXT, confidence REAL,
                expected_return_low REAL, expected_return_high REAL,
                rationale_md TEXT, key_risks TEXT, analogues_used TEXT,
                model TEXT, input_hash TEXT, input_brief_md TEXT, dry_run INTEGER,
                realized_return REAL, direction_hit INTEGER, range_hit INTEGER,
                scored_at TEXT, error TEXT
            );
            INSERT INTO backtest_runs (
                name, created_at, started_at, finished_at, status, config_json
            ) VALUES (
                'smoke', '2026-05-12T10:00:00', '2026-05-12T10:00:00',
                '2026-05-12T10:01:00', 'completed', '{}'
            );
            INSERT INTO backtest_predictions (
                run_id, asset, horizon, as_of, generated_at, direction,
                confidence, model, input_hash, dry_run, realized_return,
                direction_hit, range_hit
            ) VALUES (
                1, 'SPY', '1m', '2021-01-31', '2026-05-12T10:00:00',
                'up', 0.6, 'dry', 'abc', 1, 0.04, 0, 0
            ), (
                1, 'SPY', '1m', '2021-02-28', '2026-05-12T10:00:00',
                'up', 0.6, 'live', 'def', 0, 0.04, 1, 1
            );
            """
        )
        runs = queries.backtest_runs(conn)
        summary = queries.backtest_summary(conn, run_id=1)
        rows = queries.backtest_predictions(conn, run_id=1)
        self.assertEqual(len(runs), 1)
        self.assertEqual(summary.iloc[0]["dry_rows"], 1)
        self.assertEqual(summary.iloc[0]["scored"], 1)
        self.assertEqual(summary.iloc[0]["directional_accuracy"], 1.0)
        self.assertEqual(rows.iloc[0]["asset"], "SPY")

    def test_data_label_schema_supports_many_to_many_assignments(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        label_count, assignment_count = seed_data_labels(conn)
        self.assertGreater(label_count, 0)
        self.assertGreater(assignment_count, 0)

        conn.execute(
            """INSERT INTO data_labels
               (label_id, label_type, label, description, default_weight)
               VALUES ('theme:test_extra', 'theme', 'test_extra', 'extra', 0.5)"""
        )
        conn.execute(
            """INSERT INTO data_label_assignments
               (label_id, target_type, target_table, target_column, target_value,
                weight_override, confidence, notes)
               VALUES ('theme:test_extra', 'symbol', 'targets', 'symbol', 'GC=F',
                       1.7, 0.8, 'test override')"""
        )
        conn.execute(
            """INSERT INTO data_label_assignments
               (label_id, target_type, target_table, target_column, target_value,
                confidence, notes)
               VALUES ('theme:inflation', 'category', 'news_items', 'category',
                       'inflation', 0.9, 'same label on another table')"""
        )

        gold_labels = labels.label_assignments(
            conn, target_type="symbol", target_table="targets",
            target_column="symbol", target_value="GC=F",
        )
        self.assertGreaterEqual(len(gold_labels), 2)
        override = gold_labels[gold_labels["label_id"] == "theme:test_extra"].iloc[0]
        self.assertEqual(float(override["effective_weight"]), 1.7)

        inflation_labels = labels.label_assignments(conn, label_type="theme")
        inflation_targets = inflation_labels[inflation_labels["label_id"] == "theme:inflation"]
        self.assertGreaterEqual(inflation_targets["target_table"].nunique(), 2)

        stream_labels = labels.label_assignments(
            conn, target_type="stream", target_table="gdelt_streams",
            target_column="stream", target_value="market_stress",
        )
        self.assertIn("theme:market_moving_news", set(stream_labels["label_id"]))
        self.assertIn("asset_impact:sp500", set(stream_labels["label_id"]))

    def test_new_core_queries_handle_missing_optional_tables(self):
        conn = sqlite3.connect(":memory:")
        self.assertTrue(queries.sanctions(conn).empty)
        self.assertTrue(queries.twitter_topic_heat(conn).empty)
        self.assertTrue(queries.twitter_macro_posts(conn).empty)
        self.assertTrue(queries.reddit_context(conn).empty)
        self.assertTrue(queries.rumour_spikes(conn).empty)
        self.assertTrue(queries.weather_correlations(conn).empty)
        self.assertTrue(queries.company_fundamentals_summary(conn).empty)
        self.assertTrue(queries.insider_activity(conn).empty)
        self.assertTrue(queries.news_items(conn).empty)
        self.assertTrue(queries.trade_indicators(conn).empty)
        self.assertTrue(queries.gdelt_streams(conn).empty)
        self.assertTrue(queries.gdelt_stream_examples(conn).empty)
        self.assertTrue(queries.historical_state(conn).empty)
        self.assertTrue(queries.historical_comparison(conn).empty)

    def test_jsonable_records_normalizes_nan_to_none(self):
        import pandas as pd

        records = queries.jsonable_records(pd.DataFrame([{"x": float("nan"), "y": None}]))
        self.assertEqual(records, [{"x": None, "y": None}])

    def test_new_core_queries_return_backend_source_rows(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE sanctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT, list_name TEXT, program TEXT, entity_name TEXT,
                entity_type TEXT, country TEXT, target_type TEXT, product TEXT,
                measure TEXT, fetched_at TEXT
            );
            CREATE TABLE twitter_macro_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posted_at TEXT, query TEXT, topic TEXT, author_username TEXT,
                text TEXT, like_count INTEGER, repost_count INTEGER,
                reply_count INTEGER, quote_count INTEGER, url TEXT, fetched_at TEXT
            );
            CREATE TABLE social_mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, source TEXT, ticker TEXT, mention_count INTEGER,
                sentiment_score REAL, top_post_title TEXT, top_post_score INTEGER
            );
            CREATE TABLE rumour_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, ticker TEXT, mentions_today INTEGER,
                baseline_mean REAL, z_score REAL, sentiment_today REAL,
                realized_return_5d REAL, realized_return_21d REAL
            );
            CREATE TABLE weather_correlations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT, location TEXT, weather_var TEXT, window_days INTEGER,
                correlation REAL, n_observations INTEGER, computed_at TEXT
            );
            CREATE TABLE company_fundamentals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT, period_end TEXT, period_type TEXT, concept TEXT,
                value REAL, unit TEXT, form TEXT, accession_number TEXT, filed TEXT
            );
            CREATE TABLE insider_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_date TEXT, transaction_date TEXT, ticker TEXT,
                insider_name TEXT, insider_role TEXT, transaction_type TEXT,
                shares REAL, price REAL, value_usd REAL, accession_number TEXT
            );
            CREATE TABLE news_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                published_at TEXT, source TEXT, title TEXT, summary TEXT, url TEXT,
                region TEXT, category TEXT, used_for_predictions INTEGER,
                fetched_at TEXT
            );
            CREATE TABLE indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, country TEXT, category TEXT, indicator_name TEXT,
                value REAL, unit TEXT, expected_value REAL, impact TEXT
            );
            INSERT INTO sanctions VALUES
                (1, 'ofac', 'SDN', 'TEST', 'Entity A', 'company', 'US', 'entity', 'oil', 'asset freeze', datetime('now'));
            INSERT INTO twitter_macro_posts VALUES
                (1, datetime('now'), 'rates', 'rates', 'macro_author', 'rates text', 3, 2, 1, 1, 'https://x.test/1', datetime('now'));
            INSERT INTO social_mentions VALUES
                (1, date('now'), 'reddit', 'NVDA', 9, 0.2, 'top title', 42);
            INSERT INTO rumour_signals VALUES
                (1, date('now'), 'NVDA', 9, 2.0, 3.5, 0.2, NULL, NULL);
            INSERT INTO weather_correlations VALUES
                (1, 'CL=F', 'Dubai', 'temp_mean_c', 90, 0.42, 45, datetime('now'));
            INSERT INTO company_fundamentals VALUES
                (1, 'MSFT', '2026-03-31', 'quarter', 'Revenue', 1.0, 'USD', '10-Q', 'abc', date('now'));
            INSERT INTO insider_trades VALUES
                (1, date('now'), date('now'), 'MSFT', 'A Person', 'CEO', 'P', 10, 1.0, 10.0, 'abc');
            INSERT INTO news_items VALUES
                (1, datetime('now'), 'Fed', 'Title', 'Summary', 'https://news.test/1', 'US', 'central_bank', 1, datetime('now'));
            INSERT INTO indicators VALUES
                (1, '2026-01-01', 'US', 'trade', 'Exports', 100.0, 'USD', NULL, 'slow macro context');
            """
        )
        self.assertEqual(len(queries.sanction_clusters(conn)), 1)
        self.assertEqual(queries.sanction_program_stats(conn, "TEST")["rows"], 1)
        self.assertEqual(len(queries.twitter_topic_heat(conn)), 1)
        self.assertEqual(len(queries.twitter_macro_posts(conn)), 1)
        self.assertEqual(len(queries.reddit_context(conn)), 1)
        self.assertEqual(len(queries.rumour_spikes(conn)), 1)
        self.assertEqual(len(queries.weather_correlations(conn, min_observations=30)), 1)
        self.assertEqual(len(queries.company_fundamentals_summary(conn)), 1)
        self.assertEqual(len(queries.insider_activity(conn)), 1)
        self.assertEqual(len(queries.news_items(conn)), 1)
        self.assertEqual(len(queries.trade_indicators(conn)), 1)

    def test_audit_reports_missing_empty_stale_and_label_orphans(self):
        empty_conn = sqlite3.connect(":memory:")
        missing = audit.source_audit(empty_conn)
        self.assertIn("missing", set(missing["status"]))
        self.assertIn("optional_missing", set(missing["status"]))

        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        conn.execute(
            """INSERT INTO prices (date, symbol, asset_class, name, price, currency)
               VALUES ('2000-01-01', 'SPY', 'equity_etf', 'SPY', 100, 'USD')"""
        )
        conn.execute(
            """INSERT INTO data_label_assignments
               (label_id, target_type, target_table, target_column, target_value)
               VALUES ('missing:label', 'table', 'missing_table', '', '')"""
        )
        result = audit.run_audit(conn)
        source_status = dict(zip(result["sources"]["table"], result["sources"]["status"]))
        self.assertEqual(source_status["prices"], "stale")
        self.assertEqual(source_status["signals"], "empty")
        self.assertGreaterEqual(len(result["label_orphans"]), 1)
        self.assertIn("orphan_label", set(result["label_orphans"]["status"]))
        self.assertIn("orphan_target_table", set(result["label_orphans"]["status"]))

    def test_source_runs_schema_and_audit_surface_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "runs.db")
            conn = connect_writable(db_path)
            conn.executescript(SCHEMA)
            self.assertEqual(conn.execute("PRAGMA busy_timeout").fetchone()[0], 60000)
            conn.commit()
            conn.close()

            ok_id = start_source_run("daily", "fred", db_path=db_path)
            finish_source_run(ok_id, status="success", rows_seen=3, rows_inserted=2, db_path=db_path)
            fail_id = start_source_run("frequent", "bls", db_path=db_path)
            finish_source_run(fail_id, status="failure", error_message="network down", db_path=db_path)

            conn = sqlite3.connect(db_path)
            try:
                health = queries.ingestion_health(conn, days=7)
                self.assertEqual(set(health["source"]), {"fred", "bls"})
                bls = health[health["source"] == "bls"].iloc[0]
                self.assertEqual(bls["last_status"], "failure")
                self.assertEqual(int(bls["failure_streak"]), 1)

                result = audit.run_audit(conn)
                self.assertIn("source_runs", result)
                self.assertEqual(int(result["summary"]["run_failures"]), 1)
            finally:
                conn.close()

    def test_macro_actuals_match_calendar_to_latest_official_indicator(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        today = dt.date.today()
        release_date = today.isoformat()
        value_date = (today.replace(day=1) - dt.timedelta(days=1)).replace(day=1).isoformat()
        conn.execute(
            """INSERT INTO calendar_events
               (date, time_local, region, category, importance, title, expected, source)
               VALUES (?, '08:30 ET', 'US', 'inflation', 5, 'US CPI for test month', '2.8%', 'test')""",
            (release_date,),
        )
        conn.execute(
            """INSERT INTO indicators
               (date, country, category, indicator_name, value, unit, impact)
               VALUES (?, 'US', 'inflation', 'BLS CPI-U All Items NSA YoY', 3.1, '%', 'negative')""",
            (value_date,),
        )

        event = conn.execute("SELECT * FROM calendar_events").fetchone()
        payload = macro_actuals._actual_row(conn, event, today)
        conn.execute(
            """INSERT INTO macro_release_actuals (
                 calendar_event_id, release_key, region, category, title,
                 scheduled_date, scheduled_time_local, importance,
                 actual_value, expected_value, expected_text, expected_unit,
                 previous_value, surprise_value, surprise_text,
                 unit, source_table, source_id,
                 source_indicator_name, value_date, status, metadata_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            payload,
        )

        latest = queries.latest_macro_releases(conn, hours=72)
        catalysts = queries.next_macro_catalysts(conn)
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest.iloc[0]["status"], "released")
        self.assertAlmostEqual(float(latest.iloc[0]["actual_value"]), 3.1)
        self.assertAlmostEqual(float(latest.iloc[0]["expected_value"]), 2.8)
        self.assertEqual(latest.iloc[0]["expected_unit"], "%")
        self.assertTrue(catalysts.empty)

    def test_macro_actuals_reject_prose_expectations_and_prefer_yoy_percent(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        today = dt.date.today()
        release_date = today.isoformat()
        conn.execute(
            """INSERT INTO calendar_events
               (date, time_local, region, category, importance, title, expected, source)
               VALUES (?, '08:00 CET', 'SE', 'inflation', 5,
                       'Sweden CPI / CPIF April 2026',
                       'Regular SCB CPI publication after flash CPI showed CPIF slowing to 0.8%.',
                       'test')""",
            (release_date,),
        )
        conn.executescript(
            """
            INSERT INTO indicators
               (date, country, category, indicator_name, value, unit, impact)
               VALUES
               ('2026-04-01', 'SE', 'inflation', 'SCB CPIF excl. energy, Index 2020=100', 121.58, 'Index', 'negative'),
               ('2026-04-01', 'SE', 'inflation', 'SCB CPIF, annual changes, 2020=100', 0.8, '%', 'negative'),
               ('2026-03-01', 'SE', 'inflation', 'SCB CPIF, annual changes, 2020=100', 1.6, '%', 'negative');
            """
        )

        event = conn.execute("SELECT * FROM calendar_events").fetchone()
        payload = macro_actuals._actual_row(conn, event, today)
        self.assertEqual(payload[8], 0.8)
        self.assertIsNone(payload[9])
        self.assertIn("flash CPI", payload[10])
        self.assertEqual(payload[15], "%")
        self.assertEqual(payload[18], "SCB CPIF, annual changes, 2020=100")

    def test_macro_actuals_use_specific_unmatched_statuses_and_coverage(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        today = dt.date.today()
        past = (today - dt.timedelta(days=20)).isoformat()
        conn.executemany(
            """INSERT INTO calendar_events
               (date, time_local, region, category, importance, title, expected, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (past, "12:00 ET", "US", "energy", 4, "EIA Weekly Petroleum Status", None, "test"),
                (today.isoformat(), "08:30 ET", "US", "unknown", 3, "Unknown release", None, "test"),
            ],
        )
        rows = conn.execute("SELECT * FROM calendar_events ORDER BY id").fetchall()
        energy_payload = macro_actuals._actual_row(conn, rows[0], today)
        unknown_payload = macro_actuals._actual_row(conn, rows[1], today)
        self.assertEqual(energy_payload[20], "unmatched_source_missing")
        self.assertEqual(unknown_payload[20], "unmatched_rule_gap")
        conn.executemany(
            """INSERT INTO macro_release_actuals (
                 calendar_event_id, release_key, region, category, title,
                 scheduled_date, scheduled_time_local, importance,
                 actual_value, expected_value, expected_text, expected_unit,
                 previous_value, surprise_value, surprise_text,
                 unit, source_table, source_id,
                 source_indicator_name, value_date, status, metadata_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [energy_payload, unknown_payload],
        )
        coverage = queries.macro_actual_match_coverage(conn, days_back=30, days_forward=30)
        self.assertIn("unmatched_source_missing", ",".join(coverage["status_summary"].astype(str)))
        self.assertIn("unmatched_rule_gap", ",".join(coverage["status_summary"].astype(str)))

    def test_revision_aware_price_upsert_updates_existing_close(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        upsert_many(
            conn,
            "prices",
            ("date", "symbol", "asset_class", "name", "price", "currency"),
            ("date", "symbol"),
            [("2026-01-02", "SPY", "equity_etf", "SPY", 100.0, "USD")],
            update_columns=("asset_class", "name", "price", "currency"),
        )
        upsert_many(
            conn,
            "prices",
            ("date", "symbol", "asset_class", "name", "price", "currency"),
            ("date", "symbol"),
            [("2026-01-02", "SPY", "equity_etf", "SPY", 101.5, "USD")],
            update_columns=("asset_class", "name", "price", "currency"),
        )
        rows = conn.execute("SELECT COUNT(*), MAX(price) FROM prices WHERE symbol = 'SPY'").fetchone()
        self.assertEqual(rows[0], 1)
        self.assertAlmostEqual(float(rows[1]), 101.5)

    def test_label_weight_profiles_override_canonical_defaults(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)

        profiles = labels.label_weight_profiles(conn)
        self.assertIn("risk_stress", set(profiles["profile_id"]))

        default_catalog = labels.weighted_label_catalog(conn)
        risk_catalog = labels.weighted_label_catalog(conn, profile_id="risk_stress")
        default_weight = float(default_catalog[default_catalog["label_id"] == "stream:conflict_security"].iloc[0]["effective_weight"])
        risk_weight = float(risk_catalog[risk_catalog["label_id"] == "stream:conflict_security"].iloc[0]["effective_weight"])
        self.assertGreater(risk_weight, default_weight)

        stream_assignments = labels.label_assignments(
            conn,
            target_type="stream",
            target_table="gdelt_streams",
            target_column="stream",
            target_value="conflict_security",
            profile_id="risk_stress",
        )
        conflict = stream_assignments[stream_assignments["label_id"] == "stream:conflict_security"].iloc[0]
        self.assertEqual(float(conflict["effective_weight"]), risk_weight)

    def test_gdelt_classifier_store_examples_and_review_gate(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)

        self.assertEqual(gdelt.classify_streams({"ECON_CENTRALBANK"}), ["policy_rates"])
        self.assertIn("major_disaster", gdelt.classify_streams({"NATURAL_DISASTER_FLOOD"}))
        self.assertIn("conflict_security", gdelt.classify_streams({"TERROR"}))
        self.assertIn("trade_sanctions_supply", gdelt.classify_streams({"ECON_SANCTIONS"}))
        self.assertIn("energy_commodities", gdelt.classify_streams({"ECON_OILPRICE"}))

        def gkg_row(themes: str, country: str = "US", place: str = "Test City", url: str = "https://news.test/a") -> list[str]:
            row = [""] * 16
            row[gdelt.COL_DATE] = "20260514000000"
            row[gdelt.COL_SOURCE_DOMAIN] = "news.test"
            row[gdelt.COL_DOCUMENT] = url
            row[gdelt.COL_THEMES] = themes
            row[gdelt.COL_LOCATIONS] = f"1#{place}#{country}#00#40.0#-70.0#x"
            row[gdelt.COL_TONE] = "0.1,0,0"
            return row

        rows = [
            gkg_row("ELECTION", url="https://news.test/political-low"),
            gkg_row("ECON_CENTRALBANK;ECON_INTEREST_RATES", url="https://news.test/rates"),
            *[
                gkg_row("NATURAL_DISASTER_FLOOD", url=f"https://news.test/flood-{i}")
                for i in range(5)
            ],
            *[
                gkg_row("TERROR", country="IR", place="Strait", url=f"https://news.test/conflict-{i}")
                for i in range(220)
            ],
        ]
        total, counts, disasters, streams = gdelt.count_rows(rows)
        self.assertEqual(total, len(rows))
        self.assertIn(("major_disaster", "Global", ""), streams)
        self.assertIn(("conflict_security", "Middle East", "IR"), streams)

        gdelt.store(conn, dt.date(2026, 5, 14), total, counts, disasters, streams)
        disaster_streams = queries.gdelt_streams(conn, days=365, stream="major_disaster")
        self.assertFalse(disaster_streams.empty)
        self.assertIn("stream:major_disaster", disaster_streams.iloc[0]["labels_json"])

        conflict_streams = queries.gdelt_streams(conn, days=365, stream="conflict_security")
        self.assertIn("theme:market_moving_news", conflict_streams.iloc[0]["labels_json"])
        self.assertIn("asset_impact:gold", conflict_streams.iloc[0]["labels_json"])

        disaster_examples = queries.gdelt_stream_examples(conn, stream="major_disaster")
        conflict_examples = queries.gdelt_stream_examples(conn, stream="conflict_security")
        political_examples = queries.gdelt_stream_examples(conn, stream="political_risk")
        self.assertGreaterEqual(len(disaster_examples), 1)
        self.assertGreaterEqual(len(conflict_examples), 1)
        self.assertIn("labels_json", conflict_examples.columns)
        self.assertIn("theme:market_moving_news", conflict_examples.iloc[0]["labels_json"])
        self.assertTrue(political_examples.empty)

        reviews = conn.execute(
            "SELECT COUNT(*) FROM oracle_review_annotations WHERE source_table = 'gdelt_streams'"
        ).fetchone()[0]
        self.assertGreaterEqual(reviews, 1)
        self.assertLessEqual(reviews, gdelt.MAX_REVIEWS_PER_DAY)

        coverage = queries.gdelt_stream_coverage(conn, days=365)
        self.assertIn("major_disaster", set(coverage["stream"]))
        self.assertGreater(int(coverage[coverage["stream"] == "major_disaster"].iloc[0]["examples"]), 0)

    def test_gdelt_noisy_disaster_rows_stay_aggregate_only_at_low_severity(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)

        def gkg_row(url: str) -> list[str]:
            row = [""] * 16
            row[gdelt.COL_DATE] = "20260514000000"
            row[gdelt.COL_SOURCE_DOMAIN] = "weather.test"
            row[gdelt.COL_DOCUMENT] = url
            row[gdelt.COL_THEMES] = "NATURAL_DISASTER_ICE"
            row[gdelt.COL_LOCATIONS] = "1#Road#US#00#40.0#-70.0#x"
            return row

        rows = [gkg_row(f"https://weather.test/ice-{i}") for i in range(2)]
        total, counts, disasters, streams = gdelt.count_rows(rows)
        self.assertIn(("major_disaster", "Global", ""), streams)

        gdelt.store(conn, dt.date(2026, 5, 14), total, counts, disasters, streams)
        aggregate = queries.gdelt_streams(conn, days=365, stream="major_disaster")
        examples = queries.gdelt_stream_examples(conn, stream="major_disaster")
        reviews = conn.execute(
            "SELECT COUNT(*) FROM oracle_review_annotations WHERE source_table = 'gdelt_streams'"
        ).fetchone()[0]
        self.assertFalse(aggregate.empty)
        self.assertTrue(examples.empty)
        self.assertEqual(reviews, 0)

    def test_gdelt_enforce_current_caps_compacts_existing_over_cap_rows(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        conn.execute(
            """INSERT INTO gdelt_streams (
                 date, stream, region, country, article_count, total_articles,
                 article_share, baseline_30d, z_score, severity,
                 societal_impact_score, labels_json
               ) VALUES (
                 '2026-05-14', 'major_disaster', 'Global', '', 500, 1000,
                 0.5, 100, 4.0, 5.0, 5.0, '["stream:major_disaster"]'
               )"""
        )
        stream_id = conn.execute("SELECT id FROM gdelt_streams").fetchone()[0]
        conn.executemany(
            """INSERT INTO gdelt_stream_examples (
                 date, stream, region, country, example_rank, title, url
               ) VALUES (
                 '2026-05-14', 'major_disaster', 'Global', '', ?, ?, ?
               )""",
            [
                (i, f"example {i}", f"https://news.test/{i}")
                for i in range(1, gdelt.MAX_EXAMPLES_PER_DAY + 3)
            ],
        )
        conn.executemany(
            """INSERT INTO gdelt_streams (
                 date, stream, region, country, article_count, total_articles,
                 article_share, baseline_30d, z_score, severity,
                 societal_impact_score, labels_json
               ) VALUES (
                 '2026-05-14', 'conflict_security', 'Global', ?, 500, 1000,
                 0.5, 100, 4.0, 5.0, 5.0, '["stream:conflict_security"]'
               )""",
            [(f"C{i}",) for i in range(gdelt.MAX_REVIEWS_PER_DAY + 2)],
        )
        review_source_ids = [
            row[0] for row in conn.execute(
                "SELECT id FROM gdelt_streams WHERE id <> ? ORDER BY id",
                (stream_id,),
            )
        ]
        conn.executemany(
            """INSERT INTO oracle_review_annotations (
                 source_table, source_id, as_of, review_type, severity,
                 confidence, comment
               ) VALUES (
                 'gdelt_streams', ?, '2026-05-14', 'high_impact_stream',
                 5.0, 0.9, 'review'
               )""",
            [(source_id,) for source_id in review_source_ids],
        )

        dry_run = gdelt.enforce_current_caps(conn)
        self.assertEqual(dry_run["over_cap_examples"], 2)
        self.assertEqual(dry_run["over_cap_reviews"], 2)

        applied = gdelt.enforce_current_caps(conn, apply=True)
        self.assertEqual(applied["deleted"], 4)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM gdelt_stream_examples").fetchone()[0],
            gdelt.MAX_EXAMPLES_PER_DAY,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM oracle_review_annotations").fetchone()[0],
            gdelt.MAX_REVIEWS_PER_DAY,
        )

    def test_historical_state_uses_no_lookahead_and_forward_returns(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2009-11-29', 'SPY', 'equity_etf', 'SPY', 100, 'USD'),
              ('2009-11-30', 'SPY', 'equity_etf', 'SPY', 110, 'USD'),
              ('2009-12-05', 'SPY', 'equity_etf', 'SPY', 121, 'USD');
            INSERT INTO indicators (date, country, category, indicator_name, value, unit) VALUES
              ('2009-11-01', 'US', 'inflation', 'CPI', 1.0, 'idx'),
              ('2009-12-01', 'US', 'inflation', 'CPI', 2.0, 'idx');
            """
        )

        result = historical_state.build_historical_state(
            conn,
            dt.date(2009, 11, 30),
            dt.date(2009, 11, 30),
            symbols=["SPY"],
            horizons=(5,),
            required_keys=("price:SPY",),
            min_coverage=1.0,
        )
        self.assertEqual(result["daily"], 1)

        state = queries.historical_state(conn, "2009-11-30", keys=["indicator:US:cpi"])
        self.assertEqual(float(state.iloc[0]["value"]), 1.0)
        self.assertEqual(str(state.iloc[0]["value_date"])[:10], "2009-11-01")

        comparison = queries.historical_comparison(
            conn, start="2009-11-30", end="2009-11-30", symbols=["SPY"],
        )
        price_row = comparison[comparison["value_key"] == "price:SPY"].iloc[0]
        self.assertEqual(int(price_row["training_eligible"]), 1)
        self.assertEqual(int(price_row["evaluation_eligible"]), 1)
        self.assertAlmostEqual(float(price_row["forward_return"]), 0.10, places=6)

    def test_historical_evaluation_ignores_unpriced_target_symbols(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2009-11-30', 'SPY', 'equity_etf', 'SPY', 100, 'USD'),
              ('2009-12-05', 'SPY', 'equity_etf', 'SPY', 110, 'USD');
            """
        )

        historical_state.build_historical_state(
            conn,
            dt.date(2009, 11, 30),
            dt.date(2009, 11, 30),
            symbols=["SPY", "UNPRICED"],
            horizons=(5,),
            required_keys=("price:SPY",),
            min_coverage=1.0,
        )
        state = queries.historical_state(conn, "2009-11-30", keys=["price:SPY"])
        self.assertEqual(int(state.iloc[0]["evaluation_eligible"]), 1)

    def test_latest_complete_market_date_requires_all_selected_symbols(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2026-05-13', 'SPY', 'equity_etf', 'SPY', 100, 'USD'),
              ('2026-05-13', 'TLT', 'bond_etf', 'TLT', 90, 'USD'),
              ('2026-05-14', 'SPY', 'equity_etf', 'SPY', 101, 'USD');
            """
        )

        latest = historical_state._latest_complete_market_date(conn, symbols=["SPY", "TLT"])
        self.assertEqual(latest, dt.date(2026, 5, 13))

    def test_parallel_historical_builder_writes_file_db(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.executescript(SCHEMA)
            seed_data_labels(conn)
            conn.executescript(
                """
                INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
                  ('2009-11-30', 'SPY', 'equity_etf', 'SPY', 100, 'USD'),
                  ('2009-12-05', 'SPY', 'equity_etf', 'SPY', 110, 'USD');
                """
            )
            conn.commit()

            result = historical_state.build_historical_state_parallel(
                conn,
                dt.date(2009, 11, 30),
                dt.date(2009, 11, 30),
                symbols=["SPY"],
                horizons=(5,),
                required_keys=("price:SPY",),
                min_coverage=1.0,
                workers=2,
                db_path=tmp.name,
            )
            conn.commit()

            self.assertEqual(result["daily"], 1)
            state = queries.historical_state(conn, "2009-11-30", keys=["price:SPY"])
            self.assertEqual(int(state.iloc[0]["evaluation_eligible"]), 1)
            conn.close()

    def test_oracle_snapshot_horizons_have_distinct_historical_keys(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2026-05-13', 'SPY', 'equity_etf', 'SPY', 100, 'USD');
            INSERT INTO oracle_index_snapshots (
                as_of, generated_at, entity_id, entity_label, entity_type,
                theme, direction, horizon, score, magnitude, confidence,
                evidence_count, market_bias, plain_read, model
            ) VALUES
              ('2026-05-13', '2026-05-13T12:00:00', 'global', 'Global',
               'global', 'inflation', 'inflation-risk', 'near_term',
               55, 2, 0.8, 3, 'macro headwind', 'near read', 'test'),
              ('2026-05-13', '2026-05-13T12:00:00', 'global', 'Global',
               'global', 'inflation', 'inflation-risk', 'medium_term',
               70, 3, 0.8, 4, 'macro headwind', 'medium read', 'test');
            """
        )

        historical_state.build_historical_state(
            conn,
            dt.date(2026, 5, 13),
            dt.date(2026, 5, 13),
            symbols=["SPY"],
            horizons=(5,),
            required_keys=("price:SPY",),
            min_coverage=1.0,
        )
        state = queries.historical_state(conn, "2026-05-13")
        oracle_rows = state[state["value_key"].str.startswith("oracle_index:global:inflation")]
        self.assertEqual(set(oracle_rows["value_key"]), {
            "oracle_index:global:inflation:near_term",
            "oracle_index:global:inflation:medium_term",
        })

    def test_delete_historical_rows_after_cutoff(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        conn.executescript(
            """
            INSERT INTO historical_state_daily (as_of) VALUES ('2026-05-13'), ('2026-05-14');
            INSERT INTO historical_state_values (
                as_of, value_key, source_table, value_date
            ) VALUES
              ('2026-05-13', 'price:SPY', 'prices', '2026-05-13'),
              ('2026-05-14', 'price:SPY', 'prices', '2026-05-14');
            INSERT INTO historical_forward_returns (
                as_of, symbol, horizon_days, start_price
            ) VALUES
              ('2026-05-13', 'SPY', 5, 100),
              ('2026-05-14', 'SPY', 5, 101);
            """
        )

        historical_state._delete_historical_rows_after(conn, dt.date(2026, 5, 13))
        for table in ("historical_state_daily", "historical_state_values", "historical_forward_returns"):
            remaining = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE as_of > '2026-05-13'").fetchone()[0]
            self.assertEqual(remaining, 0)

    def test_historical_signal_labels_freshness_and_coverage(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2026-05-13', 'GC=F', 'commodity', 'Gold', 2000, 'USD');
            INSERT INTO signals (date, symbol, signal_name, value) VALUES
              ('2026-05-13', 'GC=F', 'ret_1d', 0.02),
              ('2026-05-13', 'GC=F', 'drawdown_252d', -0.05);
            """
        )

        historical_state.build_historical_state(
            conn,
            dt.date(2026, 5, 14),
            dt.date(2026, 5, 14),
            symbols=["GC=F"],
            horizons=(5,),
            required_keys=("price:GC=F",),
            min_coverage=1.0,
        )
        state = queries.historical_state(conn, "2026-05-14")
        ret = state[state["value_key"] == "signal:GC=F:ret_1d"].iloc[0]
        self.assertIn("asset_group:commodity", ret["label_ids_json"])
        self.assertIn("theme:momentum", ret["label_ids_json"])
        self.assertEqual(ret["freshness_class"], "fresh")
        self.assertEqual(int(ret["evaluation_eligible"]), 0)

        coverage = queries.historical_state_coverage(conn, "2026-05-14")
        signals = coverage[coverage["source_table"] == "signals"].iloc[0]
        self.assertEqual(int(signals["unlabeled_rows"]), 0)
        self.assertEqual(float(signals["label_coverage_pct"]), 100.0)

    def test_historical_builder_adds_optional_source_rows_when_available(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2026-05-13', 'SPY', 'equity_etf', 'SPY', 100, 'USD');
            INSERT INTO sanctions (
                source, list_name, program, entity_name, entity_type, country,
                target_type, product, measure, fetched_at
            ) VALUES
              ('ofac', 'SDN', 'TEST', 'A', 'company', 'US', 'entity', 'oil', 'freeze', '2026-05-13T00:00:00'),
              ('ofac', 'SDN', 'TEST', 'B', 'company', 'US', 'entity', 'oil', 'freeze', '2026-05-13T00:00:00'),
              ('ofac', 'SDN', 'TEST', 'C', 'company', 'US', 'entity', 'oil', 'freeze', '2026-05-13T00:00:00');
            INSERT INTO risk_hotspots (
                name, region, country, category, severity, lat, lon, summary, source, updated_at
            ) VALUES ('Test Hotspot', 'Middle East', 'IR', 'conflict', 5, 1, 1, 'risk', 'test', '2026-05-13');
            INSERT INTO social_mentions (
                date, source, ticker, mention_count, sentiment_score, top_post_title, top_post_score
            ) VALUES ('2026-05-13', 'reddit', 'SPY', 12, 0.2, 'SPY heat', 5);
            """
        )

        historical_state.build_historical_state(
            conn,
            dt.date(2026, 5, 14),
            dt.date(2026, 5, 14),
            symbols=["SPY"],
            horizons=(5,),
            required_keys=("price:SPY",),
            min_coverage=1.0,
        )
        state = queries.historical_state(conn, "2026-05-14")
        self.assertIn("sanctions", set(state["source_table"]))
        self.assertIn("risk_hotspots", set(state["source_table"]))
        self.assertIn("social_mentions", set(state["source_table"]))

    def test_legacy_events_are_labeled_in_historical_state(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2026-05-14', 'SPY', 'equity_etf', 'SPY', 100, 'USD');
            INSERT INTO events (date, country, type, title, source) VALUES
              ('2026-05-14', 'US', 'crisis', 'Test financial crisis event', 'test');
            """
        )

        historical_state.build_historical_state(
            conn,
            dt.date(2026, 5, 14),
            dt.date(2026, 5, 14),
            symbols=["SPY"],
            horizons=(5,),
            required_keys=("price:SPY",),
            min_coverage=1.0,
        )
        state = queries.historical_state(conn, "2026-05-14")
        event = state[state["source_table"] == "events"].iloc[0]
        self.assertIn("source_family:calendar", event["label_ids_json"])
        self.assertIn("theme:crisis", event["label_ids_json"])

    def test_historical_fundamentals_deduplicate_latest_fact(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_labels(conn)
        conn.executescript(
            """
            INSERT INTO prices (date, symbol, asset_class, name, price, currency) VALUES
              ('2026-05-13', 'SPY', 'equity_etf', 'SPY', 100, 'USD');
            INSERT INTO company_fundamentals (
                ticker, period_end, period_type, concept, value, unit, form, filed
            ) VALUES
              ('ABC', '2026-03-31', 'Q', 'Revenue', 10, 'USD', '10-Q', '2026-05-01'),
              ('ABC', '2026-03-31', 'Q', 'Revenue', 11, 'USD', '10-Q/A', '2026-05-01');
            """
        )

        historical_state.build_historical_state(
            conn,
            dt.date(2026, 5, 13),
            dt.date(2026, 5, 13),
            symbols=["SPY"],
            horizons=(5,),
            required_keys=("price:SPY",),
            min_coverage=1.0,
        )
        state = queries.historical_state(conn, "2026-05-13")
        rows = state[state["value_key"] == "fundamental:ABC:revenue"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(float(rows.iloc[0]["value"]), 11.0)

    def test_sparse_early_historical_state_is_readable_not_training_eligible(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        conn.execute(
            """INSERT INTO prices (date, symbol, asset_class, name, price, currency)
               VALUES ('2001-04-20', 'SPY', 'equity_etf', 'SPY', 100, 'USD')"""
        )

        historical_state.build_historical_state(
            conn,
            dt.date(2001, 4, 20),
            dt.date(2001, 4, 20),
            symbols=["SPY"],
            horizons=(5,),
            required_keys=("price:SPY",),
            min_coverage=1.0,
        )
        state = queries.historical_state(conn, "2001-04-20", keys=["price:SPY"])
        self.assertEqual(len(state), 1)
        self.assertEqual(int(state.iloc[0]["training_eligible"]), 0)

    def test_data_contract_seeds_catalog_and_named_overview(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seeded = seed_data_objects(conn)
        self.assertGreater(seeded, 0)
        catalog = queries.data_catalog(conn)
        self.assertIn("calendar.event", set(catalog["object_id"]))
        calendar_obj = catalog[catalog["object_id"] == "macro.release.actual"].iloc[0]
        self.assertEqual(calendar_obj["display_name"], "Macro release actual")
        self.assertEqual(calendar_obj["prompt_role"], "fresh_result")

        overview = queries.named_data_overview(conn)
        self.assertIn("rows", overview.columns)
        self.assertIn("Calendar event", set(overview["display_name"]))

    def test_change_events_gate_high_impact_and_noop_upserts(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_objects(conn)
        self.assertFalse(requires_oracle_review("gdelt.stream", priority=2.0))
        self.assertTrue(requires_oracle_review("gdelt.stream", priority=4.5))

        first = enqueue_change_event(
            conn,
            object_id="macro.release.actual",
            source_table="macro_release_actuals",
            source_id=7,
            event_type="released",
            priority=5,
            labels=["release_family:cpi", "theme:inflation"],
            metadata={"status": "released", "scheduled_date": "2026-06-10"},
            event_key="macro.release.actual:test",
        )
        second = enqueue_change_event(
            conn,
            object_id="macro.release.actual",
            source_table="macro_release_actuals",
            source_id=7,
            event_type="released",
            priority=5,
            labels=["release_family:cpi", "theme:inflation"],
            metadata={"status": "released", "scheduled_date": "2026-06-10"},
            event_key="macro.release.actual:test",
        )
        events = queries.data_change_events(conn)
        self.assertGreaterEqual(first, 1)
        self.assertGreaterEqual(second, 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(int(events.iloc[0]["oracle_review_required"]), 1)

    def test_prediction_context_pack_is_bounded_and_stored(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_objects(conn)
        today = dt.date(2026, 5, 14)
        conn.execute(
            """INSERT INTO signals (date, symbol, signal_name, value)
               VALUES (?, 'SPY', 'ret_1w', 0.02), (?, '_macro', 'vix_regime', 1)""",
            (today.isoformat(), today.isoformat()),
        )
        conn.execute(
            """INSERT INTO macro_release_actuals (
                 calendar_event_id, release_key, region, category, title,
                 scheduled_date, importance, actual_value, expected_value,
                 status
               ) VALUES (NULL, 'us_cpi:test', 'US', 'inflation', 'US CPI',
                         ?, 5, 3.1, 2.8, 'released')""",
            (today.isoformat(),),
        )
        analogues = [Analogue("2019-01-31", 0.91, 0.034, {})]
        pack = context_packs.build_context_pack(conn, "SPY", "1m", today, analogues, asset_name="S&P 500 ETF")
        prompt = context_packs.render_context_prompt(pack)
        context_packs.store_context_pack(conn, pack, prompt)

        self.assertIn("prediction_context_pack.v1", pack["contract_version"])
        self.assertIn("Historical Reference IDs", prompt)
        self.assertLess(len(prompt), 25_000)
        stored = queries.prediction_context_packs(conn, asset="SPY", horizon="1m", include_prompt=True)
        self.assertEqual(len(stored), 1)
        self.assertIn("Macro Intelligence Engine Prediction Context Pack", stored.iloc[0]["prompt_md"])

    def test_source_registry_event_window_cadence(self):
        now = dt.datetime(2026, 6, 10, 11, 0, tzinfo=dt.UTC)
        events = [{"scheduled_at_utc": "2026-06-10T12:30:00+00:00"}]
        self.assertTrue(in_event_window(now, events))
        self.assertEqual(source_cadence_hours("bls"), 24)
        self.assertEqual(source_cadence_hours("bls", event_window=True), 1)

    def test_calendar_schema_extended_fields_are_readable(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        seed_data_objects(conn)
        conn.execute(
            """INSERT INTO calendar_events (
                 date, time_local, region, category, importance, title,
                 expected, market_note, source, url, event_key, release_family,
                 scheduled_at_utc, labels_json, status
               ) VALUES (
                 date('now', '+1 day'), '08:30 ET', 'US', 'inflation', 5,
                 'US CPI test', '2.8%', 'test catalyst', 'BLS', 'https://example.test',
                 'us.cpi.test', 'cpi', '2026-06-10T12:30:00+00:00',
                 '["theme:inflation"]', 'scheduled'
               )"""
        )
        events = queries.calendar_events(conn, days_forward=3)
        self.assertEqual(events.iloc[0]["event_key"], "us.cpi.test")
        self.assertEqual(events.iloc[0]["release_family"], "cpi")
        result = audit.run_audit(conn)
        self.assertNotEqual(
            dict(zip(result["sources"]["table"], result["sources"]["status"]))["calendar_events"],
            "schema_mismatch",
        )


if __name__ == "__main__":
    unittest.main()
