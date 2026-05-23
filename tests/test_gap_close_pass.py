import datetime as dt
import sqlite3
import unittest
from unittest.mock import patch

from config import db_setup
from src.core import labels, queries
from src.core.source_registry import SOURCE_REGISTRY, source_enabled
from src.inference import predict
from src.intelligence import current_events_canary, label_evaluation


class GapClosePassTests(unittest.TestCase):
    def connect(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(db_setup.SCHEMA)
        db_setup.seed_data_objects(conn)
        db_setup.seed_data_labels(conn)
        current_events_canary.ensure_schema(conn)
        return conn

    def test_current_events_dedupes_canary_over_daily_and_caps_streams(self):
        conn = self.connect()
        now = dt.datetime(2026, 5, 14, 12, 0, tzinfo=dt.UTC)
        conn.execute(
            """
            INSERT INTO gdelt_streams (
                date, stream, region, country, article_count, total_articles,
                article_share, z_score, severity, societal_impact_score,
                labels_json, top_theme_codes_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-05-14", "conflict_security", "US", "", 50, 1000,
                0.05, 4.0, 4.4, 4.4, '["theme:conflict"]', '["WAR"]', now.isoformat(),
            ),
        )
        sample_streams = {
            ("conflict_security", "US", ""): {"count": 120, "themes": {"WAR": 120}, "examples": []},
            ("conflict_security", "CA", ""): {"count": 80, "themes": {"WAR": 80}, "examples": []},
            ("conflict_security", "MX", ""): {"count": 70, "themes": {"WAR": 70}, "examples": []},
            ("energy_commodities", "Global", ""): {"count": 90, "themes": {"ECON_OILPRICE": 90}, "examples": []},
        }
        with patch.object(current_events_canary, "_recent_gkg_stamps", return_value=[("20260514114500", "https://example.test/gkg.zip")]), patch.object(
            current_events_canary.gdelt, "fetch_file_counts", return_value=(360, {}, {}, sample_streams, None)
        ):
            events, _rows_seen, _latest_stamp = current_events_canary.build_events(conn, now=now, fetch_incremental_gdelt=True, recent_gdelt_hours=1)
        gdelt = [e for e in events if e["event_type"] == "breaking_news" and e["source_table"] in {"gdelt_streams", "gdelt_canary_files"}]
        conflict = [e for e in gdelt if e["category"] == "conflict_security"]
        self.assertLessEqual(len(conflict), current_events_canary.GDELT_EVENT_PER_STREAM_CAP)
        self.assertEqual(len({e["event_key"] for e in gdelt}), len(gdelt))
        same_scope = [e for e in gdelt if e["category"] == "conflict_security" and e["region"] == "US"]
        self.assertEqual(len(same_scope), 1)
        self.assertEqual(same_scope[0]["source_table"], "gdelt_canary_files")

    def test_shadow_prediction_prompt_stores_pack_but_selects_legacy(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        with patch.object(predict, "render_brief", return_value="LEGACY BRIEF") as render_brief, patch.object(
            predict, "build_context_pack", return_value={"pack_id": "x", "profile_id": "default", "sections": {"current_events": []}}
        ) as build_pack, patch.object(predict, "render_context_prompt", return_value="PACK PROMPT"), patch.object(
            predict, "store_context_pack", return_value=42
        ):
            selected, legacy, pack_prompt, pack_id = predict.prepare_prediction_prompt(
                conn, "SPY", "1m", dt.date(2026, 5, 14), [], None, "shadow"
            )
        self.assertEqual(selected, "LEGACY BRIEF")
        self.assertEqual(legacy, "LEGACY BRIEF")
        self.assertEqual(pack_prompt, "PACK PROMPT")
        self.assertEqual(pack_id, 42)
        render_brief.assert_called_once()
        build_pack.assert_called_once()

    def test_pack_prediction_prompt_selects_pack(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        with patch.object(predict, "render_brief", return_value="LEGACY"), patch.object(
            predict, "build_context_pack", return_value={"pack_id": "x"}
        ), patch.object(predict, "render_context_prompt", return_value="PACK"), patch.object(predict, "store_context_pack", return_value=7):
            selected, *_ = predict.prepare_prediction_prompt(conn, "SPY", "1m", dt.date(2026, 5, 14), [], None, "pack")
        self.assertEqual(selected, "PACK")

    def test_prediction_validation_coerces_markdown_risk_strings(self):
        forecast = {
            "direction": "up",
            "confidence_0_1": 0.55,
            "expected_return_low": -0.01,
            "expected_return_high": 0.04,
            "rationale_md": "test",
            "key_risks": "- Dollar spike\n- Demand shock",
            "analogues_used": "2021-03-31\n2024-03-31",
        }
        predict._validate(forecast)
        self.assertEqual(forecast["key_risks"], ["Dollar spike", "Demand shock"])
        self.assertEqual(forecast["analogues_used"], ["2021-03-31", "2024-03-31"])

    def test_banking_oil_and_market_bias_labels_are_wired(self):
        conn = self.connect()
        assignments = labels.label_assignments(conn)
        banking = assignments[assignments["label_id"] == "theme:banking"]
        oil = assignments[assignments["label_id"] == "theme:oil_price"]
        risk_off = assignments[assignments["label_id"] == "market_bias:risk-off"]
        self.assertIn("gdelt_streams", set(banking["target_table"]))
        self.assertIn("gdelt_stream_examples", set(oil["target_table"]))
        self.assertIn("oracle_index_snapshots", set(risk_off["target_table"]))
        weighted = labels.weighted_label_catalog(conn, profile_id="default")
        self.assertIn("theme:oil_price", set(weighted["label_id"]))

    def test_registry_classifies_sources_and_skips_inactive(self):
        rows = queries.source_registry()
        self.assertTrue(set(rows["status"]) <= {"active", "needs_review", "inactive"})
        self.assertEqual(SOURCE_REGISTRY["ecb"].status, "active")
        self.assertEqual(SOURCE_REGISTRY["acled"].status, "needs_review")
        self.assertEqual(SOURCE_REGISTRY["senate_trades"].status, "inactive")
        self.assertGreaterEqual(SOURCE_REGISTRY["research_journal"].cadence_hours, 6)
        self.assertTrue(source_enabled("acled"))
        self.assertFalse(source_enabled("senate_trades"))

    def test_label_evaluation_materializes_mature_forward_returns(self):
        conn = self.connect()
        label_evaluation.ensure_schema(conn)
        conn.executemany(
            """
            INSERT INTO historical_state_values (
                as_of, value_key, source_table, source_symbol, category,
                label_ids_json, value, value_date, active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            [
                ("2026-01-01", "spy_banking", "signals", "SPY", "market_stress", '["theme:banking"]', 1.0, "2026-01-01"),
                ("2026-01-02", "spy_banking", "signals", "SPY", "market_stress", '["theme:banking"]', 1.0, "2026-01-02"),
                ("2026-01-01", "oil_impact", "gdelt_streams", None, "energy_commodities", '["asset_impact:oil"]', 1.0, "2026-01-01"),
                ("2026-01-02", "oil_impact", "gdelt_streams", None, "energy_commodities", '["asset_impact:oil"]', 1.0, "2026-01-02"),
                ("2026-01-01", "broad_macro", "signals", "_news", "market_stress", '["theme:macro_news"]', 1.0, "2026-01-01"),
                ("2026-05-10", "spy_banking", "signals", "SPY", "market_stress", '["theme:banking"]', 1.0, "2026-05-10"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO historical_forward_returns (as_of, symbol, horizon_days, start_price, end_price, end_date, forward_return)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("2026-01-01", "SPY", 21, 100.0, 102.0, "2026-02-01", 0.02),
                ("2026-01-02", "SPY", 21, 100.0, 99.0, "2026-02-02", -0.01),
                ("2026-01-01", "CL=F", 21, 100.0, 110.0, "2026-02-01", 0.10),
                ("2026-01-02", "CL=F", 21, 100.0, 112.0, "2026-02-02", 0.12),
                ("2026-05-10", "SPY", 21, 100.0, 105.0, "2999-01-01", 0.05),
            ],
        )
        stats = label_evaluation.generate_and_store(conn, profile_id="default", min_observations=1)
        self.assertGreaterEqual(stats["rows"], 1)
        df = queries.label_evaluation(conn, profile_id="default", asset="SPY", horizon="1m", min_observations=1)
        row = df[df["label_id"] == "theme:banking"].iloc[0]
        self.assertEqual(int(row["observations"]), 2)
        self.assertAlmostEqual(float(row["hit_rate"]), 0.5)
        self.assertAlmostEqual(float(row["avg_forward_return"]), 0.005)
        self.assertEqual(row["methodology_version"], "v2_asset_scoped")
        oil_df = queries.label_evaluation(conn, profile_id="default", asset="CL=F", horizon="1m", min_observations=1)
        self.assertIn("asset_impact:oil", set(oil_df["label_id"]))
        broad_df = queries.label_evaluation(conn, profile_id="default", asset="SPY", horizon="1m", min_observations=1)
        self.assertNotIn("theme:macro_news", set(broad_df["label_id"]))
        self.assertTrue(queries.label_evaluation(conn, profile_id="default", asset="__missing__").empty)


if __name__ == "__main__":
    unittest.main()
