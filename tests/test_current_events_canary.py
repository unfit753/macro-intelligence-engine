import datetime as dt
import sqlite3
import unittest
from unittest.mock import patch

from config.db_setup import SCHEMA, seed_data_labels, seed_data_objects
from src.core import queries
from src.intelligence import current_events_canary


class CurrentEventsCanaryTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        seed_data_objects(self.conn)
        seed_data_labels(self.conn)
        current_events_canary.ensure_schema(self.conn)
        self.now = dt.datetime(2026, 5, 14, 12, 0, tzinfo=dt.UTC)

    def tearDown(self):
        self.conn.close()

    def test_scheduled_and_released_events_do_not_let_stale_cpi_dominate(self):
        self.conn.executemany(
            """INSERT INTO macro_release_actuals (
                 release_key, region, category, title, scheduled_date,
                 scheduled_time_local, importance, actual_value, expected_text,
                 status, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                ("us_cpi:old", "US", "inflation", "US CPI April", "2026-05-13", "08:30 ET", 5, 3.1, "old CPI", "released", "2026-05-13T10:00:00Z"),
                ("us_gdp:next", "US", "gdp", "US GDP now", "2026-05-15", "08:30 ET", 4, None, "GDP release", "waiting", "2026-05-14T10:00:00Z"),
            ],
        )
        current_events_canary.generate_and_store(self.conn, now=self.now, fetch_incremental_gdelt=False)
        rows = self.conn.execute("SELECT event_type, title, priority FROM current_events ORDER BY priority DESC, event_time").fetchall()
        self.assertEqual({row["event_type"] for row in rows}, {"scheduled_catalyst", "released_actual"})
        scheduled = [row for row in rows if row["event_type"] == "scheduled_catalyst"][0]
        released = [row for row in rows if row["event_type"] == "released_actual"][0]
        self.assertIn("GDP", scheduled["title"])
        self.assertGreater(float(scheduled["priority"]), float(released["priority"]))

        core_rows = queries.current_events(
            self.conn,
            hours_back=72,
            days_forward=14,
            include_expired=True,
            now=self.now,
        )
        self.assertFalse(core_rows.empty)
        self.assertIn("scheduled_catalyst", set(core_rows["event_type"]))

    def test_high_impact_rss_news_creates_current_event_and_review_queue(self):
        self.conn.execute(
            """INSERT INTO news_items (published_at, source, title, summary, url, region, category)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "2026-05-14T11:45:00Z",
                "Reuters",
                "Trump and Xi set emergency summit as tariff risk rises",
                "Markets watch for China trade shock.",
                "https://example.test/trump-xi-summit",
                "Global",
                "trade",
            ),
        )
        current_events_canary.generate_and_store(self.conn, now=self.now, fetch_incremental_gdelt=False)
        event = self.conn.execute("SELECT * FROM current_events WHERE event_type = 'breaking_news'").fetchone()
        self.assertIsNotNone(event)
        self.assertGreaterEqual(float(event["priority"]), 4.6)
        self.assertEqual(int(event["oracle_review_required"]), 1)
        self.assertIn("Matched market-moving terms", event["why_text"])
        self.assertIn("theme:market_moving_news", event["labels_json"])
        queued = self.conn.execute("SELECT * FROM data_change_events WHERE object_id = 'current.event'").fetchall()
        self.assertEqual(len(queued), 1)

    def test_low_impact_news_stays_out_of_current_events(self):
        self.conn.execute(
            """INSERT INTO news_items (published_at, source, title, summary, url, region, category)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "2026-05-14T11:45:00Z", "Local", "Company opens small office", "No macro content.",
                "https://example.test/small-office", "US", "general",
            ),
        )
        current_events_canary.generate_and_store(self.conn, now=self.now, fetch_incremental_gdelt=False)
        count = self.conn.execute("SELECT COUNT(*) FROM current_events WHERE event_type = 'breaking_news'").fetchone()[0]
        self.assertEqual(count, 0)

    def test_high_impact_gdelt_stream_creates_breaking_event(self):
        self.conn.execute(
            """INSERT INTO gdelt_streams (
                 date, stream, region, country, article_count, total_articles,
                 article_share, baseline_30d, z_score, severity,
                 societal_impact_score, labels_json, top_theme_codes_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "2026-05-14", "conflict_security", "Asia", "TW", 44, 1000,
                0.044, 5.0, 3.5, 4.7, 4.8,
                '["stream:conflict_security"]', '["ARMEDCONFLICT", "TAX_FNCACT_PRESIDENT"]',
            ),
        )
        self.conn.execute(
            """INSERT INTO gdelt_stream_examples (
                 date, stream, region, country, example_rank, title, url, labels_json
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "2026-05-14", "conflict_security", "Asia", "TW", 1,
                "GDELT conflict signal near Taiwan", "https://example.test/taiwan", '["stream:conflict_security"]',
            ),
        )
        current_events_canary.generate_and_store(self.conn, now=self.now, fetch_incremental_gdelt=False)
        event = self.conn.execute("SELECT * FROM current_events WHERE source_table = 'gdelt_streams'").fetchone()
        self.assertIsNotNone(event)
        self.assertNotIn("TAX_FNCACT", event["summary"])
        self.assertNotIn("themes:", event["summary"].lower())
        self.assertIn("conflict", event["why_text"].lower())
        self.assertIn("theme:market_moving_news", event["labels_json"])
        self.assertEqual(int(event["oracle_review_required"]), 1)

    def test_incremental_gdelt_files_update_stream_buckets_once(self):
        sample_streams = {
            ("energy_commodities", "Global", ""): {
                "count": 32,
                "themes": {"ECON_OILPRICE": 32, "ENERGY": 16},
                "examples": [{
                    "title": "Oil shock headline",
                    "url": "https://example.test/oil",
                    "source_domain": "example.test",
                    "location_name": "Global",
                    "theme_codes": ["ECON_OILPRICE", "ENERGY"],
                    "tone": -2.0,
                }],
            }
        }
        with patch.object(current_events_canary, "_recent_gkg_stamps", return_value=[("20260514110000", "https://example.test/gkg.zip")]),              patch.object(current_events_canary.gdelt, "fetch_file_counts", return_value=(64, {}, {}, sample_streams, None)) as fetcher:
            current_events_canary.generate_and_store(self.conn, now=self.now, fetch_incremental_gdelt=True, recent_gdelt_hours=1)
            current_events_canary.generate_and_store(self.conn, now=self.now, fetch_incremental_gdelt=True, recent_gdelt_hours=1)

        fetcher.assert_called_once()
        stream = self.conn.execute(
            """SELECT article_count, total_articles, labels_json, source
               FROM gdelt_streams
               WHERE date = '2026-05-14' AND stream = 'energy_commodities'
                 AND region = 'Global' AND country = ''"""
        ).fetchone()
        self.assertIsNotNone(stream)
        self.assertEqual(int(stream["article_count"]), 32)
        self.assertEqual(int(stream["total_articles"]), 64)
        self.assertIn("theme:market_moving_news", stream["labels_json"])
        example_count = self.conn.execute("SELECT COUNT(*) FROM gdelt_stream_examples WHERE source = 'gdelt_canary'").fetchone()[0]
        self.assertEqual(example_count, 1)
        event = self.conn.execute("SELECT * FROM current_events WHERE source_table = 'gdelt_canary_files'").fetchone()
        self.assertIsNotNone(event)
        self.assertNotIn("ECON_OILPRICE", event["summary"])
        self.assertIn("oil", event["why_text"].lower())


    def test_existing_gdelt_streams_are_capped_and_low_signal_rows_expire(self):
        for idx in range(50):
            self.conn.execute(
                """INSERT INTO gdelt_streams (
                     date, stream, region, country, article_count, total_articles,
                     article_share, z_score, severity, societal_impact_score, labels_json, top_theme_codes_json
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    "2026-05-14", "energy_commodities", "Global", f"C{idx}", 30 + idx, 2000,
                    0.02, 3.1, 4.3, 4.4,
                    '["stream:energy_commodities", "theme:market_moving_news", "asset_impact:oil"]',
                    '["ECON_OILPRICE"]',
                ),
            )
        self.conn.execute(
            """INSERT INTO current_events (
                 event_key, event_type, title, event_time, priority, status, object_id, source_table, oracle_review_required
               ) VALUES (?, 'breaking_news', 'old weak row', ?, 3.8, 'active', 'gdelt.stream', 'gdelt_streams', 1)""",
            ("current:gdelt:old", "2026-05-14T11:00:00Z"),
        )
        self.conn.execute(
            """INSERT INTO data_change_events (
                 event_key, object_id, source_table, status, oracle_review_required
               ) VALUES (?, 'current.event', 'current_events', 'queued', 1)""",
            ("current.event:current:gdelt:old",),
        )
        current_events_canary.generate_and_store(self.conn, now=self.now, fetch_incremental_gdelt=False)
        active_gdelt = self.conn.execute(
            """SELECT COUNT(*) FROM current_events
               WHERE status = 'active' AND event_type = 'breaking_news' AND source_table = 'gdelt_streams'"""
        ).fetchone()[0]
        self.assertEqual(active_gdelt, current_events_canary.GDELT_EVENT_PER_STREAM_CAP)
        old_status = self.conn.execute("SELECT status FROM current_events WHERE event_key = 'current:gdelt:old'").fetchone()[0]
        self.assertEqual(old_status, "expired")
        queue_status = self.conn.execute("SELECT status FROM data_change_events WHERE event_key = 'current.event:current:gdelt:old'").fetchone()[0]
        self.assertEqual(queue_status, "superseded")



if __name__ == "__main__":
    unittest.main()
