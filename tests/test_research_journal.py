import sqlite3
import tempfile
import unittest
from pathlib import Path

from config.db_setup import SCHEMA, seed_data_labels, seed_data_objects
from src.core import queries
from src.local_model import research_journal


class MacroEventFlagTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        seed_data_objects(self.conn)
        seed_data_labels(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_macro_event_flags_lifecycle_and_dedup(self):
        self.conn.execute(
            """INSERT INTO calendar_events (
                 date, time_local, region, category, importance, title,
                 expected, event_key, release_family, status
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("2026-05-20", "08:30 ET", "US", "inflation", 5, "US CPI May", "3.1%", "us_cpi", "cpi", "scheduled"),
        )
        self.conn.executemany(
            """INSERT INTO macro_release_actuals (
                 release_key, region, category, title, scheduled_date,
                 scheduled_time_local, importance, actual_value, expected_text,
                 expected_unit, surprise_text, unit, status, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                ("us_cpi", "US", "inflation", "US CPI May", "2026-05-20", "08:30 ET", 5, None, "3.1%", "%", None, "%", "waiting", "2026-05-14T10:00:00Z"),
                ("se_cpif", "SE", "inflation", "Sweden CPIF", "2026-05-13", "08:00 CET", 4, 2.4, "2.2%", "%", "+0.2pp", "%", "released", "2026-05-13T08:30:00Z"),
                ("ecb_rate", "EU", "rates", "ECB Rate Decision", "2026-05-02", "14:15 CET", 5, 3.5, "3.5%", "%", "inline", "%", "released", "2026-05-02T14:20:00Z"),
                ("future_gdp", "US", "gdp", "US GDP Far Future", "2026-06-20", "08:30 ET", 4, None, "2.0%", "%", None, "%", "waiting", "2026-05-14T10:00:00Z"),
            ],
        )
        flags = queries.macro_event_flags(self.conn, as_of="2026-05-14", days_before=14, days_after=14)
        self.assertEqual(flags["event_key"].nunique(), len(flags))
        self.assertNotIn("future_gdp", " ".join(flags["event_key"].astype(str)))
        by_key = {str(row["release_key"]): row for _, row in flags.iterrows()}
        self.assertEqual(by_key["us_cpi"]["flag_phase"], "upcoming")
        self.assertEqual(by_key["se_cpif"]["flag_phase"], "released")
        self.assertEqual(by_key["ecb_rate"]["flag_phase"], "fading")
        self.assertIn("Actual", by_key["se_cpif"]["tooltip"])


class ResearchJournalTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        seed_data_objects(self.conn)
        seed_data_labels(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_high_priority_current_event_creates_journal_note_and_marks_reviewed(self):
        self.conn.execute(
            """INSERT INTO data_change_events (
                 event_key, object_id, source_table, source_id, event_type,
                 priority, labels_json, metadata_json, status, oracle_review_required
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "current:breaking:oil",
                "current.event",
                "current_events",
                "44",
                "upsert",
                4.8,
                '["theme:oil_price", "theme:market_moving_news"]',
                '{"title":"Oil shock headline","summary":"Unexpected supply disruption hit the tape.","region":"Global"}',
                "queued",
                1,
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            journal = Path(tmpdir) / "MacroEngine_journal.md"
            stats = research_journal.generate_and_store(conn=self.conn, journal_path=journal, append_markdown=True)
            self.assertEqual(stats["rows_seen"], 1)
            self.assertEqual(stats["rows_inserted"], 1)
            note = self.conn.execute("SELECT * FROM oracle_review_annotations WHERE review_type='current_event_journal'").fetchone()
            self.assertIsNotNone(note)
            self.assertIn("Oil shock headline", journal.read_text())
            status = self.conn.execute("SELECT status FROM data_change_events").fetchone()[0]
            self.assertEqual(status, "reviewed")
            core_notes = queries.oracle_journal_notes(self.conn, days=30, limit=5)
            self.assertFalse(core_notes.empty)
            self.assertIn("What happened", core_notes.iloc[0]["comment"])

    def test_sparse_gdelt_event_journal_has_why_and_watch(self):
        self.conn.execute(
            """INSERT INTO data_change_events (
                 event_key, object_id, source_table, source_id, event_type,
                 priority, labels_json, metadata_json, status, oracle_review_required
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "current:gdelt:trade:middle-east",
                "current.event",
                "current_events",
                "77",
                "upsert",
                5.0,
                '["asset_impact:gold", "asset_impact:oil", "stream:trade_sanctions_supply", "theme:sanctions", "theme:trade"]',
                '{"stream":"trade_sanctions_supply","canonical_scope":"Middle East","region":"Middle East","article_count":16,"title":"Trade Sanctions Supply pulse: Middle East"}',
                "queued",
                1,
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            journal = Path(tmpdir) / "MacroEngine_journal.md"
            stats = research_journal.generate_and_store(conn=self.conn, journal_path=journal, append_markdown=True)
            self.assertEqual(stats["rows_seen"], 1)
            note = self.conn.execute("SELECT comment FROM oracle_review_annotations WHERE review_type='current_event_journal'").fetchone()[0]
            text = journal.read_text()
            self.assertIn("16 recent trade / sanctions articles clustered in Middle East", note)
            self.assertIn("Why it matters:", note)
            self.assertIn("oil/gold risk premia", note)
            self.assertIn("- What: 16 recent trade / sanctions articles clustered in Middle East", text)
            self.assertIn("- Why: Trade or sanctions news", text)
            self.assertIn("- Watch: Watch follow-up tariff/sanctions headlines", text)
            self.assertNotIn("needs a look", text.lower())

    def test_low_priority_events_are_not_journaled(self):
        self.conn.execute(
            """INSERT INTO data_change_events (
                 event_key, object_id, source_table, source_id, priority,
                 labels_json, metadata_json, status, oracle_review_required
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("current:small", "current.event", "current_events", "1", 2.0, '[]', '{}', "queued", 1),
        )
        stats = research_journal.generate_and_store(conn=self.conn, append_markdown=False)
        self.assertEqual(stats["rows_seen"], 0)
        count = self.conn.execute("SELECT COUNT(*) FROM oracle_review_annotations").fetchone()[0]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
