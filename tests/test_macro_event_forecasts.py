import datetime as dt
import sqlite3
import unittest

from config.db_setup import SCHEMA, add_missing_columns
from src.intelligence import macro_events
from src.core import queries


class MacroEventForecastTests(unittest.TestCase):
    def _seed_schema(self):
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)
        add_missing_columns(conn)
        return conn

    def test_pce_event_does_not_match_cpi_actual(self):
        conn = self._seed_schema()
        conn.execute(
            """INSERT INTO calendar_events (
                date, time_local, region, category, importance, title,
                expected, market_note, source, url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "2026-05-29", "14:30", "US", "inflation", 5,
                "US PCE price index April 2026", None, None, "test", None,
            ),
        )
        conn.executemany(
            """INSERT INTO indicators (date, country, category, indicator_name, value, unit)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [
                ("2026-04-01", "US", "inflation", "BLS CPI-U All Items NSA YoY", 3.0, "%"),
                ("2026-03-01", "US", "inflation", "PCE Price Index", 126.0, "Index"),
                ("2026-02-01", "US", "inflation", "PCE Price Index", 125.7, "Index"),
            ],
        )
        rows = macro_events.build_predictions(conn, as_of=dt.date(2026, 5, 14), days_forward=45)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["result_status"], "awaiting_actual")
        self.assertEqual(rows[0]["historical_pattern"]["family"], "pce_inflation")
        self.assertTrue(rows[0]["forecast_summary"])

    def test_store_and_core_read_exposes_forecast_fields(self):
        conn = self._seed_schema()
        conn.execute(
            """INSERT INTO calendar_events (
                date, time_local, region, category, importance, title,
                expected, market_note, source, url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                (dt.date.today() + dt.timedelta(days=7)).isoformat(), "14:30", "US", "inflation", 5,
                "US CPI May 2026", None, None, "test", None,
            ),
        )
        conn.executemany(
            """INSERT INTO indicators (date, country, category, indicator_name, value, unit)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [
                ((dt.date.today() - dt.timedelta(days=10)).isoformat(), "US", "inflation", "BLS CPI-U All Items NSA YoY", 3.0, "%"),
                ((dt.date.today() - dt.timedelta(days=40)).isoformat(), "US", "inflation", "BLS CPI-U All Items NSA YoY", 2.7, "%"),
            ],
        )
        rows = macro_events.build_predictions(conn, as_of=dt.date.today(), days_forward=45)
        self.assertEqual(macro_events.store_predictions(conn, rows), 1)
        df = queries.macro_event_predictions(conn, days_forward=90)
        self.assertIn("forecast_direction", df.columns)
        self.assertIn("historical_pattern_json", df.columns)
        self.assertEqual(df.iloc[0]["forecast_direction"], "hotter")


if __name__ == "__main__":
    unittest.main()
