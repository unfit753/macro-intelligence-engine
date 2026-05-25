import json
import unittest

from src.cli import explorer


class ExplorerTests(unittest.TestCase):
    def test_demo_snapshot_has_expected_views(self):
        snapshot = explorer.load_demo_snapshot()
        for key in [
            "source_health",
            "current_events",
            "market_tape",
            "next_macro_catalysts",
            "data_catalog",
        ]:
            self.assertIn(key, snapshot)
            self.assertTrue(snapshot[key])

    def test_demo_snapshot_is_synthetic_and_public_safe(self):
        text = json.dumps(explorer.load_demo_snapshot(), sort_keys=True).lower()
        forbidden = [
            "/" + "mnt" + "/",
            "/" + "home" + "/",
            "pass" + "word",
            "api" + "_key",
            "bear" + "er",
            "private" + " key",
        ]
        for value in forbidden:
            self.assertNotIn(value, text)
        self.assertIn("synthetic", text)

    def test_render_demo_market_tape(self):
        source = explorer.DataSource(demo=True)
        rendered, rows, detail_fields = explorer.render_view(source, "market-tape", 3)
        self.assertIn("Market Tape", rendered)
        self.assertIn("Gold", rendered)
        self.assertEqual(len(rows), 3)
        self.assertTrue(detail_fields)

    def test_overview_uses_all_demo_sections(self):
        source = explorer.DataSource(demo=True)
        rendered, rows, _ = explorer.render_view(source, "overview", 4)
        self.assertIn("Source health", rendered)
        self.assertIn("Current events", rendered)
        self.assertIn("Market tape", rendered)
        self.assertIn("Catalysts", rendered)
        self.assertEqual(len(rows), 4)

    def test_json_output_is_research_only(self):
        source = explorer.DataSource(demo=True)
        payload = explorer.render_json(source, "current-events", 2)
        self.assertEqual(payload["positioning"], "research_only")
        self.assertTrue(payload["demo"])
        self.assertEqual(len(payload["rows"]), 2)
        self.assertIn("Not personalized investment advice", payload["disclaimer"])


if __name__ == "__main__":
    unittest.main()
