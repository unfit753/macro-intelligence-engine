import unittest

from src.core.compliance import PUBLIC_REQUIRED_COPY, public_disclaimer, validate_public_copy


class ComplianceTests(unittest.TestCase):
    def test_public_disclaimer_contains_required_copy(self):
        disclaimer = public_disclaimer()
        for phrase in PUBLIC_REQUIRED_COPY:
            self.assertIn(phrase, disclaimer)

    def test_forbidden_phrases_are_flagged(self):
        warnings = validate_public_copy("You should buy this now for guaranteed return.")
        self.assertTrue(any("you should buy" in w for w in warnings))
        self.assertTrue(any("guaranteed return" in w for w in warnings))

    def test_research_copy_passes_smoke_check(self):
        self.assertEqual(validate_public_copy(public_disclaimer()), [])


if __name__ == "__main__":
    unittest.main()

