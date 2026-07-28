from __future__ import annotations

import unittest

from projects.model_monitoring.project import sql_targeted_range_filter


class TargetedCealogTests(unittest.TestCase):
    def test_current_range_is_full_and_history_is_boundary_only(self) -> None:
        sql = sql_targeted_range_filter(
            "ID",
            current_ranges=[(100, 200)],
            historical_ranges=[(10, 20), (30, 40)],
        )

        self.assertIn("ID >= 100", sql)
        self.assertIn("ID <= 200", sql)
        self.assertIn("ID >= 10", sql)
        self.assertIn("DESCRIPTION in ('-START-', '-END-')", sql)
        self.assertIn("DESCRIPTION like '% before %'", sql)
        self.assertNotIn("trim(", sql)
        self.assertNotIn("lower(", sql)

    def test_empty_ranges_do_not_scan_the_table(self) -> None:
        self.assertEqual(
            sql_targeted_range_filter(
                "ID", current_ranges=[], historical_ranges=[]
            ),
            "1 = 0",
        )


if __name__ == "__main__":
    unittest.main()
