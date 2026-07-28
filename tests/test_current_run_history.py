from __future__ import annotations

import unittest

import pandas as pd

from app import _current_run_logs


class CurrentRunHistoryTests(unittest.TestCase):
    def test_historical_rows_are_excluded_from_current_run(self) -> None:
        logs = pd.DataFrame(
            [
                {"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 10, "IST_TILL_MINUTES": "2026-03-07 02:44", "DESCRIPTION": "-START-"},
                {"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 11, "IST_TILL_MINUTES": "2026-03-07 02:45", "DESCRIPTION": "old detail"},
                {"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 12, "IST_TILL_MINUTES": "2026-03-07 02:46", "DESCRIPTION": "-END-"},
                {"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 100, "IST_TILL_MINUTES": "2026-07-26 08:48", "DESCRIPTION": "-START-"},
                {"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 101, "IST_TILL_MINUTES": "2026-07-26 08:49", "DESCRIPTION": "current detail"},
                {"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 102, "IST_TILL_MINUTES": "2026-07-26 08:50", "DESCRIPTION": "-END-"},
                {"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 103, "IST_TILL_MINUTES": "2026-07-26 08:51", "DESCRIPTION": "next run data"},
            ]
        )

        result = _current_run_logs(
            logs,
            client_id="1",
            client_name="Test",
            trigger_time="2026-07-26 08:48",
        )

        self.assertEqual(result["ID"].tolist(), [100, 101, 102])
        self.assertNotIn("old detail", result["DESCRIPTION"].tolist())
        self.assertNotIn("next run data", result["DESCRIPTION"].tolist())

    def test_missing_current_start_does_not_fall_back_to_historical_rows(self) -> None:
        logs = pd.DataFrame(
            [{"CLIENTID": 1, "CLIENTNAME": "Test", "ID": 10, "IST_TILL_MINUTES": "2026-03-07 02:44", "DESCRIPTION": "-START-"}]
        )
        result = _current_run_logs(
            logs,
            client_id="1",
            client_name="Test",
            trigger_time="2026-07-26 08:48",
        )
        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
