from __future__ import annotations

import json
import unittest

import pandas as pd

from projects.model_monitoring.model_run_history import ModelRunHistoryStore


class FakeDb:
    def query_df(self, _sql, _params=None):
        return pd.DataFrame(
            [
                {
                    "STEPS": json.dumps(
                        [{"description": "Step A", "duration_seconds": 10}]
                    )
                },
                {
                    "STEPS": json.dumps(
                        [{"description": "Step A", "duration_seconds": 20}]
                    )
                },
            ]
        )


class ModelRunHistoryTests(unittest.TestCase):
    def test_run_id_uses_xactly_run_id_instead_of_etl_observation_time(self) -> None:
        first_observation = {
            "model_run_id": "RUN-123",
            "clientid": "1",
            "model_key": "model",
            "trigger_time": "2026-07-26T10:09:00",
        }
        later_observation = {
            **first_observation,
            "trigger_time": "2026-07-26T10:22:00",
        }
        different_execution = {**first_observation, "model_run_id": "RUN-456"}

        self.assertEqual(
            ModelRunHistoryStore.run_id(first_observation),
            ModelRunHistoryStore.run_id(later_observation),
        )
        self.assertNotEqual(
            ModelRunHistoryStore.run_id(first_observation),
            ModelRunHistoryStore.run_id(different_execution),
        )

    def test_load_runs_adds_average_duration_to_each_step(self) -> None:
        store = ModelRunHistoryStore(db=FakeDb())
        store.ready = True
        rows = store.load_runs(client_id="1", model_key="model")
        self.assertEqual(rows[0]["steps"][0]["average_duration_seconds"], 15)
        self.assertEqual(rows[1]["steps"][0]["average_duration_seconds"], 15)


if __name__ == "__main__":
    unittest.main()
