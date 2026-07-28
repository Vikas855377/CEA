from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from projects.model_monitoring.project import ModelMonitoringProject


class FakeCoordinator:
    def __init__(self, status: str, error_message: str = "") -> None:
        self.status = status
        self.error_message = error_message

    def latest_batch(self) -> dict[str, object]:
        return {
            "BATCH_ID": "batch-123",
            "STATUS": self.status,
            "ERROR_MESSAGE": self.error_message,
        }


class ExplicitRefreshMonitoringTests(unittest.TestCase):
    def project_with_status(self, status: str, error_message: str = "") -> ModelMonitoringProject:
        project = ModelMonitoringProject.__new__(ModelMonitoringProject)
        project.db = object()
        project._refresh_coordinator = FakeCoordinator(status, error_message)
        return project

    def load_and_run(self, status: str, error_message: str = ""):
        project = self.project_with_status(status, error_message)
        with patch.dict(
            os.environ,
            {
                "CEA_REFRESH_COORDINATION_ENABLED": "true",
                "MODEL_MONITORING_USE_CSV": "false",
            },
        ):
            tables = project.load_selected_model_context(limit_per_table=1)
        return project.run(tables=tables)

    def test_in_progress_batch_pauses_monitoring(self) -> None:
        run = self.load_and_run("IN_PROGRESS")
        self.assertEqual(run.result["overall_status"], "DATA_REFRESH_IN_PROGRESS")
        self.assertEqual(run.result["batch_id"], "batch-123")

    def test_failed_batch_is_not_reported_as_in_progress(self) -> None:
        run = self.load_and_run("FAILED", "Client 2 failed")
        self.assertEqual(run.result["overall_status"], "DATA_REFRESH_FAILED")
        self.assertEqual(run.result["error_message"], "Client 2 failed")


if __name__ == "__main__":
    unittest.main()
