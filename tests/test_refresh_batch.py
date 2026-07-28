from __future__ import annotations

import logging
import unittest
from dataclasses import dataclass

from projects.ProcessRunAutomation.refresh_batch import RefreshBatchCoordinator


@dataclass(frozen=True)
class Client:
    name: str
    monitoring_client_id: int
    monitoring_client_name: str


class RecordingCoordinator(RefreshBatchCoordinator):
    def __init__(self) -> None:
        self.logger = logging.getLogger("test-refresh-batch")
        self.timeout_seconds = 1
        self.poll_seconds = 0.001
        self.statements: list[str] = []
        self.latest: dict[str, object] | None = None
        self.status_snapshots: list[dict[int, str]] = []
        self.finished: list[tuple[str, str, str]] = []
        self.stale = False

    def _execute(self, sql: str) -> None:
        self.statements.append(sql)

    def latest_batch(self) -> dict[str, object] | None:
        return self.latest

    def client_statuses(self, batch_id: str) -> dict[int, str]:
        if self.status_snapshots:
            return self.status_snapshots.pop(0)
        return {}

    def _finish_batch(self, batch_id: str, status: str, error_message: str = "") -> None:
        self.finished.append((batch_id, status, error_message))

    def _is_stale(self, batch: dict[str, object]) -> bool:
        return self.stale


class RefreshBatchCoordinatorTests(unittest.TestCase):
    def test_prepare_batch_creates_batch_and_pending_client_rows(self) -> None:
        coordinator = RecordingCoordinator()
        clients = [Client("one", 1, "Client One"), Client("two", 2, "Client Two")]

        batch_id = coordinator.prepare_batch(clients)

        self.assertIsNotNone(batch_id)
        self.assertEqual(len(coordinator.statements), 2)
        self.assertIn("'IN_PROGRESS', 2", coordinator.statements[0])
        self.assertIn("1, 'Client One', 'PENDING'", coordinator.statements[1])
        self.assertIn("2, 'Client Two', 'PENDING'", coordinator.statements[1])

    def test_wait_completes_only_when_every_expected_client_completed(self) -> None:
        coordinator = RecordingCoordinator()
        coordinator.status_snapshots = [
            {1: "COMPLETED", 2: "IN_PROGRESS"},
            {1: "COMPLETED", 2: "COMPLETED"},
        ]

        result = coordinator.wait_for_batch("batch-1", {1, 2})

        self.assertEqual(result.status, "COMPLETED")
        self.assertEqual(coordinator.finished, [("batch-1", "COMPLETED", "")])

    def test_wait_fails_batch_when_any_client_failed(self) -> None:
        coordinator = RecordingCoordinator()
        coordinator.status_snapshots = [{1: "COMPLETED", 2: "FAILED"}]

        result = coordinator.wait_for_batch("batch-2", {1, 2})

        self.assertEqual(result.status, "FAILED")
        self.assertEqual(coordinator.finished[0][1], "FAILED")

    def test_prepare_reconciles_completed_orphan_and_starts_new_batch(self) -> None:
        coordinator = RecordingCoordinator()
        coordinator.latest = {
            "BATCH_ID": "orphaned-batch",
            "STATUS": "IN_PROGRESS",
            "EXPECTED_CLIENTS": 2,
        }
        coordinator.status_snapshots = [{1: "COMPLETED", 2: "COMPLETED"}]

        batch_id = coordinator.prepare_batch(
            [Client("one", 1, "Client One"), Client("two", 2, "Client Two")]
        )

        self.assertIsNotNone(batch_id)
        self.assertEqual(coordinator.finished, [("orphaned-batch", "COMPLETED", "")])
        self.assertEqual(len(coordinator.statements), 2)

    def test_prepare_reconciles_failed_orphan_and_starts_new_batch(self) -> None:
        coordinator = RecordingCoordinator()
        coordinator.latest = {
            "BATCH_ID": "failed-batch",
            "STATUS": "IN_PROGRESS",
            "EXPECTED_CLIENTS": 2,
        }
        coordinator.status_snapshots = [{1: "COMPLETED", 2: "FAILED"}]

        batch_id = coordinator.prepare_batch(
            [Client("one", 1, "Client One"), Client("two", 2, "Client Two")]
        )

        self.assertIsNotNone(batch_id)
        self.assertEqual(coordinator.finished[0][0:2], ("failed-batch", "FAILED"))

    def test_prepare_fails_empty_orphan_and_starts_new_batch(self) -> None:
        coordinator = RecordingCoordinator()
        coordinator.latest = {
            "BATCH_ID": "empty-batch",
            "STATUS": "IN_PROGRESS",
            "EXPECTED_CLIENTS": 4,
        }
        coordinator.status_snapshots = [{}]

        batch_id = coordinator.prepare_batch([Client("one", 1, "Client One")])

        self.assertIsNotNone(batch_id)
        self.assertEqual(coordinator.finished[0][0:2], ("empty-batch", "FAILED"))
        self.assertIn("no client rows", coordinator.finished[0][2])
        self.assertEqual(len(coordinator.statements), 2)

    def test_prepare_keeps_genuinely_active_batch(self) -> None:
        coordinator = RecordingCoordinator()
        coordinator.latest = {
            "BATCH_ID": "active-batch",
            "STATUS": "IN_PROGRESS",
            "EXPECTED_CLIENTS": 2,
        }
        coordinator.status_snapshots = [{1: "COMPLETED", 2: "IN_PROGRESS"}]

        batch_id = coordinator.prepare_batch(
            [Client("one", 1, "Client One"), Client("two", 2, "Client Two")]
        )

        self.assertIsNone(batch_id)
        self.assertEqual(coordinator.finished, [])
        self.assertEqual(coordinator.statements, [])


if __name__ == "__main__":
    unittest.main()
