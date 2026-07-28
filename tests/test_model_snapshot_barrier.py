from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from projects.ProcessRunAutomation import process_scheduler, service


class ModelSnapshotBarrierTests(unittest.TestCase):
    def test_marker_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "snapshot.json"
            with patch.object(service, "_RUNTIME_ROOT", Path(directory)), patch.object(
                service, "_MODEL_SNAPSHOT_MARKER", marker
            ):
                service.publish_model_snapshot_complete(7, "2026-07-26 09:00:00")
                self.assertEqual(
                    service.load_model_snapshot_marker(),
                    (7, "2026-07-26 09:00:00"),
                )

    def test_scheduler_requires_a_new_marker_for_the_cycle(self) -> None:
        old = (1, "2026-07-26 08:00:00")
        new = (1, "2026-07-26 09:00:00")
        process_scheduler.SHUTDOWN.clear()
        with patch.object(
            process_scheduler, "load_model_snapshot_marker", side_effect=[old, new]
        ), patch.object(process_scheduler.SHUTDOWN, "wait"):
            process_scheduler.wait_for_model_snapshot(1, old)


if __name__ == "__main__":
    unittest.main()
