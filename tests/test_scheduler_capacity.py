from __future__ import annotations

import unittest
from unittest.mock import patch

from projects.ProcessRunAutomation import process_scheduler


class SchedulerCapacityTests(unittest.TestCase):
    def test_wave_size_is_reduced_to_preserve_host_memory(self) -> None:
        # 4 GB total - 2 GB reserve, at 350 MB per browser => 5 browsers.
        with patch.object(process_scheduler.os, "sysconf", side_effect=[4096, 1_048_576]):
            size, total_mb = process_scheduler.memory_aware_wave_size(50)

        self.assertEqual(total_mb, 4096)
        self.assertEqual(size, 5)

    def test_wave_size_never_exceeds_client_count(self) -> None:
        with patch.object(process_scheduler.os, "sysconf", side_effect=OSError):
            size, total_mb = process_scheduler.memory_aware_wave_size(3)

        self.assertIsNone(total_mb)
        self.assertEqual(size, 3)


if __name__ == "__main__":
    unittest.main()
