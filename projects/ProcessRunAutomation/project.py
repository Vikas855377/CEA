"""Programmatic API for the Process Run Automation project."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from projects.ProcessRunAutomation.process_scheduler import load_clients, run_cycle


@dataclass(frozen=True)
class ProcessRunAutomationResult:
    succeeded: bool
    clients: list[dict[str, Any]]


class ProcessRunAutomationProject:
    """Validate configuration or execute one strict-parallel process cycle."""

    def validate(self) -> list[dict[str, Any]]:
        return [asdict(client) for client in load_clients()]

    def run(self) -> ProcessRunAutomationResult:
        results = run_cycle(load_clients(), cycle_number=1)
        return ProcessRunAutomationResult(
            succeeded=all(result.succeeded for result in results),
            clients=[asdict(result) for result in results],
        )
