from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from projects.model_monitoring.project import ModelMonitoringProject


@dataclass(frozen=True)
class ProjectDefinition:
    name: str
    factory: Callable[[], object]


PROJECTS = {
    "Model Monitoring": ProjectDefinition(
        name="Model Monitoring",
        factory=ModelMonitoringProject,
    )
}
