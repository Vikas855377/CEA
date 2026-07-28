__all__ = ["ProcessRunAutomationProject", "ProcessRunAutomationResult"]


def __getattr__(name: str):
    if name in __all__:
        from projects.ProcessRunAutomation.project import (
            ProcessRunAutomationProject,
            ProcessRunAutomationResult,
        )

        return {
            "ProcessRunAutomationProject": ProcessRunAutomationProject,
            "ProcessRunAutomationResult": ProcessRunAutomationResult,
        }[name]
    raise AttributeError(name)
