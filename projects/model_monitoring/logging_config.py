"""Project-local logging for model monitoring."""

from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIR = Path(
    os.getenv(
        "MODEL_MONITORING_LOG_DIR",
        "/tmp/cea_agentic/model_monitoring/logs"
        if os.getenv("VERCEL")
        else str(PROJECT_ROOT / "logs"),
    )
)
LOG_FILE = LOG_DIR / "model_monitoring.log"


def get_logger() -> logging.Logger:
    logger = logging.getLogger("projects.model_monitoring")
    if logger.handlers:
        return logger

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.setLevel(os.getenv("MODEL_MONITORING_LOG_LEVEL", "INFO").upper())
    logger.propagate = False

    handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    return logger
