import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure logs directory exists
LOGS_DIR = Path(os.getenv("LOGS_DIR", "logs"))
LOGS_DIR.mkdir(parents=True, exist_ok=True)

WORKER_LOG_FILE = LOGS_DIR / "worker.log"
AUDIT_LOG_FILE = LOGS_DIR / "audit.log"


def setup_logger(name: str = "workqueue", level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a logger with dual output: console and rotating log file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers if setup is called multiple times
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File Handler (5MB per file, max 3 backups)
        file_handler = RotatingFileHandler(
            WORKER_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


class AuditLogger:
    """
    Persistent audit logger writing structured event records to logs/audit.log.
    Matches and enhances the Go project's logs.txt audit trail.
    """

    def __init__(self, log_path: Path = AUDIT_LOG_FILE):
        self.log_path = log_path
        self._logger = logging.getLogger("workqueue.audit")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False

        if not self._logger.handlers:
            handler = RotatingFileHandler(
                self.log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
            )
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)

    def log_event(
        self,
        event: str,
        job_id: str,
        job_type: str,
        worker_id: Optional[str] = None,
        duration_ms: Optional[float] = None,
        retries_left: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        parts = [
            f"[{timestamp}]",
            f"[{event.upper()}]",
            f"JobID={job_id}",
            f"Type={job_type}",
        ]
        if worker_id:
            parts.append(f"WorkerID={worker_id}")
        if duration_ms is not None:
            parts.append(f"Duration={duration_ms:.1f}ms")
        if retries_left is not None:
            parts.append(f"RetriesLeft={retries_left}")
        if payload is not None:
            parts.append(f"Payload={json.dumps(payload, default=str)}")
        if error:
            parts.append(f"Error={json.dumps(error)}")
        if extra:
            for k, v in extra.items():
                parts.append(f"{k}={v}")

        log_line = " ".join(parts)
        self._logger.info(log_line)


# Global instances
audit_logger = AuditLogger()


def get_recent_audit_logs(limit: int = 50) -> List[str]:
    """
    Reads the last N lines from the audit log file for API inspection.
    """
    if not AUDIT_LOG_FILE.exists():
        return []

    try:
        with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
            return [line.strip() for line in lines[-limit:]]
    except Exception:
        return []
