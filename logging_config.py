# logging_config.py
import json
import logging
import logging.handlers
import os
import platform
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psutil

# Get the project root directory
project_root = Path(__file__).parent.parent.parent

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "json"
LOG_DIR = os.getenv("LOG_DIR", str(project_root / "logs"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "recruitment")

# Ensure log directory exists
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except OSError as e:
    # If we can't create the directory, fall back to console-only logging
    LOG_DIR = None
    print(f"Warning: Could not create log directory: {e}. Falling back to console-only logging.")


class SensitiveDataFilter(logging.Filter):
    """Filter to remove sensitive data from log records."""

    def __init__(self, sensitive_patterns=None):
        super().__init__()
        self.sensitive_patterns = sensitive_patterns or [
            "password",
            "token",
            "api_key",
            "secret",
        ]

    def filter(self, record):
        if isinstance(record.msg, str):
            for pattern in self.sensitive_patterns:
                # Simple pattern replacement, could be more sophisticated
                record.msg = record.msg.replace(pattern, "****REDACTED****")
        return True


class StructuredLogFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def __init__(self, service_name: str):
        """Initialize the formatter.

        Args:
            service_name: Name of the service for log context
        """
        super().__init__()
        self.service_name = service_name
        self.hostname = socket.gethostname()
        self.platform = platform.platform()

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as JSON.

        Args:
            record: Log record to format

        Returns:
            str: JSON formatted log entry
        """
        # Base log entry
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "service": self.service_name,
            "hostname": self.hostname,
            "platform": self.platform,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process_id": record.process,
            "thread_id": record.thread,
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        # Add extra fields if present
        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        return json.dumps(log_entry)


class MetricsLogger:
    """Logger for system metrics."""

    def __init__(self, logger: logging.Logger):
        """Initialize the metrics logger.

        Args:
            logger: Logger instance to use
        """
        self.logger = logger

    def log_system_metrics(self) -> None:
        """Log current system metrics."""
        try:
            metrics = {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_usage_percent": psutil.disk_usage("/").percent,
                "network_io": dict(psutil.net_io_counters()._asdict()),
                "process_count": len(psutil.pids()),
            }

            self.logger.info("System metrics", extra={"metrics": metrics})
        except Exception as e:
            self.logger.error(f"Failed to log system metrics: {e!s}")

    def log_process_metrics(self, pid: Optional[int] = None) -> None:
        """Log metrics for a specific process.

        Args:
            pid: Process ID to log metrics for. If None, logs for current process.
        """
        try:
            process = psutil.Process(pid) if pid else psutil.Process()
            metrics = {
                "cpu_percent": process.cpu_percent(),
                "memory_percent": process.memory_percent(),
                "memory_info": dict(process.memory_info()._asdict()),
                "num_threads": process.num_threads(),
                "num_connections": len(process.connections()),
                "num_open_files": len(process.open_files()),
            }

            self.logger.info("Process metrics", extra={"metrics": metrics})
        except Exception as e:
            self.logger.error(f"Failed to log process metrics: {e!s}")


def setup_logging(name: str) -> logging.Logger:
    """Set up logging for a module.

    Args:
        name: Name of the module

    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # Remove existing handlers
    logger.handlers = []

    # Create handlers
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(StructuredLogFormatter(SERVICE_NAME))
    handlers.append(console_handler)

    # File handler (only if LOG_DIR is available)
    if LOG_DIR:
        try:
            log_file = os.path.join(LOG_DIR, f"{SERVICE_NAME}.log")
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
            )
            file_handler.setFormatter(StructuredLogFormatter(SERVICE_NAME))
            handlers.append(file_handler)
        except Exception as e:
            logger.warning(f"Could not set up file logging: {e}")

    # Add handlers to logger
    for handler in handlers:
        logger.addHandler(handler)

    # Create metrics logger
    metrics_logger = MetricsLogger(logger)

    # Log initial system state
    logger.info(
        "Logging initialized",
        extra={
            "log_level": LOG_LEVEL,
            "log_format": LOG_FORMAT,
            "log_dir": LOG_DIR,
            "service_name": SERVICE_NAME,
        },
    )

    # Log system metrics
    metrics_logger.log_system_metrics()

    return logger


def get_metrics_logger(name: str) -> MetricsLogger:
    """Get a metrics logger for a module.

    Args:
        name: Name of the module

    Returns:
        MetricsLogger: Configured metrics logger instance
    """
    logger = setup_logging(name)
    return MetricsLogger(logger)


# Utility functions for logging structured data
def log_structured(logger, level, message, data=None, **kwargs):
    """Log a message with structured data."""
    if data is not None:
        if isinstance(data, (dict, list)):
            try:
                message = f"{message} {json.dumps(data, default=str)}"
            except (TypeError, ValueError):
                message = f"{message} {data!s}"
        else:
            message = f"{message} {data}"

    # Add any additional kwargs to the message
    if kwargs:
        message = f"{message} {json.dumps(kwargs, default=str)}"

    if level == "debug":
        logger.debug(message)
    elif level == "info":
        logger.info(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
    elif level == "critical":
        logger.critical(message)


# Create a default logger for the application
app_logger = setup_logging("app")
