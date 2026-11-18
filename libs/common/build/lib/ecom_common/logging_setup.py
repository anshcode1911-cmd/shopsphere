"""Structured JSON logging.

Every service logs one JSON object per line to stdout. GKE ships stdout to
Cloud Logging (which understands the `severity` field) and Promtail ships the
same lines to Loki, so both centralized logging back-ends get identical data.
"""
import json
import logging
import os
import sys
import time

SERVICE_NAME = os.getenv("SERVICE_NAME", "unknown-service")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "severity": record.levelname,
            "service": SERVICE_NAME,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str | None = None) -> logging.Logger:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level or os.getenv("LOG_LEVEL", "INFO"))
    # uvicorn access logs are replaced by our own metrics middleware log line
    logging.getLogger("uvicorn.access").disabled = True
    return logging.getLogger(SERVICE_NAME)


def log_event(logger: logging.Logger, message: str, level: int = logging.INFO, **fields) -> None:
    logger.log(level, message, extra={"extra_fields": fields})
