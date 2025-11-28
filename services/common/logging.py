import json
import logging
import re
from datetime import datetime
from typing import Any, Dict


class PiiMaskingJSONFormatter(logging.Formatter):
    """Format logs as JSON while masking sensitive PII values."""

    CNPJ_RE = re.compile(r"\b(\d{2})\d{10}(\d{2})\b")
    EMAIL_RE = re.compile(r"([\w.+-]+)@([\w-]+)(\.[\w.-]+)")

    def __init__(self, service_name: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.service_name = service_name

    @classmethod
    def _mask_cnpj(cls, value: str) -> str:
        return cls.CNPJ_RE.sub(lambda m: f"{m.group(1)}**********{m.group(2)}", value)

    @classmethod
    def _mask_email(cls, value: str) -> str:
        def _replacer(match: re.Match[str]) -> str:
            local = match.group(1)
            domain = match.group(2) + match.group(3)
            if len(local) <= 2:
                masked_local = "***"
            else:
                masked_local = f"{local[0]}***{local[-1]}"
            return f"{masked_local}@{domain}"

        return cls.EMAIL_RE.sub(_replacer, value)

    @classmethod
    def _mask_value(cls, value: Any) -> Any:
        if isinstance(value, str):
            return cls._mask_email(cls._mask_cnpj(value))
        if isinstance(value, dict):
            return {k: cls._mask_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._mask_value(v) for v in value]
        return value

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting
        message = self._mask_value(record.getMessage())
        log_record: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "service": self.service_name,
            "message": message,
        }

        for key, value in record.__dict__.items():
            if key.startswith("_") or key in {
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            }:
                continue
            log_record[key] = self._mask_value(value)

        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(log_record, ensure_ascii=False)


def configure_structured_logging(service_name: str, level: int = logging.INFO) -> None:
    """Configure root logger with JSON formatter and PII masking."""

    handler = logging.StreamHandler()
    handler.setFormatter(PiiMaskingJSONFormatter(service_name))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)
    logging.captureWarnings(True)
