import os
import logging
from contextvars import ContextVar

from temporalio import activity
from temporalio.worker import (
    Interceptor,
    ActivityInboundInterceptor,
    ExecuteActivityInput,
)

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


class CorrelationFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get() or "-"
        return True


class _CorrelationActivityInterceptor(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput):
        wf_id = activity.info().workflow_id
        token = correlation_id_var.set(wf_id)
        try:
            return await super().execute_activity(input)
        finally:
            correlation_id_var.reset(token)


class LoggingInterceptor(Interceptor):
    def intercept_activity(self, next):
        return _CorrelationActivityInterceptor(next)


def setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    fmt = os.environ.get("LOG_FORMAT", "json")

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    handler = logging.StreamHandler()

    if fmt == "json":
        try:
            from pythonjsonlogger.json import JsonFormatter
            formatter = JsonFormatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s",
                rename_fields={
                    "asctime": "timestamp",
                    "levelname": "level",
                    "name": "logger",
                },
            )
        except ImportError:
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s [%(correlation_id)s] %(message)s"
            )
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s [%(correlation_id)s] %(message)s"
        )

    handler.setFormatter(formatter)
    handler.addFilter(CorrelationFilter())
    root.addHandler(handler)
