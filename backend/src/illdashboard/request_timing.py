"""Request-local Server-Timing metrics, without recording SQL or request data."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from time import perf_counter

from sqlalchemy import event
from sqlalchemy.engine import Engine, ExceptionContext, ExecutionContext
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


@dataclass
class RequestTiming:
    sql_seconds: float = 0.0
    sql_statements: int = 0
    statement_starts: dict[ExecutionContext, float] = field(default_factory=dict)


_current_timing: ContextVar[RequestTiming | None] = ContextVar("request_timing", default=None)


def _before_cursor_execute(_connection, _cursor, _statement, _parameters, context, _executemany) -> None:
    timing = _current_timing.get()
    if timing is not None:
        timing.statement_starts[context] = perf_counter()
        timing.sql_statements += 1


def _finish_statement(context: ExecutionContext | None) -> None:
    timing = _current_timing.get()
    if timing is not None and context is not None:
        started = timing.statement_starts.pop(context, None)
        if started is not None:
            timing.sql_seconds += perf_counter() - started


def _after_cursor_execute(_connection, _cursor, _statement, _parameters, context, _executemany) -> None:
    _finish_statement(context)


def _handle_query_error(context: ExceptionContext) -> None:
    _finish_statement(context.execution_context)


def instrument_database_engine(engine: Engine) -> None:
    event.listen(engine, "before_cursor_execute", _before_cursor_execute)
    event.listen(engine, "after_cursor_execute", _after_cursor_execute)
    event.listen(engine, "handle_error", _handle_query_error)


class ServerTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        timing = RequestTiming()
        token = _current_timing.set(timing)
        started = perf_counter()

        async def send_with_timing(message: Message) -> None:
            if message["type"] == "http.response.start":
                elapsed_ms = (perf_counter() - started) * 1000
                headers = MutableHeaders(scope=message)
                headers.append(
                    "Server-Timing",
                    f'app;dur={elapsed_ms:.2f};desc="Backend to response headers", '
                    f'db;dur={timing.sql_seconds * 1000:.2f};desc="SQL execution ({timing.sql_statements} statements)"',
                )
            await send(message)

        try:
            await self.app(scope, receive, send_with_timing)
        finally:
            _current_timing.reset(token)
