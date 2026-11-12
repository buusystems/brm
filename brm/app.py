"""The brm App: schema + store + handler registry + dispatcher + FastAPI."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, Iterable, Iterator, Literal

from .hooks import OPS, Dispatcher, Event, Failure, Handler, HandlerFn, Registry
from .schema import Schema
from .store import Store, Tx


def _set(v: str | Iterable[str] | None, default: frozenset[str] | None) -> frozenset[str] | None:
    if v is None:
        return default
    return frozenset((v,) if isinstance(v, str) else v)


class App:
    """Best Relational Model.

    >>> app = App("app.db", schema="schema.sql")
    >>> @app.on("device", watch="descendants", ops="insert", source="reading")
    ... def new_reading(ev): print(ev.row["name"], ev.change.new)
    >>> # uvicorn mymodule:app   (an App is an ASGI app)

    ``schema`` is SQL DDL text or a path to a .sql file. On an empty database it is
    executed; on an existing one it must match what is already there.
    """

    def __init__(
        self,
        db: str = ":memory:",
        schema: str = "",
        *,
        order: Literal["bfs", "dfs"] = "bfs",
        concurrency: int = 1,
        on_error: Callable[[Failure], Any] | None = None,
        title: str = "brm",
    ):
        if order not in ("bfs", "dfs"):
            raise ValueError("order must be 'bfs' or 'dfs'")
        self.dispatcher = Dispatcher(concurrency=concurrency, on_error=on_error)
        self.store = Store(db, schema, Registry, self.dispatcher, order=order)
        self.schema: Schema = self.store.schema
        self.registry: Registry = self.store.registry
        self.title = title
        self._fastapi = None
        self._by_fn: dict[Any, list[Handler]] = {}

    # -- handlers ------------------------------------------------------------

    def on(
        self,
        table: str,
        fn: HandlerFn | None = None,
        *,
        watch: str | Iterable[str] = "self",
        ops: str | Iterable[str] | None = None,
        depth: int | None = None,
        source: str | Iterable[str] | None = None,
        where: Callable[[Event], bool] | None = None,
        retries: int = 0,
        backoff: float = 0.5,
        name: str | None = None,
    ):
        """Register ``fn(event)`` on rows of ``table``.

        watch   "self"        a row of ``table`` was inserted/updated/deleted
                "descendants" a row referencing a ``table`` row (transitively) changed
                "ancestors"   a row a ``table`` row references (transitively) changed
                (any combination, as a list)
        ops     restrict to "insert" / "update" / "delete" (of the changed row)
        depth   max foreign-key hops between the changed row and ``table``'s row
        source  only changes to these tables
        where   predicate on the Event, checked at dispatch time
        retries extra attempts on exception, sleeping backoff * 2**n between them

        Use as ``@app.on(...)`` or ``app.on(table, fn, ...)``; returns ``fn``.
        """

        def register(f: HandlerFn) -> HandlerFn:
            h = Handler(
                fn=f,
                table=table,
                watch=_set(watch, None) or frozenset(("self",)),
                ops=_set(ops, OPS) or OPS,
                depth=depth,
                source=_set(source, None),
                where=where,
                retries=retries,
                backoff=backoff,
                name=name or getattr(f, "__name__", repr(f)),
            )
            self.registry.add(h)
            self._by_fn.setdefault(f, []).append(h)
            return f

        return register if fn is None else register(fn)

    def off(self, fn: HandlerFn) -> None:
        """Unregister every registration of ``fn``."""
        for h in self._by_fn.pop(fn, []):
            self.registry.remove(h)

    @property
    def handlers(self) -> list[Handler]:
        return list(self.registry.handlers)

    def drain(self, timeout: float | None = None) -> bool:
        """Wait until all committed changes have been handled."""
        return self.dispatcher.drain(timeout)

    @property
    def failures(self) -> list[Failure]:
        return list(self.dispatcher.failures)

    # -- data ----------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[Tx]:
        """Group writes; handlers for all of them run after the commit."""
        with self.store.transaction() as tx:
            yield tx

    def insert(self, table: str, values: dict[str, Any] | None = None, **kw: Any) -> dict:
        with self.transaction() as tx:
            return tx.insert(table, {**(values or {}), **kw})

    def update(self, table: str, key: Any, values: dict[str, Any] | None = None, **kw: Any) -> dict:
        with self.transaction() as tx:
            return tx.update(table, key, {**(values or {}), **kw})

    def delete(self, table: str, key: Any) -> dict:
        with self.transaction() as tx:
            return tx.delete(table, key)

    def get(self, table: str, key: Any) -> dict | None:
        with self.store.lock:
            return Tx(self.store).get(table, key)

    def select(self, table: str, where: dict[str, Any] | None = None, **kw: Any) -> list[dict]:
        with self.store.lock:
            return Tx(self.store).select(table, where, **kw)

    def close(self) -> None:
        self.dispatcher.drain(10)
        self.dispatcher.stop()
        self.store.close()

    # -- HTTP ----------------------------------------------------------------

    def router(self):
        """An APIRouter with all generated routes, to mount in your own FastAPI app."""
        from .api import build_router

        return build_router(self)

    @property
    def fastapi(self):
        """A FastAPI app serving the generated routes (built once)."""
        if self._fastapi is None:
            from fastapi import FastAPI

            @asynccontextmanager
            async def lifespan(_):
                self.dispatcher.start()
                yield
                self.dispatcher.drain(10)

            api = FastAPI(title=self.title, lifespan=lifespan)
            api.include_router(self.router())
            self._fastapi = api
        return self._fastapi

    async def __call__(self, scope, receive, send):
        await self.fastapi(scope, receive, send)
