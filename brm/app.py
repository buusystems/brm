"""The brm App: schema + store + handler registry + dispatcher + FastAPI."""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import Any, Callable, Iterable, Iterator, Literal

from .hooks import OPS, Event, Handler, HandlerFn, Registry
from .outbox import Dispatcher, Failure, Outbox
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

    Handler jobs go through a durable outbox (``_brm_outbox``) in the same database,
    so they survive crashes and are delivered at least once. ``dispatch=False`` makes
    a process that only writes, and leaves running handlers to other processes on the
    same database file. ``lease`` is how long a claimed job may run before another
    dispatcher may take it over; a job taken over more than ``max_reclaims`` times
    (its runs keep dying) is marked dead. Crashes do not use up ``retries``.
    """

    def __init__(
        self,
        db: str = ":memory:",
        schema: str = "",
        *,
        order: Literal["bfs", "dfs"] = "bfs",
        concurrency: int = 1,
        on_error: Callable[[Failure], Any] | None = None,
        dispatch: bool = True,
        poll: float = 0.25,
        lease: float = 60.0,
        max_reclaims: int = 3,
        title: str = "brm",
    ):
        if order not in ("bfs", "dfs"):
            raise ValueError("order must be 'bfs' or 'dfs'")
        self.store = Store(db, schema, Registry, order=order)
        self.schema: Schema = self.store.schema
        self.registry: Registry = self.store.registry
        self.outbox = Outbox(self.store)
        self.store.outbox = self.outbox
        self.dispatcher = Dispatcher(self.outbox, self.registry, concurrency=concurrency,
                                     on_error=on_error, poll=poll, lease=lease,
                                     max_reclaims=max_reclaims)
        self.store.on_commit = self.dispatcher.wake
        if dispatch:
            self.dispatcher.start()
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
        retries extra attempts on exception, waiting backoff * 2**n between them
        name    stable id for queued jobs (default "<table>:<function name>"); jobs
                survive restarts by name, so rename a handler only after draining it

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
                name=name or f"{table}:{getattr(f, '__name__', type(f).__name__)}",
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
        """Wait until every queued job for this process's handlers has run (or died)."""
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
        if self.dispatcher.running:
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
                yield
                if self.dispatcher.running:
                    self.dispatcher.drain(10)

            api = FastAPI(title=self.title, lifespan=lifespan)
            api.include_router(self.router())
            self._fastapi = api
        return self._fastapi

    async def __call__(self, scope, receive, send):
        await self.fastapi(scope, receive, send)
