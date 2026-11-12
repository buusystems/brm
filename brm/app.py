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
    >>> @app.on("device", via="reading", ops="insert")
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
        via: str | Iterable[str] | None = None,
        ops: str | Iterable[str] | None = None,
        where: Callable[[Event], bool] | None = None,
        max_hops: int | None = None,
        retries: int = 0,
        backoff: float = 0.5,
        name: str | None = None,
    ):
        """Register ``fn(event)`` on rows of ``table``.

        via      where the change comes from, by foreign keys (see brm.routes):
                   None                        the handler's own rows changed
                   "reading"                   a reading anywhere related (any route)
                   "reading.sensor_id"         a reading through exactly this key
                   "reading.sensor_id > sensor.device_id"   exactly this path
                   ["sensor", "reading"]       several routes (one registration each;
                                               a given name= gets "@<route>" appended)
                 Up or down is worked out from the schema; the route is checked now.
        ops      "insert" / "update" / "delete" of the changed row (default all)
        where    predicate on the Event, run in the write's transaction: events it
                 rejects are never queued. Keep it fast and side-effect free.
        max_hops cap on hops walked; for self-references like "node.parent*"
        retries  extra attempts on exception, waiting backoff * 2**n between them
        name     stable id for queued jobs (default "<table>:<function>[@<via>]");
                 jobs survive restarts by name, so rename only after draining

        Use as ``@app.on(...)`` or ``app.on(table, fn, ...)``; returns ``fn``.
        """
        vias = [via] if via is None or isinstance(via, str) else list(via)
        if not vias:
            raise ValueError("via is an empty list")
        routes = [None if v is None else self.registry.route(table, v) for v in vias]

        def register(f: HandlerFn) -> HandlerFn:
            for route in routes:
                _register(f, route, name if len(routes) == 1 or name is None
                          else f"{name}@{route.text}")  # type: ignore[union-attr]
            return f

        def _register(f: HandlerFn, route, name: str | None) -> None:
            default = f"{table}:{getattr(f, '__name__', type(f).__name__)}"
            if route is not None:
                default += f"@{route.text}"
            h = Handler(
                fn=f,
                table=table,
                route=route,
                ops=_set(ops, OPS) or OPS,
                max_hops=max_hops,
                where=where,
                retries=retries,
                backoff=backoff,
                name=name or default,
            )
            self.registry.add(h)
            self._by_fn.setdefault(f, []).append(h)

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
