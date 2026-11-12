"""Change events, handler registration, the foreign-key walk that decides which
handlers a change reaches, and the background dispatcher that runs them."""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable, Literal

from .schema import ForeignKey, Schema

log = logging.getLogger("brm")

Op = Literal["insert", "update", "delete"]
Watch = Literal["self", "descendants", "ancestors"]
OPS: frozenset[str] = frozenset(("insert", "update", "delete"))
WATCHES: frozenset[str] = frozenset(("self", "descendants", "ancestors"))


@dataclass(frozen=True)
class Change:
    """One committed row change. ``old`` is None for inserts, ``new`` for deletes."""

    table: str
    op: Op
    key: tuple
    old: dict | None
    new: dict | None

    @property
    def row(self) -> dict:
        return self.new if self.new is not None else self.old  # type: ignore[return-value]

    @property
    def changed(self) -> set[str]:
        """Columns whose value differs between old and new (all of them for insert/delete)."""
        if self.old is None or self.new is None:
            return set(self.row)
        return {k for k in self.new if self.old.get(k) != self.new.get(k)}

    def to_dict(self) -> dict:
        return {"table": self.table, "op": self.op, "key": list(self.key), "old": self.old, "new": self.new}


@dataclass(frozen=True)
class Hop:
    table: str
    key: tuple
    via: str | None  # foreign key used to arrive here (None at the origin)


@dataclass(frozen=True)
class Event:
    """What a handler receives: the row it is registered on, and the change that reached it.

    relation is "self" when ``row`` is the changed row, "descendants" when the change
    happened in a row that (transitively) references ``row``, and "ancestors" when it
    happened in a row that ``row`` (transitively) references.
    """

    change: Change
    table: str
    row: dict
    relation: Watch
    depth: int
    path: tuple[Hop, ...]

    @property
    def key(self) -> tuple:
        return self.path[-1].key

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "key": list(self.key),
            "row": self.row,
            "relation": self.relation,
            "depth": self.depth,
            "path": [{"table": h.table, "key": list(h.key), "via": h.via} for h in self.path],
            "change": self.change.to_dict(),
        }


HandlerFn = Callable[[Event], Any] | Callable[[Event], Awaitable[Any]]


@dataclass(eq=False)
class Handler:
    fn: HandlerFn
    table: str
    watch: frozenset[str]
    ops: frozenset[str] = OPS
    depth: int | None = None  # max hops from the changed row; None = unlimited
    source: frozenset[str] | None = None  # only changes originating in these tables
    where: Callable[[Event], bool] | None = None
    retries: int = 0
    backoff: float = 0.5  # seconds, doubled per retry
    name: str = ""

    def accepts(self, change: Change, depth: int) -> bool:
        return (
            change.op in self.ops
            and (self.source is None or change.table in self.source)
            and (self.depth is None or depth <= self.depth)
        )


class Registry:
    def __init__(self, schema: Schema):
        self.schema = schema
        self.handlers: list[Handler] = []
        self._by: dict[tuple[str, str], list[Handler]] = {}
        # shortest hop counts between tables, walking up (to parents) and down (to children)
        self.dist = {
            "up": {t.name: self._hops(t.name, lambda n: [fk.parent for fk in schema[n].fks]) for t in schema},
            "down": {t.name: self._hops(t.name, lambda n: [fk.child for fk in schema[n].refs]) for t in schema},
        }
        self._slack: dict[tuple, float] = {}

    @staticmethod
    def _hops(start: str, nxt: Callable[[str], Iterable[str]]) -> dict[str, int]:
        dist = {start: 0}
        todo = deque([start])
        while todo:
            n = todo.popleft()
            for m in nxt(n):
                if m not in dist:
                    dist[m] = dist[n] + 1
                    todo.append(m)
        return dist

    def add(self, h: Handler) -> Handler:
        if h.table not in self.schema:
            raise KeyError(f"unknown table {h.table!r}")
        bad = h.watch - WATCHES
        if bad:
            raise ValueError(f"watch must be among {sorted(WATCHES)}, got {sorted(bad)}")
        bad = h.ops - OPS
        if bad:
            raise ValueError(f"ops must be among {sorted(OPS)}, got {sorted(bad)}")
        for s in h.source or ():
            if s not in self.schema:
                raise KeyError(f"unknown source table {s!r}")
        self.handlers.append(h)
        self._slack.clear()
        for w in h.watch:
            self._by.setdefault((h.table, w), []).append(h)
        return h

    def remove(self, h: Handler) -> None:
        self.handlers.remove(h)
        self._slack.clear()
        for w in h.watch:
            self._by[(h.table, w)].remove(h)

    def matching(self, table: str, relation: str, change: Change, depth: int) -> list[Handler]:
        return [h for h in self._by.get((table, relation), ()) if h.accepts(change, depth)]

    def slack(self, table: str, direction: str, change: Change) -> float:
        """Deepest hop at which arriving at ``table`` can still lead to a handler.

        Walking ``direction`` from ``table``, a handler on table Y watching the matching
        relation, ``dist`` hops further on, fires up to its own ``depth``; so arriving
        here at hop d is useful only while d <= depth - dist. -inf: nothing ahead.
        """
        k = (table, direction, change.table, change.op)
        v = self._slack.get(k)
        if v is None:
            rel = "descendants" if direction == "up" else "ancestors"
            v = float("-inf")
            for y, d in self.dist[direction][table].items():
                for h in self._by.get((y, rel), ()):
                    if h.accepts(change, 0):
                        v = max(v, float("inf") if h.depth is None else h.depth - d)
            self._slack[k] = v
        return v


Fetch = Callable[[str, tuple[str, ...], tuple], list[dict]]


def plan(
    registry: Registry, change: Change, fetch: Fetch, order: Literal["bfs", "dfs"] = "bfs"
) -> list[tuple[Handler, Event]]:
    """Walk the FK graph out from ``change`` and list every (handler, event) it reaches.

    Called inside the write transaction (before a delete, after an insert/update) so
    the rows seen are exactly those around the change at commit time. Each row is
    visited once per change, along the first path found (shortest for BFS). The walk
    up goes to parents through foreign keys; the walk down goes to children that
    reference the row. Both stop early where no handler further on could fire.
    """
    schema = registry.schema
    origin = Hop(change.table, change.key, None)
    jobs: list[tuple[Handler, Event]] = []
    for h in registry.matching(change.table, "self", change, 0):
        jobs.append((h, Event(change, change.table, change.row, "self", 0, (origin,))))

    # node: (table, row, depth, direction, path)
    frontier: deque = deque()
    visited: set[tuple[str, tuple]] = {(change.table, change.key)}

    def expand(table: str, row: dict, depth: int, direction: str, path: tuple[Hop, ...]) -> list:
        out = []
        tb = schema[table]
        edges: list[ForeignKey] = tb.fks if direction == "up" else tb.refs
        for fk in edges:
            if direction == "up":
                vals = tuple(row.get(c) for c in fk.child_cols)
                target, cols = fk.parent, fk.parent_cols
            else:
                vals = tuple(row.get(c) for c in fk.parent_cols)
                target, cols = fk.child, fk.child_cols
            if depth + 1 > registry.slack(target, direction, change):
                continue  # no handler at or beyond target can fire from this far away
            if any(v is None for v in vals):
                continue
            for r in fetch(target, cols, vals):
                k = schema[target].key_of(r)
                out.append((target, r, depth + 1, direction, path + (Hop(target, k, fk.name),)))
        return out

    seeds = []
    for r in (change.new, change.old):  # old *and* new: a row that moved parent notifies both
        if r is not None:
            seeds += expand(change.table, r, 0, "up", (origin,))
    seeds += expand(change.table, change.row, 0, "down", (origin,))

    def visit(node) -> list:
        table, row, depth, direction, path = node
        relation = "descendants" if direction == "up" else "ancestors"
        for h in registry.matching(table, relation, change, depth):
            jobs.append((h, Event(change, table, row, relation, depth, path)))  # type: ignore[arg-type]
        return expand(table, row, depth, direction, path)

    if order == "bfs":
        for n in seeds:
            k = (n[0], schema[n[0]].key_of(n[1]))
            if k not in visited:
                visited.add(k)
                frontier.append(n)
        while frontier:
            for n in visit(frontier.popleft()):
                k = (n[0], schema[n[0]].key_of(n[1]))
                if k not in visited:
                    visited.add(k)
                    frontier.append(n)
    elif order == "dfs":
        stack = list(reversed(seeds))
        while stack:
            n = stack.pop()
            k = (n[0], schema[n[0]].key_of(n[1]))
            if k in visited:
                continue
            visited.add(k)
            stack.extend(reversed(visit(n)))
    else:
        raise ValueError(f"order must be 'bfs' or 'dfs', got {order!r}")
    return jobs


@dataclass
class Failure:
    handler: Handler
    event: Event
    error: BaseException
    attempts: int


class Dispatcher:
    """Runs handlers after commit on a private asyncio loop in a daemon thread.

    Jobs from one change run in walk order; changes run in commit order. With
    ``concurrency > 1`` several changes are worked on at once (order across
    changes is then not kept). Sync handlers run in the loop's thread pool.
    """

    def __init__(self, concurrency: int = 1, on_error: Callable[[Failure], Any] | None = None):
        self.concurrency = concurrency
        self.on_error = on_error
        self.failures: deque[Failure] = deque(maxlen=1000)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._lock = threading.Lock()
        self._idle = threading.Condition()
        self._pending = 0

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._run, name="brm-dispatch", daemon=True)
            self._thread.start()
        self._started.wait()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._queue = asyncio.Queue()
        workers = [loop.create_task(self._worker()) for _ in range(self.concurrency)]
        self._started.set()
        try:
            loop.run_forever()
        finally:
            for w in workers:
                w.cancel()
            loop.run_until_complete(asyncio.gather(*workers, return_exceptions=True))
            loop.close()

    def submit(self, jobs: list[tuple[Handler, Event]]) -> None:
        if not jobs:
            return
        self.start()
        with self._idle:
            self._pending += 1
        assert self._loop is not None and self._queue is not None
        self._loop.call_soon_threadsafe(self._queue.put_nowait, jobs)

    async def _worker(self) -> None:
        assert self._queue is not None
        while True:
            jobs = await self._queue.get()
            try:
                for h, ev in jobs:
                    await self._run_one(h, ev)
            finally:
                with self._idle:
                    self._pending -= 1
                    self._idle.notify_all()

    async def _run_one(self, h: Handler, ev: Event) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                if h.where is not None and not h.where(ev):
                    return
                if inspect.iscoroutinefunction(h.fn):
                    await h.fn(ev)
                else:
                    res = await asyncio.get_running_loop().run_in_executor(None, h.fn, ev)
                    if inspect.isawaitable(res):
                        await res
                return
            except Exception as e:  # noqa: BLE001 - handler errors must not kill the worker
                if attempt <= h.retries:
                    await asyncio.sleep(h.backoff * 2 ** (attempt - 1))
                    continue
                f = Failure(h, ev, e, attempt)
                self.failures.append(f)
                log.error("brm handler %s failed on %s %s after %d attempt(s): %r",
                          h.name, ev.table, list(ev.key), attempt, e)
                if self.on_error is not None:
                    try:
                        self.on_error(f)
                    except Exception:  # noqa: BLE001
                        log.exception("brm on_error callback failed")
                return

    def drain(self, timeout: float | None = None) -> bool:
        """Block until every submitted change has been handled. False on timeout."""
        with self._idle:
            return self._idle.wait_for(lambda: self._pending == 0, timeout)

    def stop(self) -> None:
        if self._loop is not None and self._thread is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join()
            self._thread = None
            self._loop = None
            self._started.clear()
