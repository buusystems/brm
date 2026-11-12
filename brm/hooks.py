"""Change events, handler registration, and the foreign-key walk that decides
which handlers a change reaches (see routes.py for the ``via`` syntax)."""

from __future__ import annotations

import base64
import json
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable, Literal

from .routes import Route, parse
from .schema import Schema

log = logging.getLogger("brm")

Op = Literal["insert", "update", "delete"]
OPS: frozenset[str] = frozenset(("insert", "update", "delete"))


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
    via: str | None  # foreign key used to arrive here, "table.col" (None at the origin)
    row: dict  # the row as the walk saw it, inside the write's transaction


@dataclass(frozen=True)
class Event:
    """What a handler receives: the row it is registered on, and the change that reached it.

    ``via`` is the handler's route (None when ``row`` is the changed row itself),
    ``depth`` the number of foreign-key hops walked, and ``path`` every row on the
    way, from the changed row (``path[0]``) to this one (``path[-1]``).
    """

    change: Change
    table: str
    row: dict
    via: str | None
    depth: int
    path: tuple[Hop, ...]

    @property
    def key(self) -> tuple:
        return self.path[-1].key

    def at(self, table: str) -> dict | None:
        """The first row of ``table`` on the path, e.g. ``ev.at("sensor")`` for a reading's
        sensor while a device hears about the reading."""
        for h in self.path:
            if h.table == table:
                return h.row
        return None

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "key": list(self.key),
            "row": self.row,
            "via": self.via,
            "depth": self.depth,
            "path": [{"table": h.table, "key": list(h.key), "via": h.via, "row": h.row} for h in self.path],
            "change": self.change.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Event":
        c = d["change"]
        change = Change(c["table"], c["op"], tuple(c["key"]), c["old"], c["new"])
        path = tuple(Hop(h["table"], tuple(h["key"]), h["via"], h["row"]) for h in d["path"])
        return cls(change, d["table"], d["row"], d["via"], d["depth"], path)

    def dumps(self) -> str:
        """Lossless JSON (BLOBs as {"$b64": ...}) for the outbox."""
        return json.dumps(self.to_dict(), default=_enc, separators=(",", ":"))

    @classmethod
    def loads(cls, s: str) -> "Event":
        return cls.from_dict(json.loads(s, object_hook=_dec))


def _enc(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"$b64": base64.b64encode(bytes(v)).decode()}
    raise TypeError(f"cannot store {type(v).__name__} in an event")


def _dec(d: dict) -> Any:
    if len(d) == 1 and "$b64" in d:
        return base64.b64decode(d["$b64"])
    return d


HandlerFn = Callable[[Event], Any] | Callable[[Event], Awaitable[Any]]


@dataclass(eq=False)
class Handler:
    fn: HandlerFn
    table: str
    route: Route | None = None  # None: the handler's own rows only
    ops: frozenset[str] = OPS
    max_hops: int | None = None  # cap on hops walked (for self-references); None = no cap
    where: Callable[[Event], bool] | None = None  # checked in the write's transaction
    retries: int = 0
    backoff: float = 0.5  # seconds, doubled per retry
    name: str = ""

    @property
    def via(self) -> str | None:
        return None if self.route is None else self.route.text


class Registry:
    def __init__(self, schema: Schema):
        self.schema = schema
        self.handlers: list[Handler] = []
        self.by_name: dict[str, Handler] = {}
        self._own: dict[str, list[Handler]] = {}  # table -> handlers with no route
        # start table -> (target table, route text, max_hops) -> (route, handlers)
        self._routes: dict[str, dict[tuple[str, str, int | None], tuple[Route, list[Handler]]]] = {}

    def route(self, table: str, via: str) -> Route:
        return parse(self.schema, table, via)

    def add(self, h: Handler) -> Handler:
        if h.table not in self.schema:
            raise KeyError(f"unknown table {h.table!r}")
        bad = h.ops - OPS
        if bad:
            raise ValueError(f"ops must be among {sorted(OPS)}, got {sorted(bad)}")
        if h.max_hops is not None and h.max_hops < 1:
            raise ValueError("max_hops must be at least 1")
        if h.name in self.by_name:
            raise ValueError(
                f"a handler named {h.name!r} is already registered; pass name= to tell them apart "
                "(names identify queued jobs across restarts, so keep them stable)")
        self.by_name[h.name] = h
        self.handlers.append(h)
        if h.route is None:
            self._own.setdefault(h.table, []).append(h)
        else:
            group = self._routes.setdefault(h.route.start, {})
            k = (h.route.target, h.route.text, h.max_hops)
            group.setdefault(k, (h.route, []))[1].append(h)
        return h

    def remove(self, h: Handler) -> None:
        self.handlers.remove(h)
        del self.by_name[h.name]
        if h.route is None:
            self._own[h.table].remove(h)
        else:
            group = self._routes[h.route.start]
            k = (h.route.target, h.route.text, h.max_hops)
            group[k][1].remove(h)
            if not group[k][1]:
                del group[k]

    def own(self, change: Change) -> list[Handler]:
        return [h for h in self._own.get(change.table, ()) if change.op in h.ops]

    def routes_from(self, change: Change) -> list[tuple[Route, int | None, list[Handler]]]:
        out = []
        for (_, _, max_hops), (route, hs) in self._routes.get(change.table, {}).items():
            hs = [h for h in hs if change.op in h.ops]
            if hs:
                out.append((route, max_hops, hs))
        return out


Fetch = Callable[[str, tuple[str, ...], tuple], list[dict]]


def plan(
    registry: Registry, change: Change, fetch: Fetch, order: Literal["bfs", "dfs"] = "bfs"
) -> list[tuple[Handler, Event]]:
    """Every (handler, event) a change reaches.

    Called inside the write transaction (before a delete, after an insert/update) so
    the rows seen are exactly those around the change at commit time. Only routes
    that start at the changed table are walked, and each only along its own foreign
    keys. A row is reached once per route, along the first path found (the shortest
    with BFS); the changed row itself is never reached through a route. ``where`` is
    checked here, so filtered-out events are never queued.
    """
    schema = registry.schema
    origin = Hop(change.table, change.key, None, change.row)
    jobs: list[tuple[Handler, Event]] = []

    def offer(h: Handler, ev: Event) -> None:
        if h.where is not None:
            try:
                if not h.where(ev):
                    return
            except Exception:  # noqa: BLE001 - a broken predicate must not block writes
                log.exception("brm: where= of %s raised; queueing the job anyway", h.name)
        jobs.append((h, ev))

    for h in registry.own(change):
        offer(h, Event(change, change.table, change.row, None, 0, (origin,)))

    cache: dict[tuple, list[dict]] = {}

    def get(table: str, cols: tuple[str, ...], vals: tuple) -> list[dict]:
        k = (table, cols, vals)
        if k not in cache:
            cache[k] = fetch(table, cols, vals)
        return cache[k]

    for route, max_hops, handlers in registry.routes_from(change):
        for table, row, depth, path in _walk(schema, route, max_hops, change, origin, get, order):
            for h in handlers:
                offer(h, Event(change, table, row, route.text, depth, path))
    return jobs


def _walk(schema: Schema, route: Route, max_hops: int | None, change: Change, origin: Hop,
          get, order: str):
    """Yield (table, row, depth, path) for every row ``route`` reaches from the change."""
    if order not in ("bfs", "dfs"):
        raise ValueError(f"order must be 'bfs' or 'dfs', got {order!r}")
    start = (change.table, change.key)
    visited = {(change.table, change.key, 0)}

    def expand(table: str, rows: list[dict], state: int, depth: int, path: tuple[Hop, ...]) -> list:
        if max_hops is not None and depth >= max_hops:
            return []
        out = []
        for m in route.moves(state, table):
            # leaving the changed row upward, use its old *and* new values, so a row
            # that moved to another parent reaches both parents
            for row in (rows if m.up else rows[:1]):
                vals = tuple(row.get(c) for c in m.from_cols())
                if any(v is None for v in vals):
                    continue
                for r in get(m.target, m.to_cols(), vals):
                    k = schema[m.target].key_of(r)
                    hop = Hop(m.target, k, f"{m.fk.child}.{','.join(m.fk.child_cols)}", r)
                    out.append((m.target, r, m.state, depth + 1, path + (hop,)))
        return out

    first = [r for r in (change.new, change.old) if r is not None]
    seeds = expand(change.table, first, 0, 0, (origin,))

    def key(n) -> tuple:
        return (n[0], schema[n[0]].key_of(n[1]), n[2])

    def emit(n):
        table, row, state, depth, path = n
        if route.accepts(state, table) and (table, path[-1].key) != start:
            return (table, row, depth, path)
        return None

    if order == "bfs":
        frontier: deque = deque()
        for n in seeds:
            if key(n) not in visited:
                visited.add(key(n))
                frontier.append(n)
        while frontier:
            n = frontier.popleft()
            hit = emit(n)
            if hit:
                yield hit
            for m in expand(n[0], [n[1]], n[2], n[3], n[4]):
                if key(m) not in visited:
                    visited.add(key(m))
                    frontier.append(m)
    else:
        stack = list(reversed(seeds))
        while stack:
            n = stack.pop()
            if key(n) in visited:
                continue
            visited.add(key(n))
            hit = emit(n)
            if hit:
                yield hit
            stack.extend(reversed(expand(n[0], [n[1]], n[2], n[3], n[4])))
