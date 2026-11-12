"""Routes: which related rows a handler hears from, named by foreign keys.

A route is written from the table where the change happens to the table the
handler is on:

    via="reading"                                   any route from reading
    via="reading.sensor_id"                         exactly this foreign key
    via="reading.sensor_id > sensor.device_id"      exactly these, in order
    via="device > sensor.device_id"                 start table, then walk *down*
    via="node.parent*"                              a self-reference, one or more times
    via="sensor.device_id > <sensor.device_id"      up to the device, back down: siblings

Each foreign key is named by its child side, ``table.col`` (``table.a,b`` for a
composite key). brm works out whether each hop goes up (child row to the parent
it references) or down (parent row to the children referencing it) from where the
path is. A leading ``<`` forces a hop down, which only matters for
self-references. Routes are checked against the schema when a handler is
registered, so a schema change that breaks one fails at startup, not silently.
"""

from __future__ import annotations

from dataclasses import dataclass

from .schema import ForeignKey, Schema


class RouteError(ValueError):
    pass


@dataclass(frozen=True)
class Move:
    fk: ForeignKey
    up: bool  # child row -> the parent it references; else parent -> its children
    state: int  # route state after the move

    @property
    def target(self) -> str:
        return self.fk.parent if self.up else self.fk.child

    def from_cols(self) -> tuple[str, ...]:
        return self.fk.child_cols if self.up else self.fk.parent_cols

    def to_cols(self) -> tuple[str, ...]:
        return self.fk.parent_cols if self.up else self.fk.child_cols


def fk_name(fk: ForeignKey) -> str:
    return f"{fk.child}.{','.join(fk.child_cols)}"


class Route:
    """A small automaton over foreign-key hops. Walks start in state 0 at the changed row."""

    text: str
    start: str  # table the change happens in
    target: str  # table of the handler

    def moves(self, state: int, table: str) -> list[Move]:
        raise NotImplementedError

    def accepts(self, state: int, table: str) -> bool:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Route({self.text!r})"


@dataclass(frozen=True)
class Step:
    fk: ForeignKey
    up: bool
    repeat: bool

    @property
    def source(self) -> str:
        return self.fk.child if self.up else self.fk.parent


class PathRoute(Route):
    """Exactly the listed hops. State i = the first i steps are done."""

    def __init__(self, start: str, steps: list[Step]):
        self.start = start
        self.steps = steps
        last = steps[-1]
        self.target = last.fk.parent if last.up else last.fk.child
        parts = [start]
        for s in steps:
            parts.append(("" if s.up else "<") + fk_name(s.fk) + ("*" if s.repeat else ""))
        self.text = " > ".join(parts)

    def moves(self, state: int, table: str) -> list[Move]:
        out = []
        if state < len(self.steps):
            s = self.steps[state]
            if s.source == table:
                out.append(Move(s.fk, s.up, state + 1))
        if state > 0 and self.steps[state - 1].repeat:
            s = self.steps[state - 1]
            if s.source == table:
                out.append(Move(s.fk, s.up, state))
        return out

    def accepts(self, state: int, table: str) -> bool:
        return state == len(self.steps)


class AnyRoute(Route):
    """Every route from ``start`` to ``target`` going one way (all up or all down)."""

    def __init__(self, start: str, target: str, up: bool, edges: list[ForeignKey]):
        self.start, self.target, self.up = start, target, up
        self.text = start
        self._by_source: dict[str, list[ForeignKey]] = {}
        for fk in edges:
            self._by_source.setdefault(fk.child if up else fk.parent, []).append(fk)

    def moves(self, state: int, table: str) -> list[Move]:
        return [Move(fk, self.up, 0) for fk in self._by_source.get(table, ())]

    def accepts(self, state: int, table: str) -> bool:
        return table == self.target


def _reach(schema: Schema, start: str, up: bool) -> set[str]:
    seen = {start}
    todo = [start]
    while todo:
        t = schema[todo.pop()]
        for fk in (t.fks if up else t.refs):
            n = fk.parent if up else fk.child
            if n not in seen:
                seen.add(n)
                todo.append(n)
    return seen


def _fk(schema: Schema, token: str) -> ForeignKey:
    if "." not in token:
        raise RouteError(f"{token!r}: name a foreign key as table.column")
    table, cols = token.split(".", 1)
    if table not in schema:
        raise RouteError(f"{token!r}: unknown table {table!r}")
    want = tuple(c.strip() for c in cols.split(","))
    for fk in schema[table].fks:
        if fk.child_cols == want:
            return fk
    have = ", ".join(fk_name(f) for f in schema[table].fks) or "none"
    raise RouteError(f"{token!r}: {table} has no foreign key on ({', '.join(want)}); its keys: {have}")


def parse(schema: Schema, target: str, via: str) -> Route:
    """Compile ``via`` for a handler on ``target``; raises RouteError with the reason."""
    tokens = [t.strip() for t in via.split(">")]
    if any(not t for t in tokens):
        raise RouteError(f"{via!r}: empty hop")
    start = None
    if "." not in tokens[0]:
        start = tokens.pop(0)
        if start not in schema:
            raise RouteError(f"{via!r}: unknown table {start!r}")
    if not tokens:
        return _any_route(schema, start, target, via)  # type: ignore[arg-type]

    steps: list[Step] = []
    cur = start
    for tok in tokens:
        down = tok.startswith("<")
        repeat = tok.endswith("*")
        fk = _fk(schema, tok.lstrip("<").rstrip("*").strip())
        if cur is None:  # no start table: the first hop goes the way it is written
            cur = fk.parent if down else fk.child
        can_up, can_down = fk.child == cur, fk.parent == cur
        if down and not can_down:
            raise RouteError(f"{via!r}: at {cur!r}, {tok!r} cannot go down ({fk.parent} -> {fk.child})")
        if not (can_up or can_down):
            raise RouteError(f"{via!r}: the path is at {cur!r} but {fk_name(fk)} joins "
                             f"{fk.child} -> {fk.parent}")
        up = can_up and not down
        if repeat and fk.child != fk.parent:
            raise RouteError(f"{via!r}: '*' repeats a self-referencing key only ({tok!r} is not one)")
        steps.append(Step(fk, up, repeat))
        cur = fk.parent if up else fk.child
    route = PathRoute(start or (steps[0].source), steps)
    if route.target != target:
        raise RouteError(f"{via!r}: the path ends at {route.target!r}, but the handler is on {target!r}")
    return route


def _any_route(schema: Schema, start: str, target: str, via: str) -> Route:
    if start == target:
        selfref = [fk_name(fk) for fk in schema[target].fks if fk.parent == target]
        hint = (f"; for its self-reference write the key: via='{selfref[0]}*' (up) or "
                f"'<{selfref[0]}*' (down)" if selfref else "")
        raise RouteError(f"via={via!r} on {target!r}: leave via out for the handler's own rows{hint}")
    above = _reach(schema, start, up=True)  # tables the start row references, transitively
    below = _reach(schema, start, up=False)  # tables referencing it, transitively
    up, down = target in above, target in below
    if up and down:
        raise RouteError(f"via={via!r}: {start} reaches {target} both up and down; spell out the path")
    if not (up or down):
        raise RouteError(f"via={via!r}: no chain of foreign keys connects {start} and {target}")
    # keep edges that lie on some path from start to target
    reach_target = _reach(schema, target, up=not up)  # tables from which target is reachable
    span = above if up else below
    edges = []
    for t in span:
        for fk in (schema[t].fks if up else schema[t].refs):
            nxt = fk.parent if up else fk.child
            if nxt in reach_target:
                edges.append(fk)
    return AnyRoute(start, target, up, edges)
