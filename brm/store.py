"""SQLite storage: applies the DDL, runs CRUD inside transactions, and turns each
write into a Change whose handler jobs are planned in-transaction and dispatched
after commit."""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from .hooks import Change, Dispatcher, Registry, plan
from .schema import Schema, Table, introspect, q


class NotFound(LookupError):
    pass


class SchemaMismatch(RuntimeError):
    pass


def _load_ddl(schema: str) -> str:
    if "\n" not in schema and schema.strip().endswith(".sql") and os.path.exists(schema):
        with open(schema) as f:
            return f.read()
    return schema


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    if path != ":memory:" and not path.startswith("file::memory:"):
        conn.execute("PRAGMA journal_mode = WAL")
    return conn


class Store:
    """One SQLite connection guarded by a re-entrant lock. Writes serialize anyway in
    SQLite, and one connection keeps ``:memory:`` databases working."""

    def __init__(self, path: str, ddl: str, registry_factory, dispatcher: Dispatcher, order: str = "bfs"):
        self.path = path
        self.conn = _connect(path)
        self.lock = threading.RLock()
        self.order = order
        self.dispatcher = dispatcher
        self.schema = self._apply(_load_ddl(ddl))
        self.registry: Registry = registry_factory(self.schema)
        self._local = threading.local()

    def _apply(self, ddl: str) -> Schema:
        """Run the DDL on a fresh database; on an existing one, check it matches."""
        scratch = _connect(":memory:")
        try:
            scratch.executescript(ddl)
            want = introspect(scratch)
        finally:
            scratch.close()
        have = introspect(self.conn)
        if not have.tables:
            self.conn.executescript(ddl)
            return introspect(self.conn)
        if have.to_dict() != want.to_dict():
            missing = sorted(set(want.tables) - set(have.tables))
            extra = sorted(set(have.tables) - set(want.tables))
            differ = sorted(
                n for n in set(want.tables) & set(have.tables)
                if want.to_dict()[n] != have.to_dict()[n]
            )
            raise SchemaMismatch(
                f"{self.path} does not match the DDL (missing tables {missing}, extra {extra}, "
                f"different {differ}); migrate it by hand or point brm at a new file"
            )
        return have

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator["Tx"]:
        """Re-entrant per thread: nested calls join the outer transaction, and every
        change's handlers are dispatched once the outermost one commits."""
        cur: Tx | None = getattr(self._local, "tx", None)
        if cur is not None:
            yield cur
            return
        with self.lock:
            tx = Tx(self)
            self._local.tx = tx
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield tx
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")
            finally:
                self._local.tx = None
        for jobs in tx.jobs:
            self.dispatcher.submit(jobs)


class Tx:
    def __init__(self, store: Store):
        self.store = store
        self.conn = store.conn
        self.schema = store.schema
        self.jobs: list[list] = []
        self.changes: list[Change] = []

    # -- reads ---------------------------------------------------------------

    def _cols(self, t: Table) -> str:
        return ("rowid, *" if t.rowid_pk else "*")

    def _row(self, r: sqlite3.Row | None) -> dict | None:
        return None if r is None else dict(r)

    def _key_where(self, t: Table) -> str:
        return " AND ".join(f"{q(c)} = ?" for c in t.pk)

    def _key(self, t: Table, key: Any) -> tuple:
        key = key if isinstance(key, tuple) else (key,)
        if len(key) != len(t.pk):
            raise ValueError(f"{t.name} key has {len(t.pk)} column(s) {list(t.pk)}, got {len(key)}")
        return key

    def get(self, table: str, key: Any) -> dict | None:
        t = self.schema[table]
        key = self._key(t, key)
        return self._row(self.conn.execute(
            f"SELECT {self._cols(t)} FROM {q(t.name)} WHERE {self._key_where(t)}", key).fetchone())

    def select(self, table: str, where: dict[str, Any] | None = None, *, limit: int | None = None,
               offset: int = 0, order_by: list[str] | None = None) -> list[dict]:
        """Rows matching ``where`` (column -> value, equality; None means IS NULL).
        ``order_by`` entries are column names, prefixed with '-' for descending."""
        t = self.schema[table]
        where = where or {}
        clauses, args = [], []
        for c, v in where.items():
            self._check_col(t, c)
            if v is None:
                clauses.append(f"{q(c)} IS NULL")
            else:
                clauses.append(f"{q(c)} = ?")
                args.append(v)
        sql = f"SELECT {self._cols(t)} FROM {q(t.name)}"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        order = []
        for o in order_by or list(t.pk):
            desc = o.startswith("-")
            c = o.lstrip("-")
            self._check_col(t, c)
            order.append(f"{q(c)} {'DESC' if desc else 'ASC'}")
        sql += " ORDER BY " + ", ".join(order)
        if limit is not None or offset:
            sql += " LIMIT ? OFFSET ?"
            args += [-1 if limit is None else limit, offset]
        return [dict(r) for r in self.conn.execute(sql, args)]

    def _fetch(self, table: str, cols: tuple[str, ...], vals: tuple) -> list[dict]:
        return self.select(table, dict(zip(cols, vals)))

    def _check_col(self, t: Table, c: str) -> None:
        if c not in t.columns and not (c == "rowid" and t.rowid_pk):
            raise KeyError(f"{t.name} has no column {c!r}")

    # -- writes --------------------------------------------------------------

    def _plan(self, change: Change) -> list:
        return plan(self.store.registry, change, self._fetch, self.store.order)  # type: ignore[arg-type]

    def _record(self, change: Change, jobs: list | None = None) -> None:
        self.changes.append(change)
        jobs = self._plan(change) if jobs is None else jobs
        if jobs:
            self.jobs.append(jobs)

    def insert(self, table: str, values: dict[str, Any]) -> dict:
        t = self.schema[table]
        for c in values:
            self._check_col(t, c)
        if values:
            cols = ", ".join(q(c) for c in values)
            marks = ", ".join("?" for _ in values)
            sql = f"INSERT INTO {q(t.name)} ({cols}) VALUES ({marks}) RETURNING {self._cols(t)}"
        else:
            sql = f"INSERT INTO {q(t.name)} DEFAULT VALUES RETURNING {self._cols(t)}"
        row = dict(self.conn.execute(sql, list(values.values())).fetchone())
        self._record(Change(t.name, "insert", t.key_of(row), None, row))
        return row

    def update(self, table: str, key: Any, values: dict[str, Any]) -> dict:
        t = self.schema[table]
        key = self._key(t, key)
        old = self.get(table, key)
        if old is None:
            raise NotFound(f"{table} {list(key)}")
        for c in values:
            self._check_col(t, c)
        values = {c: v for c, v in values.items() if old.get(c) != v}
        if not values:
            return old
        sets = ", ".join(f"{q(c)} = ?" for c in values)
        row = dict(self.conn.execute(
            f"UPDATE {q(t.name)} SET {sets} WHERE {self._key_where(t)} RETURNING {self._cols(t)}",
            [*values.values(), *key]).fetchone())
        self._record(Change(t.name, "update", t.key_of(row), old, row))
        return row

    def delete(self, table: str, key: Any) -> dict:
        t = self.schema[table]
        key = self._key(t, key)
        old = self.get(table, key)
        if old is None:
            raise NotFound(f"{table} {list(key)}")
        # plan before deleting so ON DELETE CASCADE children are still there to walk
        change = Change(t.name, "delete", key, old, None)
        jobs = self._plan(change)
        self.conn.execute(f"DELETE FROM {q(t.name)} WHERE {self._key_where(t)}", key)
        self._record(change, jobs)
        return old
