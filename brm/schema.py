"""Schema introspection: turn a SQLite database (built from plain DDL) into a
table/column/foreign-key graph that the rest of brm works from."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # declared SQL type, upper-cased ("" if none)
    notnull: bool
    default: str | None  # SQL text of the default expression, if any
    pk: int  # 1-based position in the primary key, 0 if not part of it

    @property
    def affinity(self) -> str:
        """SQLite type affinity (https://sqlite.org/datatype3.html#type_affinity)."""
        t = self.type
        if "INT" in t:
            return "INTEGER"
        if any(s in t for s in ("CHAR", "CLOB", "TEXT")):
            return "TEXT"
        if t == "" or "BLOB" in t:
            return "BLOB"
        if any(s in t for s in ("REAL", "FLOA", "DOUB")):
            return "REAL"
        return "NUMERIC"


@dataclass(frozen=True)
class ForeignKey:
    """child(child_cols) REFERENCES parent(parent_cols)."""

    child: str
    child_cols: tuple[str, ...]
    parent: str
    parent_cols: tuple[str, ...]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"

    @property
    def name(self) -> str:
        return f"{self.child}({','.join(self.child_cols)})->{self.parent}({','.join(self.parent_cols)})"


@dataclass
class Table:
    name: str
    columns: dict[str, Column]
    pk: tuple[str, ...]  # primary-key columns; ("rowid",) for tables without one
    fks: list[ForeignKey] = field(default_factory=list)  # edges to parents
    refs: list[ForeignKey] = field(default_factory=list)  # edges from children
    without_rowid: bool = False

    @property
    def rowid_pk(self) -> bool:
        """True when the key is the implicit rowid (no declared primary key)."""
        return self.pk == ("rowid",)

    @property
    def auto_pk(self) -> bool:
        """True when inserts may omit the key (INTEGER PRIMARY KEY or rowid)."""
        if self.rowid_pk:
            return True
        if len(self.pk) == 1 and not self.without_rowid:
            return self.columns[self.pk[0]].type == "INTEGER"
        return False

    def key_of(self, row: dict) -> tuple:
        return tuple(row[c] for c in self.pk)


@dataclass
class Schema:
    tables: dict[str, Table]

    def __getitem__(self, name: str) -> Table:
        try:
            return self.tables[name]
        except KeyError:
            raise KeyError(f"unknown table {name!r}") from None

    def __contains__(self, name: str) -> bool:
        return name in self.tables

    def __iter__(self):
        return iter(self.tables.values())

    @property
    def foreign_keys(self) -> list[ForeignKey]:
        return [fk for t in self for fk in t.fks]

    def to_dict(self) -> dict:
        return {
            t.name: {
                "pk": list(t.pk),
                "columns": {
                    c.name: {"type": c.type, "notnull": c.notnull, "default": c.default}
                    for c in t.columns.values()
                },
                "references": [
                    {"columns": list(fk.child_cols), "table": fk.parent, "to": list(fk.parent_cols)}
                    for fk in t.fks
                ],
                "referenced_by": [
                    {"table": fk.child, "columns": list(fk.child_cols), "to": list(fk.parent_cols)}
                    for fk in t.refs
                ],
            }
            for t in self
        }


def q(ident: str) -> str:
    """Quote an SQL identifier."""
    return '"' + ident.replace('"', '""') + '"'


def introspect(conn: sqlite3.Connection) -> Schema:
    names = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' "
            "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_brm\\_%' ESCAPE '\\' "
            "ORDER BY name"
        )
    ]
    tables: dict[str, Table] = {}
    for name in names:
        cols = {
            r[1]: Column(name=r[1], type=(r[2] or "").upper(), notnull=bool(r[3]), default=r[4], pk=r[5])
            for r in conn.execute(f"PRAGMA table_info({q(name)})")
        }
        pk = tuple(c.name for c in sorted((c for c in cols.values() if c.pk), key=lambda c: c.pk))
        sql = conn.execute("SELECT sql FROM sqlite_schema WHERE name=?", (name,)).fetchone()[0] or ""
        without_rowid = "WITHOUT ROWID" in sql.upper()
        tables[name] = Table(name=name, columns=cols, pk=pk or ("rowid",), without_rowid=without_rowid)

    for t in tables.values():
        groups: dict[int, list] = {}
        for r in conn.execute(f"PRAGMA foreign_key_list({q(t.name)})"):
            # id, seq, table, from, to, on_update, on_delete, match
            groups.setdefault(r[0], []).append(r)
        for rows in groups.values():
            rows.sort(key=lambda r: r[1])
            parent = rows[0][2]
            if parent not in tables:
                raise ValueError(f"{t.name}: foreign key references unknown table {parent!r}")
            child_cols = tuple(r[3] for r in rows)
            if rows[0][4] is None:  # REFERENCES parent  (implicit: parent's primary key)
                parent_cols = tables[parent].pk
            else:
                parent_cols = tuple(r[4] for r in rows)
            fk = ForeignKey(
                child=t.name,
                child_cols=child_cols,
                parent=parent,
                parent_cols=parent_cols,
                on_update=rows[0][5],
                on_delete=rows[0][6],
            )
            t.fks.append(fk)
            tables[parent].refs.append(fk)
    return Schema(tables)
