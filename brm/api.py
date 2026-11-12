"""FastAPI routes generated from the schema: CRUD per table plus child-relation
listings along every foreign key.

No ``from __future__ import annotations`` here: FastAPI must see the per-table
pydantic models as real objects, not as strings it cannot resolve."""

import functools
import sqlite3
from typing import TYPE_CHECKING, Any, Optional

from fastapi import APIRouter, Body, HTTPException, Request
from pydantic import BaseModel, ConfigDict, create_model

from .schema import Column, Table
from .store import NotFound

if TYPE_CHECKING:
    from .app import App

RESERVED = {"_limit", "_offset", "_order"}
MAX_LIMIT = 1000


def py_type(c: Column) -> Any:
    t = c.type
    if "BOOL" in t:
        return bool
    if any(s in t for s in ("DATE", "TIME")):
        return str
    return {"INTEGER": int, "REAL": float, "TEXT": str, "NUMERIC": float | int}.get(c.affinity, Any)


def models(t: Table) -> tuple[type[BaseModel], type[BaseModel], type[BaseModel]]:
    """(Create, Patch, Row) pydantic models for a table."""
    cfg = ConfigDict(extra="forbid")
    create, patch, row = {}, {}, {}
    if t.rowid_pk:
        row["rowid"] = (int, ...)
    for c in t.columns.values():
        typ = py_type(c)
        auto = t.auto_pk and c.name in t.pk
        required = c.notnull and c.default is None and not auto
        create[c.name] = (typ, ...) if required else (Optional[typ], None)
        if c.name not in t.pk:
            patch[c.name] = (Optional[typ], None)
        row[c.name] = (Optional[typ], None)
    name = "".join(p.capitalize() for p in t.name.replace("-", "_").split("_")) or "T"
    return (
        create_model(f"{name}Create", __config__=cfg, **create),
        create_model(f"{name}Patch", __config__=cfg, **patch),
        create_model(f"{name}", **row),
    )


def parse_key(t: Table, raw: str) -> tuple:
    """URL key -> tuple; composite keys are comma separated in pk order."""
    parts = raw.split(",") if len(t.pk) > 1 else [raw]
    if len(parts) != len(t.pk):
        raise HTTPException(400, f"{t.name} key is {','.join(t.pk)}")
    out = []
    for col, p in zip(t.pk, parts):
        aff = "INTEGER" if col == "rowid" else t.columns[col].affinity
        try:
            out.append(int(p) if aff == "INTEGER" else float(p) if aff == "REAL" else p)
        except ValueError:
            raise HTTPException(400, f"{t.name}.{col} must be {aff.lower()}, got {p!r}") from None
    return tuple(out)


def coerce(t: Table, col: str, raw: str) -> Any:
    if raw == "null":
        return None
    aff = "INTEGER" if col == "rowid" else t.columns[col].affinity
    try:
        if aff == "INTEGER":
            return int(raw)
        if aff in ("REAL", "NUMERIC"):
            return float(raw)
    except ValueError:
        raise HTTPException(400, f"{t.name}.{col} must be {aff.lower()}, got {raw!r}") from None
    return raw


def list_args(t: Table, request: Request) -> tuple[dict, int, int, list[str] | None]:
    where: dict[str, Any] = {}
    for k, v in request.query_params.items():
        if k in RESERVED:
            continue
        if k not in t.columns and not (k == "rowid" and t.rowid_pk):
            raise HTTPException(400, f"{t.name} has no column {k!r}")
        where[k] = coerce(t, k, v)
    p = request.query_params
    try:
        limit = min(int(p.get("_limit", 100)), MAX_LIMIT)
        offset = int(p.get("_offset", 0))
    except ValueError:
        raise HTTPException(400, "_limit and _offset must be integers") from None
    order = p["_order"].split(",") if "_order" in p else None
    for o in order or ():
        c = o.lstrip("-")
        if c not in t.columns and not (c == "rowid" and t.rowid_pk):
            raise HTTPException(400, f"{t.name} has no column {c!r}")
    return where, limit, offset, order


def _db_errors(fn):
    @functools.wraps(fn)
    def wrapped(*a, **kw):
        try:
            return fn(*a, **kw)
        except NotFound as e:
            raise HTTPException(404, f"not found: {e}") from None
        except sqlite3.IntegrityError as e:
            raise HTTPException(409, str(e)) from None
    return wrapped


def build_router(app: "App") -> APIRouter:
    router = APIRouter()
    schema = app.schema

    @router.get("/_schema", tags=["_brm"], summary="Tables, columns and foreign keys")
    def get_schema() -> dict:
        return schema.to_dict()

    _outbox_routes(router, app)
    for t in schema:
        _table_routes(router, app, t)
    return router


class Requeue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ids: Optional[list[int]] = None
    handler: Optional[str] = None


def _outbox_routes(router: APIRouter, app: "App") -> None:
    tags = ["_brm"]

    @router.get("/_brm/outbox", tags=tags, summary="Outbox job counts per status and handler")
    def outbox_stats() -> dict:
        return app.outbox.stats()

    @router.get("/_brm/outbox/{status}", tags=tags, summary="List outbox jobs (dead, pending or running)")
    def outbox_list(status: str, handler: Optional[str] = None, limit: int = 100) -> list[dict]:
        if status not in ("dead", "pending", "running"):
            raise HTTPException(400, "status is dead, pending or running")
        return app.outbox.list(status, min(limit, MAX_LIMIT), handler)

    @router.post("/_brm/outbox/requeue", tags=tags, summary="Retry dead jobs (all, by id, or by handler)")
    def outbox_requeue(body: Requeue = Body(default_factory=Requeue)) -> dict:
        n = app.outbox.requeue(body.ids, body.handler)
        app.dispatcher.wake()
        return {"requeued": n}

    @router.delete("/_brm/outbox/dead", tags=tags, summary="Delete dead jobs (optionally one handler's)")
    def outbox_purge(handler: Optional[str] = None) -> dict:
        return {"deleted": app.outbox.purge("dead", handler)}


def _table_routes(router: APIRouter, app: "App", t: Table) -> None:
    Create, Patch, Row = models(t)
    base = f"/{t.name}"
    keyp = "/{key}"
    keydoc = ",".join(t.pk)
    tags = [t.name]

    @router.get(base, response_model=list[Row], tags=tags, summary=f"List {t.name}",
                description="Filter with ?column=value (null for IS NULL); page with _limit/_offset; "
                            "sort with _order=col,-col.")
    @_db_errors
    def list_rows(request: Request):
        where, limit, offset, order = list_args(t, request)
        return app.select(t.name, where, limit=limit, offset=offset, order_by=order)

    @router.post(base, response_model=Row, status_code=201, tags=tags, summary=f"Create {t.name}")
    @_db_errors
    def create_row(body: Create = Body(...)):  # type: ignore[valid-type]
        return app.insert(t.name, body.model_dump(exclude_unset=True))

    @router.get(base + keyp, response_model=Row, tags=tags, summary=f"Get {t.name} by {keydoc}")
    @_db_errors
    def get_row(key: str):
        row = app.get(t.name, parse_key(t, key))
        if row is None:
            raise NotFound(f"{t.name} {key}")
        return row

    @router.patch(base + keyp, response_model=Row, tags=tags, summary=f"Update {t.name}")
    @_db_errors
    def patch_row(key: str, body: Patch = Body(...)):  # type: ignore[valid-type]
        return app.update(t.name, parse_key(t, key), body.model_dump(exclude_unset=True))

    @router.delete(base + keyp, response_model=Row, tags=tags, summary=f"Delete {t.name}")
    @_db_errors
    def delete_row(key: str):
        return app.delete(t.name, parse_key(t, key))

    # children along each foreign key: /parent/{key}/child  (or child.col if ambiguous)
    counts: dict[str, int] = {}
    for fk in t.refs:
        counts[fk.child] = counts.get(fk.child, 0) + 1
    for fk in t.refs:
        child = app.schema[fk.child]
        seg = fk.child if counts[fk.child] == 1 else f"{fk.child}.{'.'.join(fk.child_cols)}"
        _, _, ChildRow = models(child)

        def make(fk=fk, child=child):
            @router.get(f"{base}{keyp}/{seg}", response_model=list[ChildRow], tags=tags,
                        summary=f"{fk.child} rows referencing this {t.name}")
            @_db_errors
            def list_children(key: str, request: Request):
                parent = app.get(t.name, parse_key(t, key))
                if parent is None:
                    raise NotFound(f"{t.name} {key}")
                where, limit, offset, order = list_args(child, request)
                for cc, pc in zip(fk.child_cols, fk.parent_cols):
                    where[cc] = parent[pc]
                return app.select(child.name, where, limit=limit, offset=offset, order_by=order)

        make()
