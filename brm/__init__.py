"""brm: Best Relational Model.

Define the database in plain SQL DDL; brm serves CRUD routes for it over FastAPI
and runs handlers you register when rows change, walking the foreign-key graph
to reach handlers on related rows."""

from . import handlers
from .app import App
from .hooks import Change, Event, Handler, Hop
from .outbox import Failure, Outbox
from .schema import Column, ForeignKey, Schema, Table
from .store import NotFound, SchemaMismatch, Tx

__all__ = [
    "App", "Change", "Event", "Failure", "Outbox", "Handler", "Hop", "Column", "ForeignKey",
    "Schema", "Table", "NotFound", "SchemaMismatch", "Tx", "handlers",
]
__version__ = "0.1.0"
