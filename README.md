# brm: Best Relational Model

Write your database as plain SQL DDL. brm then

- **serves CRUD routes** for every table over FastAPI, with Pydantic models generated
  from the columns and OpenAPI docs at `/docs`;
- **runs handlers when rows change.** A handler is a plain function you register on a
  table. A change walks the foreign-key graph (BFS or DFS), so a handler on one row can
  fire because a related row changed. Typical uses are "ping me over MQTT when any
  reading under this device arrives" or "tell every sensor when its device goes offline".

```sql
-- schema.sql
CREATE TABLE device  (id INTEGER PRIMARY KEY, name TEXT NOT NULL, online BOOLEAN NOT NULL DEFAULT 1);
CREATE TABLE sensor  (id INTEGER PRIMARY KEY, device_id INTEGER NOT NULL REFERENCES device(id) ON DELETE CASCADE,
                      kind TEXT NOT NULL, max_value REAL);
CREATE TABLE reading (id INTEGER PRIMARY KEY, sensor_id INTEGER NOT NULL REFERENCES sensor(id) ON DELETE CASCADE,
                      value REAL NOT NULL);
```

```python
# app.py
from brm import App
from brm.handlers import MQTT, when

app = App("iot.db", schema="schema.sql")
bus = MQTT("broker.local")

# anything below a device changed -> MQTT ping on brm/device/<id>
app.on("device", bus.publish("brm/device/{key}"), watch="descendants", retries=3)

# a new reading over its sensor's threshold (reading -> sensor is 1 hop)
@app.on("sensor", watch="descendants", source="reading", ops="insert", depth=1)
def over_threshold(ev):
    if ev.row["max_value"] is not None and ev.change.new["value"] > ev.row["max_value"]:
        print("alert", ev.row, ev.change.new)

# a device went offline -> every sensor below it hears about it (walk down)
app.on("sensor", lambda ev: print("offline:", ev.row), watch="ancestors", source="device",
       ops="update", where=lambda ev: ev.change.new["online"] == 0)
```

```sh
pip install -e '.[mqtt]'
uvicorn app:app          # an App is an ASGI app; or mount app.router() in your own FastAPI
```

## Routes

For each table `t` with primary key `k`:

| method | path | |
|---|---|---|
| GET | `/t` | list rows. `?col=value` filters (`null` means IS NULL), `_limit` (default 100, max 1000), `_offset`, `_order=col,-col` |
| POST | `/t` | create; returns the row (201) |
| GET | `/t/{k}` | one row. Composite keys are written `a,b` |
| PATCH | `/t/{k}` | update the given columns; key columns cannot be changed |
| DELETE | `/t/{k}` | delete; returns the deleted row |
| GET | `/t/{k}/child` | rows of `child` that reference this row (`child.col` when there are several FKs to `t`) |
| GET | `/_schema` | tables, columns and the FK graph |

A foreign-key, UNIQUE or NOT NULL violation returns 409; a bad body returns 422; an unknown key returns 404.
Tables without a declared primary key are addressed by `rowid`.

## Handlers

```python
app.on(table, fn=None, *, watch="self", ops=None, depth=None, source=None,
       where=None, retries=0, backoff=0.5, name=None)
```

| `watch` | fires on a row of `table` when… | walk |
|---|---|---|
| `"self"` | that row was inserted, updated or deleted | none |
| `"descendants"` | a row that references it, directly or transitively, changed | up the FKs, child → parent |
| `"ancestors"` | a row it references, directly or transitively, changed | down the FKs, parent → children |

Pass a list to combine them. `ops` limits which operations count (`"insert"`, `"update"`,
`"delete"`), and applies to the row that changed. `depth` is the maximum number of FK hops
between the changed row and the handler's row. `source` limits the handler to changes in
the given tables. `where(event)` is a predicate checked at dispatch time.

A handler receives an `Event`:

- `ev.table`, `ev.row`, `ev.key`: the row the handler is registered on.
- `ev.relation`: `"self"`, `"descendants"` or `"ancestors"`.
- `ev.depth`: number of FK hops from the changed row.
- `ev.path`: the `Hop(table, key, via_fk)` list from the changed row to this one.
- `ev.change`: the change itself, with `table`, `op`, `key`, `old`, `new`, `.row` and
  `.changed` (the set of columns that changed).

### When they run

1. A write (over HTTP or `app.insert/update/delete`, or several grouped in
   `with app.transaction() as tx:`) plans its walk **inside the transaction**. Inserts and
   updates are planned after the write and deletes before it, so rows that
   `ON DELETE CASCADE` will remove are still reached.
2. After **commit**, the jobs go to a background dispatcher, so a slow or failing handler
   never blocks or rolls back the write. Nothing runs if the transaction rolls back.
3. Jobs from one change run in walk order. With the default `concurrency=1`, changes run in
   commit order. Sync handlers run in a thread pool, and `async def` handlers are awaited.
4. If a handler raises, it is retried `retries` times, waiting `backoff * 2**n` between
   attempts. The final failure is logged, kept in `app.failures` and passed to
   `App(on_error=...)`.

How the walk behaves:

- Each related row is visited once per change, along the first path found. With
  `App(order="bfs")` (the default) that path is the shortest one; `order="dfs"` goes depth
  first.
- Diamonds and cycles, including self-referencing tables, are safe.
- When an update moves a row to a new parent, both the old and the new parent hear about it.
- The walk only follows an FK edge if some handler at or beyond that table could still fire
  within its `depth`, so no lookups happen when nothing is listening.

A handler may write to the database itself; its writes are new changes. Avoid handlers that
trigger each other forever.

`app.drain()` waits until every committed change has been handled, which is useful in tests
and scripts. `app.off(fn)` unregisters a handler.

### Built-ins (`brm.handlers`)

- `MQTT(host, port, username=, password=, tls=)` is a lazily connected shared client.
  `.publish(topic, qos=1, retain=False, payload=to_json)` returns a handler, and waits for
  the broker to acknowledge the message so that `retries` apply.
- `mqtt(topic, host, port)` is shorthand for the above.
- `webhook(url, method="POST", headers=)` sends one HTTP request per event.
- `log(level)` and `collect(list)`.
- Combinators: `when(pred, fn)`, `fanout(f, g, ...)`, `changed("col", ...)` and
  `matches(col=value)`.

Topic and URL templates can use `{table} {key} {op} {relation} {depth} {source} {source_key}`.
The default payload is the event as JSON.

## Schema rules

- `schema` is DDL text or a path to a `.sql` file. On an empty database brm runs it. On an
  existing database the tables must match the DDL; otherwise `SchemaMismatch` is raised.
  There are no migrations yet.
- brm turns on `PRAGMA foreign_keys` and WAL. It uses one connection behind a lock, which
  suits SQLite's single writer.
- Changes made outside brm, such as from the `sqlite3` shell, by `ON DELETE CASCADE`, or by
  SQL triggers, do not fire handlers. Cascaded rows are reached by an `"ancestors"` walk
  from the row deleted through brm.

## Development

```sh
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

`examples/iot` is a site → device → sensor → reading demo. To run it with a broker:
`BRM_MQTT=localhost uvicorn examples.iot.app:app`.
