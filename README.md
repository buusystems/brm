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

## How propagation works

A generic schema. Every table `x` that holds a foreign key to `y` is a *child* of `y`.
`c` references both `b1` and `b2` (a diamond), and `d` hangs below `c`. The same shape
continues to any depth or width.

```sql
CREATE TABLE a  (id INTEGER PRIMARY KEY);
CREATE TABLE b1 (id INTEGER PRIMARY KEY, a_id  INT REFERENCES a(id));
CREATE TABLE b2 (id INTEGER PRIMARY KEY, a_id  INT REFERENCES a(id));
CREATE TABLE c  (id INTEGER PRIMARY KEY, b1_id INT REFERENCES b1(id), b2_id INT REFERENCES b2(id));
CREATE TABLE d  (id INTEGER PRIMARY KEY, c_id  INT REFERENCES c(id));
```

```mermaid
erDiagram
    a  ||--o{ b1 : "b1.a_id"
    a  ||--o{ b2 : "b2.a_id"
    b1 ||--o{ c  : "c.b1_id"
    b2 ||--o{ c  : "c.b2_id"
    c  ||--o{ d  : "d.c_id"
```

Suppose row `c #7` is updated. brm walks out from it one FK hop at a time; each column below
is one BFS level. Going **up**, it fires `watch="descendants"` handlers on the parents.
Going **down**, it fires `watch="ancestors"` handlers on the children. Handlers on `c #7`
itself are `watch="self"`.

```mermaid
flowchart LR
    C7(["c #7<br/>UPDATE<br/>fires watch=self"])

    subgraph D1 ["depth 1"]
        B13["b1 #3<br/>fires watch=descendants"]
        B25["b2 #5<br/>fires watch=descendants"]
        D40["d #40<br/>fires watch=ancestors"]
        D41["d #41<br/>fires watch=ancestors"]
    end

    subgraph D2 ["depth 2"]
        A1["a #1<br/>fires watch=descendants"]
    end

    C7 -- "up: c.b1_id" --> B13
    C7 -- "up: c.b2_id" --> B25
    C7 -- "down: d.c_id" --> D40
    C7 -- "down: d.c_id" --> D41
    B13 -- "up: b1.a_id" --> A1
    B25 -. "a #1 already visited:<br/>fires once per change" .-> A1
```

- The two walks are separate. The up-walk never turns around to visit siblings: `b1 #3`'s
  other `c` rows are not reached. The down-walk never climbs back up.
- `depth=1` on a handler on `a` would not fire here, because `a #1` is 2 hops away.
  `source="c"` or `ops="update"` narrow a handler further.
- Branches with nothing listening are pruned before any row is fetched. If no table at or
  past `a` has a reachable handler, `b1 #3 → a #1` is never looked up.
- If the update had moved `c #7` from `b1 #3` to `b1 #9`, both `b1 #3` and `b1 #9` would be
  visited.

When it happens, relative to the write:

```mermaid
sequenceDiagram
    participant Client
    participant API as brm (FastAPI)
    participant DB as SQLite (tables + _brm_outbox)
    participant Q as Dispatcher (any process)
    participant H as Handlers (MQTT, webhook, ...)
    Client->>API: PATCH /c/7
    API->>DB: BEGIN, UPDATE c
    API->>DB: walk FK graph (SELECTs)
    API->>DB: INSERT one outbox job per (handler, row reached)
    API->>DB: COMMIT (row change and jobs together, or neither)
    API-->>Client: 200 (does not wait for handlers)
    API-)Q: wake up
    loop until the outbox is empty
        Q->>DB: claim jobs under a lease
        Q->>H: fn(event)
        alt success
            Q->>DB: delete job
        else error, attempts left
            Q->>DB: back to pending after backoff * 2^n
        else out of attempts
            Q->>DB: mark dead (requeue with POST /_brm/outbox/requeue)
        end
    end
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
| GET | `/_brm/outbox` | job counts per status and handler |
| GET | `/_brm/outbox/{dead,pending,running}` | list jobs with their events and last error (`?handler=`) |
| POST | `/_brm/outbox/requeue` | retry dead jobs: all, `{"ids": [...]}` or `{"handler": "..."}` |
| DELETE | `/_brm/outbox/dead` | discard dead jobs (`?handler=`) |

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

### When they run: the durable outbox

1. A write plans its walk **inside its transaction**. A write is a call over HTTP, a call
   to `app.insert/update/delete`, or several grouped in `with app.transaction() as tx:`.
   Inserts and updates are planned after the write and deletes before it, so rows that
   `ON DELETE CASCADE` will remove are still reached.
2. Each (handler, row reached) pair becomes a row in **`_brm_outbox`**, written in that same
   transaction. The change and its jobs commit together. A rollback leaves no jobs, and a
   crash after commit loses none.
3. A **dispatcher** claims jobs under a lease (`App(lease=60)` seconds), runs the handler,
   and deletes the job. It is woken right after each commit and otherwise polls every
   `App(poll=0.25)` seconds. A slow or failing handler never blocks or rolls back a write.
4. If a handler raises, the job goes back to pending and waits `backoff * 2**n` before the
   next attempt; jobs behind it are not held up. After `retries` extra attempts it is
   marked **dead**. Dead jobs stay in the table with their event and last error, are passed
   to `App(on_error=...)`, and can be requeued with `app.outbox.requeue()` or
   `POST /_brm/outbox/requeue`.
5. If a dispatcher dies mid-job, its lease runs out and another dispatcher (or the same
   one after a restart) claims the job again. A crash does not use up `retries`. A job
   whose runs keep dying is marked dead after `App(max_reclaims=3)` takeovers, so one
   handler that crashes the process cannot take the queue down with it.

**Delivery is at-least-once, so make handlers idempotent.** For example, use
`ev.change.key` together with `ev.change.op` as a deduplication key downstream.

Order: with the default `concurrency=1`, jobs start in commit order, and in walk order
within a change. A job waiting to retry is overtaken by the jobs behind it. Sync handlers run
in a thread pool, and `async def` handlers are awaited.

**Handler names identify jobs across restarts.** A name defaults to `"<table>:<function name>"`,
and must be unique (pass `name=` to tell two handlers apart). Renaming a handler leaves
any jobs still queued under the old name waiting. Drain the queue first, or clear them with
`app.outbox.purge("pending", handler=old_name)`.

**Several processes** can share one database file, for example several uvicorn workers, or
API processes plus separate worker processes. Each claim is exclusive, so every job runs in
one process. The process that writes is the one that plans, so **every process must
register every handler**. `App(dispatch=False)` makes a process that only writes and leaves
running the handlers to the others.

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

`app.drain()` waits until no job for this process's handlers is pending or running, which
is useful in tests and scripts. `app.off(fn)` unregisters a handler. `app.outbox.stats()`
shows the queue.

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
- brm turns on `PRAGMA foreign_keys` and WAL. Each process uses one connection behind a
  lock, which suits SQLite's single writer. Tables named `_brm_*` belong to brm and are
  left out of the API and the schema check.
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

## License

brm is free software under the [GNU General Public License v3.0 or later](LICENSE).
