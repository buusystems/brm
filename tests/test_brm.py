import asyncio
import threading

import pytest
from fastapi.testclient import TestClient

from brm import App, SchemaMismatch
from brm.handlers import changed, collect, fanout, matches, when

IOT = """
CREATE TABLE site   (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE device (id INTEGER PRIMARY KEY,
                     site_id INTEGER NOT NULL REFERENCES site(id) ON DELETE CASCADE,
                     name TEXT NOT NULL, online BOOLEAN NOT NULL DEFAULT 1);
CREATE TABLE sensor (id INTEGER PRIMARY KEY,
                     device_id INTEGER NOT NULL REFERENCES device(id) ON DELETE CASCADE,
                     kind TEXT NOT NULL, max_value REAL);
CREATE TABLE reading (id INTEGER PRIMARY KEY,
                      sensor_id INTEGER NOT NULL REFERENCES sensor(id) ON DELETE CASCADE,
                      value REAL NOT NULL);
"""


@pytest.fixture
def app():
    a = App(":memory:", schema=IOT)
    yield a
    a.close()


@pytest.fixture
def tree(app):
    """site 1 -> device 1,2 ; device 1 -> sensor 1,2 ; sensor 1 -> reading 1"""
    app.insert("site", name="lab")
    app.insert("device", site_id=1, name="d1")
    app.insert("device", site_id=1, name="d2")
    app.insert("sensor", device_id=1, kind="temp", max_value=30)
    app.insert("sensor", device_id=1, kind="hum")
    app.insert("reading", sensor_id=1, value=20)
    app.drain()
    return app


def seen(events):
    return [(e.table, e.key, e.via, e.depth) for e in events]


# -- schema ----------------------------------------------------------------------


def test_introspection(app):
    s = app.schema
    assert set(s.tables) == {"site", "device", "sensor", "reading"}
    assert s["device"].pk == ("id",)
    [fk] = s["sensor"].fks
    assert (fk.child, fk.child_cols, fk.parent, fk.parent_cols) == ("sensor", ("device_id",), "device", ("id",))
    assert [r.child for r in s["device"].refs] == ["sensor"]
    assert fk.on_delete == "CASCADE"


def test_composite_and_rowid_and_implicit_reference():
    a = App(schema="""
        CREATE TABLE a (x INTEGER, y TEXT, PRIMARY KEY (x, y));
        CREATE TABLE b (x INTEGER, y TEXT, note TEXT, FOREIGN KEY (x, y) REFERENCES a);
        CREATE TABLE c (n TEXT);
    """)
    assert a.schema["a"].pk == ("x", "y")
    [fk] = a.schema["b"].fks
    assert fk.parent_cols == ("x", "y")  # implicit: parent's primary key
    assert a.schema["c"].rowid_pk
    row = a.insert("c", n="hi")
    assert row == {"rowid": 1, "n": "hi"}
    a.insert("a", x=1, y="k")
    ev = []
    a.on("a", collect(ev), via="b.x,y")  # composite foreign key
    a.insert("b", x=1, y="k", note="n")
    a.drain()
    assert seen(ev) == [("a", (1, "k"), "b > b.x,y", 1)]
    c = TestClient(a)
    assert c.get("/a/1,k").json() == {"x": 1, "y": "k"}
    assert c.get("/c/1").json() == {"rowid": 1, "n": "hi"}
    a.close()


def test_existing_db_must_match(tmp_path):
    p = str(tmp_path / "x.db")
    App(p, schema="CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").close()
    App(p, schema="CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);").close()  # same: fine
    with pytest.raises(SchemaMismatch):
        App(p, schema="CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, w INT);")


def test_schema_from_file(tmp_path):
    f = tmp_path / "s.sql"
    f.write_text("CREATE TABLE t (id INTEGER PRIMARY KEY);")
    a = App(schema=str(f))
    assert "t" in a.schema
    a.close()


# -- CRUD over HTTP ------------------------------------------------------------------


def test_crud_routes(tree):
    c = TestClient(tree)
    assert c.get("/device").json()[0] == {"id": 1, "site_id": 1, "name": "d1", "online": True}
    r = c.post("/sensor", json={"device_id": 2, "kind": "co2", "max_value": 900})
    assert r.status_code == 201 and r.json()["id"] == 3
    assert c.get("/sensor/3").json()["kind"] == "co2"
    assert c.patch("/sensor/3", json={"max_value": 1000}).json()["max_value"] == 1000
    assert [s["id"] for s in c.get("/sensor", params={"device_id": 1}).json()] == [1, 2]
    assert [s["id"] for s in c.get("/sensor", params={"max_value": "null"}).json()] == [2]
    assert [s["id"] for s in c.get("/sensor", params={"_order": "-id", "_limit": 2}).json()] == [3, 2]
    assert [s["id"] for s in c.get("/device/1/sensor").json()] == [1, 2]
    assert [s["id"] for s in c.get("/device/1/sensor", params={"kind": "hum"}).json()] == [2]
    assert c.delete("/sensor/3").json()["kind"] == "co2"
    assert c.get("/sensor/3").status_code == 404
    assert c.patch("/sensor/3", json={"kind": "x"}).status_code == 404


def test_crud_errors(tree):
    c = TestClient(tree)
    assert c.post("/sensor", json={"device_id": 99, "kind": "x"}).status_code == 409  # FK
    assert c.post("/site", json={"name": "lab"}).status_code == 409  # UNIQUE
    assert c.post("/sensor", json={"device_id": 1}).status_code == 422  # kind missing
    assert c.post("/sensor", json={"device_id": 1, "kind": "x", "bogus": 1}).status_code == 422
    assert c.patch("/sensor/1", json={"id": 5}).status_code == 422  # keys not patchable
    assert c.get("/sensor", params={"bogus": 1}).status_code == 400
    assert c.get("/sensor/abc").status_code == 400
    assert c.get("/_schema").json()["sensor"]["references"] == [
        {"columns": ["device_id"], "table": "device", "to": ["id"]}]


# -- hooks ----------------------------------------------------------------------------


def test_self(tree):
    ev = []
    tree.on("sensor", collect(ev))
    tree.update("sensor", 2, max_value=50)
    tree.update("sensor", 2, max_value=50)  # no-op: no event
    tree.drain()
    assert seen(ev) == [("sensor", (2,), None, 0)]
    assert ev[0].change.old["max_value"] is None and ev[0].change.new["max_value"] == 50
    assert ev[0].change.changed == {"max_value"}


def test_any_route_up(tree):
    ev = []
    for t in ("sensor", "device", "site"):
        tree.on(t, collect(ev), via="reading")
    tree.insert("reading", sensor_id=1, value=25)
    tree.drain()
    assert seen(ev) == [("sensor", (1,), "reading", 1),
                        ("device", (1,), "reading", 2),
                        ("site", (1,), "reading", 3)]
    assert [h.table for h in ev[2].path] == ["reading", "sensor", "device", "site"]
    assert ev[2].at("sensor")["kind"] == "temp" and ev[2].at("reading")["value"] == 25
    assert ev[2].change.table == "reading"


def test_exact_paths_and_ops(tree):
    ev = []
    tree.on("device", collect(ev), via="reading.sensor_id > sensor.device_id", ops="delete")
    tree.on("device", collect(ev), via="sensor.device_id", name="direct")  # sensors, not readings
    tree.insert("reading", sensor_id=1, value=1)  # neither: insert, and not a sensor
    tree.insert("sensor", device_id=2, kind="x")  # direct
    tree.delete("reading", 1)  # path
    tree.drain()
    assert seen(ev) == [("device", (2,), "sensor > sensor.device_id", 1),
                        ("device", (1,), "reading > reading.sensor_id > sensor.device_id", 2)]


def test_threshold_example(tree):
    alerts = []
    tree.on("sensor", lambda e: alerts.append(e.change.new["value"]), via="reading.sensor_id",
            ops="insert", name="over",
            where=lambda e: e.row["max_value"] is not None and e.change.new["value"] > e.row["max_value"])
    for v in (10, 31, 29, 45):
        tree.insert("reading", sensor_id=1, value=v)
    tree.insert("reading", sensor_id=2, value=99)  # sensor 2 has no threshold
    tree.drain()
    assert alerts == [31, 45]


def test_where_filters_before_queueing(tree):
    tree.dispatcher.stop()
    tree.on("sensor", lambda e: None, where=lambda e: e.row["kind"] == "temp", name="w")
    tree.on("site", lambda e: None, where=lambda e: 1 / 0, name="broken")  # fails open
    tree.update("sensor", 2, max_value=1)  # hum: filtered, never queued
    tree.update("sensor", 1, max_value=1)
    tree.update("site", 1, name="q")
    assert tree.outbox.stats()["handlers"] == {"w": {"pending": 1}, "broken": {"pending": 1}}
    tree.dispatcher.start()


def test_any_route_down(tree):
    ev = []
    tree.on("sensor", collect(ev), via="device")
    tree.on("sensor", collect(ev), via="site", name="from-site")
    tree.on("reading", collect(ev), via="site", max_hops=2)  # readings are 3 below a site
    tree.update("device", 1, online=False)
    tree.update("site", 1, name="lab2")
    tree.drain()
    assert seen(ev) == [
        ("sensor", (1,), "device", 1), ("sensor", (2,), "device", 1),
        ("sensor", (1,), "site", 2), ("sensor", (2,), "site", 2),
    ]


def test_siblings_up_then_down(tree):
    ev = []
    tree.on("sensor", collect(ev), via="sensor.device_id > <sensor.device_id")
    tree.update("sensor", 1, kind="t")
    tree.drain()
    assert seen(ev) == [("sensor", (2,), "sensor > sensor.device_id > <sensor.device_id", 2)]


def test_route_errors(app):
    from brm import RouteError
    bad = [
        ("device", "reading.value", "no foreign key on"),
        ("device", "nope", "unknown table"),
        ("device", "device", "leave via out"),
        ("sensor", "reading.sensor_id > sensor.device_id", "ends at 'device'"),
        ("site", "reading.sensor_id > device.site_id", "path is at 'sensor'"),
        ("sensor", "sensor.device_id*", "self-referencing key only"),
    ]
    for table, via, msg in bad:
        with pytest.raises(RouteError, match=msg):
            app.on(table, lambda e: None, via=via)
    a = App(schema="CREATE TABLE x (id INTEGER PRIMARY KEY); CREATE TABLE y (id INTEGER PRIMARY KEY);")
    with pytest.raises(RouteError, match="no chain"):
        a.on("x", lambda e: None, via="y")
    a.close()


def test_delete_cascade_is_walked_before_delete(tree):
    ev = []
    tree.on("reading", collect(ev), via="device")
    tree.on("sensor", collect(ev))
    tree.on("sensor", collect(ev), via="device", name="sensor-down")
    tree.delete("device", 1)
    tree.drain()
    assert seen(ev) == [("reading", (1,), "device", 2),
                        ("sensor", (1,), "device", 1), ("sensor", (2,), "device", 1)]
    assert tree.select("sensor") == [] and tree.select("reading") == []


def test_moving_a_row_notifies_old_and_new_parent(tree):
    ev = []
    tree.on("device", collect(ev), via="sensor")
    tree.update("sensor", 2, device_id=2)
    tree.drain()
    assert sorted(seen(ev)) == [("device", (1,), "sensor", 1), ("device", (2,), "sensor", 1)]


def test_bfs_vs_dfs_order():
    for order, want in [("bfs", ["s1", "s2", "s3", "r1", "r2", "r3"]),
                        ("dfs", ["s1", "r1", "r2", "s2", "s3", "r3"])]:
        a = App(schema="""
            CREATE TABLE n (id INTEGER PRIMARY KEY, name TEXT, parent INT REFERENCES n(id));
        """, order=order)
        a.insert("n", name="root")
        for name, parent in [("s1", 1), ("s2", 1), ("s3", 1), ("r1", 2), ("r2", 2), ("r3", 4)]:
            a.insert("n", name=name, parent=parent)
        names = []
        a.on("n", lambda e: names.append(e.row["name"]), via="<n.parent*")
        a.update("n", 1, name="root2")
        a.drain()
        assert names == want, order
        a.close()


def test_diamond_and_cycle_visit_once():
    a = App(schema="""
        CREATE TABLE top  (id INTEGER PRIMARY KEY);
        CREATE TABLE l    (id INTEGER PRIMARY KEY, top_id INT REFERENCES top(id));
        CREATE TABLE r    (id INTEGER PRIMARY KEY, top_id INT REFERENCES top(id));
        CREATE TABLE leaf (id INTEGER PRIMARY KEY, l_id INT REFERENCES l(id), r_id INT REFERENCES r(id));
        CREATE TABLE node (id INTEGER PRIMARY KEY, parent INT REFERENCES node(id));
    """)
    a.insert("top", id=1)
    a.insert("l", top_id=1)
    a.insert("r", top_id=1)
    ev = []
    a.on("top", collect(ev), via="leaf")  # two routes, one event
    a.on("top", collect(ev), via="leaf.r_id > r.top_id", name="right-only")
    a.insert("leaf", l_id=1, r_id=1)
    # self-referencing cycle: 1 -> 2 -> 3 -> 1
    a.insert("node", id=1)
    a.insert("node", id=2, parent=1)
    a.insert("node", id=3, parent=2)
    a.update("node", 1, parent=3)
    a.drain()
    assert sorted(seen(ev)) == [("top", (1,), "leaf", 2), ("top", (1,), "leaf > leaf.r_id > r.top_id", 2)]
    assert [h.table for h in ev[0].path][1] in ("l", "r")
    ev.clear()
    a.on("node", collect(ev), via="node.parent*")
    a.on("node", collect(ev), via="node.parent*", max_hops=2, name="near")
    a.insert("node", id=4, parent=1)
    a.drain()
    v = "node > node.parent*"
    assert seen(ev) == [("node", (1,), v, 1), ("node", (3,), v, 2), ("node", (2,), v, 3),  # all
                        ("node", (1,), v, 1), ("node", (3,), v, 2)]  # max_hops=2
    a.close()


def test_rollback_drops_events(tree):
    ev = []
    tree.on("sensor", collect(ev))
    with pytest.raises(RuntimeError):
        with tree.transaction() as tx:
            tx.insert("sensor", {"device_id": 1, "kind": "x"})
            raise RuntimeError
    with tree.transaction() as tx:
        tx.insert("sensor", {"device_id": 1, "kind": "a"})
        tx.insert("sensor", {"device_id": 1, "kind": "b"})
        assert ev == []  # nothing runs before commit
    tree.drain()
    assert [e.row["kind"] for e in ev] == ["a", "b"]


def test_failed_delete_does_not_fire():
    a = App(schema="""
        CREATE TABLE p (id INTEGER PRIMARY KEY);
        CREATE TABLE c (id INTEGER PRIMARY KEY, p_id INT REFERENCES p(id) ON DELETE RESTRICT);
    """)
    a.insert("p", id=1)
    a.insert("c", p_id=1)
    ev = []
    a.on("p", collect(ev))
    with a.transaction() as tx:
        with pytest.raises(Exception):
            tx.delete("p", 1)
    a.drain()
    assert ev == [] and a.get("p", 1) is not None
    a.close()


def test_where_combinators_async_retries_failures(tree):
    ev, got = [], []
    tree.on("sensor", collect(ev), where=matches(kind="temp"))
    tree.on("sensor", when(changed("max_value"), collect(got)))

    async def async_handler(e):
        await asyncio.sleep(0)
        got.append("async")
    tree.on("sensor", fanout(async_handler, lambda e: got.append("sync")), ops="delete")

    calls = []

    def flaky(e):
        calls.append(1)
        if len(calls) < 3:
            raise IOError("broker down")
    tree.on("site", flaky, retries=2, backoff=0.001)
    tree.on("site", lambda e: 1 / 0, name="boom")

    tree.update("sensor", 1, kind="t2")  # kind changed: not temp after; matches row (new)
    tree.update("sensor", 2, max_value=3)
    tree.delete("sensor", 2)
    tree.update("site", 1, name="z")
    tree.drain()
    assert ev == []  # sensor 1 is now 't2', sensor 2 'hum'
    assert [g if isinstance(g, str) else g.key for g in got] == [(2,), (2,), "async", "sync"]
    assert len(calls) == 3
    [f] = tree.failures
    assert f.handler.name == "boom" and isinstance(f.error, ZeroDivisionError)


def test_off_and_handlers_writing(tree):
    def cascade(e):  # handler that writes: bump a reading under the sensor
        tree.insert("reading", sensor_id=e.row["id"], value=0)
    tree.on("sensor", cascade, ops="insert")
    tree.insert("sensor", device_id=2, kind="new")
    tree.drain()
    assert tree.select("reading", {"sensor_id": 3})[0]["value"] == 0
    tree.off(cascade)
    tree.insert("sensor", device_id=2, kind="new2")
    tree.drain()
    assert tree.select("reading", {"sensor_id": 4}) == []


def test_no_walk_without_handlers(tree, monkeypatch):
    import brm.store as st
    fetched = []
    real = st.Tx._fetch
    monkeypatch.setattr(st.Tx, "_fetch", lambda self, *a: fetched.append(a) or real(self, *a))
    tree.on("reading", lambda e: None)  # self only: nothing to walk
    tree.on("device", lambda e: None, via="sensor.device_id")
    tree.insert("reading", sensor_id=1, value=2)  # no route starts at reading: no lookups
    assert fetched == []
    tree.insert("sensor", device_id=1, kind="x")  # device 1 hop up: one lookup
    assert fetched == [("device", ("id",), (1,))]


def test_http_writes_fire_hooks(tree):
    ev = []
    tree.on("device", collect(ev), via="reading")
    with TestClient(tree) as c:
        c.post("/reading", json={"sensor_id": 2, "value": 9})
        tree.drain()
    assert seen(ev) == [("device", (1,), "reading", 2)]


def test_threads(tmp_path):
    a = App(str(tmp_path / "t.db"), schema=IOT)
    a.insert("site", name="s")
    a.insert("device", site_id=1, name="d")
    a.insert("sensor", device_id=1, kind="k")
    n = []
    a.on("site", lambda e: n.append(1), via="reading")

    def work():
        for i in range(50):
            a.insert("reading", sensor_id=1, value=i)
    ts = [threading.Thread(target=work) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    a.drain()
    assert len(n) == 200 and len(a.select("reading")) == 200
    a.close()


def test_mqtt_publish_handler(tree):
    from brm.handlers import MQTT

    class FakeInfo:
        def wait_for_publish(self, timeout): pass
        def is_published(self): return True

    class FakeClient:
        sent = []
        def publish(self, topic, payload, qos, retain):
            self.sent.append((topic, payload, qos))
            return FakeInfo()

    bus = MQTT("nowhere")
    bus._client = FakeClient()
    tree.on("device", bus.publish("alerts/{table}/{key}/{op}/{source}"), via="reading")
    tree.insert("reading", sensor_id=2, value=7)
    tree.drain()
    import json
    [(topic, payload, qos)] = FakeClient.sent
    assert topic == "alerts/device/1/insert/reading" and qos == 1
    body = json.loads(payload)
    assert body["change"]["new"]["value"] == 7 and body["via"] == "reading"


# -- durable outbox ----------------------------------------------------------------------


def test_jobs_survive_a_restart(tmp_path):
    p = str(tmp_path / "d.db")
    a = App(p, schema=IOT, dispatch=False)  # writer only: nothing runs here
    a.on("site", lambda e: None, via="device", name="ping")
    a.insert("site", name="s")
    a.insert("device", site_id=1, name="d")
    assert a.outbox.stats()["pending"] == 1
    a.close()  # "crash": the job is only in the database

    got = []
    b = App(p, schema=IOT)
    b.on("site", collect(got), via="device", name="ping")
    assert b.drain(5)
    assert seen(got) == [("site", (1,), "device", 1)]
    assert got[0].change.new == {"id": 1, "site_id": 1, "name": "d", "online": 1}
    assert b.outbox.stats()["pending"] == 0
    b.close()


def test_rollback_queues_nothing(tree):
    tree.on("sensor", lambda e: None)
    with pytest.raises(RuntimeError):
        with tree.transaction() as tx:
            tx.insert("sensor", {"device_id": 1, "kind": "x"})
            assert tree.outbox.outstanding() == 1  # visible inside the transaction
            raise RuntimeError
    assert tree.outbox.outstanding() == 0


def test_dead_letters_and_requeue(tree):
    ok = []
    broken = [True]

    def flaky(e):
        if broken[0]:
            raise IOError("down")
        ok.append(e.key)
    tree.on("device", flaky)
    tree.update("device", 1, name="x")
    tree.drain()
    st = tree.outbox.stats()
    assert st["dead"] == 1 and st["handlers"] == {"device:flaky": {"dead": 1}}
    [dead] = tree.outbox.list("dead")
    assert dead["last_error"] == "OSError: down" and dead["event"]["row"]["name"] == "x"
    broken[0] = False
    c = TestClient(tree)
    assert c.get("/_brm/outbox").json()["dead"] == 1
    assert c.post("/_brm/outbox/requeue", json={}).json() == {"requeued": 1}
    tree.drain()
    assert ok == [(1,)] and tree.outbox.stats()["dead"] == 0


def test_retry_does_not_block_later_jobs(tree):
    order = []

    def slow_fail(e):
        order.append(("fail", e.key))
        if len([o for o in order if o[0] == "fail"]) == 1:
            raise IOError
    tree.on("device", slow_fail, retries=1, backoff=0.2)
    tree.on("sensor", lambda e: order.append(("ok", e.key)), name="ok")
    tree.update("device", 1, name="x")
    tree.update("sensor", 1, kind="y")
    tree.drain()
    assert order == [("fail", (1,)), ("ok", (1,)), ("fail", (1,))]


def test_expired_lease_is_reclaimed_and_poison_buried(tree):
    import time
    got = []
    tree.dispatcher.stop()
    tree.on("site", collect(got))  # retries=0: a crash must still not cost the delivery
    tree.update("site", 1, name="n1")
    tree.update("site", 1, name="n2")
    conn = tree.store.conn
    # dispatchers that died mid-run: one job after its first start, one after 4 crashed starts
    conn.execute("UPDATE _brm_outbox SET status='running', claimed_by='dead', claimed_until=?, "
                 "attempts=1 WHERE id=(SELECT min(id) FROM _brm_outbox)", (time.time() - 1,))
    conn.execute("UPDATE _brm_outbox SET status='running', claimed_by='dead', claimed_until=?, "
                 "attempts=4, reclaims=3 WHERE id=(SELECT max(id) FROM _brm_outbox)", (time.time() - 1,))
    tree.dispatcher.start()
    tree.drain(5)
    assert [e.row["name"] for e in got] == ["n1"]
    [dead] = tree.outbox.list("dead")
    assert "lease expired 4 times" in dead["last_error"]


def test_crash_then_error_still_gets_its_retries(tree):
    import time
    calls = []

    def fails_once(e):
        calls.append(1)
        if len(calls) == 1:
            raise IOError
    tree.dispatcher.stop()
    tree.on("site", fails_once, retries=1, backoff=0.001)
    tree.update("site", 1, name="n")
    tree.store.conn.execute("UPDATE _brm_outbox SET status='running', claimed_by='dead', "
                            "claimed_until=?, attempts=1", (time.time() - 1,))
    tree.dispatcher.start()
    tree.drain(5)
    assert len(calls) == 2 and tree.outbox.stats()["dead"] == 0


def test_two_dispatchers_run_each_job_once(tmp_path):
    p = str(tmp_path / "s.db")
    a, b = App(p, schema=IOT), App(p, schema=IOT)
    ga, gb = [], []
    a.on("site", collect(ga), name="ping")  # same handler (by name) in both processes
    b.on("site", collect(gb), name="ping")
    for i in range(40):
        a.insert("site", name=f"s{i}")
    assert a.drain(5) and b.drain(5)
    keys = sorted(e.key for e in ga + gb)
    assert keys == [(i,) for i in range(1, 41)]  # every job once, none twice
    a.close(), b.close()


def test_handler_names_are_unique(app):
    def f(e): pass
    app.on("site", f)
    with pytest.raises(ValueError, match="already registered"):
        app.on("site", f, ops="insert")
    app.on("site", f, via="device")  # the route is part of the default name


def test_blob_round_trip():
    a = App(schema="CREATE TABLE b (id INTEGER PRIMARY KEY, data BLOB);")
    got = []
    a.on("b", collect(got))
    a.insert("b", data=b"\x00\xff")
    a.drain()
    assert got[0].row["data"] == b"\x00\xff"
    a.close()


def test_via_list_registers_each_route(tree):
    ev = []
    tree.on("site", collect(ev), via=["device", "reading"], name="under")
    assert {h.name for h in tree.handlers} == {"under@device", "under@reading"}
    tree.insert("reading", sensor_id=1, value=3)
    tree.update("device", 2, name="z")
    tree.drain()
    assert seen(ev) == [("site", (1,), "reading", 3), ("site", (1,), "device", 1)]


def test_example_app_loads(tmp_path, monkeypatch):
    import importlib
    monkeypatch.setenv("BRM_DB", str(tmp_path / "iot.db"))
    monkeypatch.delenv("BRM_MQTT", raising=False)
    mod = importlib.import_module("examples.iot.app")
    c = TestClient(mod.app)
    c.post("/site", json={"name": "s"})
    c.post("/device", json={"site_id": 1, "name": "d"})
    c.post("/sensor", json={"device_id": 1, "kind": "temp", "max_value": 30})
    c.post("/reading", json={"sensor_id": 1, "value": 12})
    c.post("/reading", json={"sensor_id": 1, "value": 40})
    assert mod.app.drain(5)
    mod.app.close()
