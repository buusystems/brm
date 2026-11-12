"""Ready-made handlers and small combinators. A handler is any ``fn(event)`` (sync
or async); these build such functions."""

from __future__ import annotations

import inspect
import json
import logging
import threading
import urllib.request
from typing import Any, Callable

from .hooks import Event

Pred = Callable[[Event], bool]


def _fields(ev: Event) -> dict[str, Any]:
    return {
        "table": ev.table,
        "key": ",".join(str(k) for k in ev.key),
        "via": ev.via or "",
        "depth": ev.depth,
        "op": ev.change.op,
        "source": ev.change.table,
        "source_key": ",".join(str(k) for k in ev.change.key),
    }


def to_json(ev: Event) -> str:
    return json.dumps(ev.to_dict(), default=str)


# -- combinators ---------------------------------------------------------------


def when(pred: Pred, fn: Callable[[Event], Any]) -> Callable[[Event], Any]:
    """Run ``fn`` only when ``pred(event)`` holds."""
    if inspect.iscoroutinefunction(fn):
        async def agated(ev: Event):
            if pred(ev):
                return await fn(ev)
        agated.__name__ = f"when_{getattr(fn, '__name__', 'fn')}"
        return agated

    def gated(ev: Event):
        if pred(ev):
            return fn(ev)
    gated.__name__ = f"when_{getattr(fn, '__name__', 'fn')}"
    return gated


def fanout(*fns: Callable[[Event], Any]) -> Callable[[Event], Any]:
    """One handler that runs several in order (async ones are awaited)."""
    async def run(ev: Event):
        for f in fns:
            r = f(ev)
            if inspect.isawaitable(r):
                await r
    run.__name__ = "fanout_" + "_".join(getattr(f, "__name__", "fn") for f in fns)
    return run


def changed(*cols: str) -> Pred:
    """True when the changed row's ``cols`` differ (always true for insert/delete)."""
    s = set(cols)
    return lambda ev: bool(ev.change.changed & s)


def matches(**values: Any) -> Pred:
    """True when the handler's row has these column values."""
    return lambda ev: all(ev.row.get(k) == v for k, v in values.items())


# -- sinks ---------------------------------------------------------------------


def log(level: int = logging.INFO, logger: logging.Logger | None = None) -> Callable[[Event], None]:
    lg = logger or logging.getLogger("brm.events")

    def log_event(ev: Event) -> None:
        f = _fields(ev)
        lg.log(level, "%s %s.%s -> %s %s (%s, depth %d)", f["op"], f["source"], f["source_key"],
               f["table"], f["key"], f["via"] or "self", f["depth"])
    return log_event


def collect(into: list) -> Callable[[Event], None]:
    """Append each event to ``into`` (handy in tests and REPLs)."""
    lock = threading.Lock()

    def collect_event(ev: Event) -> None:
        with lock:
            into.append(ev)
    return collect_event


def webhook(url: str, *, method: str = "POST", headers: dict[str, str] | None = None,
            timeout: float = 10, body: Callable[[Event], str] = to_json) -> Callable[[Event], None]:
    """HTTP request per event; ``url`` may use {table} {key} {op} {via} {source} fields.
    Non-2xx raises, so ``retries`` on the registration applies."""
    hdrs = {"Content-Type": "application/json", **(headers or {})}

    def send_webhook(ev: Event) -> None:
        req = urllib.request.Request(url.format(**_fields(ev)), data=body(ev).encode(),
                                     method=method, headers=hdrs)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read()
    return send_webhook


class MQTT:
    """A lazily connected paho-mqtt client shared by any number of ``publish`` handlers.

    >>> bus = MQTT("broker.local")
    >>> app.on("device", bus.publish("alerts/{table}/{key}"), via="reading")
    """

    def __init__(self, host: str = "localhost", port: int = 1883, *, client_id: str = "",
                 username: str | None = None, password: str | None = None, tls: bool = False,
                 keepalive: int = 60, timeout: float = 10):
        self.host, self.port, self.keepalive, self.timeout = host, port, keepalive, timeout
        self._opts = dict(client_id=client_id, username=username, password=password, tls=tls)
        self._client = None
        self._lock = threading.Lock()

    def client(self):
        with self._lock:
            if self._client is None:
                try:
                    import paho.mqtt.client as mqtt
                except ImportError as e:
                    raise ImportError("MQTT handlers need paho-mqtt: pip install 'brm[mqtt]'") from e
                c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self._opts["client_id"])
                if self._opts["username"]:
                    c.username_pw_set(self._opts["username"], self._opts["password"])
                if self._opts["tls"]:
                    c.tls_set()
                c.connect(self.host, self.port, self.keepalive)
                c.loop_start()
                self._client = c
            return self._client

    def publish(self, topic: str = "brm/{table}/{key}", *, qos: int = 1, retain: bool = False,
                payload: Callable[[Event], str | bytes] = to_json) -> Callable[[Event], None]:
        """Handler publishing ``payload(event)`` to ``topic`` (formatted with
        {table} {key} {op} {via} {depth} {source} {source_key}). Waits for the
        broker to accept it, so failures raise and ``retries`` apply."""

        def mqtt_publish(ev: Event) -> None:
            info = self.client().publish(topic.format(**_fields(ev)), payload(ev), qos=qos, retain=retain)
            info.wait_for_publish(self.timeout)
            if not info.is_published():
                raise TimeoutError(f"MQTT publish to {self.host}:{self.port} not acknowledged")
        return mqtt_publish

    def close(self) -> None:
        with self._lock:
            if self._client is not None:
                self._client.loop_stop()
                self._client.disconnect()
                self._client = None


def mqtt(topic: str = "brm/{table}/{key}", host: str = "localhost", port: int = 1883, **kw: Any):
    """Shorthand for ``MQTT(host, port).publish(topic, **kw)``."""
    pub_kw = {k: kw.pop(k) for k in ("qos", "retain", "payload") if k in kw}
    return MQTT(host, port, **kw).publish(topic, **pub_kw)
