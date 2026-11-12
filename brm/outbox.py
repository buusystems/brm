"""Durable delivery: the transactional outbox and the dispatcher that drains it.

Every (handler, event) pair a change reaches is written to ``_brm_outbox`` inside
the same transaction as the change. Nothing is queued if it rolls back, and nothing
committed is lost if the process dies. Dispatchers (any number of threads or
processes sharing the database file) claim jobs under a time lease, run them and
delete them. Failures are retried with exponential backoff, then marked ``dead``.
A job whose lease runs out, because its dispatcher crashed, is claimed again.
Delivery is therefore at-least-once: write handlers to be idempotent.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import socket
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from .hooks import Event, Handler, Registry

if TYPE_CHECKING:
    from .store import Store

log = logging.getLogger("brm")

DDL = """
CREATE TABLE IF NOT EXISTS _brm_outbox (
    id            INTEGER PRIMARY KEY,
    handler       TEXT    NOT NULL,
    event         TEXT    NOT NULL,          -- Event as JSON
    status        TEXT    NOT NULL DEFAULT 'pending',  -- pending | running | dead
    attempts      INTEGER NOT NULL DEFAULT 0,  -- times started
    reclaims      INTEGER NOT NULL DEFAULT 0,  -- times taken over after a lease expired
    not_before    REAL    NOT NULL,          -- unix time the job may next run
    claimed_by    TEXT,
    claimed_until REAL,                      -- lease expiry while running
    last_error    TEXT,
    created       REAL    NOT NULL,
    finished      REAL
);
CREATE INDEX IF NOT EXISTS _brm_outbox_ready ON _brm_outbox (status, not_before, id);
"""


@dataclass
class Job:
    id: int
    handler: str
    event: Event
    attempts: int  # times started, this one included
    reclaims: int  # of those, starts after an earlier run's lease expired (a crash)

    @property
    def errors(self) -> int:
        """Earlier attempts that ended in a handler error."""
        return self.attempts - 1 - self.reclaims


@dataclass
class Failure:
    handler: Handler
    event: Event
    error: BaseException
    attempts: int
    job_id: int | None = None


class Outbox:
    """SQL over ``_brm_outbox``. Each statement is atomic on its own (autocommit),
    except ``enqueue``, which runs inside the caller's write transaction."""

    def __init__(self, store: "Store"):
        self.store = store
        with store.lock:
            store.conn.executescript(DDL)

    def _x(self, sql: str, args: Any = ()) -> list:
        with self.store.lock:
            return self.store.conn.execute(sql, args).fetchall()

    def enqueue(self, jobs: list[tuple[Handler, Event]]) -> None:
        now = time.time()
        self.store.conn.executemany(
            "INSERT INTO _brm_outbox (handler, event, not_before, created) VALUES (?, ?, ?, ?)",
            [(h.name, ev.dumps(), now, now) for h, ev in jobs],
        )

    def claim(self, names: list[str], me: str, lease: float, n: int) -> list[Job]:
        if not names:
            return []
        now = time.time()
        marks = ",".join("?" for _ in names)
        rows = self._x(
            f"""UPDATE _brm_outbox
                SET reclaims = reclaims + (status = 'running'),
                    status = 'running', claimed_by = ?, claimed_until = ?, attempts = attempts + 1
                WHERE id IN (
                    SELECT id FROM _brm_outbox
                    WHERE handler IN ({marks})
                      AND ((status = 'pending' AND not_before <= ?)
                           OR (status = 'running' AND claimed_until < ?))
                    ORDER BY id LIMIT ?)
                RETURNING id, handler, event, attempts, reclaims""",
            [me, now + lease, *names, now, now, n],
        )
        return sorted((Job(r[0], r[1], Event.loads(r[2]), r[3], r[4]) for r in rows), key=lambda j: j.id)

    def done(self, job: Job, me: str) -> None:
        self._x("DELETE FROM _brm_outbox WHERE id = ? AND claimed_by = ?", (job.id, me))

    def retry_later(self, job: Job, me: str, at: float, error: str) -> None:
        self._x(
            "UPDATE _brm_outbox SET status = 'pending', not_before = ?, last_error = ?, "
            "claimed_by = NULL, claimed_until = NULL WHERE id = ? AND claimed_by = ?",
            (at, error, job.id, me))

    def release(self, job: Job, me: str) -> None:
        """Hand a claimed job back untouched (its handler went away mid-claim)."""
        self._x(
            "UPDATE _brm_outbox SET status = 'pending', attempts = attempts - 1, "
            "claimed_by = NULL, claimed_until = NULL WHERE id = ? AND claimed_by = ?",
            (job.id, me))

    def bury(self, job: Job, me: str, error: str) -> None:
        self._x(
            "UPDATE _brm_outbox SET status = 'dead', last_error = ?, finished = ?, "
            "claimed_by = NULL, claimed_until = NULL WHERE id = ? AND claimed_by = ?",
            (error, time.time(), job.id, me))

    # -- inspection and repair ------------------------------------------------

    def outstanding(self, names: list[str] | None = None) -> int:
        """Jobs still to run (pending or running), optionally only for ``names``."""
        sql = "SELECT count(*) FROM _brm_outbox WHERE status IN ('pending', 'running')"
        args: list = []
        if names is not None:
            if not names:
                return 0
            sql += f" AND handler IN ({','.join('?' for _ in names)})"
            args = list(names)
        return self._x(sql, args)[0][0]

    def stats(self) -> dict:
        """Counts per status, plus per handler."""
        out: dict = {"pending": 0, "running": 0, "dead": 0, "handlers": {}}
        for handler, status, n in self._x(
                "SELECT handler, status, count(*) FROM _brm_outbox GROUP BY handler, status"):
            out[status] = out.get(status, 0) + n
            out["handlers"].setdefault(handler, {})[status] = n
        return out

    def list(self, status: str = "dead", limit: int = 100, handler: str | None = None) -> list[dict]:
        sql = ("SELECT id, handler, event, status, attempts, not_before, last_error, created, finished "
               "FROM _brm_outbox WHERE status = ?")
        args: list = [status]
        if handler is not None:
            sql += " AND handler = ?"
            args.append(handler)
        sql += " ORDER BY id LIMIT ?"
        args.append(limit)
        cols = ["id", "handler", "event", "status", "attempts", "not_before", "last_error", "created", "finished"]
        out = []
        for r in self._x(sql, args):
            d = dict(zip(cols, r))
            d["event"] = Event.loads(d["event"]).to_dict()
            out.append(d)
        return out

    def requeue(self, ids: list[int] | None = None, handler: str | None = None) -> int:
        """Put dead jobs back in the queue with fresh attempts. Returns how many."""
        sql = ("UPDATE _brm_outbox SET status = 'pending', attempts = 0, reclaims = 0, not_before = ?, finished = NULL "
               "WHERE status = 'dead'")
        args: list = [time.time()]
        if ids is not None:
            if not ids:
                return 0
            sql += f" AND id IN ({','.join('?' for _ in ids)})"
            args += ids
        if handler is not None:
            sql += " AND handler = ?"
            args.append(handler)
        return len(self._x(sql + " RETURNING id", args))

    def purge(self, status: str = "dead", handler: str | None = None) -> int:
        """Delete jobs, e.g. dead ones, or pending ones for a handler that was removed."""
        if status not in ("dead", "pending"):
            raise ValueError("purge only 'dead' or 'pending' jobs")
        sql = "DELETE FROM _brm_outbox WHERE status = ?"
        args: list = [status]
        if handler is not None:
            sql += " AND handler = ?"
            args.append(handler)
        return len(self._x(sql + " RETURNING id", args))


class Dispatcher:
    """Claims and runs outbox jobs on a private asyncio loop in a daemon thread.

    Jobs are planned by the process that writes, so every writer must register
    every handler. A dispatcher claims only jobs whose handler it has, and claims
    are exclusive, so any number of dispatchers can share the database file. With ``concurrency=1``, jobs start in commit-then-walk
    order. A job waiting to retry does not hold up the jobs behind it. Sync
    handlers run in a thread pool, and ``async def`` handlers are awaited.
    """

    def __init__(self, outbox: Outbox, registry: Registry, *, concurrency: int = 1,
                 on_error: Callable[[Failure], Any] | None = None, poll: float = 0.25,
                 lease: float = 60.0, batch: int = 32, max_reclaims: int = 3):
        self.outbox = outbox
        self.registry = registry
        self.concurrency = concurrency
        self.on_error = on_error
        self.poll = poll
        self.lease = lease
        self.batch = batch
        self.max_reclaims = max_reclaims
        self.me = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.failures: deque[Failure] = deque(maxlen=1000)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake: asyncio.Event | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._lock = threading.Lock()

    @property
    def running(self) -> bool:
        return self._thread is not None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._started.clear()
            self._thread = threading.Thread(target=self._run, name="brm-dispatch", daemon=True)
            self._thread.start()
        self._started.wait()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._wake = asyncio.Event()
        workers = [loop.create_task(self._worker()) for _ in range(self.concurrency)]
        self._started.set()
        try:
            loop.run_forever()
        finally:
            for w in workers:
                w.cancel()
            loop.run_until_complete(asyncio.gather(*workers, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

    def wake(self) -> None:
        """New jobs were committed: look now instead of at the next poll."""
        loop, ev = self._loop, self._wake
        if loop is not None and ev is not None:
            try:
                loop.call_soon_threadsafe(ev.set)
            except RuntimeError:  # loop closed during shutdown
                pass

    async def _worker(self) -> None:
        loop = asyncio.get_running_loop()
        assert self._wake is not None
        while True:
            try:
                names = list(self.registry.by_name)
                jobs = await loop.run_in_executor(
                    None, self.outbox.claim, names, self.me, self.lease, self.batch)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - e.g. database busy; try again shortly
                log.exception("brm: claiming outbox jobs failed")
                jobs = []
            if not jobs:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), self.poll)
                except asyncio.TimeoutError:
                    pass
                continue
            for job in jobs:
                await self._run_one(job)

    async def _run_one(self, job: Job) -> None:
        loop = asyncio.get_running_loop()
        h = self.registry.by_name.get(job.handler)
        db = lambda f, *a: loop.run_in_executor(None, f, job, self.me, *a)  # noqa: E731
        if h is None:  # unregistered after we claimed it
            await db(self.outbox.release)
            return
        ev = job.event
        if job.reclaims > self.max_reclaims:  # its runs keep dying: likely it crashes us
            await db(self.outbox.bury, f"lease expired {job.reclaims} times (dispatcher died mid-run)")
            return
        try:
            if inspect.iscoroutinefunction(h.fn):
                await h.fn(ev)
            else:
                res = await loop.run_in_executor(None, h.fn, ev)
                if inspect.isawaitable(res):
                    await res
        except Exception as e:  # noqa: BLE001 - handler errors must not kill the worker
            err = f"{type(e).__name__}: {e}"
            errors = job.errors + 1
            if errors <= h.retries:
                at = time.time() + h.backoff * 2 ** (errors - 1)
                await db(self.outbox.retry_later, at, err)
                return
            await db(self.outbox.bury, err)
            f = Failure(h, ev, e, job.attempts, job.id)
            self.failures.append(f)
            log.error("brm handler %s failed on %s %s after %d attempt(s): %s",
                      h.name, ev.table, list(ev.key), job.attempts, err)
            if self.on_error is not None:
                try:
                    self.on_error(f)
                except Exception:  # noqa: BLE001
                    log.exception("brm on_error callback failed")
            return
        await db(self.outbox.done)

    def drain(self, timeout: float | None = None) -> bool:
        """Block until no job for this process's handlers is pending or running
        (including ones waiting out a retry backoff). False on timeout."""
        if not self.running:
            raise RuntimeError("dispatcher is not running (App(dispatch=False)?)")
        end = None if timeout is None else time.monotonic() + timeout
        while self.outbox.outstanding(list(self.registry.by_name)):
            if end is not None and time.monotonic() >= end:
                return False
            time.sleep(0.005)
        return True

    def stop(self) -> None:
        with self._lock:
            loop, thread = self._loop, self._thread
            if loop is None or thread is None:
                return
            loop.call_soon_threadsafe(loop.stop)
            thread.join()
            self._thread = None
            self._loop = None
            self._wake = None
