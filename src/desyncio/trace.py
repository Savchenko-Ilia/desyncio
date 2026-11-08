"""The trace hook: observe every task start, resume, finish and cancel.

What a trace record is
----------------------
``TraceRecord(t, seq, task_name, kind)``:

* ``t``: virtual time (``loop.time()``) at which it happened;
* ``seq``: 0, 1, 2, ... per loop (so per run), in emission order;
* ``task_name``: ``Task.get_name()``; deterministic, see
  :meth:`desyncio.loop.VirtualTimeLoop.create_task`;
* ``kind``: one of ``"start"``, ``"resume"``, ``"finish"``, ``"cancel"``.

A *step* is one run of a task until it next awaits something that is not
ready. The first step records ``start``; every later step records ``resume``.
A task that ends (return, or any exception) records ``finish``; one that ends
because ``CancelledError`` propagated out of it records ``cancel``. Within a
step the order is ``start``/``resume``, then (if the task ends) ``finish`` or
``cancel``. A task cancelled before it ever ran records ``start`` then
``cancel``.

How it observes steps
---------------------
Each task is driven by ``Task.__step``, which is C code (``_asyncio``) and
cannot be hooked. Every step does the same thing, though: it calls
``coro.send(value)`` or ``coro.throw(exc)`` on the task's coroutine, and
learns the outcome from what comes back (a yielded future, ``StopIteration``
for a return, or an exception). So when tracing is on, the loop wraps the
user's coroutine in :class:`_TracedCoroutine`, a small object with the
coroutine protocol that records around the inner ``send`` / ``throw``. The C
``Task`` accepts it because ``asyncio.iscoroutine`` is true for any
``collections.abc.Coroutine``.

When tracing is off nothing is wrapped: the task runs the user's own
coroutine object and no tracing code is on any path (zero cost).

Sink errors
-----------
The sink is called synchronously inside the step. If it raises, the exception
propagates out of ``send``/``throw`` like any exception from the task's code:
that task ends with the sink's exception (for ``desyncio.run``'s main task,
``run`` raises it). It is not swallowed. Because asyncio only re-raises
``KeyboardInterrupt``/``SystemExit`` out of a step, a sink error in a child
task surfaces when that task is awaited (or as "exception was never
retrieved"). Keep sinks simple and non-raising.
"""

import asyncio
import collections.abc
import itertools
from collections.abc import Callable, Coroutine, Generator
from typing import Any, Literal, NamedTuple

Kind = Literal["start", "resume", "finish", "cancel"]


class TraceRecord(NamedTuple):
    """One observation: task ``task_name`` did ``kind`` at virtual time ``t``."""

    t: float
    seq: int
    task_name: str
    kind: Kind


TraceSink = Callable[[TraceRecord], object]
"""Any callable taking one :class:`TraceRecord`."""


class ListSink:
    """A sink that appends every record to ``records``."""

    def __init__(self) -> None:
        self.records: list[TraceRecord] = []

    def __call__(self, record: TraceRecord) -> None:
        self.records.append(record)


class Tracer:
    """Stamps records with virtual time and a per-run sequence number."""

    def __init__(self, sink: TraceSink, time: Callable[[], float]) -> None:
        self._sink = sink
        self._time = time
        self._seq = itertools.count()

    def emit(self, task_name: str, kind: Kind) -> None:
        self._sink(TraceRecord(self._time(), next(self._seq), task_name, kind))


class _TracedCoroutine(collections.abc.Coroutine[Any, Any, Any]):
    """Wraps a coroutine and records around each ``send`` / ``throw``.

    Only used when tracing is on. ``Task.__step`` calls ``send(None)`` the
    first time and ``send(None)`` or ``throw(exc)`` afterwards, so:

    * first call: ``start``; later calls: ``resume``;
    * ``StopIteration`` (the coroutine returned) or any other exception:
      ``finish``;
    * ``CancelledError`` escaping: ``cancel``.

    Awaiting the wrapper directly (not what a Task does) bypasses tracing.
    """

    __slots__ = ("_coro", "_name", "_started", "_tracer")

    def __init__(self, coro: Coroutine[Any, Any, Any], name: str, tracer: Tracer):
        self._coro = coro
        self._name = name
        self._tracer = tracer
        self._started = False

    def _enter(self) -> None:
        kind: Kind = "resume" if self._started else "start"
        self._started = True
        self._tracer.emit(self._name, kind)

    def _run(self, call: Callable[[], Any]) -> Any:
        self._enter()
        try:
            return call()
        except StopIteration:
            self._tracer.emit(self._name, "finish")
            raise
        except BaseException as exc:
            # CancelledError is a BaseException, not an Exception. Anything
            # that is not a cancellation is a normal (if failed) finish.
            cancelled = isinstance(exc, asyncio.CancelledError)
            kind: Kind = "cancel" if cancelled else "finish"
            self._tracer.emit(self._name, kind)
            raise

    def send(self, value: Any) -> Any:
        return self._run(lambda: self._coro.send(value))

    def throw(self, typ: Any, val: Any = None, tb: Any = None) -> Any:
        if val is None and tb is None:
            return self._run(lambda: self._coro.throw(typ))
        return self._run(lambda: self._coro.throw(typ, val, tb))

    def close(self) -> None:
        self._coro.close()

    def __await__(self) -> Generator[Any, Any, Any]:
        return self._coro.__await__()

    def __getattr__(self, attr: str) -> Any:
        # __name__, __qualname__, cr_frame, cr_code, ...: used by Task.__repr__
        # and Task.get_stack. Delegate to the real coroutine.
        return getattr(self._coro, attr)

