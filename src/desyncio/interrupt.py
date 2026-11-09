"""Interrupts: wake a task out of its current ``await`` with an exception.

``sim.interrupt(task, cause)`` makes the ``await`` the target is blocked on
raise :class:`Interrupt` (carrying ``cause``) at the current virtual time.
The target can catch it and carry on (a machine that breaks down, is
repaired and resumes its work), or let it escape and fail like with any other
exception.

How it works, and why it is *not* ``Task.cancel()``
===================================================

Background: what a task is waiting on
-------------------------------------

Whatever a task awaits, at the very bottom of the chain of ``await``\\ s there
is a :class:`asyncio.Future`. The task yields that future to its ``Task``
object, which remembers it in ``task._fut_waiter`` and registers its own
wake-up function as a done-callback. When the future finishes, the callback
runs ``Task.__wakeup(fut)``, which calls ``fut.result()``. If the future holds
an exception, ``result()`` raises it, and ``Task.__step`` throws that
exception into the coroutine, where it surfaces at the innermost ``await``,
travels up through every ``await`` frame (``Semaphore.acquire``,
``Queue.get``, ``sleep`` ...), and finally reaches the user's code.

Every asyncio primitive already cleans up after a *failed* wait. Look at
``Semaphore.acquire``: ``try: await fut  finally: self._waiters.remove(fut)``
followed by a ``finally`` that wakes the next waiter. ``Queue.get`` removes
its getter in a bare ``except``, ``Event.wait`` in a ``finally``,
``Condition.wait`` re-acquires its lock in a ``finally``, and ``sleep``
cancels its timer in a ``finally``. They do this for **any** exception, so
if the exception that reaches them is :class:`Interrupt`, all of that
cleanup happens for free and nothing in this module knows about any
primitive.

The mechanism: fail the future the target is waiting on
-------------------------------------------------------

``interrupt`` reads ``task._fut_waiter`` and calls ``set_exception(Interrupt)``
on it, which is public ``Future`` API. The task's own wake-up machinery then
delivers the exception. Nothing else is involved: no cancel is requested, so
``task.cancelling()`` is never touched, and ``TaskGroup``, ``timeout()`` and
``gather`` (which all react to cancels, via ``Task.cancel``/``uncancel`` and
``CancelledError``) never see anything.

Why not deliver it through cancellation and translate it back
-------------------------------------------------------------

The obvious alternative is ``task.cancel()`` and then turning the
``CancelledError`` into ``Interrupt`` and calling ``uncancel()`` (what
``asyncio.timeout()`` does for ``TimeoutError``). That translation is easy for
``timeout()`` only because its ``__aexit__`` sits in the *user's* frame. Here
the ``CancelledError`` is raised in the innermost frame and travels up
through the user's frames before anyone can translate it, and a coroutine
that has let an exception escape cannot be resumed. The only places to
intercept it would be a wrapper around every task's coroutine plus
inspection of generator internals; see the INT-01 report. Failing the wait
future gets the same cleanup, with the right exception type from the start.

The cost: a cancel's *grant-race* handling (``Semaphore`` noticing "my
future had already been resolved with a grant") is only written for
``CancelledError``. We therefore never fail a future that is already
done. See the next section.

When the target is not waiting on a pending future
--------------------------------------------------

A target can be in one of these states when ``interrupt`` is called:

* Parked on a pending future: the interrupt is delivered right now.
* Not started yet, or woken but not yet running, or in a bare
  ``await asyncio.sleep(0)``: it has no pending future to fail. The
  interrupt is queued and delivered at its next ``await`` on a pending
  future, at the same virtual time (D021).
* Parked on a future that is *already resolved* (the "grant race":
  ``release()`` just handed it the semaphore unit, but its step has not run).
  The wait has really succeeded, so the target owns the unit; failing the
  future is impossible (it is done) and undoing the grant would be wrong.
  The interrupt is queued, and arrives at the target's next ``await``, i.e.
  just after it acquired, as if it had been sent a moment later.

Queued interrupts are retried by ``loop.call_soon`` callbacks. Those run in
FIFO order in the same loop iteration (virtual time cannot advance while a
callback is ready), so the retry runs right after the target's step and
catches it at its next ``await``. A per-task queue keeps interrupts in the
order sent and delivers one per ``await``.

A real cancel always wins
-------------------------

``Task.cancel()`` sets ``_must_cancel`` (if the waited-on future is already
done) or cancels the future. In ``Task.__step`` a pending cancel replaces
whatever exception was about to be thrown with ``CancelledError``, so
a cancel that arrives together with an interrupt is delivered and is never
swallowed. If the cancel came first, the future is already done, so the
interrupt is queued, and dropped when the task finishes.

Limits
------

* A target that is awaiting another *Task* directly (``await child_task``)
  cannot be interrupted: a ``Task`` refuses ``set_exception`` (and
  cancelling it would cancel the child). ``interrupt`` raises
  ``RuntimeError``. Wrap the child with ``asyncio.shield(child)`` or use
  ``asyncio.wait`` if it must be interruptible.
* Failing the future that the target waits on is what ``Task.cancel`` does
  too (it cancels it), so a *shared* future (several tasks awaiting one
  future) is failed for all of them. Primitives use one private future per
  waiter, so this does not affect them.
* ``task._fut_waiter`` is a private attribute (the C and Python tasks both
  have it; ``asyncio``'s own debugging tools read it too).
"""

import asyncio
import weakref
from collections import deque
from typing import Any


class Interrupt(Exception):
    """Raised inside a task by ``sim.interrupt(task, cause)``.

    Not a ``CancelledError``: ``except Exception`` catches it, an uncaught
    one fails the task, and asyncio treats it as any other error.

    Attributes:
        cause: whatever the interrupter passed (default ``None``).
    """

    def __init__(self, cause: object = None) -> None:
        super().__init__(cause)
        self.cause = cause


# Interrupts sent to a task but not delivered yet, oldest first. Keyed weakly,
# so a finished task's entry disappears with the task. The dict is never
# iterated, so it cannot influence determinism. (No per-loop state: D017.)
_pending: weakref.WeakKeyDictionary[asyncio.Task[Any], deque[Interrupt]] = (
    weakref.WeakKeyDictionary()
)


def _waiting_on(task: asyncio.Task[Any]) -> asyncio.Future[Any] | None:
    """The future ``task`` is parked on, or None (not started / ready)."""
    return getattr(task, "_fut_waiter", None)


def interrupt_task(task: asyncio.Task[Any], cause: object = None) -> None:
    """Implementation of ``sim.interrupt`` (the loop check is done there).

    Raises:
        TypeError: ``task`` is not an ``asyncio.Task``.
        RuntimeError: ``task`` is finished, is the calling task, or is
            awaiting another Task directly (see the module docstring).
    """
    if not isinstance(task, asyncio.Task):
        raise TypeError(f"interrupt() needs a Task, got {type(task).__name__}")
    if task.done():
        raise RuntimeError(f"cannot interrupt {task.get_name()!r}: it has finished")
    if task is asyncio.current_task():
        raise RuntimeError(
            f"{task.get_name()!r} cannot interrupt itself (it is not awaiting)"
        )
    queue = _pending.get(task)
    if not queue:
        waiter = _waiting_on(task)
        if isinstance(waiter, asyncio.Task) and not waiter.done():
            raise RuntimeError(
                f"cannot interrupt {task.get_name()!r}: it is awaiting the Task "
                f"{waiter.get_name()!r} directly, and a Task cannot be failed "
                "from outside; await asyncio.shield(task) instead"
            )
    if queue is None:
        queue = _pending[task] = deque()
    queue.append(Interrupt(cause))
    if len(queue) == 1:  # no delivery attempt is in flight: start one
        _deliver(task)


def _deliver(task: asyncio.Task[Any]) -> None:
    """Deliver the oldest queued interrupt if ``task`` is parked, else retry.

    Invariant: while a task's queue is non-empty, exactly one retry is
    scheduled (a ``call_soon`` or a done-callback), except when this function
    runs from ``interrupt_task`` for the first interrupt.
    """
    queue = _pending.get(task)
    if not queue:
        return
    if task.done():  # finished meanwhile (e.g. a real cancel won): drop them
        queue.clear()
        return
    loop = task.get_loop()
    waiter = _waiting_on(task)
    if waiter is None or waiter.done():
        # Not parked on a pending future (see "When the target is not waiting"
        # in the module docstring). Its next step is already in the loop's
        # ready queue, ahead of this retry, so try again after it has run.
        loop.call_soon(_deliver, task)
    elif isinstance(waiter, asyncio.Task):
        # Only reachable for a queued interrupt (the first is refused up
        # front). Retry once the awaited Task finishes: the target's wake-up
        # was registered first, so it runs first and we meet it at its next
        # await.
        waiter.add_done_callback(lambda _: _deliver(task))
    else:
        # The whole mechanism: fail the future the target is waiting on. The
        # target is woken (one loop iteration later, at the same virtual time)
        # with Interrupt raised at its await.
        waiter.set_exception(queue.popleft())
        if queue:  # more to deliver, one per await: retry after the wake-up
            loop.call_soon(_deliver, task)
