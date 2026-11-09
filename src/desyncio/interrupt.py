"""Interrupts: ``sim.interruptible()`` and ``sim.interrupt(task, cause)`` (INT-01).

The analogy: this is ``asyncio.timeout()`` with a different trigger
--------------------------------------------------------------------
You already know this pattern::

    try:
        async with asyncio.timeout(10):      # (1) mark the code that may be cut short
            await something_slow()
    except TimeoutError:                      # (3) the *user's* frame sees TimeoutError
        ...

``asyncio.timeout()`` schedules a timer. When it fires, the timer callback
calls ``task.cancel()`` on the task that entered the block. The task gets a
``CancelledError`` at its current ``await``; the cancellation travels up
through the code in the block, so every asyncio primitive (``Lock``,
``Semaphore``, ``Queue``, ``Condition``, ``TaskGroup``, ...) runs its normal
"I was cancelled" cleanup. Finally the block's ``__aexit__`` sees the
``CancelledError``, recognises it as *its own* (not a real cancel), and raises
``TimeoutError`` instead (this is step 3).

``sim.interruptible()`` is the same thing with the trigger replaced. There is
no timer; another process calls ``sim.interrupt(task, cause)``::

    async def worker():
        try:
            async with sim.interruptible():        # (1) mark interruptible code
                await machine.work(10)
        except Interrupt as exc:                    # (3) the user's frame sees it
            print("interrupted because", exc.cause)

    async def breakdown(worker_task):
        await sim.timeout(3)
        sim.interrupt(worker_task, cause="machine broke")   # (2) plays the timer

``Timeout`` in CPython       here
---------------------------  ------------------------------------------------
``timeout(10)``              ``sim.interruptible()`` (returns an ``Interruptible``)
timer fires ``_on_timeout``  another task calls ``sim.interrupt(task, cause)``
``task.cancel()``            ``task.cancel()``
``__aexit__`` -> TimeoutError ``__aexit__`` -> ``Interrupt(cause)``
``Timeout.expired()``        ``Interruptible.interrupted``

Why the translation happens in ``__aexit__``
--------------------------------------------
The ``CancelledError`` must not reach the user's ``except Interrupt:`` clause
untranslated, and only the code *around* the ``async with`` can catch what the
block raises. ``__aexit__`` runs in the target task's own frame, at the exact
point where the cancellation leaves the block. That is the one place where a
``CancelledError`` can still be swapped for another exception. Translating
anywhere else (in the interrupter, or in a callback) cannot work, because the
exception is raised inside the target's coroutine.

The cancel/uncancel bookkeeping (why real cancels still win)
------------------------------------------------------------
``Task.cancel()`` increments a counter, ``Task.cancelling()``, and
``Task.uncancel()`` decrements it. The block records ``cancelling()`` on
entry. On exit after an interrupt it calls ``uncancel()`` once, which undoes
the cancel *we* requested. If the result is still above the recorded value,
someone else (a real ``task.cancel()``, an enclosing ``TaskGroup`` or
``asyncio.timeout()``) has also asked for cancellation, so the
``CancelledError`` is left alone and propagates: a real cancel always wins.
Otherwise the cancellation was ours and becomes ``Interrupt``. ``uncancel()``
undoes only the interrupt's own cancel, so ``cancelling()`` is back to its
value before the block only if the interrupt was the only cancel. If a real
cancel also fired it stays one higher: that is how an enclosing
``asyncio.timeout()`` or ``TaskGroup`` sees that a cancel is still pending
(rule 4).

Where the bookkeeping lives
---------------------------
``sim.interrupt`` runs in *another* task, so it needs to find the target's
open blocks. They are kept in a module-level ``WeakKeyDictionary`` mapping
``task -> [blocks, innermost last]``. A ``ContextVar`` would not fit: a context
belongs to the task that set it, is copied into every child task (a child
spawned inside a block would wrongly see the parent's block as its own), and
another task cannot reliably read it. A weak mapping drops an entry when the
task is garbage collected (each block holds only a ``weakref.ref`` to its
task, so the stored blocks do not keep the key alive), nothing is added to
``VirtualTimeLoop`` (D017), and it is never iterated, so its ordering cannot
affect determinism.

Provenance
----------
This is not original code: ``_State`` and ``Interruptible.__aenter__``,
``__aexit__`` and ``_deliver`` are adapted from CPython's
``Lib/asyncio/timeouts.py`` (class ``Timeout``, Python 3.13) and stay under
the PSF License Agreement v2; see ``LICENSES/PSF.txt``. The comment above
``_State`` marks the adapted part and lists the changes.

Known limits
------------
* If a ``TaskGroup`` inside the block ends with an ``ExceptionGroup`` (a child
  failed for real), the group propagates untranslated, because an
  ``ExceptionGroup`` is a real error, not our cancellation.
  ``Interruptible.interrupted`` is still ``True``.
* Code that catches ``CancelledError`` inside the block and swallows it also
  swallows the interrupt, exactly as it would swallow a timeout.
"""

import asyncio
import enum
import weakref
from types import TracebackType
from typing import Self


class Interrupt(Exception):  # noqa: N818  (public name fixed by the spec)
    """Raised by the ``sim.interruptible()`` block that ``sim.interrupt`` hit.

    It is an ordinary ``Exception``, deliberately not a ``CancelledError``:
    library code that treats ``CancelledError`` as "shut down" must not treat
    an interrupt that way. ``cause`` is the value passed to ``sim.interrupt``.
    """

    def __init__(self, cause: object = None) -> None:
        super().__init__(cause)
        self.cause = cause


# Adapted from CPython Lib/asyncio/timeouts.py, class Timeout (Python 3.13).
# Copyright (c) 2001 Python Software Foundation. Licensed under the PSF
# License Agreement v2; see LICENSES/PSF.txt.
# The adapted parts are _State and Interruptible.__aenter__, __aexit__ and
# _deliver (from Timeout._State, __aenter__, __aexit__ and _on_timeout).
# Changes from the original: no timer and no reschedule(); delivery comes
# from sim.interrupt() through a per-task registry (_open_blocks) instead of
# a loop callback; __aexit__ raises Interrupt(cause) instead of TimeoutError;
# no ExceptionGroup splice (an ExceptionGroup propagates untranslated).
class _State(enum.Enum):
    CREATED = "created"
    ENTERED = "active"
    INTERRUPTING = "interrupting"  # cancel() requested, not yet translated
    EXITED = "exited"


class Interruptible:
    """Async context manager returned by ``sim.interruptible()``.

    Use ``async with sim.interruptible() as scope:``. After the block,
    ``scope.interrupted`` tells whether ``sim.interrupt`` hit this block (like
    ``Timeout.expired()``), and ``scope.cause`` is the first cause delivered
    (``None`` if not interrupted). A scope can be entered only once.
    """

    def __init__(self) -> None:
        self._state = _State.CREATED
        # A weak reference: the registry below stores this scope as a value
        # keyed weakly by the task, so a strong reference here would keep the
        # key alive forever and the entry could never be dropped.
        self._task: weakref.ref[asyncio.Task[object]] | None = None
        self._cancelling = 0  # task.cancelling() on entry
        self._cause: object = None
        self._interrupted = False

    @property
    def interrupted(self) -> bool:
        """True once an interrupt was delivered to this block."""
        return self._interrupted

    @property
    def cause(self) -> object:
        """The cause of the interrupt delivered here, or ``None``."""
        return self._cause

    def __repr__(self) -> str:
        return f"<Interruptible [{self._state.value}] interrupted={self._interrupted}>"

    async def __aenter__(self) -> Self:
        if self._state is not _State.CREATED:
            raise RuntimeError("Interruptible has already been entered")
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Interruptible should be used inside a task")
        self._state = _State.ENTERED
        self._task = weakref.ref(task)
        self._cancelling = task.cancelling()
        _open_blocks.setdefault(task, []).append(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        assert self._task is not None
        task = self._task()
        if task is None:
            # The task is being garbage collected while suspended in the block
            # (its coroutine is closed with GeneratorExit). Its registry entry
            # is already gone with it, and there is nothing to translate.
            self._state = _State.EXITED
            return
        stack = _open_blocks.get(task)
        if stack is not None and self in stack:
            stack.remove(self)
            if not stack:
                del _open_blocks[task]
        was_interrupting = self._state is _State.INTERRUPTING
        self._state = _State.EXITED
        if not was_interrupting:
            return
        # Undo the cancel() we requested. A result above the entry value means
        # another cancel request is outstanding: that one wins.
        if task.uncancel() <= self._cancelling and exc_type is not None:
            if issubclass(exc_type, asyncio.CancelledError):
                raise Interrupt(self._cause) from exc_val

    def _deliver(self, cause: object) -> None:
        """Mark this block interrupted and cancel its task (first cause wins)."""
        if self._state is not _State.ENTERED:
            return  # already interrupting: later causes are dropped
        assert self._task is not None
        task = self._task()
        if task is None:
            return  # unreachable in practice: the caller holds the task
        self._state = _State.INTERRUPTING
        self._interrupted = True
        self._cause = cause
        task.cancel()


# task -> its open interruptible blocks, innermost last. See module docstring.
_open_blocks: weakref.WeakKeyDictionary[asyncio.Task[object], list[Interruptible]] = (
    weakref.WeakKeyDictionary()
)


def interrupt_task(task: asyncio.Task[object], cause: object = None) -> None:
    """Interrupt ``task``'s innermost ``interruptible()`` block (``Sim.interrupt``)."""
    if task.done():
        raise RuntimeError(f"cannot interrupt {task.get_name()!r}: it has finished")
    if task is asyncio.current_task():
        raise RuntimeError("a task cannot interrupt itself")
    stack = _open_blocks.get(task)
    if not stack:
        raise RuntimeError(
            f"cannot interrupt {task.get_name()!r}: it is not inside an "
            "'async with sim.interruptible()' block"
        )
    stack[-1]._deliver(cause)
