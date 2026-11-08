"""Refuse real I/O inside a simulation (AGENTS.md principle 5).

Why a simulation must not do real I/O
-------------------------------------
:class:`~desyncio.loop.VirtualTimeLoop` never waits: when nothing is ready it
jumps the virtual clock to the next timer. Real I/O needs the opposite. A
socket read completes when a remote machine decides, a subprocess when the OS
schedules it, an executor job when a worker thread finishes. None of these are
on the virtual clock, so the loop would either skip past them (virtual time
leaps ahead while the real operation is still pending) or declare a deadlock
(see D006 and :class:`~desyncio.loop.SimulationDeadlock`). Either way the
result is not deterministic. We refuse up front, with a clear error, instead.

Which loop entry points do real I/O, and how
--------------------------------------------
Every kind of real I/O reaches an asyncio loop through one of these public
:class:`asyncio.AbstractEventLoop` methods:

* **Readiness callbacks** (``add_reader``, ``add_writer``): register a real
  file descriptor with the selector (``epoll``/``kqueue``). The loop's one
  blocking call, ``selector.select()``, then reports when it is readable. This
  is the primitive that all socket and pipe support is built on.
* **Socket coroutines** (``sock_recv``, ``sock_connect``, ...): non-blocking
  syscalls that call ``add_reader``/``add_writer`` internally to wait.
* **Transports and servers** (``create_connection``, ``create_server``,
  ``create_datagram_endpoint``, ``connect_read_pipe``, ``start_tls``, ...):
  wrap real sockets or pipes in a transport, again via the selector.
* **Name lookup** (``getaddrinfo``, ``getnameinfo``): the C resolver call
  blocks, so asyncio runs it in the default thread-pool executor.
* **Subprocesses** (``subprocess_exec``, ``subprocess_shell``): ``fork``/
  ``exec`` plus pipes to the child and a child-exit watcher.
* **Executors** (``run_in_executor``, ``set_default_executor``): run a
  function on a real OS thread and wake the loop up when it finishes by writing
  a byte to the loop's *self-pipe* (that is what ``call_soon_threadsafe`` does).
  ``asyncio.to_thread`` is a thin wrapper over ``run_in_executor``.
* **Signals** (``add_signal_handler``): the OS delivers them at a real moment
  and wakes the loop through ``signal.set_wakeup_fd``.

The high-level helpers (``asyncio.open_connection``, ``asyncio.to_thread``,
``asyncio.create_subprocess_exec``, ``loop.getaddrinfo``) all funnel into the
methods above, so refusing those is enough: the helper raises
:class:`RealIOError` instead of hanging.

What is deliberately *not* refused
----------------------------------
The loop itself uses real file descriptors. ``BaseSelectorEventLoop`` creates
a self-pipe (a ``socketpair``) so another thread can wake ``select()``, and
registers it with the *private* ``_add_reader``; ``close()`` unregisters it
with ``_remove_reader``. The public ``add_reader`` is a thin wrapper around
the private one, so we override only the public names and the loop keeps
working. ``call_soon_threadsafe`` also stays available (from the loop's own
thread it is fine; from a real thread it is unsupported, see D006).
``shutdown_default_executor`` stays available because ``asyncio.Runner.close``
awaits it; it returns at once when no executor was ever created, which is
always the case here because ``run_in_executor`` is refused.

``remove_reader`` / ``remove_writer`` / ``remove_signal_handler`` are
**no-ops that return False** ("nothing was registered"), not refusals. Nothing
can have been registered, and cleanup code written for ordinary asyncio
(``finally: loop.remove_reader(fd)``) would otherwise raise a second error
that masks the real one.

``add_signal_handler`` is **refused**: signals are asynchronous, real-world
events, which makes the run depend on when the OS delivers them.
"""

from collections.abc import Callable
from typing import Any, NoReturn

# Public AbstractEventLoop methods that raise RealIOError. Checked against
# CPython 3.13 asyncio/events.py (AbstractEventLoop) by tests/test_guards.py,
# which fails if a method is added to AbstractEventLoop and not classified.
GUARDED_METHODS: tuple[str, ...] = (
    # readers, writers, signals
    "add_reader",
    "add_writer",
    "add_signal_handler",
    # socket coroutines
    "sock_recv",
    "sock_recv_into",
    "sock_recvfrom",
    "sock_recvfrom_into",
    "sock_sendall",
    "sock_sendto",
    "sock_connect",
    "sock_accept",
    "sock_sendfile",
    # connections and servers
    "create_connection",
    "create_server",
    "create_unix_connection",
    "create_unix_server",
    "create_datagram_endpoint",
    "connect_accepted_socket",
    # pipes, files, TLS
    "connect_read_pipe",
    "connect_write_pipe",
    "sendfile",
    "start_tls",
    # DNS
    "getaddrinfo",
    "getnameinfo",
    # subprocesses
    "subprocess_exec",
    "subprocess_shell",
    # executors
    "run_in_executor",
    "set_default_executor",
)

# Public methods that succeed as no-ops returning False.
SAFE_NOOP_METHODS: tuple[str, ...] = (
    "remove_reader",
    "remove_writer",
    "remove_signal_handler",
)


class RealIOError(RuntimeError):
    """A simulation tried to do real I/O (socket, subprocess, executor, ...).

    Subclasses :class:`RuntimeError`, like
    :class:`~desyncio.loop.SimulationDeadlock`: it reports misuse of the
    simulation, a bug in the model. It deliberately is **not** an
    :class:`OSError`: code that retries or ignores ``OSError`` /
    ``ConnectionError`` (very common around network calls) must not swallow
    this by accident.
    """


def _refuse(name: str) -> NoReturn:
    raise RealIOError(
        f"{name}() does real I/O, which is not allowed inside a simulation: "
        "the virtual-time loop jumps over waiting, so real sockets, "
        "subprocesses, threads and executors cannot be waited for "
        "deterministically. Model the delay with a timer instead "
        "(asyncio.sleep / loop.call_later) or use desyncio primitives."
    )


class RealIOGuardMixin:
    """Mixin that makes an event loop refuse real I/O.

    List it *before* the concrete loop class so these overrides win in the
    method resolution order. The overrides are plain (non-``async``) functions
    even where asyncio's version is a coroutine: the error is raised at the
    call, so a missing ``await`` cannot hide it. ``await loop.sock_recv(...)``
    still raises :class:`RealIOError` at the same place.

    Most signatures are ``*args, **kwargs`` because the arguments are
    irrelevant to a refusal; the ``NoReturn`` result is compatible with every
    base return type, so mypy accepts them. The four methods that take a
    callback plus ``*args`` (``add_reader``, ``add_writer``,
    ``add_signal_handler``, ``run_in_executor``) repeat typeshed's variadic
    generic signature, because mypy rejects a bare ``*args`` there.
    """

    # readers, writers, signals -------------------------------------------
    def add_reader[*Ts](
        self, fd: Any, callback: Callable[[*Ts], Any], *args: *Ts
    ) -> NoReturn:
        _refuse("add_reader")

    def add_writer[*Ts](
        self, fd: Any, callback: Callable[[*Ts], Any], *args: *Ts
    ) -> NoReturn:
        _refuse("add_writer")

    def add_signal_handler[*Ts](
        self, sig: int, callback: Callable[[*Ts], object], *args: *Ts
    ) -> NoReturn:
        _refuse("add_signal_handler")

    def remove_reader(self, *args: Any, **kwargs: Any) -> bool:
        """No-op: nothing can have been registered. See module docstring."""
        return False

    def remove_writer(self, *args: Any, **kwargs: Any) -> bool:
        """No-op: nothing can have been registered. See module docstring."""
        return False

    def remove_signal_handler(self, *args: Any, **kwargs: Any) -> bool:
        """No-op: nothing can have been registered. See module docstring."""
        return False

    # socket coroutines ---------------------------------------------------
    def sock_recv(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_recv")

    def sock_recv_into(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_recv_into")

    def sock_recvfrom(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_recvfrom")

    def sock_recvfrom_into(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_recvfrom_into")

    def sock_sendall(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_sendall")

    def sock_sendto(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_sendto")

    def sock_connect(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_connect")

    def sock_accept(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_accept")

    def sock_sendfile(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sock_sendfile")

    # connections and servers ---------------------------------------------
    def create_connection(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("create_connection")

    def create_server(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("create_server")

    def create_unix_connection(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("create_unix_connection")

    def create_unix_server(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("create_unix_server")

    def create_datagram_endpoint(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("create_datagram_endpoint")

    def connect_accepted_socket(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("connect_accepted_socket")

    # pipes, files, TLS ---------------------------------------------------
    def connect_read_pipe(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("connect_read_pipe")

    def connect_write_pipe(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("connect_write_pipe")

    def sendfile(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("sendfile")

    def start_tls(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("start_tls")

    # DNS -----------------------------------------------------------------
    def getaddrinfo(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("getaddrinfo")

    def getnameinfo(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("getnameinfo")

    # subprocesses --------------------------------------------------------
    def subprocess_exec(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("subprocess_exec")

    def subprocess_shell(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("subprocess_shell")

    # executors -----------------------------------------------------------
    def run_in_executor[*Ts, T](
        self, executor: Any, func: Callable[[*Ts], T], *args: *Ts
    ) -> NoReturn:
        _refuse("run_in_executor")

    def set_default_executor(self, *args: Any, **kwargs: Any) -> NoReturn:
        _refuse("set_default_executor")
