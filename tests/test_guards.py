"""Tests for the no-real-I/O guards (AGENTS.md principle 5)."""

import asyncio
import inspect
import socket
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from desyncio.guards import GUARDED_METHODS, SAFE_NOOP_METHODS, RealIOError
from desyncio.loop import VirtualTimeLoop

# Written out by hand on purpose, independent of desyncio.guards, so a method
# dropped from the implementation's list fails the first test below.
EXPECTED_REFUSED = {
    "add_reader",
    "add_writer",
    "add_signal_handler",
    "sock_recv",
    "sock_recv_into",
    "sock_recvfrom",
    "sock_recvfrom_into",
    "sock_sendall",
    "sock_sendto",
    "sock_connect",
    "sock_accept",
    "sock_sendfile",
    "create_connection",
    "create_server",
    "create_unix_connection",
    "create_unix_server",
    "create_datagram_endpoint",
    "connect_accepted_socket",
    "connect_read_pipe",
    "connect_write_pipe",
    "sendfile",
    "start_tls",
    "getaddrinfo",
    "getnameinfo",
    "subprocess_exec",
    "subprocess_shell",
    "run_in_executor",
    "set_default_executor",
}

# Public AbstractEventLoop methods that are deliberately NOT refused: they are
# scheduling, task, lifecycle or configuration calls that touch no real I/O.
NOT_GUARDED = {
    "call_at",
    "call_exception_handler",
    "call_later",
    "call_soon",
    "call_soon_threadsafe",
    "close",
    "create_future",
    "create_task",
    "default_exception_handler",
    "get_debug",
    "get_exception_handler",
    "get_task_factory",
    "is_closed",
    "is_running",
    "run_forever",
    "run_until_complete",
    "set_debug",
    "set_exception_handler",
    "set_task_factory",
    "shutdown_asyncgens",
    "shutdown_default_executor",
    "stop",
    "time",
}


def run[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    with asyncio.Runner(loop_factory=VirtualTimeLoop) as runner:
        return runner.run(main())


def test_declared_list_matches_expected() -> None:
    assert set(GUARDED_METHODS) == EXPECTED_REFUSED
    assert not set(GUARDED_METHODS) & set(SAFE_NOOP_METHODS)


def test_every_public_loop_method_is_classified() -> None:
    """A method added to AbstractEventLoop by a new Python must be decided on."""
    public = {n for n in dir(asyncio.AbstractEventLoop) if not n.startswith("_")}
    classified = set(GUARDED_METHODS) | set(SAFE_NOOP_METHODS) | NOT_GUARDED
    assert public - classified == set(), "unclassified loop methods"
    assert classified - public == set(), "stale names in the lists"


def test_every_guarded_method_is_overridden_on_the_loop() -> None:
    for name in GUARDED_METHODS:
        assert getattr(VirtualTimeLoop, name) is not getattr(
            asyncio.SelectorEventLoop, name
        ), name


@pytest.mark.parametrize("name", sorted(EXPECTED_REFUSED))
def test_guarded_method_raises_real_io_error(name: str) -> None:
    loop = VirtualTimeLoop()
    try:
        method = getattr(loop, name)
        with pytest.raises(RealIOError) as excinfo:
            method(0, print)  # arguments are irrelevant; two satisfy every signature
        msg = str(excinfo.value)
        assert name in msg
        assert "real I/O" in msg
        assert "timer" in msg
    finally:
        loop.close()


def test_guarded_methods_accept_any_arguments() -> None:
    loop = VirtualTimeLoop()
    try:
        with pytest.raises(RealIOError):
            loop.add_reader(0, print, "x")
        with pytest.raises(RealIOError):
            loop.run_in_executor(None, print, 1)
        with socket.socket() as s, pytest.raises(RealIOError):
            loop.sock_recv(s, 10)
    finally:
        loop.close()


def test_real_io_error_is_a_runtime_error_not_an_oserror() -> None:
    assert issubclass(RealIOError, RuntimeError)
    assert not issubclass(RealIOError, OSError)


def test_remove_reader_writer_and_signal_handler_are_noops() -> None:
    loop = VirtualTimeLoop()
    try:
        assert loop.remove_reader(0) is False
        assert loop.remove_writer(0) is False
        assert loop.remove_signal_handler(2) is False
    finally:
        loop.close()


def test_loop_constructs_runs_and_closes() -> None:
    """The loop's own self-pipe uses private _add_reader and must still work."""
    loop = VirtualTimeLoop()
    assert not loop.is_closed()

    async def main() -> int:
        await asyncio.sleep(1)
        return 7

    assert loop.run_until_complete(main()) == 7
    loop.close()
    assert loop.is_closed()


def test_call_soon_threadsafe_from_loop_thread_still_works() -> None:
    async def main() -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        loop.call_soon_threadsafe(fut.set_result, 3)
        return await fut

    assert run(main) == 3


def test_runner_close_works_without_an_executor() -> None:
    """Runner.close() awaits shutdown_default_executor(); it must not be refused."""

    async def main() -> None:
        await asyncio.sleep(1)

    with asyncio.Runner(loop_factory=VirtualTimeLoop) as runner:
        runner.run(main())


def test_shutdown_default_executor_still_allowed() -> None:
    loop = VirtualTimeLoop()
    try:
        loop.run_until_complete(loop.shutdown_default_executor())
    finally:
        loop.close()


# --- high-level helpers must fail fast, not hang ---------------------------


def test_to_thread_raises() -> None:
    async def main() -> None:
        await asyncio.to_thread(print, "never")

    with pytest.raises(RealIOError):
        run(main)


def test_run_in_executor_raises_inside_coroutine() -> None:
    async def main() -> None:
        await asyncio.get_running_loop().run_in_executor(None, print)

    with pytest.raises(RealIOError):
        run(main)


def test_open_connection_raises() -> None:
    async def main() -> None:
        await asyncio.open_connection("127.0.0.1", 9)

    with pytest.raises(RealIOError):
        run(main)


def test_create_subprocess_exec_raises() -> None:
    async def main() -> None:
        await asyncio.create_subprocess_exec("true")

    with pytest.raises(RealIOError):
        run(main)


def test_getaddrinfo_raises() -> None:
    async def main() -> None:
        await asyncio.get_running_loop().getaddrinfo("localhost", 80)

    with pytest.raises(RealIOError):
        run(main)


def test_sock_connect_raises_inside_coroutine() -> None:
    async def main() -> None:
        loop = asyncio.get_running_loop()
        with socket.socket() as s:
            s.setblocking(False)
            await loop.sock_connect(s, ("127.0.0.1", 9))

    with pytest.raises(RealIOError):
        run(main)


def test_refusal_is_immediate_even_for_async_methods() -> None:
    """The guards are plain functions, so the error surfaces at the call and a
    forgotten ``await`` cannot hide it behind a never-awaited coroutine."""
    assert not inspect.iscoroutinefunction(VirtualTimeLoop.sock_recv)
