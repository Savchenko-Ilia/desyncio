"""Tests for the no-real-I/O guards (AGENTS.md principle 5)."""

import asyncio
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


def test_shutdown_default_executor_still_allowed() -> None:
    """Runner.close() awaits shutdown_default_executor(); it must not be refused."""
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
