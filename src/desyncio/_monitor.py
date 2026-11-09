"""Private helper: report an exception raised by a monitor (D023 v2, D045)."""

import asyncio
import logging

_logger = logging.getLogger("desyncio")


def report_monitor_error(
    message: str, exc: Exception, key: str, primitive: object
) -> None:
    """Report ``exc``, raised by ``primitive``'s monitor.

    Inside a running loop this is ``loop.call_exception_handler`` with the
    context ``{"message", "exception", key: primitive}``, as asyncio does for a
    failed callback. Outside a loop (e.g. ``Store.shutdown()`` called before
    ``run``) there is no handler to call, so the error is logged at ERROR on
    the ``desyncio`` logger, with the traceback, instead.
    """
    try:
        # get_running_loop() raises RuntimeError when the calling thread has no
        # running loop: unlike get_event_loop(), it never creates or guesses one.
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _logger.error("%s", message, exc_info=exc)
        return
    loop.call_exception_handler({"message": message, "exception": exc, key: primitive})
