"""Private shared validation for real-number arguments (CLN-05)."""

import math


def to_float(
    value: object,
    *,
    type_error: str,
    overflow_error: str,
    finite_error: str | None = None,
    normalise_zero: bool = False,
) -> float:
    """Validate a real number and return it as a ``float``.

    ``bool`` and anything that is not an ``int`` or ``float`` (subclasses are
    fine) raises ``TypeError``. An ``int`` too large for a float raises
    ``ValueError`` (not ``OverflowError``). Range checks stay with the caller.

    Args:
        value: the argument to check.
        type_error: ``TypeError`` message; ``{type}`` is replaced by the
            name of ``type(value)``.
        overflow_error: ``ValueError`` message for an int too large for a float.
        finite_error: if given, NaN and infinity raise ``ValueError`` with this
            message (``{value!r}`` is replaced by the converted float). If
            ``None``, NaN and infinity are returned unchanged.
        normalise_zero: if true, ``-0.0`` is returned as ``0.0``.

    Raises:
        TypeError: ``value`` is not an ``int`` or ``float``, or is a ``bool``.
        ValueError: overflow, or non-finite when ``finite_error`` is given.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(type_error.format(type=type(value).__name__))
    try:
        result = float(value)
    except OverflowError:
        raise ValueError(overflow_error) from None
    if finite_error is not None and not math.isfinite(result):
        raise ValueError(finite_error.format(value=result))
    if normalise_zero:
        result += 0.0  # -0.0 + 0.0 == 0.0
    return result
