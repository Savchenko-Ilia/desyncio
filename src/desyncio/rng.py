"""Seeded, named random streams.

Each named stream is an independent :class:`random.Random` whose seed is derived
from the master seed and the stream name with SHA-256, so results are identical
across processes, runs and ``PYTHONHASHSEED`` values (Python's ``hash()`` is
salted per process and is never used here).
"""

import hashlib
import random

_DOMAIN = b"desyncio.rng.v1"


def _check_seed(seed: object) -> int:
    """Return ``seed`` if it is a non-negative ``int`` (``bool`` is rejected)."""
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError(f"seed must be an int, got {type(seed).__name__}")
    if seed < 0:
        raise ValueError(f"seed must be non-negative, got {seed}")
    return seed


def _check_name(name: object) -> str:
    """Return ``name`` if it is a non-empty ``str``."""
    if not isinstance(name, str):
        raise TypeError(f"stream name must be a str, got {type(name).__name__}")
    if not name:
        raise ValueError("stream name must be non-empty")
    return name


def derive_seed(master_seed: int, name: str) -> int:
    """Derive a stream seed from a master seed and a stream name.

    Encoding (changing it changes every user's results; bump the domain tag
    ``desyncio.rng.v1`` if it ever must change)::

        payload = b"desyncio.rng.v1"
                  + len(seed_ascii).to_bytes(4, "big")
                  + seed_ascii            # str(master_seed) as ASCII decimal
                  + name.encode("utf-8")  # last field, so it needs no prefix
        seed    = int.from_bytes(sha256(payload).digest(), "big")

    The seed field is length-prefixed, so ``(1, "23")`` and ``(12, "3")`` differ.

    Args:
        master_seed: Non-negative int (``bool`` is rejected).
        name: Non-empty stream name.

    Returns:
        A 256-bit non-negative int suitable for ``random.Random``.

    Raises:
        TypeError: If ``master_seed`` is not an int or ``name`` is not a str.
        ValueError: If ``master_seed`` is negative or ``name`` is empty.
    """
    seed_ascii = str(_check_seed(master_seed)).encode("ascii")
    payload = (
        _DOMAIN
        + len(seed_ascii).to_bytes(4, "big")
        + seed_ascii
        + _check_name(name).encode("utf-8")
    )
    return int.from_bytes(hashlib.sha256(payload).digest(), "big")


class RngStreams:
    """Registry of named, independently seeded random streams.

    ``stream(name)`` returns the same cached ``random.Random`` for a given name
    within one registry, so repeated calls continue one sequence rather than
    restarting it.
    """

    def __init__(self, seed: int) -> None:
        """Create a registry.

        Args:
            seed: Master seed, a non-negative int.
        """
        self._seed = _check_seed(seed)
        self._streams: dict[str, random.Random] = {}

    @property
    def seed(self) -> int:
        """The master seed."""
        return self._seed

    def stream(self, name: str) -> random.Random:
        """Return the stream called ``name``, creating it on first use."""
        _check_name(name)
        rng = self._streams.get(name)
        if rng is None:
            rng = random.Random(derive_seed(self._seed, name))
            self._streams[name] = rng
        return rng
