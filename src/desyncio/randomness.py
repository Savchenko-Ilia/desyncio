"""Seeded, named random streams.

Each named stream is an independent :class:`random.Random` seeded with the
string ``f"{seed}:{name}"``. CPython seeds a ``str`` deterministically (version-2
seeding hashes it with SHA-512, not ``hash()``), so results are identical across
processes, runs and ``PYTHONHASHSEED`` values. Nothing is re-implemented here:
the stdlib does the seeding, this module only validates and caches.

The ``:`` separator is unambiguous: a seed is a non-negative int, so it is digits
only and the first ``:`` always ends it, even if the name contains ``:``.
"""

import random


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


class RandomStreams:
    """Registry of named, independently seeded random streams.

    ``stream(name)`` returns the same cached ``random.Random`` for a given name
    within one registry, so repeated calls continue one sequence rather than
    restarting it. A stream is ``random.Random(f"{seed}:{name}")``.
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
            rng = random.Random(f"{self._seed}:{name}")
            self._streams[name] = rng
        return rng
