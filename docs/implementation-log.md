# Implementation log

How each feature was actually built: approach, asyncio internals involved, trade-offs,
surprises, and what to remember next time. Newest entry at the bottom.
`docs/features.md` says *what* is done; `docs/decisions.md` says *what was decided*;
this file says *how it works and why*.

## Entry template

```
## <ID> — <feature name> (<YYYY-MM-DD>, batch <n>)
Branch: feat/<ID> (+ fix/<ID>-<n>)  ·  Merged: <commit sha>

**Approach.** What was built and how, in a few sentences.

**asyncio internals.** Loop methods, scheduling order, cancellation or task-lifetime
behaviour this relies on, with links to CPython source where useful.

**Trade-offs.** Alternatives considered and why this one won.

**Review findings.** MUST findings from the reviewer and how they were fixed.

**Gotchas.** Anything surprising that a future contributor should know.

**Follow-ups.** Known gaps, TODOs, ideas (link issues or feature IDs).
```

---

## PKG-01 — Package skeleton (uv) (2026-11-08, batch 1)
Branch: feat/PKG-01  ·  Merged: 186a18c

**Approach.** I added a `src/`-layout package. `pyproject.toml` uses the `uv_build` backend, pinned `>=0.11,<0.12` to match uv 0.11.32. It sets `requires-python = ">=3.13"` and `license = "Apache-2.0"` with `license-files = ["LICENSE"]`. It has no runtime dependencies. The dev tools sit in `[dependency-groups] dev`. The resolved versions are pytest 9.2.0, pytest-asyncio 1.4.1.post1, hypothesis 6.170.15, ruff 0.17.4 and mypy 2.4.1. `.python-version` is `3.13`. `uv.lock` is committed and resolves from PyPI only. `src/desyncio/__init__.py` exposes `__version__ = "0.1.0"`. An empty `py.typed` marker ships, so type checkers in downstream code use our hints (PEP 561).

**asyncio internals.** None yet. pytest-asyncio runs with `asyncio_mode = "auto"`, so every `async def test_*` runs without a marker. It also sets `asyncio_default_fixture_loop_scope = "function"`, so each test gets a fresh loop and no state leaks between tests. Determinism tests need that isolation. Once LOOP-02 exists, tests will need to run on the virtual-time loop. They can do that through `desyncio.run(...)` or a loop-factory fixture. That is decided in LOOP-02/LOOP-04.

**Trade-offs.** `uv_build` was chosen over hatchling because it needs no extra tool and uv is the mandated toolchain. The narrow pin means the backend needs bumping when uv moves to 0.12. mypy runs with `strict = true`. ruff selects E, F, I, B, UP and ASYNC; ASYNC catches blocking calls inside coroutines (see D002).

**Review findings.** PASS, no MUST findings. SHOULD: (1) bump the `uv_build` pin with uv; (2) `__version__` is duplicated in `pyproject.toml` and `__init__.py`, so derive one from the other or test them with `importlib.metadata`.

**Gotchas.** The cloud sandbox prints a harmless `UV_NATIVE_TLS` deprecation warning on every uv command. `uv sync` fails until `src/desyncio/__init__.py` exists. That failure was the "red" step of the test.

**Follow-ups.** Single-source the version. Bump the `uv_build` pin.

## LOOP-01 — Virtual clock (2026-11-08, batch 2)
Branch: feat/LOOP-01  ·  Merged: 239d580

**Approach.** `VirtualClock(start=0.0)` in `src/desyncio/clock.py` is a plain class with no asyncio. It has a read-only `now` property and `advance_to(t)`. Time is a unitless float, *seconds by convention* (D004). Inputs are coerced with `float()`; NaN and ±inf raise `ValueError`. Moving backwards raises `ValueError("cannot move clock backwards: now=10.0, requested=9.5")`. Advancing to the current time is a no-op. A rejected call leaves the clock unchanged.

**asyncio internals.** None yet. This is the value LOOP-02's `loop.time()` will return. asyncio's `BaseEventLoop` calls `time()` to stamp `call_later` timers (`when = self.time() + delay`) and to decide which timers in `_scheduled` are due in `_run_once`. A virtual loop works by making that clock jump, not tick. The same-time no-op matters because many timers share one timestamp.

**Trade-offs.** Plain `ValueError`, not a custom exception subclass. Kept minimal until a caller needs to catch clock errors separately.

**Review findings.** PASS, no MUST findings. SHOULD:
- `float()` silently accepts `"5"`, `b"5"` and `True`. Reject non-real inputs with `TypeError` instead.
- `float(10**400)` raises `OverflowError`. Map it to `ValueError`.
- Normalise `-0.0` to `0.0`.
- Add a hypothesis property for the backwards path.
- Generate sorted floats directly instead of sorting inside the test.

**Gotchas.** `VirtualClock(-0.0).now` is `-0.0`. Equality still holds, but the repr looks odd.

**Follow-ups.** The SHOULD items above. Consider them before LOOP-02 makes the clock load-bearing.

## LOOP-08 — Seeded RNG streams (2026-11-08, batch 2)
Branch: feat/LOOP-08  ·  Merged: 2badb9a

**Approach.** `src/desyncio/rng.py` provides `derive_seed(master_seed, name)` and `RngStreams(seed).stream(name)`. `stream(name)` caches one `random.Random` per name, so repeated calls continue a single sequence. The seed is derived as `int.from_bytes(sha256(b"desyncio.rng.v1" + len(seed_ascii).to_bytes(4,"big") + seed_ascii + name.encode("utf-8")).digest(), "big")`. The domain tag allows a future `v2` encoding without silent changes. The length prefix makes `(1,"23")` and `(12,"3")` differ. Validation:
- Seed: bool or non-int raises `TypeError`; a negative seed raises `ValueError`.
- Name: non-str raises `TypeError`; an empty name raises `ValueError`.

Not yet wired into `sim.rng()`. LOOP-05 does that (D004).

**asyncio internals.** None. Determinism background: Python's `hash()` for str is salted per process (`PYTHONHASHSEED`), so it can never feed a seed. `random.Random(int)` seeds through `init_by_array` on the absolute value. That path has been stable across 3.x; only str/bytes seeds use "version 2" seeding.

**Trade-offs.** SHA-256 from the stdlib, rather than a faster non-cryptographic hash. It runs once per stream, so speed is irrelevant and stability is the point. Seeds are non-negative ints only.

**Review findings.** PASS, no MUST findings. SHOULD:
- Names with lone surrogates raise `UnicodeEncodeError`. Document this, or encode with `errors="surrogatepass"`.
- The hypothesis determinism property is near-vacuous. Replace it with an injectivity property.
- The subprocess test passes an env with only `PYTHONHASHSEED`. This will break on Windows and on non-editable installs. Use `{**os.environ, "PYTHONHASHSEED": ...}`.
- Add a second golden pair with a non-ASCII name and a large seed.
- Note in the docstring that `stream()` is not thread-safe.

**Gotchas.** The golden values were generated by this implementation and then pinned. They guard against change; they are not an external reference.

**Follow-ups.** The SHOULD items above. The subprocess-env one matters as soon as CI exists.
