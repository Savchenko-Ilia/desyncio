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

## LOOP-01b — Clock input hardening (2026-11-08, batch 3)
Branch: feat/LOOP-01b  ·  Merged: 4fe577b

**Approach.** `VirtualClock._check_finite` now type-checks before converting. `bool`, and anything that is not `int`/`float`, raises `TypeError("<param> must be a real number (int or float), got <type>")`. Subclasses of int and float other than bool are accepted. An `OverflowError` from `float(10**400)` is re-raised as `ValueError`. The result is returned as `result + 0.0`, which turns `-0.0` into `0.0` (IEEE 754: `-0.0 + 0.0 == +0.0`). The public API and the existing messages are unchanged.

**asyncio internals.** None.

**Trade-offs.** An explicit `isinstance` check, rather than duck-typing with `__float__`. It rejects numpy scalars and `Decimal`, which keeps the core strict; this can be widened later if a user needs it.

**Review findings.** PASS. SHOULD:
- `assume(t < now)` in the backwards property discards ~77% of examples. Use a constructive strategy, e.g. draw two distinct floats and take the max as `now`.
- Pin with a test that a float subclass whose `__float__` returns NaN still gets rejected.

**Gotchas.** The forward property was rewritten to use `st.lists(finite).map(sorted)` clamped to `start`. The spec allowed this, and the reviewer confirmed it is no weaker.

**Follow-ups.** The SHOULDs above.

## CI-01 — CI on pull requests (2026-11-08, batch 3)
Branch: feat/CI-01  ·  Merged: b746110

**Approach.** `.github/workflows/ci.yml` triggers on `pull_request` (any base branch) and `workflow_dispatch`, with no `push` trigger (owner request, D005). Settings:
- `permissions: contents: read`.
- A concurrency group keyed by PR number, with `cancel-in-progress`.
- One `ubuntu-latest` job, 10-minute timeout.
- `actions/checkout@v7` and `astral-sh/setup-uv@v7` (latest major tags, verified with `git ls-remote`). uv cache keyed on `uv.lock`.
- Steps: `uv python install` (reads `.python-version`), then `uv sync --locked`, `pytest -q`, `ruff check .`, `mypy src`, the same as `scripts/gate.env`.

**asyncio internals.** None.

**Trade-offs.** Floating major tags rather than SHA pins. That is acceptable with read-only permissions and no secrets. The only `${{ }}` expression is in `concurrency.group`; no PR-controlled text reaches a `run:` step.

**Review findings.** PASS. SHOULD:
- The header comment says "every change reaches a branch through a PR anyway". That is wrong: the lead merges feature branches locally, gated by `scripts/gate.sh`.
- `uv python install` is redundant. Use setup-uv's `python-version: "3.13"` input instead.
- Consider `persist-credentials: false` on checkout.

**Gotchas.** Not yet run on GitHub. The first PR is its first real run. actionlint was not available, so validation was a PyYAML structure check.

**Follow-ups.** The SHOULDs above. Watch the first run.

## LOOP-02 — Virtual-time event loop (2026-11-08, batch 3)
Branch: feat/LOOP-02  ·  Merged: 8acd3e5

**Approach.** `VirtualTimeLoop(start=0.0, clock=None)` subclasses `asyncio.SelectorEventLoop`. `time()` returns `self.clock.now`. The clock is set *before* `super().__init__()`, because the base constructor may call `time()`. After construction, the loop's real selector, which already has the self-pipe registered, is wrapped in `_JumpingSelector`. `SimulationDeadlock(RuntimeError)` is raised when nothing can ever happen. No CPython code is copied, so no PSF notice is needed. Run it with `asyncio.Runner(loop_factory=VirtualTimeLoop)`.

**asyncio internals.** Each `BaseEventLoop._run_once` iteration (CPython 3.13, `Lib/asyncio/base_events.py`) does five things:
1. It pops cancelled timers from the head of the `_scheduled` heap.
2. It picks a selector timeout: `0` if `_ready` is non-empty or the loop is stopping. Otherwise it is `_scheduled[0]._when - time()`, or `None` if there are no timers.
3. It calls `self._selector.select(timeout)`. This is the only place a stock loop blocks.
4. It moves every timer with `_when < time() + _clock_resolution` to `_ready`.
5. It runs `_ready`.

`_JumpingSelector.select(timeout)` always polls the real selector with `select(0)`. If that returns events, or `timeout == 0`, it returns them untouched. Otherwise the loop is idle. It advances the clock to `_scheduled[0]._when`, so step 4 finds that timer due, and returns no events. With no timers it raises `SimulationDeadlock`, never blocking. Cancelled timers can't move the clock, because step 1 already removed them from the head. Overdue timers (`when < now`) fire without moving the clock.

The design depends on these private attributes: `loop._scheduled`, `TimerHandle._when` and `loop._selector`. They are documented in the module docstring, and the tests will fail loudly if CPython changes them.

**Trade-offs.**
- Wrapping the selector rather than overriding `_run_once` avoids copying PSF-licensed code. It also keeps stock asyncio scheduling intact.
- Deadlock raises rather than returning. Returning would spin forever and blocking would hang silently; a deadlock is a model bug, so it should be loud.

**Review findings.** PASS. Probe results (reviewer):
- **Threads deadlock immediately.** Examples: `call_soon_threadsafe` from a worker thread with no timers pending, `asyncio.to_thread` and `run_in_executor`. The `select(0)` poll never waits for the self-pipe byte. With a timer pending, the clock jumps past the thread's result. Accepted under principle 5 (D006). LOOP-07 will refuse executors explicitly.
- **Clock-resolution window.** Timers within `_clock_resolution` (~1e-9) of each other fire in the same iteration, both seeing the earlier time. Hand to LOOP-03.
- `Runner.close()` after a deadlock is clean. If the default executor was used, `shutdown_default_executor`'s 300 s timeout elapses instantly in virtual time and emits a `RuntimeWarning`.
- SHOULD:
  - `test_threadsafe_wakeup_is_still_polled` only calls from the loop thread. It does not test a cross-thread wake-up, so the docstring's self-pipe claim is misleading.
  - Add tests for overdue timers and for cancelled timers in the middle of the heap.
  - The 50 ms wall-time test includes runner setup and could flake on a loaded CI box.
  - The `max(when, now)` guard is unreachable.

**Gotchas.** asyncio's `TimerHandle.__lt__` compares `_when` only, and `heapq` is not stable. So equal-time timers have no guaranteed order yet; that is LOOP-03's job.

**Follow-ups.** The SHOULDs above, proposed to be folded into LOOP-03 (resolution, overdue and mid-heap tests, wall-time test) and LOOP-07 (thread/executor docs and tests).

## LOOP-04 — Run entry point (2026-11-08, batch 4)
Branch: feat/LOOP-04  ·  Merged: 6951557

**Approach.** `desyncio.run(main, *, until=None, seed=0)` in `src/desyncio/run.py` is exported from `desyncio/__init__.py`. It builds `asyncio.Runner(loop_factory=lambda: VirtualTimeLoop(clock=clock))` and calls `runner.run(main, context=ctx)`. No `loop.py` change was needed.
- **Stopping at `until`:** `_BoundedClock(VirtualClock)` has a `limit`. The loop advances the clock only when idle, so when it asks for a time past `limit` the clock sets itself to exactly `limit` and raises a private `_LimitReached`. That exception unwinds `select` → `_run_once` → `run_forever` → `Runner.run`, and `run` returns `None`. Everything due at `t <= until` has already run by then.
- **Cleanup:** before `runner.close()`, the clock is frozen (`limit = now`). Close cancels leftover tasks, as `asyncio.run` does, and their `except`/`finally` blocks run in zero virtual time. A handler that awaits a later timer makes `run` raise `SimulationCleanupError`.
- **Seed:** an `RngStreams(seed)` is stored in a `ContextVar` set inside a fresh `contextvars.Context`. Read it with `current_rng_streams()`, which raises `RuntimeError` outside `run`.
- **Validation:** bad arguments close the coroutine first, so no "never awaited" warning appears. `run` refuses to start inside a running loop. Main's exceptions, and `SimulationDeadlock`, propagate after the loop closes.

**asyncio internals.**
- Every `Task` copies the context that is current when it is created (`contextvars.copy_context()`). So the main task and every task it spawns, transitively, see the same `RngStreams`, and the variable never leaks into the caller.
- `asyncio.Runner(context=...)` only exists from 3.14. On 3.13 the context goes to `Runner.run(coro, context=...)`.
- `Runner.close()` runs `_cancel_all_tasks`, `shutdown_asyncgens` and `shutdown_default_executor`, then `loop.close()` in a `finally`. If cancellation raises, the asyncgen and executor shutdowns are skipped, but the loop is still closed.
- An exception raised from `select()` leaves the loop consistent: the running loop is reset and the SIGINT handler is restored (reviewer probe, no warnings under `-X dev -W error`).

**Trade-offs.**
- A clock subclass that raises, rather than scheduling `loop.stop()` at `until`. A stop callback would let other timers due at exactly `until` race with it, and it would need loop changes.
- Freezing the clock during close keeps "nothing runs after `until`" strict. The cost is that cleanup code that sleeps is an error.

**Review findings.** PASS. SHOULD:
- Add `@overload`s, so that `until=None` returns `T` rather than `T | None`.
- **Name shadowing:** `desyncio.run` is the function, which hides the `desyncio.run` module. `mock.patch("desyncio.run.current_rng_streams")` silently patches the function object. Recommend renaming the module to `runners.py`, as asyncio does, before LOOP-05 imports it.
- Add a test for an infinite `while True: sleep(1)` background task. The reviewer probed it, and it is clean on both paths.
- Document the "Task was destroyed but it is pending" message on the `SimulationCleanupError` path.
- Drop the double seed validation, which uses the private `_check_seed` import.
- The `until + 5e-10` resolution-window case is fixed by LOOP-03.

**Gotchas.** Arguments are keyword-only (`run(main, *, until, seed)`). The start time is always 0.0.

**Follow-ups.** The overloads and the module rename (owner decision, see the batch 4 report). Add the infinite-background-task test.

## LOOP-03 — Deterministic tie-breaking (2026-11-08, batch 4)
Branch: feat/LOOP-03 + fix/LOOP-03-1  ·  Merged: 8624c3f

**Approach.**
- `src/desyncio/scheduling.py` adds `PriorityTimerHandle(TimerHandle)`, whose comparisons use the key `(when, priority, sequence)`. It also adds a per-loop `SequenceCounter` and `DEFAULT_PRIORITY = 0`.
- `VirtualTimeLoop.call_at` is overridden to build these handles with priority 0. `call_later` → `self.call_at`, `sleep` → `call_later`, and `asyncio.timeout` → `call_at` (all verified in the 3.13 source), so every stock timer is covered.
- `loop.call_at_priority(when, priority, callback, *args, context=None)` sets an explicit priority; it must be an int, and bool is rejected.
- `_clock_resolution` is a property returning `nextafter(now, inf) - now`, so only timers at exactly `now` share an instant.
- The `max(when, now)` guard was removed, because it is unreachable: an overdue timer gives a timeout of 0.

**asyncio internals.**
- `loop._scheduled` is a `heapq` min-heap. `heapq` only uses `<`, and stock `TimerHandle.__lt__` compares `_when` alone. Equal times are "not less" than each other, so their pop order is whatever the swaps left; `heapq` is not stable. A unique key leaves exactly one valid pop order, so equal `(when, priority)` pairs pop FIFO. This also holds after `_run_once` rebuilds the heap with `heapify` once more than half of 100+ timers are cancelled.
- `_run_once` treats a timer as due if `_when < time() + _clock_resolution` and runs all due timers with `time()` frozen. With the real ~1e-9 resolution, `1.0 + 5e-10` would observe `1.0`. A resolution of 0 would hang, because a timer at exactly `now` would never be due.
- A `call_soon` made from a timer callback runs after the other timers due at that instant. That is stock behaviour, and priority does not affect `call_soon`.

**Trade-offs.**
- A handle subclass, rather than a parallel heap or a wrapper list. It keeps CPython's `_run_once` untouched.
- `_schedule` had to re-create `BaseEventLoop.call_at`'s body, because `call_at` hardcodes `events.TimerHandle`. That is adapted PSF code (D009).

**Review findings.**
- Round 0 FAILED with one MUST. `_schedule` reproduces `BaseEventLoop.call_at` line for line: the same checks, the same error string, the traceback trim, `heappush` and `_scheduled = True`. That is adapted code under AGENTS.md §3, and the module docstring falsely claimed "no CPython code is copied".
- Fix round 1 (`fix/LOOP-03-1`):
  - added `LICENSES/PSF.txt`: a header listing the adapted portions, plus the verbatim PSF License v2, byte-identical to the local interpreter's `LICENSE.txt`;
  - added marking comments, with a change summary, on `_schedule` and `PriorityTimerHandle.drop_creation_frame`;
  - corrected the docstring;
  - added `test_cpython_adapted_code_carries_psf_notice`.
  Re-review: PASS.
- SHOULD:
  - `pyproject.toml`'s `license-files = ["LICENSE"]` does not ship `LICENSES/PSF.txt` in wheels. Add `"LICENSES/*"`; pyproject is outside LOOP-03's ownership.
  - Use CPython's exact copyright wording, "2001-2024".
  - Skip the notice test when `LICENSES/` is absent, e.g. when tests run from an installed package.
  - The determinism tests run in-process only. Add a run in a subprocess with a different `PYTHONHASHSEED`.
  - The inherited `__eq__` ignores priority and sequence.
  - The `_clock_resolution` setter silently discards writes.

**Gotchas.** The private attributes in use are now `_scheduled`, `_when`, `_selector`, `_clock_resolution`, `_check_closed`, `_debug`, `_check_thread`, `_check_callback` and `_source_traceback`. Any CPython bump should run the full suite. At t ≈ 1e300, `t + 5 == t`, an inherent float limit.

**Follow-ups.** The `license-files` fix in `pyproject.toml`, needed before any release. The SHOULDs above. AGENTS.md §3: flag the adapted code in the `integration` → `main` PR.

## LOOP-04b — Run entry point follow-ups (2026-11-08, batch 5)
Branch: feat/LOOP-04b  ·  Merged: 073e026

**Approach.**
- `git mv src/desyncio/run.py src/desyncio/runners.py`, with no shim; `__init__` now does `from desyncio.runners import run`. A test asserts four things: `desyncio.run` is the function, `find_spec("desyncio.run") is None`, `desyncio.runners.current_rng_streams` exists, and `mock.patch("desyncio.runners.current_rng_streams")` really patches the module.
- Two `@overload`s: no `until` / `until=None` → `T`; `until: float` → `T | None`. Checked with `assert_type` in `tests/test_run.py`, plus a manual `uv run mypy tests/test_run.py`.
- A new parametrized test: a `while True: await asyncio.sleep(1)` background task is cancelled cleanly both when main returns at 3.5 and at `until=10`, with warnings as errors.
- `pyproject.toml`: `license-files = ["LICENSE", "LICENSES/*"]`. Verified with `uv build`: the wheel has `.dist-info/licenses/LICENSES/PSF.txt`, and the sdist has `LICENSES/PSF.txt`. `uv.lock` is unchanged.
- The private `_check_seed` import was dropped; `RngStreams(seed)` validates.

**asyncio internals.** In Python 3.13, `import a.b as m` resolves `m` with `getattr(a, "b")`, so a function named `b` in `a/__init__` wins over the submodule. That is why the module was renamed rather than worked around.

**Trade-offs.** No third catch-all overload: mypy (and pyright) expand a `float | None` argument across the two overloads and infer `T | None` (reviewer probe).

**Review findings.** PASS. Four existing `# type: ignore[arg-type]` comments became `[call-overload]`. Only the ignore codes changed, which the spec change permits.

**Gotchas.** A deliberate bad-argument call to `run` in a test now needs `# type: ignore[call-overload]`.

**Follow-ups.** None.

## LOOP-07 — No real I/O guard (2026-11-08, batch 5)
Branch: feat/LOOP-07  ·  Merged: dc234c0

**Approach.** `src/desyncio/guards.py` defines `RealIOError(RuntimeError)` and `RealIOGuardMixin`. The loop is now `class VirtualTimeLoop(RealIOGuardMixin, asyncio.SelectorEventLoop)`; the mixin comes first in the method resolution order, so its overrides win.
- **Refused:** 28 public methods (`GUARDED_METHODS`) raise at call time. They cover readers, writers and signals, `sock_*`, connection and server creation, pipes, `sendfile` and `start_tls`, `getaddrinfo`/`getnameinfo`, `subprocess_*`, `run_in_executor` and `set_default_executor`.
- **Messages:** each names the method and suggests modelling the delay with a timer.
- **No-ops:** `remove_reader`, `remove_writer` and `remove_signal_handler` return False (`SAFE_NOOP_METHODS`), so ordinary-asyncio cleanup code doesn't mask the real error.
- **Classification test:** a test requires every public `AbstractEventLoop` method to be classified as guarded, no-op or explicitly allowed. The reviewer confirmed it fails when a fake method is monkeypatched in.
- **D007 carry-overs:**
  - the in-loop thread test was renamed to `test_threadsafe_wakeup_from_loop_thread_is_polled`;
  - `test_worker_thread_wakeup_is_a_deadlock` was added (D006);
  - `_JumpingSelector`'s docstring now says cross-thread wake-ups are unsupported.

**asyncio internals.**
- The loop's self-pipe (a `socketpair` that lets other threads wake `select()`) is registered via the private `_add_reader` in `_make_self_pipe` and removed via `_remove_reader` in `_close_self_pipe`. The public `add_reader` is a thin wrapper, so overriding only public names leaves the loop working.
- `Runner.close()` awaits `shutdown_default_executor()`, which returns at once because no executor can ever be created.
- `Runner` installs SIGINT with `signal.signal` + `set_wakeup_fd`, not `add_signal_handler`, so it is unaffected.
- The guards are plain `def`s, even where asyncio's method is a coroutine. They raise before returning, so a forgotten `await` can't hide the error and no un-awaited coroutine leaks. `asyncio.to_thread` (itself a coroutine function) raises when awaited.

**Trade-offs.**
- `RuntimeError`, not `OSError`, as the base, so that network retry/ignore logic can't swallow it.
- `add_signal_handler` is refused: signal delivery time is real-world and nondeterministic.

**Review findings.** PASS. Probed bypasses:
- `loop._add_reader` (private API) is not refused. Accepted.
- `signal.set_wakeup_fd` (stdlib) is not refused. Accepted.
- A `concurrent.futures` executor used directly with `asyncio.wrap_future` is not refused (medium-low). It behaves like a user thread (D006); documented in DOC-01 §4.

SHOULD:
- `test_worker_thread_wakeup_is_a_deadlock` sleeps 0.3 s of real time and asserts `elapsed < 0.25`. Replace both with a `threading.Event` gate, because the wall-clock bound could flake on slow CI.
- List the known bypasses in the `guards.py` docstring.
- Make the `_refuse` messages more specific for signals and DNS.

**Gotchas.** A refused `to_thread` reports `run_in_executor()` in its message, because that is the primitive that does the I/O.

**Follow-ups.** The SHOULDs above. The `threading.Event` gate is the most important, given CI.

## DOC-01 — How the loop works (2026-11-08, batch 5)
Branch: feat/DOC-01 + fix/DOC-01-1  ·  Merged: 30fcc79

**Approach.** `docs/how-the-loop-works.md`, about 400 lines, is linked from a new README "Documentation" section. It covers:
- `_run_once`'s five steps;
- the timer heap and why ties need the `(when, priority, sequence)` key;
- the `_clock_resolution` window;
- `_JumpingSelector` and the deadlock rule;
- the full `sleep(3600)` → `call_later` → `call_at` → heap → `set_result` → `call_soon` → task step path;
- how `until` and cleanup freezing work;
- a table of private attributes and the PSF-adapted part;
- a worked example (tasks A, B, C) whose iteration table was produced by an instrumented `VirtualTimeLoop` subclass. The reviewer reproduced it independently.

**asyncio internals.**
- Step 5 runs only `ntodo = len(_ready)` handles, so a task woken by a timer runs one iteration after the timer fires; the clock doesn't move in between.
- Stock heapq tie order depends on the heap's history. Pushing b..f at `when=1` and then a at 2 pops `b, d, c, e, f, a`. Pushing a first pops `b, c, f, e, d, a`.

**Trade-offs.** CPython links point at the `3.13` branch, without line anchors: they drift slowly, and functions are named instead.

**Review findings.**
- Round 0 FAILED with one MUST: the quoted stock-heapq output didn't match the stated push order.
- The lead promoted a second finding to MUST (D011): once LOOP-07 merged, §4's claim that `to_thread` and `run_in_executor` deadlock was false.
- Fix round 1:
  - re-ran the heap experiment and quoted the real outputs for both orders;
  - rewrote §4: executors raise `RealIOError`; D006 now applies only to user-created threads; `wrap_future` is a known gap;
  - added `guards.py` to the sources, a §7 row for the self-pipe's private `_add_reader`/`_remove_reader`, and `test_guards.py` to §9.
- Re-review: PASS.
- SHOULD:
  - Say `to_thread` raises when awaited, not "at the call".
  - "Deadlock immediately" holds only if the thread hasn't yet written to the self-pipe.
  - §2's timeout omits the 24 h `MAXIMUM_SELECT_TIMEOUT` clamp and the negative → 0 rule.
  - In debug mode, step 5 also reads `time()`.
  - Say "the main changes" rather than "three changes" from `call_at`.

**Gotchas.** The doc has to change whenever loop internals change. Add "update docs/how-the-loop-works.md" to the review checklist for any feature that owns `loop.py`.

**Follow-ups.** The SHOULDs above.

## CI-02 — CI manual-only (2026-11-08, batch 6, owner request)
Branch: feat/CI-02  ·  Merged: 29e5222

**Approach.** `.github/workflows/ci.yml` now has only `workflow_dispatch` under `on:`; `pull_request` was removed. The header comment explains why (no Actions credits, D013), names `scripts/gate.sh` as the merge check, and says how to re-enable. The job, steps, permissions and concurrency are unchanged.

**asyncio internals.** None.

**Trade-offs.** The workflow was kept rather than deleted, so re-enabling it is a one-line change.

**Review findings.** PASS. Notes from the reviewer (GitHub behaviour, from its knowledge, untested):
- `pull_request` runs use the workflow from the PR's merge ref, so PR #2 stops triggering once this lands on `integration`. The two earlier failed runs stay in its history.
- `workflow_dispatch` only appears in the Actions UI once the file is on the default branch (`main`), i.e. after PR #2 merges.
- Make sure no branch-protection rule on `main` requires the `check` status, or the merge would be blocked.

**Gotchas.** The two failed runs on PR #2 (about 3 s each, no steps run) were caused by billing, not by test failures.

**Follow-ups.** Restore `pull_request:` when credits are available.

## LOOP-05 — Sim facade (2026-11-08, batch 6)
Branch: feat/LOOP-05  ·  Merged: 6a3920c

**Approach.** `src/desyncio/sim.py` has a `Sim` class and the singleton `sim`, exported as `from desyncio import sim` (D012). Every member looks up the running loop each time. If there is no running `VirtualTimeLoop`, it raises `RuntimeError` saying to use it inside `desyncio.run(...)`.
- `sim.now` is `loop.time()`.
- `await sim.timeout(dt, value=None)` is `asyncio.sleep(dt, value)` with validation: `dt` must be finite, `>= 0`, and not a bool. It is an `async def`, so the check happens at `await`.
- `sim.spawn(coro, name=None)` calls `loop.create_task(coro, name=...)`. Default names are `proc-N`, from a per-loop counter in a `WeakKeyDictionary` (loop.py wasn't owned). Bad input closes the coroutine before raising.
- `sim.rng(name)` is `current_rng_streams().stream(name)`.

**asyncio internals.**
- asyncio names tasks `Task-N` from a counter shared by the whole process, so a second run in the same process gets different names. That is why spawn has its own counter.
- Tasks copy the current context when created, which is how `sim.rng` reaches the run's `RngStreams`.
- The loop keeps only weak references to tasks, so callers must keep the `Task` that `spawn` returns.

**Trade-offs.** A singleton plus a running-loop lookup, rather than passing a `Sim` into `main`. It reads like asyncio's own functions.

**Review findings.** PASS. SHOULD:
- **Fresh context:** a task created with `loop.create_task(..., context=contextvars.Context())` gets a misleading "no simulation is running" from `sim.rng`. Fall back to the loop, or improve the message.
- **Names only for `sim.spawn`:** only spawned tasks get deterministic names. `asyncio.create_task`, `TaskGroup` and the main task keep the process-wide `Task-N`. LOOP-09 should install a task factory so all names are deterministic.
- **More tests:** spawn order with simultaneous spawns; the log line for a failed task nobody awaited; an un-awaited `sim.timeout(-1)`; checking that the re-raised exception is the same object.
- **Non-str `name`:** accepted and converted with `str()`.

**Gotchas.** Validation in `timeout` only happens when it is awaited.

**Follow-ups.** The task-factory note is for LOOP-09. The other SHOULDs above.

## LOOP-11 — Scaled real-time mode (2026-11-08, batch 6)
Branch: feat/LOOP-11 + fix/LOOP-11-1  ·  Merged: 2d06d94

**Approach.**
- `src/desyncio/realtime.py` has three pieces:
  - `RealTimePacer(speed=1.0, wall_clock=None)`, with `speed` (get/set), `pause()`, `resume()` and `paused`, all behind a lock;
  - a `WallClock` Protocol with `monotonic()` and `wait(timeout, selector)`;
  - `SystemWallClock`, which waits by calling the real `selector.select(timeout)`.
- The pacer is wired in through `VirtualTimeLoop(..., pacer=None)` and `desyncio.run(..., speed=None, pacer=None)`. `speed=x` is shorthand for `pacer=RealTimePacer(x)`, and giving both raises `TypeError`.
- When the loop is idle, `_JumpingSelector._select_paced` waits `(min(when, until) - now) / speed` of real time, through the wall clock:
  - **Full wait:** it calls `advance_to(when)`, so `_BoundedClock` still stops at `until`.
  - **Interrupted wait** (a byte arrives on the self-pipe): it advances by `elapsed*speed`, capped, and returns the events.
  - **Paused:** it waits with no timeout until resumed. Virtual time doesn't move.
- **Fast mode** (no pacer, `speed=inf`, or the cleanup freeze) uses exactly the old code path.
- **Thread safety:** every `speed=`, `pause()` and `resume()` calls `loop.call_soon_threadsafe(noop)` to wake the loop.

**asyncio internals.** The self-pipe makes the real-time wait interruptible. A thread's `call_soon_threadsafe` writes one byte, which ends `select(timeout)` early. From inside the loop thread, the same call lands in `_ready`, so the next `_run_once` uses timeout 0 and no wait is interrupted. Stock asyncio caps select timeouts at `MAXIMUM_SELECT_TIMEOUT` (24 h) for the same overflow reason as the fix below.

**Trade-offs.**
- Waits are relative per timer, not anchored to a start time. Real time can drift a little behind the ideal schedule, by the time callbacks take to run.
- With no timers and not paused, the loop still raises `SimulationDeadlock` (consistent with D006). While paused, it waits.

**Review findings.**
- Round 0 FAILED with two MUSTs, and the lead promoted a third (D014):
  - the real wait wasn't capped: `select` raises `OverflowError` above ~9.2e9 s, e.g. `speed=1e-300` or a `1e10` timer;
  - the `guards.py` docstring said "never waits", now false (D011);
  - (promoted) a failed `pacer.bind` leaked the half-built loop and its sockets.
- Fix round 1:
  - `MAX_REAL_WAIT = 86400`. A cut-short chunk advances by `elapsed*speed`, or at least one float step so it can't spin forever.
  - The loop closes itself if `bind` fails. `run()` builds the loop inside its validation block, so `main` is closed on failure.
  - The `guards.py` docstring is qualified.
  - Re-review: PASS. The reviewer checked chunked runs under every `until` value against fast-mode traces and found them identical. There were no ResourceWarnings on any `run()` path under `-X dev -W error`.
- SHOULD:
  - `pause()` from inside the simulation, with no other thread, blocks forever. Add a one-line warning to the `pause()` and `run()` docstrings.
  - A pacer reused after a run that ended paused starts the next run paused.
  - If the float-step floor regresses, `test_chunk_below_one_float_step_still_makes_progress` hangs instead of failing. Bound its fake wall clock.
  - Add a comment explaining the floor's effect: at most one float step per day, only for absurdly small speeds.

**Gotchas.**
- `_select_paced` reads `getattr(clock, "limit", inf)`, a light coupling to `runners._BoundedClock`.
- The `run()` tests for `speed` and `pacer` are in `tests/test_realtime.py`, not `tests/test_run.py`.

**Follow-ups.** The SHOULDs above. Time anchoring, for long real-time runs.

## LOOP-06 — asyncio compatibility suite (2026-11-08, batch 7)
Branch: feat/LOOP-06  ·  Merged: e95ae83

**Approach.** `tests/test_asyncio_compat.py` has 47 tests, all on the public API. Each one records `(virtual_time, label)` events and checks the whole timeline. The tests cover:
- `asyncio.Runner(loop_factory=VirtualTimeLoop)` and wall-time negligibility;
- `Queue`: blocking get and put, FIFO getters, and `shutdown()` (graceful, `immediate=True` with `join()` release, waking blocked getters and putters);
- `Lock`, `Event`, `Semaphore`;
- `gather`: `return_exceptions`, first failure, cancellation propagation;
- `TaskGroup`: siblings cancelled at the failure time, `ExceptionGroup`;
- `as_completed` as an async iterator, and with a timeout;
- `wait_for`;
- `timeout()`/`timeout_at()` with `reschedule` (later, earlier, `None`, into the past);
- cancellation: landing time, cleanup that takes virtual time, suppression, cancel before the first step;
- `shield`.
No desyncio bugs were found, and there are no xfails.

**asyncio internals.**
- `reschedule()` into the past uses `call_soon`, so the timeout fires at the current time.
- `gather` without `return_exceptions` does not cancel the other children.
- Two TaskGroup child failures at the same t arrive together, because both timers fire in the same `_run_once` iteration.
- These are plain sync tests that call `desyncio.run`, which cannot nest inside pytest-asyncio's loop.

**Trade-offs.** Same-instant order is asserted only where asyncio or desyncio guarantees it (FIFO waiters, D008 timer ties).

**Review findings.** PASS. Mutation results:
- clock off by +1 (implementer): 47/47 tests fail;
- jump to `when + 0.5`: 47/47 fail;
- LIFO timer ties: 0/47 fail. That case is covered by `tests/test_ordering.py`'s 1,000-sleeper test.
SHOULD:
- Add a FIFO-wake test here, citing D008.
- Comment on the `in log` check.
- Graceful `shutdown()` with a blocked putter.
- `timeout()` combined with a TaskGroup.

**Gotchas.** Mutating `time()` to return 0 makes the loop spin forever, because no timer is ever due. Don't use that mutation.

**Follow-ups.** The SHOULDs above.

## LOOP-09 — Trace hook (2026-11-09, batch 7)
Branch: feat/LOOP-09 + fix/LOOP-09-1 + fix/LOOP-09-2  ·  Merged: 62caa24

**Approach.** `src/desyncio/trace.py` defines `TraceRecord(t, seq, task_name, kind)` (`kind` ∈ start/resume/finish/cancel), a `Tracer`, a `ListSink`, and `_TracedCoroutine`, a subclass of `collections.abc.Coroutine`.
- Tracing is enabled with `desyncio.run(..., trace=sink)` or `VirtualTimeLoop(..., trace=sink)`, and is off by default.
- `VirtualTimeLoop.create_task` is overridden, not `set_task_factory`, so a user's own factory still works. It does two things:
  - gives every unnamed task `proc-N` from one per-loop counter, shared with `sim.spawn`; `desyncio.run` names the main task `main` via `name_next_task`;
  - wraps the coroutine only when tracing is on.
- The wrapper records `start` on the first send/throw and `resume` on later ones. A `StopIteration` or other exception records `finish`, and an escaping `CancelledError` records `cancel`. Records are made inside the step, so `t` is that step's virtual time.
- `stop_tracing_new_tasks()` runs before `Runner.close()`, so Runner's housekeeping tasks don't appear in the trace. Pending tasks still get their cleanup `cancel` records, at the frozen clock.
- **Sink errors (D016):** `Tracer.emit` catches the first sink exception, turns the sink off and calls the loop's `_on_trace_error`, which records it and calls `stop()`. The `run_forever` override re-raises it after `super().run_forever()`. During cleanup it is only recorded, and `run` raises it after close.
- All loop state of ours is in one `__slots__` `_LoopState` at `self._state`, which replaced `_sequence`.

**asyncio internals.**
- The C `Task.__step` can't be overridden. A coroutine-protocol object (`send`/`throw`/`close`) is the observable seam, and Task falls back from its fast path to generic `send` for such objects.
- `asyncio.create_task`, `TaskGroup`, `gather`, `ensure_future` and `Runner.run` all call `loop.create_task`.
- **CPython 3.13 instance-dict limit:** after construction, a loop has 28 `vars()` entries. Adding three more pushed it past the inline-values / shared-keys layout limit and slowed attribute access in stock `_run_once` by about 8–10% per step, even with tracing off. `test_loop_instance_dict_is_not_larger_than_before_loop_09` pins `len(vars(loop)) <= 28`.
- `loop.stop()` only sets a flag. Callbacks already in `_ready` in that iteration still run.

**Trade-offs.**
- A `create_task` override rather than a task factory, so user factories keep working.
- Containing sink errors and aborting the run, rather than letting them propagate into tasks (principle 4).

**Review findings.**
- Round 0 FAILED:
  - (MUST) tracing off was ~8–10% slower per step, due to the attribute count; the old benchmark test could not detect it because both sides used `VirtualTimeLoop`;
  - (MUST) §10's zero-cost claim was false;
  - (promoted to MUST, D016) a sink error replaced the task's result or its `CancelledError`, changing TaskGroup and timeout semantics.
- Fix round 1:
  - `_LoopState`. Re-measured by the reviewer: integration 0.76–0.81 s vs the fix branch 0.757–0.759 s for 100k sleeps, best of 7;
  - the deterministic attribute-count test replaces the vacuous benchmark test;
  - §10 rewritten with measured per-task numbers (the override adds ~0.3 µs to a ~2.5 µs `create_task`);
  - sink containment, with tests for start, finish and cancel errors, the main task, TaskGroup and cleanup. A subprocess check runs under `-W error -X dev`.
  Re-review FAILED with one MUST: §11 listed `run_forever` as stock although it is overridden.
- Fix round 2 corrected §11 and §2 and added a docs test. Re-review: PASS.
- The reviewer also ran all 47 LOOP-06 compat tests with tracing forced on, and they all pass.
- SHOULD:
  - A `KeyboardInterrupt`/`SystemExit` raised by the sink is deferred to the end of the iteration. Stock asyncio aborts immediately. Document it, or re-raise those two at once.
  - `run()`'s `finally` can replace an in-flight exception with a pending sink error. Chain it instead.
  - Direct `asyncio.Runner` use with a failing sink during cleanup is undocumented.
  - With tracing on, `inspect.iscoroutine(task.get_coro())` is False.
  - Tasks created by GC (async-generator finalisation) are traced and use up a `proc-N`. This is deterministic on CPython only.
  - §11 should list the `_clock_resolution` override.

**Gotchas.**
- **Adding attributes to `VirtualTimeLoop` is a performance hazard.** Put new state on `self._state`.
- A test that deliberately makes `run` fail with a bad argument needs `# type: ignore[call-overload]`.

**Follow-ups.** The SHOULDs above.

## LOOP-10 — Determinism invariant (2026-11-09, batch 8)
Branch: feat/LOOP-10  ·  Merged: 30103ad

**Approach.** `tests/models/mm1.py` is a reference M/M/1 queue, heavily commented for learners, using the public API only:
- exponential inter-arrival times from `sim.rng("arrivals")` and service times from `sim.rng("service")` (λ=0.9, μ=1.0);
- each customer is a `sim.spawn`ed task;
- the single FIFO server is an `asyncio.Lock`.
It returns frozen `CustomerRecord(arrival, service_start, departure)` objects.

`tests/test_determinism.py` covers:
- the same seed gives an identical full trace (over 10k `TraceRecord`s) and identical results;
- a different seed gives a different trace and results;
- the same seed in a subprocess, with `PYTHONHASHSEED` 0 and 12345, gives identical SHA-256 digests of `repr(trace)` and `repr(records)`;
- a sanity check: the mean time in system is 8.64 for seed 42 (theory 10, band 8–12), plus FIFO and single-server invariants.
The file takes about 3–4 s, and it runs in `scripts/gate.sh`, since CI is manual-only (D013).

**asyncio internals.** `asyncio.Lock` can't be barged in 3.13. `acquire`'s fast path needs no un-cancelled waiters. `release` → `_wake_up_first` resolves the first waiter's future, but the waiter stays in `_waiters` until it resumes, so a same-instant arrival queues behind it. That is why a Lock is a correct FIFO server.

**Trade-offs.** The same-seed comparison covers the whole trace, not just the statistics. Any nondeterminism anywhere in the stack (names, ordering, RNG) shows up as a diff.

**Review findings.** PASS.
- Mutations:
  - an unseeded RNG is caught by 3 tests;
  - swapped rates are caught by the sanity band (mean 582);
  - LIFO timer ties are NOT caught here. The model has no equal timestamps (20,001 distinct), and any deterministic mutation passes a same-seed test by construction. `tests/test_ordering.py` catches it (6 tests).
- SHOULD:
  - `from models.mm1 import` depends on pytest's default `prepend` import mode (`--import-mode=importlib` fails). Add a note, or load the model by path.
  - The band's lower bound is 0.64 below the seeded value; a change to RNG derivation might need it widened.
  - Add a model variant with quantised service times so ties occur.
  - Use `itertools.pairwise`.

**Gotchas.** A same-seed test can only catch *nondeterminism*, not a deterministic change in semantics.

**Follow-ups.** The SHOULDs above. With this, Module 1 is complete except LOOP-12 (deferred, D018).

## RES-01 — Resource (2026-11-09, batch 9)
Branch: feat/RES-01 + fix/RES-01-1  ·  Merged: f13f3bc

**Approach.** `src/desyncio/resource.py`: `Resource(capacity=1, *, monitor=None)` is a thin wrapper over `asyncio.Semaphore(capacity)`, used as `async with res:` (D019).
- **Delegated to asyncio:** the FIFO waiter queue, no barging, removing a cancelled waiter, and returning a unit granted to a task that was then cancelled.
- **Ours:** validation (capacity an int ≥ 1, bool rejected), read-only `capacity`/`count`/`queue_length` kept by our own counters (no private semaphore reads), the D023 v2 monitor, and the `__aenter__`/`__aexit__` plumbing. A Resource binds to the run in which it is first used.

**asyncio internals.**
- `_acquire` checks `sem.locked()` before awaiting `acquire()`. This is race-free, because acquire's fast path doesn't yield. `locked()` means "would wait", including when a waiter has been woken but hasn't run yet, which is where no-barging comes from.
- A request that has been granted but hasn't resumed still counts in `queue_length`. That state lasts zero virtual time.
- The cross-check is exact: an M/M/1 model built on `Resource` gives the same per-customer records as `tests/models/mm1.py` (an `asyncio.Lock` server) for the same seed.

**Trade-offs.** Our own counters rather than reading `Semaphore._value`/`_waiters`: that's the only public way to expose `queue_length`.

**Review findings.**
- Round 0 PASSED, with a 240-run counter stress probe. The lead then promoted one SHOULD to MUST under D023: a raising monitor left a unit held forever.
- Fix round 1 was redirected mid-round to D023 v2: monitor errors go to `loop.call_exception_handler`, so no undo logic is needed.
- Re-review PASSED: 300 seeds with a monitor raising at p=0.5 plus random cancels; no unit leaked; 16k errors reached the handler.
- SHOULD:
  - `_changed` calls `get_running_loop()`, which fails if the coroutine is driven by hand.
  - A monitor that raises a `BaseException` (e.g. `CancelledError`) on queue enter leaves `queue_length` incremented.

**Gotchas.** Reusing one Resource across two `desyncio.run` calls raises "bound to a different event loop" (asyncio behaviour). The docstring says so.

**Follow-ups.** The SHOULDs above.

## CON-01 — Container (2026-11-09, batch 9)
Branch: feat/CON-01 + fix/CON-01-1  ·  Merged: 4831169

**Approach.** `src/desyncio/container.py`: `Container(capacity=inf, init=0, *, monitor=None)`. asyncio has no level/amount primitive, so it is built on `asyncio.Condition` (principle 6).
- `put(amount)` waits while it would overflow; `get(amount)` waits while the level is too low. Levels are floats.
- Each direction has a deque of `object()` tickets, and the wait predicate is `queue[0] is ticket and fits()`. That gives strict FIFO, head-of-line (D021).
- A `finally` removes the ticket and calls `notify_all`. The monitor is then called as `monitor(container)` with the lock still held.

**asyncio internals.**
- `Condition` wakes its waiters in FIFO order, but each one re-checks only its own predicate. Without the tickets, a small `get` could overtake a large one waiting ahead of it.
- `Condition.wait` re-acquires the lock even when cancelled, so the `finally` always runs while holding the lock, which `notify_all` requires.
- Every change wakes all waiters, so a change costs O(N). This is documented.

**Trade-offs.** `Condition` plus tickets, rather than hand-rolled futures. The tickets are the minimum needed for head-of-line order.

**Review findings.**
- Round 0 FAILED with one MUST: the monitor was called as `monitor(level)` rather than `monitor(container)`.
- Fix round 1 applied D023 and D023 v2. The edit to `test_monitor_hook` is permitted, because the spec changed.
- Re-review PASSED: 200 seeds, a monitor raising at p=0.5, random cancels and timeouts. The level stayed within bounds, conservation held, head-of-line order held, and the deques ended empty.
- SHOULD: a test should pin "settled state" (no ticket left and the lock held when the monitor runs).

**Gotchas.** Amounts are validated when the call is awaited, not when the coroutine is created.

**Follow-ups.** The SHOULD above.

## STO-01 — Store (2026-11-09, batch 9)
Branch: feat/STO-01 + fix/STO-01-1  ·  Merged: a833dd6

**Approach.** `src/desyncio/store.py`: `Store[T](capacity=inf, *, monitor=None)` is a thin wrapper over `asyncio.Queue(maxsize)` (inf means maxsize 0).
- **Delegated to asyncio:** FIFO items and waiters, blocking put/get, cancellation cleanup (an item is never lost or duplicated) and `shutdown()`.
- **Ours:** validation (an int ≥ 1, or inf), `len()` (`qsize`), a read-only `items` snapshot, the shutdown passthrough and the D023 v2 monitor.
- `items` is a parallel deque, updated in the same synchronous step as the queue operation and cleared on `shutdown(immediate=True)`. `put_nowait`/`get_nowait` are not exposed, because they would bypass the mirror.

**asyncio internals.** In `Queue.put`/`get` the step that completes the operation is synchronous (`put_nowait`/`get_nowait`), so a cancel can't land between Queue finishing and our update. A probe over 600 seeds found no drift.

**Trade-offs.** The mirror vs a documented read of `Queue._queue`. The reviewer prefers the private read (one stable attribute, no second structure). Kept as an open SHOULD.

**Review findings.**
- Round 0 FAILED: a raising monitor lost the item in `get` (removed from the queue, never returned), and no test covered a raising monitor. This finding is what led to D023 v2.
- Fix round 1: monitor errors go to the handler, the monitor is also called on immediate shutdown, and the docstring says monitors must not block.
- Re-review PASSED: 1500 seeds, a randomly raising monitor, cancels and shutdowns. No lost or duplicated item, and the mirror always matched the queue.
- SHOULD:
  - `shutdown(immediate=True)` outside a loop with a raising monitor gives "no running event loop" instead of the monitor's error. Fall back to logging.
  - Add a test for a raising monitor on immediate shutdown.
  - A monitor raising `CancelledError` loses the item.

**Gotchas.** Capacity `3.0` is rejected; it must be an int.

**Follow-ups.** The SHOULDs above, and the mirror-vs-`_queue` question.

## INT-01 — Interrupts, timeout-style (2026-11-09, batch 9)
Branch: feat/INT-01-v2 + fix/INT-01-v2-1  ·  Merged: f5d2caa  ·  (discarded: feat/INT-01, design 2 of D024)

**Approach.** In `src/desyncio/interrupt.py`, `Interrupt(Exception)` carries `.cause`, and `Interruptible` is the scope returned by `sim.interruptible()`, with `.interrupted` and `.cause`. `sim.interrupt(task, cause)` is on the `Sim` facade.
- **Registry:** a module-level `WeakKeyDictionary[Task, list[Interruptible]]` holds each task's open blocks, innermost last. The blocks keep only a `weakref.ref` to their task, so entries really are dropped on GC (unless the cause refers back to the task).
- **Delivery:** `sim.interrupt` marks the innermost block (first cause wins) and calls `task.cancel()`. The block's `__aexit__` then calls `task.uncancel()` once, and if the result is ≤ the `cancelling()` value recorded on entry, it raises `Interrupt(cause) from CancelledError`. So a real cancel always wins.
- **RuntimeError** for: a target with no open block, a finished target, or the current task.

**asyncio internals.**
- An exception raised at the innermost `await` can only be translated by a frame it passes through on the way out. `asyncio.timeout()` works because its `__aexit__` runs in the user's frame, and this design copies exactly that (D024).
- Delivery is an ordinary cancel, so every primitive cleans up through its standard cancel path: the `Lock` waiter handover, `Condition` re-acquire, `Semaphore`'s grant fix-up, the `Queue` waiter, and a `TaskGroup` cancelling its children.
- In 3.13, `uncancel()` also clears a pending `_must_cancel` once the count reaches 0. So an interrupt that arrives after the body's last await leaves no stray `CancelledError`: the block exits normally with `interrupted=True`.
- Nesting with `asyncio.timeout()` firing at the same instant follows asyncio's own double-fire semantics, so the outer scope wins.
- A ContextVar is the wrong registry: the interrupter runs in another task, and child tasks copy the context.

**Trade-offs.** Interruptible code must be marked explicitly; in exchange, the mental model is the familiar `asyncio.timeout()` one and only public APIs are used. D024 records the two rejected designs:
- failing the awaited future: three proven bugs (Lock, Condition, TaskGroup exit) and a private `_fut_waiter`;
- `Interrupt(CancelledError)`: leaked `Semaphore` capacity and ended tasks as cancelled.

**Review findings.**
- The first design (`feat/INT-01`) FAILED review, with three MUSTs, and the design was replaced.
- v2, round 0 FAILED on one MUST: the code is adapted from CPython's `asyncio.Timeout` (`_State`, `__aenter__`, `__aexit__`, `_on_timeout`: same states, error strings and `uncancel()` condition), so it needs the PSF marking (AGENTS §3, as D009). The lead also promoted, under D011, a false docstring claim that registry entries are dropped on GC (the blocks held a strong reference to the task).
- Fix round 1: a marking block plus a list of changes, `interrupt.py` added to the `LICENSES/PSF.txt` header, a notice test, a `weakref.ref` to the task, a dead-task-safe `__aexit__`, and a GC test.
- Re-review PASSED: the adversarial probes all held (Lock handover, Condition re-acquire under contention, TaskGroup, nesting, real cancel in both orders, late interrupt), plus 300 stress seeds against Resource, Container and Store with zero failures and determinism checked.
- SHOULD:
  - Add a test and a docstring line for a late interrupt (`interrupted` can be True with no `Interrupt` raised).
  - Add a test pinning the ExceptionGroup limit: a real child failure while interrupting propagates untranslated, with `interrupted` True and `cancelling()` restored. `asyncio.timeout()` splices its error into the group; we don't.
  - Document that an interrupt aimed at an already-unwinding inner block is dropped, not passed to the outer block.
  - The shipped stress test has only 20 seeds; add the same-instant interrupt+cancel mix.
  - The "changes from the original" list misses the state rename (`EXPIRING`→`INTERRUPTING`), `expired()`→`interrupted`, the dropped assert, and `__repr__`.

**Gotchas.**
- `except Interrupt` *inside* a block never catches that block's own interrupt: translation happens at that block's exit, as with `TimeoutError`.
- The PR to `main` must flag the PSF-adapted code in `interrupt.py`.

**Follow-ups.** The SHOULDs above. RES-03 (preemption) builds on `interruptible()`.

## RND-01 — Random streams: rename and reuse stdlib seeding (2026-11-09, batch 10a)
Branch: feat/RND-01 + fix/RND-01-1  ·  Merged: ddc6dda

**Approach.** `git mv rng.py randomness.py`, with no shim. `RngStreams` became `RandomStreams`, `current_rng_streams()` became `current_random_streams()`, the ContextVar became `_random_streams` (`"desyncio_random_streams"`), and `sim.rng(name)` became `sim.random(name)`. `derive_seed` and the SHA-256 encoding are gone. A stream is `random.Random(f"{seed}:{name}")`, cached in the per-run `RandomStreams`, so repeated calls continue one sequence and a new `desyncio.run` restarts it. Seed and name validation are unchanged. Every caller in tests now uses `sim.random`.

**asyncio internals.** None new. The run's `RandomStreams` still reaches tasks through a ContextVar set in a fresh `contextvars.Context` (see LOOP-04). `random.Random(str)` uses version-2 seeding: the string is hashed with SHA-512, never with `hash()`, so `PYTHONHASHSEED` does not affect it. A subprocess test checks that with two hash seeds.

**Trade-offs.** The `:` separator needs no length prefix, because a seed is a non-negative int (digits only, `bool` rejected). The first `:` always ends it, even when the name contains `:`. A test covers `(1,"23")`/`(12,"3")` and `(1,":2")`/`(1,"2")`. Every random sequence changed once (D026). The new known values are pinned in `tests/test_randomness.py`, and the M/M/1 band was re-picked (D026 outcome).

**Review findings.**
- Round 0 FAILED on one MUST: on Python 3.14, `def random(self, name) -> random.Random` inside `Sim` resolves `random` to the method (PEP 649 evaluates annotations lazily, in class scope), so `typing.get_type_hints(Sim.random)` raised `AttributeError`. 3.13, mypy and ruff all miss it.
- Fix round 1: `from random import Random`, `-> Random`, and `test_random_return_annotation_resolves_to_random_class`, which was shown to fail on 3.14 before the fix. The fixer scanned src/ for other class members shadowing imported names and found none.
- Re-review PASSED. The gate passes with 468 tests.
- SHOULD:
  - Explain in the M/M/1 band comment that the band is about ±2 sd.
  - The cross-process test passes `env={"PYTHONHASHSEED": ...}` only, dropping PATH. Use `{**os.environ, ...}`.
  - Add an explicit test that a second `desyncio.run` with the same seed restarts a stream (covered indirectly by `tests/test_sim.py::_draw`).
  - `ruff format --check` flags 6 files, all predating this feature. The gate does not run the formatter.

**Gotchas.**
- Naming a method after a module the class uses breaks annotations on 3.14 only. Import the class itself (`from random import Random`).
- Reviewers twice ran a command in the main checkout before their worktree existed (harmless). Tell them to `mkdir -p` the scratch dir and `cd` in the same command.

**Follow-ups.** The SHOULDs above. D027: the gate tests only 3.13. Older log entries keep the old names (`sim.rng`, `RngStreams`, `derive_seed`) as history.

## MON-01 — Monitors / TimeSeries (2026-11-09, batch 10b)
Branch: feat/MON-01 + fix/MON-01-1  ·  Merged: 457ab52

**Approach.** `src/desyncio/monitor.py` defines `TimeSeries`, which imports only `sim` and nothing from the primitives (principle 4). You wire it into a primitive with the D023 v2 hook, e.g. `Resource(1, monitor=lambda r: busy.record(r.count))`, and record the initial value yourself (D028).
- `record(value)` stamps the value with `sim.now`, adds the area of the previous segment, and tracks min and max as it goes. `record` and `mean` are O(1); `samples` returns a tuple copy, which is O(n).
- `mean(until=None)` integrates over `[first, until]`, where the last value holds until `until`; `None` means `sim.now`.
- Rules, all tested: values must be finite int or float (`bool` is rejected); a rejected record changes nothing; for same-instant records the last one wins in the mean but all of them count for min and max; an empty series or an `until` before the last record raises `ValueError`; a zero-length span returns the last value; a `record` earlier than the last record raises `ValueError` (D029: one series per run).

**asyncio internals.** None. It is a plain observer called synchronously from the primitives' monitor hook.

**Trade-offs.** Incremental area against recomputing from the samples: a 1e5-record probe agreed with an `fsum` recomputation to within about 1e-14 relative error.

**Review findings.**
- Round 0 FAILED. At ρ=0.5, Lq equals utilisation (both 0.5), so recording `count` as the queue length, or swapping the two series, still passed. The lead also promoted two items under D029: a test for D016, and the time-going-backwards bug, where a reused series silently added negative area.
- Fix round 1:
  - ρ is now 0.6 (Lq=0.9), with bands of ±0.03 and ±0.12. Over seeds 1..10 the worst errors were 0.011 and 0.040.
  - A deterministic count/queue_length test was added.
  - A with/without-monitor test was added; it compares per-customer records.
  - `record` now rejects a time before the last record.
- Re-review PASSED. Each mutation was applied and caught by a failing test: count recorded as Lq, the series swapped, the time check removed, and a monitor that draws from `sim.random`.
- SHOULD: add a floating-point `mean(until=...)` assertion on random data.

**Gotchas.** A same-instant pair `(0,0),(0,1)` is normal when a task acquires at t=0. Pick an M/M/1 test load where the statistics you compare differ.

**Follow-ups.** The SHOULD above. API-01 exports `TimeSeries`.

## STO-02 — PriorityStore and FilterStore (2026-11-09, batch 10b)
Branch: feat/STO-02  ·  Merged: 11f488a

**Approach.** Both stores live in `src/desyncio/store.py`. They are separate classes, not subclasses of `Store`. The capacity check, the monitor check and the D023 v2 monitor call moved into module-level helpers that all three stores share. `Store`'s behaviour and messages are unchanged.
- **PriorityStore:** `put(item, priority=0)`. A thin wrapper over `asyncio.PriorityQueue`, holding `(priority, next(itertools.count()), item)` entries. A dict keyed by sequence number mirrors the entries, so `items` (in get order) never reads `PriorityQueue._queue`. `shutdown` is a passthrough.
- **FilterStore:** one `asyncio.Condition`.
  - `get(filter=None)` does `wait_for(take_first_match)`, removing the item inside the predicate. `put` does `wait_for(len < capacity)`.
  - Both call `notify_all()` under the lock; the monitor runs after the lock is released.
  - It has no `shutdown()` (D031).

**asyncio internals.**
- `Condition.wait_for` returns as soon as the predicate is true, with no `await` in between, so the removal is atomic with `get` returning.
- A getter cancelled after a notify re-acquires the lock inside `Condition.wait` and raises before its predicate runs, so nothing is removed.
- Unlike RES-02 round 0 (D030), nothing needs the lock *after* a state change that must not be skipped. The release of the lock in `__aexit__` is synchronous.
- `notify_all` wakes waiters in park order and the `Lock` admits them FIFO. So the longest-waiting matching getter takes the first matching item, and a non-matching getter re-parks without blocking a later one.

**Trade-offs.** Priority via `put(..., priority=)` or a key function: the first was chosen (D031). Same-instant barging is accepted, because it matches `asyncio.Queue` (D031).

**Review findings.**
- PASS on round 0, with no MUSTs.
- The reviewer ran a seeded stress test over 400 seeds per store: random putters and getters, cancels, and `sim.interrupt` inside `interruptible()`. There were no lost, duplicated or half-inserted items, and the mirror stayed consistent.
- SHOULD:
  - Pin same-instant barging parity with a test.
  - Add a test that a getter keeps its seniority after re-parking.
  - Add a test of a filter that reads `len(store)` or `items`.
  - Record the missing `shutdown` as a decision (done: D031).

**Gotchas.** `get(filter=...)` shadows a builtin, hence `# noqa: A002`. A filter runs under the store's lock, so it must not `await` or call `put`/`get`.

**Follow-ups.** The SHOULDs above. A possible async `FilterStore.shutdown()`.

## RES-02 — PriorityResource (2026-11-09, batch 10b)
Branch: feat/RES-02 + fix/RES-02-1  ·  Merged: 40e9dca

**Approach.** `PriorityResource(capacity=1, *, monitor=None)` lives in `src/desyncio/resource.py`, beside `Resource`. `request(priority=0)` returns a single-use `PriorityRequest`, a class so that RES-03 can add `preempt=`. Plain `async with res:` uses priority 0. Priorities must be int or float; `bool` and NaN are rejected, and negative and infinite values are allowed. Request order is fixed when the `async with` is entered.
- **Final design (D030), modelled on `asyncio.Semaphore`:** each waiter awaits its own `loop.create_future()`. Waiters sit in a sorted list of `(priority, seq, fut)`, kept with `insort`. `seq` comes from `itertools.count()`, so a future is never compared.
- **Release** is synchronous: decrement, then `_grant_free_units()`, which pops the best live waiters and calls `set_result`. A `_granted` counter covers units handed to waiters that have not resumed yet, so a same-instant newcomer finds no free unit and can't barge.
- **Fast path:** take a unit without suspending only if one is free and no live waiter has priority <= ours.
- **Cancellation:** a waiter that was already granted does `_granted -= 1` and passes the unit on; a pending waiter removes its own entry. Either way it then calls `_grant_free_units()`.
- **Counts:** `queue_length = len(_waiters) + _granted`, the same semantics as RES-01.

**asyncio internals.**
- `Condition.notify_all()` must hold the lock. After a notify, the woken waiters queue on `Lock._waiters` to re-acquire it, and `Lock.acquire()` skips its fast path whenever others are queued. So under round 0's Condition design, a holder's `__aexit__` could really suspend. A cancel at that point skipped the decrement for good (D030).
- `Semaphore` avoids this by releasing synchronously and fixing up on `CancelledError` in `_acquire`. RES-02 now copies that pattern; no code is copied.

**Trade-offs.**
- A sorted list, not a heap: a cancelled pending waiter must leave the order at once so that `queue_length` is right, and a list does that with `bisect_left` plus `del`. Insertion is O(n), which is fine at simulation queue sizes.
- The futures design also removes round 0's O(waiters) thundering herd.

**Review findings.**
- Round 0 FAILED: the Condition design leaked a unit when the holder was cancelled while `__aexit__` waited for the lock. The probe ended with `count 1 queue 1` and no grant.
- Fix round 1 moved to per-request futures (D030). It added tests for cancel at exit, body exception plus cancel (the body error is kept), interrupt at exit, and a waiter cancelled in the step it was granted.
- Re-review PASSED. A seeded stress test over 3000 seeds (capacity 1-3, random priorities, cancels and interrupts) found no leak, the invariants held at every monitor call, and service order was consistent. 573 tests pass.
- SHOULD:
  - Delete the dead `_hold_round0_lock` test helper, which reads the removed `_cond`, along with its spawns.
  - Fix the stale "Condition FIFO" comment in `test_two_units_freed_at_same_instant_go_to_two_best`.
  - Document in `queue_length` that a waiter cancelled while pending is counted until its task runs (the same lag as `Semaphore`).
  - `_changed` is duplicated between `Resource` and `PriorityResource`.

**Gotchas.**
- Any `await` in a context manager's `__aexit__` is a cancellation point. A release that must not be skipped has to be synchronous.
- A public-API test could not make round 0 fail, because the lock was contended only in a particular window. The fixer reproduced it by holding the private lock (see the SHOULD above).

**Follow-ups.** The SHOULDs above. RES-03 extends `PriorityRequest` with `preempt=True`.

## CI-03 — Gate: Python 3.14 and ruff format (2026-11-09, batch 11)
Branch: feat/CI-03  ·  Merged: d17d8d3

**Approach.**
- `scripts/gate.sh` has a new optional `TEST_CMD_314`. It runs after `TEST_CMD` and before lint, and fails with "tests failed on Python 3.14". When the variable is unset, the gate behaves as before.
- `gate.env` sets `TEST_CMD_314="UV_PROJECT_ENVIRONMENT=.venv-3.14 uv run --locked --python 3.14 pytest -q"` and adds `ruff format --check .` to `LINT_CMD`.
- `ci.yml` stays manual-only (D013) and mirrors both changes.
- One separate commit (7232114) formats the 6 files that were failing; the reviewer confirmed the ASTs are identical.

**asyncio internals.** The 3.14 failure was in `test_loop_instance_dict_is_not_larger_than_before_loop_09`. Stock `asyncio.SelectorEventLoop` has 26 instance attributes on 3.13 and 27 on 3.14, and `VirtualTimeLoop` adds a net +2 on both (28 and 29).
- **Test change (permitted by D033):** the hard-coded limit of 28 became `len(vars(SelectorEventLoop())) + 2`, measured on the running interpreter. The limit on 3.13 is still 28, so D017 is enforced exactly as before.
- A mutation that adds one instance attribute fails on both interpreters.

**Trade-offs.** The 3.14 run uses its own environment, so the 3.13 run and lint keep the default `.venv`. `ruff` skips `.venv-3.14` only because uv writes a `.gitignore` containing `*` into every venv it creates.

**Review findings.**
- PASS. The reviewer checked that a missing interpreter (`--python 3.99`) and a misformatted file both make the gate fail loudly. 573 tests pass on 3.13 and on 3.14.
- SHOULD:
  - Make the venv exclusion robust: add `.venv-3.14` to `.gitignore` or to ruff's `extend-exclude`.
  - Add an explicit `uv python install 3.14` step to ci.yml.

**Gotchas.** From now on, every branch must be `ruff format`-clean and pass on 3.14 (D033).

**Follow-ups.** The SHOULDs above.

## RES-03 — PreemptiveResource (2026-11-09, batch 11)
Branch: feat/RES-03 + fix/RES-03-1  ·  Merged: 525deb1

**Approach.** `PreemptiveResource(PriorityResource)` and `Preempted(by, usage_since, resource)`, a frozen dataclass, are in `src/desyncio/resource.py`. The semantics follow D032 (SimPy-style, chosen by the owner).
- `request(priority=0, preempt=True)` returns a `PreemptiveRequest`. It acquires a unit, then enters an `Interruptible()`, so every holder's body is preemptible. Plain `async with res:` keeps a per-task stack of requests.
- `PriorityResource._acquire` gained four no-op hooks (`_arriving`, `_unit_assigned`, `_became_holder`, `_unit_dropped`) and a ticket slot in each waiter entry. A `_Ticket` records the task, priority, preempt flag, seq, `since`, `held` and `evicted`.
- **Preemption (`_arriving`):** it happens only when all three hold: no unit is free, the request is at the head of the queue, and the worst holder of another task (max `(priority, seq)`) is strictly worse.
- **Eviction (`_evict`)** is synchronous. It removes the victim, decrements `count` (or `_granted`, for a reservation), and then calls `interrupt_task(victim, Preempted(...))`. The preemptor then takes the freed unit on the fast path, and the monitor is called once.
- **`__aexit__`:** it releases synchronously, unless the request was evicted, and only then exits the scope.

**asyncio internals.**
- `Interruptible.__aenter__` and `__aexit__` are `async` but never await. That is what makes "grant, then scope" and "release, then scope exit" free of leaks. `test_interruptible_enter_and_exit_never_await` pins it (D034).
- `sim.interrupt` delivers to the innermost `interruptible()` block. So a victim with its own nested block receives the preemption there, after the unit is already gone (documented).
- A real cancel always wins over a preemption (INT-01).

**Trade-offs.**
- **Hooks in RES-02 vs a separate acquire:** hooks keep a single acquire path. The cost is a few no-op calls per acquire on a plain `PriorityResource`.
- **A reservation can be preempted** (D034, for the owner to confirm): a granted waiter whose task has not resumed yet counts as held. Its `__aenter__` raises `Interrupt(Preempted)` itself, without any cancellation.

**Review findings.**
- Round 0 FAILED on formatting only: `resource.py` was not `ruff format`-clean under D033.
  - The reviewer's stress test (1000 seeds: preempt on and off, cancels, `asyncio.timeout()`, nested `interruptible()`) found no leaked or double-released unit.
  - The D032 rules and cases a–e held.
  - The lead promoted the no-await guard to MUST (D034).
- Fix round 1: `ruff format` (the ASTs are identical) and the AST guard test (a mutation that adds `await` fails it).
- Re-review PASSED on the merged tree: 642 tests on 3.13 and on 3.14, ruff format clean.
- The merge itself deviated from the process (D035).
- SHOULD:
  - The AST guard can't see an await reached through a helper.
  - Add a test that would catch a future `await` between `_acquire` and `Interruptible.__aenter__`.
- The clean-ups D032 folded in are done: the dead `_hold_round0_lock` is removed, the stale comment is fixed, and `_call_monitor` is shared between `Resource` and `PriorityResource`.

**Gotchas.**
- Within one instant, the preemptor's body runs before the victim's `except Interrupt` handler.
- A task that holds every unit and asks for another one waits for itself. Avoid nesting there.

**Follow-ups.** API-01 exports `PreemptiveResource` and `Preempted`. DOC-02 adds the machine-shop example.

## API-01 — Public exports (2026-11-09, batch 12)
Branch: feat/API-01  ·  Merged: aaa8bba

**Approach.**
- `src/desyncio/__init__.py` re-exports the D036 list: `Resource`, `PriorityResource`, `PreemptiveResource`, `Preempted`, `Container`, `Store`, `PriorityStore`, `FilterStore`, `Interrupt`, `TimeSeries`, `RealIOError`, `SimulationDeadlock` and `RealTimePacer`, plus `run`, `sim` and `__version__`. It has one import line per module, sorted, and a docstring listing the public API.
- `tests/test_import.py` checks:
  - the exact `__all__` set;
  - that each name is the same object as in its defining module;
  - that a star-import gives exactly `__all__`;
  - that each submodule imports first, then `desyncio`, in a fresh subprocess;
  - an AST guard against `from desyncio import sim` inside the package.

**asyncio internals.** None. The trap is Python's import system. `desyncio.sim` is both a submodule and, once `__init__` runs, the `sim` facade object bound over it. `monitor.py`'s `from desyncio import sim` could therefore get the *module*, depending on import order. That broke 16 monitor tests and failed mypy once `__init__` imported monitor. The fix is `from desyncio.sim import sim` (D037).

**Trade-offs.** The implementer's first fix was an import-order workaround in `__init__` (`# isort: skip`). It was replaced by fixing the import at its source.

**Review findings.**
- The implementer stopped on two blockers outside its files. The lead extended ownership under D037: the INT-01 test `test_not_exported_at_top_level` was inverted (a permitted spec change), and the monitor.py import was fixed.
- PASS on the first review. 688 tests pass on 3.13 and on 3.14.
- The mutations were caught: reverting the `sim` import fails the AST guard, and an import cycle in resource.py fails the suite.
- SHOULD:
  - add a named subprocess test that `import desyncio` succeeds, so a cycle fails by name and not as a collection error;
  - generalise the AST guard to any `from desyncio import X` inside the package.
- Process note: the lead asked the implementer to squash its WIP commit and force-push, but the environment blocked that as a destructive git action, so the WIP commit `843549e` stays in history.

**Gotchas.** Inside the package, never write `from desyncio import <name>`, because a package attribute can shadow a submodule of the same name. Import from the defining module.

**Follow-ups.** The SHOULDs above. DOC-02 uses top-level imports, and the README lists the exports.


## DOC-02 — Primitives guide (2026-11-09, batch 13)
Branch: feat/DOC-02 (+ fix/DOC-02-1)  ·  Merged: b3746f4

**Approach.**
- `docs/primitives.md` gives each primitive (Resource, PriorityResource, PreemptiveResource/Preempted, Container, Store, FilterStore, PriorityStore, interrupts, the monitor hook and TimeSeries, and `sim.random`) three things: an example, a "what is asyncio, what is ours" table, and a guarantees section.
- The six scenarios are bank tellers, fuel tank, producer/consumer, machine breakdown, machine-shop preemption and car wash (`sim.random`, D026). The guide has 24 executed examples, and most guarantees are pinned by one.
- Convention: a ```` ```python ```` block is a complete example, and the ```` ```text ```` block right after it is its exact stdout. `tests/test_docs_examples.py` runs every python block with `exec` in fresh globals, compares the stdout, and runs it twice for determinism. It also checks:
  - imports are top-level only (`from desyncio import ...`);
  - the guide mentions every primitive and the D030, D031, D034 and D023 limits;
  - the README links the guide, its example runs, and its "Public API" bullet list equals `desyncio.__all__`.
- `ruff format` also formats python blocks inside Markdown, so the gate's `ruff format --check .` covers the guide's examples.
- The README status note ("target API") was false and was rewritten.

**asyncio internals.** The guide explains, from the source:
- `Store` and `PriorityStore` delegate to `asyncio.Queue` and `asyncio.PriorityQueue`.
- `Container` and `FilterStore` are built on `asyncio.Condition` with tickets.
- `PriorityResource` waiting is hand-written per-request futures (D030).
- Interrupts are `task.cancel()` plus `uncancel()` (D024).
- A detail found in review: `uncancel()` at `__aexit__` undoes only the interrupt's own cancel. If a real cancel also fired, `task.cancelling()` stays one higher after the block. That is how an enclosing `timeout()` or `TaskGroup` knows the cancel is still pending.

**Trade-offs.**
- The examples are checked by exact stdout, not by doctest. Whole programs read better than `>>>` sessions, and a changed number fails loudly.
- The car-wash output depends on CPython's `Random(str)` seeding and its `expovariate`/`uniform` algorithms. It passes on 3.13 and 3.14. If CPython changes those algorithms, the example fails on purpose.

**Review findings.**
- FAIL in round 1:
  - MUST: the guide claimed `cancelling()` is restored after an interrupted block even when a real cancel also fired. That is false (it stays +1). The wording was corrected and an executed example pins both cases.
  - Lead promotion (D038): the extractor silently skipped indented fences, so an example could go unrun with the suite still green. Guards were added against indented and tilde fences, and fence lines must equal twice the parsed blocks. The tests are also parametrized per example (ids such as `19-guarantees`), so one failure no longer hides the rest.
- PASS on re-review. 788 tests on 3.13 and 3.14; `GATE PASS`.

**Gotchas.** Read the README export list and the guide's claims as tests: changing the public API or a guarantee breaks `tests/test_docs_examples.py`, by design (D011).

**Follow-ups (SHOULD).**
- A ```` ```Python ```` or ```` ```python3 ```` fence is parsed but never run, and the suite stays green (only the test count drops). Fix it with an allowlist of fence languages.
- The substring tests (`test_guide_covers_every_primitive`, `test_guide_documents_the_known_limits`) pin little.
- Split the three preemption edge cases into separate headed examples, for readability.
- The `src/desyncio/interrupt.py` module docstring repeats the false `cancelling()` sentence (D038).
- Turn some prose-only claims into examples. The reviewer probed them and they are true: the Resource monitor event sequence, a raising filter leaving the store unchanged, `Container` amount > capacity raising ValueError, no monitor call for a cancelled `Container.get`, and the monitor call on an immediate shutdown.

## CLN-01 — Interrupt docstring; `sim.random` error message (2026-11-09, batch 14)
Branch: feat/CLN-01 (+ fix/CLN-01-1)  ·  Merged: fa6d5dc

**Approach.**
- The `interrupt.py` module docstring now says what the guide says (D038): `uncancel()` undoes only the interrupt's own cancel. A new test pins both deltas side by side (interrupt only: +0 and `Interrupt`; interrupt + `task.cancel()`: +1 and `CancelledError`).
- `sim.random` calls `_loop()` first, so the outside-run message is unchanged. It then catches the `RuntimeError` from `current_random_streams()` and re-raises with a message that names the fresh `contextvars.Context` (D039). It uses `from None`, because the chained error would be the misleading one. There is no fallback, and the stream storage is unchanged.

**asyncio internals.** `loop.create_task(coro, context=ctx)` runs every step of the task inside `ctx` (`Context.run`). A fresh `Context()` therefore has none of the run's ContextVars. The running loop is still found through `asyncio.get_running_loop()`, which is per thread, not a ContextVar. So only `sim.random` fails in such a task; `sim.now`, `timeout`, `spawn` and `interruptible` work.

**Review findings.**
- FAIL in round 1. The lead and the reviewer each found the same MUST independently: the new "fresh Context" wording had been added to the Raises sections of `now`, `timeout`, `spawn` and `interruptible`, where it is false. The fix reverted those four docstrings and added `test_non_random_methods_work_in_fresh_context_task`. The outside-run message test now matches the real `_NOT_RUNNING` text.
- PASS on re-review.

**Follow-ups (SHOULD).** In `test_random_in_fresh_context_task_names_the_cause`, `assert "no simulation is running" not in msg` is always true; delete it.

## CLN-02 — `SequenceCounter` → `itertools.count` (2026-11-09, batch 14)
Branch: feat/CLN-02  ·  Merged: 5142464

**Approach.** The `SequenceCounter` wrapper class is gone. `_LoopState.sequence` is a plain `itertools.count()` made in `__init__` (`__slots__`, one per loop; D017 holds), and `_schedule` calls `next(self._state.sequence)`. The lead fixed `docs/how-the-loop-works.md:144`, which named the old class (e913229, D011).

**asyncio internals.** None new. `itertools.count` is a C iterator, and `next()` on it is atomic under the GIL. The loop is single-threaded anyway.

**Review findings.** PASS. The ordering and determinism tests are unmodified and pass. Two interleaved loops each start at 0.

**Follow-ups (SHOULD).** The new test partly pins the implementation (it reads `_state` and checks the type). The docstring bullet in scheduling.py has a short reflowed line. `uv run mypy src tests` reports 34 errors in tests; the gate runs `mypy src` only. Decide whether tests should be type-checked.

## CLN-03 — Real-time pacer notes (2026-11-09, batch 14)
Branch: feat/CLN-03  ·  Merged: fea39d8

**Approach.** Docstring warnings were added to `RealTimePacer.pause()` and to `run()`'s `pacer` argument: `pause()` from inside the simulation with no other thread blocks forever, and the paused state persists across runs (D039: documented, not changed). A fake-wall-clock test pins the reuse behaviour. A test asserts `MAX_REAL_WAIT <= asyncio.base_events.MAXIMUM_SELECT_TIMEOUT`, and a comment at the constant explains why.

**asyncio internals.** A paused pacer makes the selector wait with no timeout. Only a thread-safe wake-up (`call_soon_threadsafe` writes to the self-pipe) can end that wait. A coroutine on the frozen loop never runs, so it cannot resume the pacer.

**Review findings.** PASS. The reviewer confirmed "blocks forever" with a real pacer and a watchdog thread, for both a coroutine and a `call_later` callback. Mutating `bind()` to reset `_paused` fails the new test. The new test cannot hang: `FakeWall` asserts instead of blocking.

**Follow-ups (SHOULD).** The test needs `# type: ignore[attr-defined]` on `MAXIMUM_SELECT_TIMEOUT`, which is cosmetic, since mypy checks src only.

## CLN-04 — Test hardening (2026-11-09, batch 14)
Branch: feat/CLN-04  ·  Merged: 747c800

**Approach.** Tests only.
- `test_every_fence_language_is_allowed` sets the allowlist to `{python, text}`, the only languages the guide and README use. A `Python`, `python3` or bare fence now fails by name.
- `test_import_desyncio_succeeds_in_a_fresh_interpreter` is a named subprocess test.
- `test_no_from_desyncio_import_sim` became `test_no_from_desyncio_import_x`. It forbids any absolute `from desyncio import X` inside the package, which strictly includes the old check.
- `test_filter_getter_keeps_seniority_after_re_parking[1|2|3]`.

**asyncio internals.** `Condition.notify_all` wakes waiters in park order. Each woken waiter must re-acquire the `Lock`, which is FIFO, so a getter that re-parks keeps its place relative to the others.

**Review findings.**
- PASS. The reviewer repeated the mutations. A persistent loss of seniority fails all three cases. A toggling mutation (reversing the waiters on every put) fails only the even case, so the parametrization matters for toggling bugs.
- The implementer reported that relative imports (`from . import X`) are also rejected. They are not: the guard checks only `level == 0`. The old guard had the same gap.

**Follow-ups (SHOULD).** Reject `from . import X` (`level >= 1 and module is None`) too. Optionally, assert the getters' intermediate wake order.

## CLN-05 — One float validator (2026-11-09, batch 15)
Branch: feat/CLN-05 (+ fix/CLN-05-1)  ·  Merged: fb22adf

**Approach.** The new private module `src/desyncio/_validate.py` has `to_float(value, *, type_error, overflow_error, finite_error=None, normalise_zero=False)`. It rejects bool and non-numbers with `TypeError`, turns `OverflowError` into `ValueError`, and can check finiteness and normalise `-0.0`. The messages are `str.format` templates (`{type}`, `{value!r}`). Each of the four validators (clock, until, dt, speed) is now one call plus its own range check, with its exact old messages. D040 is the one behaviour change: `speed=10**400` raises `ValueError` instead of `OverflowError`.

**asyncio internals.** None.

**Trade-offs.** The source is the same size as before (a 44-line helper replaces about 40 lines in the callers). The gain is one place for the bool/overflow/NaN rules, not fewer lines. The clock builds its templates with an f-string around `what`. That is safe only while `what` is a brace-free internal constant, and a comment now says so.

**Review findings.**
- PASS. The reviewer compared old and new behaviour on 190 (input × validator) cases: type, message, return value and the sign of zero. The only differences were the D040 inputs.
- The lead promoted the test-volume SHOULD to a MUST (D041): the redundant helper tests were dropped, the per-caller messages became one exact-message table with a bool row, and D040 got its own test. That took tests/test_clock.py from 264 lines to 229.
- PASS on re-review.

**Follow-ups (SHOULD).** `_helper_kwargs()` is only used once. The ids of the `_BIG` table rows print the full `10**400` value.

## TST-01 — Trim resource and container tests (2026-11-09, batch 16)
Branch: feat/TST-01 (+ fix/TST-01-1)  ·  Merged: 116e770

**Approach.** test_resource.py went from 1813 lines to about 1500, and test_container.py from 374 to 292. Together they went from 153 items to 103.
- The Resource/PriorityResource twins are parametrized by class: release, monitor sequence, cancelled leave, granted-then-cancelled, constructor validation and read-only properties.
- The 5 raising-monitor tests became `test_raising_monitor_never_changes_model`, which asserts exactly 11 handler errors (7 + 2 + 2, commented per scenario).
- `test_preemption_stress[0..39]` became one hypothesis test (40 derandomized draws).
- Never-preempts and victim choice are parametrized, and the validation tables are trimmed.
- Dropped: Resource FIFO, the docs duplicates (machine shop, fuel tank) and Container's basic blocking.
- `test_no_barging_same_instant_newcomer` stays (D042).

**Review findings.**
- FAIL in round 1. MUST: the merged release test used plain `async with` on PriorityResource, which lost the only guard for `PriorityRequest.__aexit__`'s release. A mutation that released only when `exc_type is None` passed the trimmed suite and failed the old one.
- The fix added explicit `(cls, hold)` pairs. The request case now also covers a holder that is cancelled while holding: a release skipped on CancelledError was never caught before, by the old suite either.
- PASS on re-review: both mutations fail `[PriorityResource-request]`.

**Follow-ups (SHOULD).** `False` was dropped from the bool rows of the validation tables, and `-1`, `None` and `1j` from `test_bad_amount`. They share branches with the rows kept, so this is low risk.

## TST-02 — Trim store and monitor tests (2026-11-09, batch 16)
Branch: feat/TST-02  ·  Merged: 8c69f38

**Approach.** test_store.py went from 83 items and 1412 lines to 56 and 631; test_monitor.py from 33 and 333 to 17 and 280.
- One contract per behaviour, parametrized over `[Store, PriorityStore, FilterStore]` through a 5-line `get(store, match=None)` adapter. FilterStore always gets a filter, so its Condition path is exercised.
- The tests that re-proved `asyncio.Queue` (FIFO, blocking, shutdown wake-ups) and the docs duplicates were dropped. Those guarantees are now pinned by `<!-- pins -->` comments on the docs examples (TST-04).
- The 1.8 s M/M/1 same-seed TimeSeries test was dropped.
- New case: `TimeSeries.record(10**400)` raises ValueError.

**asyncio internals.** None new. The module docstring says why Queue behaviour is deliberately not re-proved (principle 6).

**Review findings.**
- PASS. The reviewer redid the monitor and mirror mutations for all three kinds.
- Two gaps went to TST-06, not to a fix round:
  - the default priority-0 FIFO of `PriorityStore.put(item)` was asserted only by a dropped test;
  - no test guards a cancelled putter leaking into `items`. The old suite missed that too.

## TST-03 — Trim interrupt and asyncio-compat tests (2026-11-09, batch 16)
Branch: feat/TST-03  ·  Merged: cb4fa4c

**Approach.**
- test_interrupt.py went from 1112 lines to 783 (62 → 32 tests).
  - The `asyncio.timeout` nesting tests became a 2×2 parametrize, asserting `cancelling() == 0` in all four cases. The cancel-wins orders and the delivered-at-await tests were parametrized, and the 20 stress seeds became one loop with the seed in the failure message.
  - The raw asyncio tests and the docs duplicates were dropped.
  - The D024 design-2 Lock, Condition and TaskGroup regressions, the weakref GC test and the PSF notice were kept.
- test_asyncio_compat.py went from 988 lines to 656 (47 → 25 functions, 31 cases). Pure-asyncio tests and duplicates were dropped. The reschedule, TaskGroup failure source, gather and shield tests were parametrized.

**Review findings.**
- PASS. The merged reschedule-later case still checks both "extends" and "fires". The shield cases still check that the inner task outlives the stop.
- Not regressions: a LIFO tie-break mutation passes the compat suite before and after the trim; test_resource and test_ordering catch it. "Interrupt a task not yet started" lost its own test, but it shares a branch that is still tested.

## TST-04 — Trim module 1, import and docs tests; pin docs examples (2026-11-09, batch 16)
Branch: feat/TST-04 (+ fix/TST-04-1)  ·  Merged: ff97718

**Approach.** 11 files went from 437 items and 3342 lines to 306 and 3017.
- Each docs example is one test, run twice against its expected output.
- test_import dropped the 15 `submodule_imports_first` subprocesses: Python always runs the package `__init__` first, which `-X importtime` confirmed. The AST guard is now one test.
- test_guards dropped 8 tests that duplicated the per-method refusal test.
- Duplicates and redundant validation rows were dropped across loop, ordering, realtime, run, sim, trace, clock and randomness.
- docs/primitives.md got 14 `<!-- pins: ... (D042) -->` lines above the examples that are now the only guard. No example code or output changed.

**Review findings.**
- FAIL in round 1. MUST: the PriorityResource pin claimed "default priority 0 in a plain request", but the example never makes a plain request. The fix dropped that claim. The same round reworded the `test_overload_types` comment so it does not depend on the gate (TST-05 changes the gate).
- The lead reviewed the 2-line comment-only fix diff directly, instead of running another reviewer.
- The implementer's commits lack the attribution trailers. They are accepted, because history rewrites are blocked.

## TST-06 — Gap tests (2026-11-09, batch 17)
Branch: feat/TST-06 (+ fix/TST-06-1)  ·  Merged: afaf4eb

**Approach.** Five gaps from the audit and the TST-02 review are now pinned, and the suite goes from 550 to 556 tests.
- `test_late_interrupt_after_the_last_await_is_not_raised[eager]`.
- `test_real_taskgroup_failure_while_interrupted_is_not_translated`: an `ExceptionGroup[ValueError]` propagates, `interrupted` is True and `cancelling()` is 0.
- `test_raising_monitor_on_immediate_shutdown_still_completes[Store|PriorityStore]`.
- `test_priority_default_priority_is_zero_and_ties_are_fifo`.
- `items` assertions in `test_cancelled_putter_is_removed`.
- `test_interruptible_enter_and_exit_never_await` moved unchanged to test_interrupt.py.

**asyncio internals.**
- **A late interrupt arises only from an eagerly started task.** With `eager_task_factory` installed, or `eager_start=True`, the task runs its first step inside the target's own step. So `task.cancel()` on the target only sets `_must_cancel` and throws nothing. In 3.13+, `uncancel()` in `__aexit__` then clears that pending cancel, so the block exits normally with `interrupted` True.
- **Everywhere else the interrupt is delivered normally.** Timers, `call_later`, callbacks and ordinary tasks run only between steps, while the block is suspended at an await, so they raise a normal `Interrupt`.
- **An ordinary task spawned after the last await is too late.** It first runs after the block has closed, so interrupting then raises RuntimeError "not inside".
- **Self-interrupt is refused** with "a task cannot interrupt itself".

**Review findings.**
- FAIL in round 1, D011. The new docstring bullet was true but did not say when the case can arise, so it read as if an ordinary interrupt could be dropped.
- The fix made the bullet precise and parametrized the test over eager. The fixer rightly disputed the lead's request that the non-eager case should raise `Interrupt`: the block has closed by then, and the reviewer agreed.
- PASS on re-review.

**Gotcha.** Restoring a mutated src file within the same second leaves a stale `.pyc` (same size, 1 s mtime resolution). Delete `src/desyncio/__pycache__` between mutation runs.

**Follow-ups (SHOULD).** The docstring could also say, in one clause, that the non-eager late case gets RuntimeError "not inside".

## TST-05 — Type-check the tests in the gate (2026-11-09, batch 18)
Branch: feat/TST-05  ·  Merged: f02743c

**Approach.** `LINT_CMD` now ends with `mypy src tests`. The 33 errors in tests/ were fixed in the tests: typed helpers (`_to_float`, `_record_both`, `_is_closed`), `Sequence[float]` parameters, a named inner `def` in place of a lambda with a default argument, and `list(...)` around results. There are no overrides; tests run under full strict. No `# type: ignore` was added, and 10 unused ones were removed. `test_overload_types` is now a real check: making the `until=` overload return `T` fails `mypy` at test_run.py:340–341.

**Review findings.**
- PASS.
- `list(x.items) == [...]` replaced two tuple comparisons. mypy had narrowed `items` to `tuple[int]` after an earlier assert, and then called the next comparison non-overlapping. The tuple type is still pinned by neighbouring asserts.
- **Gate quirk (D044).** `scripts/gate.sh` sources `gate.env` from the main checkout, not from the branch under test. So a branch that changes the gate is gated with the old gate. The new lint step ran on `integration` after the merge: GATE PASS, mypy on 36 files.

**Follow-ups (SHOULD).** Source `gate.env` from the checked-out worktree in `gate.sh`. The alternative `items: tuple[object, ...] = store.items` keeps the tuple check without narrowing.


## CLN-06 — Gate reads gate.env from the tested tree (2026-11-09, batch 19)
Branch: feat/CLN-06  ·  Merged: c22b7ee

**Approach.** The `gate.env` source and the `${VAR:-default}` lines moved from before `git worktree add` to just after `cd "$TMP"`. The script sources `$TMP/scripts/gate.env` if present, else the main checkout's, else uses the built-in defaults. Steps, order, exit 2 and the `GATE PASS (<branch>)` line are unchanged.

**asyncio internals.** None.

**Trade-offs.** Sourcing the tested tree's file means the branch decides its own gate commands (D046). The alternative, keeping the main checkout's file, was the D044 gap.

**Review findings.** No MUST. Proof reproduced by the reviewer: a throwaway branch with `TEST_CMD="false"` gives `GATE FAIL` with the new script and `GATE PASS` with the old; a tree without `gate.env` falls back to the main checkout's; a partial `gate.env` takes defaults for the rest under `set -u`.

**Gotchas.** `gate.sh` itself still runs from the checkout it is started in, so changes to the script are still verified after merge (D044, D046).

**Follow-ups (SHOULD).** The trap is set only after `git worktree add` succeeds, so a bad branch name leaves an empty `mktemp` directory behind (pre-existing). Set the trap right after `mktemp -d`.

## CLN-07 — Raising monitor outside a running loop (2026-11-09, batch 19)
Branch: feat/CLN-07  ·  Merged: 0da8d5e

**Approach.** A new private `src/desyncio/_monitor.py` has `report_monitor_error(message, exc, key, primitive)`. Resource/PriorityResource (`_call_monitor`), Container (`_notify_monitor`) and the stores (`_call_monitor`) call it from their existing `except Exception`. Inside a loop it calls `call_exception_handler` with the same context as before; with no running loop it logs the message at ERROR on the `desyncio` logger with `exc_info` (D045). Two new parametrized tests (Store, PriorityStore) call `shutdown(immediate=True)` after `desyncio.run` has closed its loop. 556 → 558 tests.

**asyncio internals.** `asyncio.get_running_loop()` raises `RuntimeError` when the thread has no running loop; unlike `get_event_loop()` it never creates or guesses one. The `try` covers only that call, so a `RuntimeError` from a custom exception handler is not swallowed.

**Trade-offs.** One helper instead of three copies of the reporting code. The `desyncio` logger, not `asyncio`'s, so users can tell the source; the message text matches the handler's context message.

**Review findings.** No MUST. The reviewer reverted the src changes and saw both new tests fail with the original `RuntimeError`.

**Gotchas.** Each call site keeps its own `except Exception`, so `KeyboardInterrupt`/`SystemExit` still propagate (D023 v2).

**Follow-ups (SHOULD).** Resource and Container use the helper but have no outside-loop test (the spec asked for stores only).

## CLN-08 — Relative-import guard; late-interrupt docstring (2026-11-09, batch 19)
Branch: feat/CLN-08  ·  Merged: 36d0215

**Approach.** `test_no_from_desyncio_import_x` now also asserts `node.module is not None` for any `ImportFrom` with `level >= 1`, so `from . import X` and `from .. import X` fail while `from .sim import sim` passes. `interrupt.py`'s late-interrupt bullet gains one clause: with a non-eager task the late `interrupt()` raises `RuntimeError` ("not inside ..."), because the block has already closed.

**asyncio internals.** A non-eager task created inside the block first runs on the next loop iteration, after the block has exited; an eager task (`eager_start`) runs its first step synchronously at creation, inside the block.

**Trade-offs.** None.

**Review findings.** No MUST. Mutation reproduced by the reviewer: `from . import clock` in `store.py` makes the guard fail. `SUBMODULES` globs every module, including private ones like `_validate.py`.

**Gotchas.** `__init__.py` is not scanned, where `from . import X` is legitimate.

**Follow-ups.** None.

## DEP-01 — numpy and scipy as dependencies (2026-11-09, batch 20)
Branch: feat/DEP-01  ·  Merged: 6790cdf

**Approach.** `uv add` put `numpy>=2.0` and `scipy>=1.16.2` in `[project] dependencies`, and `scipy-stubs` in the dev group (it pulls in `optype` and `numpy-typing-compat`, dev only). The lock resolves numpy 2.5.4 and scipy 1.18.1; wheels install on 3.13 and 3.14. No mypy override was needed. Two tests in `tests/test_import.py`: a smoke test (numpy major ≥ 2, `scipy.stats.norm.cdf(0) == 0.5`) and a subprocess test that `import desyncio` loads neither numpy nor scipy. 558 → 560 tests.

**asyncio internals.** None.

**Trade-offs.** `scipy-stubs` instead of a mypy override, so scipy calls are type-checked under strict mode.

**Review findings.** No MUST. The reviewer proved the sys.modules test by adding `import numpy` to `__init__.py` (it failed), and checked `--resolution lowest-direct` on 3.14: scipy 1.16.2 installs and imports.

**Gotchas.** `numpy>=2.0` is a nominal floor on 3.14: numpy 2.0.0 has no 3.14 wheels, so the real floor there is higher. The spec's reason for 2.0 is weighted `np.quantile`.

**Follow-ups (SHOULD).** Raise the numpy floor to the first release with 3.14 wheels, once checked.

## STA-01 — Tally: statistics of observations (2026-11-09, batch 21)
Branch: feat/STA-01  ·  Merged: 47dd7f5

**Approach.** New `src/desyncio/tally.py`. `Tally.record(value)` appends a validated float (it reuses `monitor._real`) and never reads `sim.now`. Every statistic converts the list to an array and calls numpy: `np.mean`, `np.var`/`np.std` with `ddof=1`, `np.min`/`np.max`, `np.quantile` (default linear method), `np.histogram`. Results are converted to Python `float`/`int`. numpy is imported inside each method.

**asyncio internals.** None: `record` is a synchronous append.

**Trade-offs.** O(n) per statistic (array conversion) instead of Welford's O(1) running mean: D048 says call numpy, and statistics are read rarely.

**Review findings.** No MUST. Mutations caught: `ddof=0`, `method="lower"`. Deferred to STA-03 (D050): `quantile` accepts `bool`/`str` p, unlike `TimeSeries.quantile`.

**Gotchas.** A numpy array as `p` is not a `Sequence`, so it takes the scalar path and numpy raises a TypeError.

**Follow-ups (SHOULD).** Move `_real` to a shared private module if a third user appears.

## STA-02 — TimeSeries: time-weighted distribution (2026-11-09, batch 21)
Branch: feat/STA-02 (+ fix/STA-02-1)  ·  Merged: 24856f2

**Approach.** A private `_weighted(until, what)` returns `(values, durations)` with the same checks and order as `mean()`, dropping zero-duration segments. `time_fractions` sums durations per value with `math.fsum`; `quantile` is `np.quantile(values, p, weights=durations, method="inverted_cdf")`; `variance` is `np.average` of squared deviations with duration weights (population). A zero-length span gives `{last: 1.0}`, last, and 0.0.

**asyncio internals.** None beyond `sim.now`.

**Trade-offs.** numpy ≥ 2.0 supports `weights=` only with `method="inverted_cdf"`, so the time-weighted quantile is a step function (no interpolation).

**Review findings.** Round 1 (D050, promoted SHOULD): the M/M/1 test's comment overstated the error (±0.02) and its band was loose (0.05). Measured max error over seeds 1..10 is 0.0123; the band is now 0.04, and a ×1.2 bias on P(N=0) that passed at 0.05 now fails.

**Gotchas.** The M/M/1 test uses its own small model, because `tests/models/mm1.py` records nothing about the number in system. It takes about 0.7 s.

**Follow-ups.** None.

## STA-04 — Replications and confidence intervals (2026-11-09, batch 21)
Branch: feat/STA-04 (+ fix/STA-04-1)  ·  Merged: 18e16ce

**Approach.** New `src/desyncio/experiment.py`. `replicate(model, n, *, seed, until, speed, pacer, trace)` calls `run(model(), seed=seed + i, ...)` for each i; explicit keyword params instead of `**run_kwargs`, with two `@overload`s mirroring `run` (`list[T]` without `until`, `list[T | None]` with it). `confidence_interval(values, level)` uses `scipy.stats.t.interval(level, n-1, loc=mean, scale=scipy.stats.sem(values))` and returns `(mean, half_width)`; scipy gives nan for a zero scale, so all-equal values return `(mean, 0.0)`.

**asyncio internals.** Each replication is a separate `desyncio.run`, so a fresh loop and fresh random streams per run.

**Trade-offs.** `n` must be an `int` (TypeError for `2.0` or `bool`), which is stricter than the row.

**Review findings.** Round 1, two MUSTs: (1) `cast(T, ...)` hid a real `None` behind `list[T]` when `until` was set; fixed with overloads and `assert_type` tests checked by mypy in the gate. (2) The seed+i contract was not pinned (`seed + i + 1` passed); the test now compares against `run(..., seed=3 + i)` directly. The docstring also says a `pacer` is shared by all runs and keeps its paused state (CLN-03).

**Gotchas.** The coverage test uses a seeded `random.Random`, not a simulation, so it is cheap and deterministic.

**Follow-ups (SHOULD).** A test that `speed`/`pacer`/`trace` reach the run in both overload branches.

## RND-02 — numpy random streams (2026-11-09, batch 21)
Branch: feat/RND-02 (+ fix/RND-02-1)  ·  Merged: 402bcb4

**Approach.** `RandomStreams.np_stream(name)` caches a `numpy.random.Generator` per name in its own dict, seeded with `default_rng(SeedSequence(int.from_bytes(sha256(f"{seed}:{name}").digest(), "big")))`. `Sim.np_random(name)` mirrors `Sim.random`, including the fresh-context error. numpy is imported inside `np_stream`; annotations use `TYPE_CHECKING`.

**asyncio internals.** The streams live in the run's `contextvars` context (D026), so a task created with a fresh `contextvars.Context` cannot see them.

**Trade-offs.** SHA-256 instead of `hash()`, because `PYTHONHASHSEED` randomises `str` hashes per process. `SeedSequence` spreads the 256-bit entropy into the generator's state.

**Review findings.** Round 1 (D050, promoted SHOULD): the formula was not pinned, so a changed key passed every test. A golden-value test now pins `(0, "x")` and `(42, "service")`; key swap, sha1 and separator mutations all fail it. The cross-process test now inherits `os.environ`.

**Gotchas.** `Sim.np_random` duplicates about 10 lines of `Sim.random`'s error handling; sharing it would have touched CLN-01's tested message.

**Follow-ups (SHOULD).** Factor the shared error handling into one helper with the method name as a parameter.
