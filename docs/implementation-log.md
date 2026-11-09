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
