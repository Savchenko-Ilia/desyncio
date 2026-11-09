# Test-suite audit (2026-11-09, after batch 15)

The owner asked whether the suite is larger than needed. Three read-only audits covered every test file on `integration`. **Nothing has been changed yet: this is a proposal awaiting the owner's decision** (see `docs/handoff.md`).

| Area | Items | Lines | Items after (est.) | Lines after (est.) |
|---|---|---|---|---|
| test_resource, test_container | 153 | 2187 | ~78 | ~1645 |
| test_store, test_interrupt, test_monitor | 179 | 2857 | ~123 | ~1780 |
| Module 1 and infrastructure (13 files) | 469 | 4331 | ~323 | ~3670 |
| **Total** | **801** | **~9400** | **~525** | **~7100** |

The runtime goes from about 11 s to about 8 s. The figures are estimates; none of the merged tests has been written.

## Rules for any trimming
- Keep every test that guards a logged regression (see the implementation-log fix rounds), every determinism test, the D017 dict guard, the PSF-notice tests, the D006 thread tests, the D016 sink tests, the LOOP-11 overflow and float-step tests, the D030 exit-leak tests, the D034 no-await guard, the case_a–e preemption tests, the CLN-01–05 tests and the DOC-02 fence guards.
- For each merge, show that the merged test fails under the mutations the old tests caught.
- Where a docs example becomes the only guard, add a comment above it in `docs/primitives.md` saying what it pins.

## Proposed changes by file

### test_resource.py / test_container.py
- `test_preemption_stress[0..39]` → one `hypothesis` test (`max_examples=40, derandomize=True`).
- 5 raising-monitor tests → one test parametrized over `[Resource, PriorityResource]`. It keeps the error count, the `__context__ is None` check and "the cancelled waiter still sees CancelledError".
- Parametrize the Resource/PriorityResource twins by class: release on exception or cancel, monitor sequence, monitor on cancelled leave, granted-then-cancelled, cancel at exit, single use.
- Validation tables: constructor, priority, preempt, Container amounts.
- Drop:
  - asyncio re-proofs: Resource FIFO, and no-barging (the owner may keep one as a Semaphore-swap guard);
  - docs duplicates: machine shop, fuel tank;
  - basic Container blocking (×2) and the Resource cancelled waiter;
  - the Priority cancelled-at-instant subset.
- Parametrize never-preempts ×3 and victim choice ×2.

### test_store.py
- One contract per behaviour, parametrized over `[Store, PriorityStore, FilterStore]` with a small `get()` adapter: cancellation (at arrival, hand-on, putter, no duplicates), monitor calls, monitor not called on cancel, raising monitor, BaseException propagation, determinism.
- Drop:
  - Queue re-proofs: FIFO, getter and putter order, blocking, the shutdown passthroughs;
  - docs duplicates: producer/consumer, the PriorityStore order tests, FilterStore block-until-match.
- Keep: `test_raising_monitor_on_get_still_returns_the_item` (the STO-01 regression), FilterStore no-head-of-line, seniority (add a `0` case), raising filters, the PriorityStore capacity mapping, items-not-comparable, the snapshot order.

### test_interrupt.py
- The 4 `asyncio.timeout` nesting tests → a 2×2 parametrize.
- The 2 cancel-wins orders → parametrized.
- The 2 "delivered at await" tests → parametrized.
- Optional: the 20 stress seeds → one test looping over them.
- Drop:
  - raw asyncio: Queue, Event and Semaphore interrupts (Semaphore is borderline);
  - duplicates: clock-never-reaches-cancelled-timer, holder releases unit;
  - docs duplicates: machine breakdown, the cancelling delta, first cause wins, not-in-block / not-started;
  - `test_exported_at_top_level`.
- Keep the D024 design-2 regressions (Lock, Condition, TaskGroup), the weakref GC test and the PSF notice.

### test_monitor.py
- Drop `test_mm1_same_seed_gives_identical_samples` (1.8 s; TimeSeries neither schedules nor draws randoms).
- Drop the docs duplicates: default `until`, Resource wiring.
- Merge the record-value validation and add the missing `10**400` overflow case.

### Module 1 and infrastructure
- **test_docs_examples:** 3 tests per example → 1 (assert output declared, then run twice against expected). Drop `readme_mentions_every_export` (implied by the list equality) and the python-count test (implied by the fence guards).
- **test_import:**
  - Drop `submodule_imports_first[15]`. Python always runs the package `__init__` first, so all 15 test the same import sequence.
  - Drop `star_import_yields_all`.
  - Make the AST guard one test.
- **test_asyncio_compat** (47 → ~29):
  - drop pure-asyncio tests: Queue shutdown without waiting, Event already set, `wait_for(None)`, suppress-cancel, cancel-before-first-step;
  - drop duplicates of test_loop, test_sim and test_trace;
  - parametrize the reschedule ×4, TaskGroup failure source, gather ×2 and shield ×2 tests.
- **test_guards:** drop 7 tests that repeat the parametrized refusal test or test_loop. Keep the 28 per-method items, because each is a separate override.
- **Cross-file duplicates:** the export tests in test_run and test_sim; the sink propagation and the hash-seed subprocess in test_trace; pacer bind; the 1e-12 speed; the seed tests in test_run; `random_outside_run_message_unchanged`; the always-true assert in test_sim (a CLN-01 SHOULD).
- **Trim validation lists:** clock BAD_TYPES, `until`, `speed`, randomness `bad_seed[False]`.

## Gaps found, for the owner to decide
- `test_overload_types` (test_run) is never type-checked: the gate runs `mypy src`, and `mypy tests` reports 34 errors.
- No direct test of a late interrupt (`interrupted` True, no `Interrupt` raised).
- No test of the ExceptionGroup limit.
- No test of a raising monitor on `shutdown(immediate=True)`.
- `test_interruptible_enter_and_exit_never_await` sits in test_resource.py but pins interrupt.py.
