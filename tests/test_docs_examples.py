"""Run every example in docs/primitives.md and check the README (DOC-02, D011).

Convention in docs/primitives.md:

* a fenced ``python`` block is a complete, runnable example;
* the fenced ``text`` block right after it is its exact expected stdout;
* a fenced ``py`` block is an illustrative fragment and is not run.

A wrong claim in an example therefore fails here: the printed output (and any
``assert`` in the example) is checked against the real library.
"""

import ast
import contextlib
import io
import re
from pathlib import Path

import pytest

import desyncio

ROOT = Path(__file__).resolve().parent.parent
GUIDE = ROOT / "docs" / "primitives.md"
README = ROOT / "README.md"

FENCE = re.compile(r"^```(\w*)[ \t]*\n(.*?)^```[ \t]*$", re.DOTALL | re.MULTILINE)
# Any line that could be read as a fence by a Markdown renderer: backticks or
# tildes, at any indentation. FENCE only understands column-0 backtick fences, so
# every such line must be one FENCE consumed, or an example is silently skipped.
ANY_FENCE_LINE = re.compile(r"^[ \t]*(```|~~~)", re.MULTILINE)
INDENTED_FENCE_LINE = re.compile(r"^[ \t]+(```|~~~).*$", re.MULTILINE)
HEADING = re.compile(r"^#{1,6}[ \t]+(.+?)[ \t]*$", re.MULTILINE)


def _blocks(path: Path) -> list[tuple[str, str]]:
    return [(m.group(1), m.group(2)) for m in FENCE.finditer(path.read_text())]


def _examples(path: Path) -> list[tuple[str, str | None]]:
    """(code, expected_output_or_None) for each ``python`` block, in order."""
    blocks = _blocks(path)
    found: list[tuple[str, str | None]] = []
    for i, (lang, body) in enumerate(blocks):
        if lang != "python":
            continue
        nxt = blocks[i + 1] if i + 1 < len(blocks) else None
        found.append((body, nxt[1] if nxt and nxt[0] == "text" else None))
    return found


def _run(code: str) -> str:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        exec(compile(code, "<docs example>", "exec"), {"__name__": "__main__"})
    return out.getvalue()


def _guide_examples() -> list[tuple[str, str | None]]:
    assert GUIDE.exists(), "docs/primitives.md is missing"
    return _examples(GUIDE)


def _example_ids(path: Path) -> list[str]:
    """One id per ``python`` block: its index and the nearest heading above it."""
    text = path.read_text()
    ids: list[str] = []
    heading, prose_start = "top", 0
    for m in FENCE.finditer(text):
        # Only look for headings in the prose between blocks: a ``# comment``
        # inside a code block is not a heading.
        found = HEADING.findall(text, prose_start, m.start())
        heading = found[-1] if found else heading
        prose_start = m.end()
        if m.group(1) == "python":
            slug = re.sub(r"[^A-Za-z0-9]+", "-", heading).strip("-").lower()
            ids.append(f"{len(ids):02d}-{slug}")
    return ids


GUIDE_EXAMPLES = _guide_examples()
GUIDE_IDS = _example_ids(GUIDE)


@pytest.mark.parametrize("path", [GUIDE, README], ids=["guide", "readme"])
def test_no_indented_or_tilde_fences(path: Path) -> None:
    """An indented fence is not matched by FENCE, so its example would never run."""
    text = path.read_text()
    indented = [m.group(0) for m in INDENTED_FENCE_LINE.finditer(text)]
    assert not indented, f"indented fence lines in {path.name}: {indented}"
    tildes = [m.group(0) for m in ANY_FENCE_LINE.finditer(text) if "~~~" in m.group(0)]
    assert not tildes, f"tilde fences in {path.name} are never run; use ```"


@pytest.mark.parametrize("path", [GUIDE, README], ids=["guide", "readme"])
def test_every_fence_line_is_parsed(path: Path) -> None:
    """Each fence-looking line is the opener or closer of one parsed block."""
    text = path.read_text()
    fence_lines = len(ANY_FENCE_LINE.findall(text))
    assert fence_lines == 2 * len(_blocks(path)), (
        f"{path.name}: {fence_lines} fence lines but {len(_blocks(path))} parsed "
        "blocks; a fence is malformed and its example would be skipped"
    )


def test_python_block_count_matches_python_fence_lines() -> None:
    text = GUIDE.read_text()
    openers = len(re.findall(r"^```python[ \t]*$", text, re.MULTILINE))
    assert len(GUIDE_EXAMPLES) == openers


def test_guide_exists_and_has_examples() -> None:
    assert len(GUIDE_EXAMPLES) >= 8
    assert len(GUIDE_IDS) == len(GUIDE_EXAMPLES)


@pytest.mark.parametrize(("code", "expected"), GUIDE_EXAMPLES, ids=GUIDE_IDS)
def test_every_guide_example_declares_its_output(
    code: str, expected: str | None
) -> None:
    assert expected is not None, f"example has no ```text output block:\n{code}"


@pytest.mark.parametrize(("code", "expected"), GUIDE_EXAMPLES, ids=GUIDE_IDS)
def test_guide_examples_run_and_print_what_the_docs_say(
    code: str, expected: str | None
) -> None:
    assert _run(code) == expected, code


@pytest.mark.parametrize(("code", "expected"), GUIDE_EXAMPLES, ids=GUIDE_IDS)
def test_guide_examples_are_deterministic(code: str, expected: str | None) -> None:
    assert _run(code) == _run(code), code


def test_examples_use_top_level_imports_only() -> None:
    for code, _ in GUIDE_EXAMPLES + _examples(README):
        for node in ast.walk(ast.parse(code)):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "desyncio"
            ):
                assert node.module == "desyncio", ast.unparse(node)
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("desyncio."), alias.name


def test_guide_covers_every_primitive() -> None:
    text = GUIDE.read_text()
    for name in (
        "Resource",
        "PriorityResource",
        "PreemptiveResource",
        "Preempted",
        "Container",
        "Store",
        "FilterStore",
        "PriorityStore",
        "Interrupt",
        "TimeSeries",
        "sim.interruptible()",
        "sim.random(",
    ):
        assert name in text, name


def test_guide_documents_the_known_limits() -> None:
    text = GUIDE.read_text()
    for needle in ("D031", "D034", "D030", "D023", "nested"):
        assert needle in text, needle


def test_readme_links_the_guide() -> None:
    assert "docs/primitives.md" in README.read_text()


def test_readme_example_runs() -> None:
    (code, _), *_ = _examples(README)
    assert _run(code).splitlines() == [
        "ship-0 leaves at t=12.0",
        "ship-1 leaves at t=24.0",
        "ship-2 leaves at t=36.0",
    ]


def test_readme_export_list_matches_all() -> None:
    text = README.read_text()
    section = text[text.index("## Public API") :]
    end = section.find("\n## ", 1)
    section = section[: end if end != -1 else None]
    listed = set(re.findall(r"^- `([A-Za-z_]+)`", section, re.MULTILINE))
    assert listed == set(desyncio.__all__)


@pytest.mark.parametrize("name", desyncio.__all__)
def test_readme_mentions_every_export(name: str) -> None:
    assert f"`{name}`" in README.read_text()
