"""Golden-file tests for the code generator.

For each example in ``GOLDEN_EXAMPLES`` we parse the ``.sigil`` source,
generate a Python application into a temp directory, and compare every
generated file byte-for-byte against a stored snapshot under
``tests/golden/<name>/``.

When the generator output legitimately changes, refresh the snapshots
with::

    UPDATE_GOLDEN=1 pytest tests/test_golden.py

Then review the diff in the golden tree as part of the PR.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from sigil.codegen import generate
from sigil.language import parse

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples"
GOLDEN_ROOT = Path(__file__).resolve().parent / "golden"

GOLDEN_EXAMPLES = [
    "01_hello_passthrough",
    "02_filtering",
    "03_tumbling_window",
    "07_full_pipeline",
    "16_conditional_routing",
]


def _generated_files(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        out[rel] = path.read_text(encoding="utf-8")
    return out


def _write_golden(snapshot_dir: Path, files: dict[str, str]) -> None:
    if snapshot_dir.exists():
        for existing in sorted(snapshot_dir.rglob("*"), reverse=True):
            if existing.is_file():
                existing.unlink()
            elif existing.is_dir() and existing != snapshot_dir:
                existing.rmdir()
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for rel, content in files.items():
        target = snapshot_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def _read_golden(snapshot_dir: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(snapshot_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(snapshot_dir).as_posix()
        out[rel] = path.read_text(encoding="utf-8")
    return out


@pytest.mark.parametrize("example", GOLDEN_EXAMPLES, ids=GOLDEN_EXAMPLES)
def test_codegen_matches_golden(example: str, tmp_path: Path) -> None:
    source = EXAMPLES_DIR / f"{example}.sigil"
    assert source.exists(), f"missing example file: {source}"

    model = parse(str(source))
    out_dir = tmp_path / "generated"
    generate(model, str(out_dir))

    actual = _generated_files(out_dir)
    snapshot_dir = GOLDEN_ROOT / example

    if os.environ.get("UPDATE_GOLDEN"):
        _write_golden(snapshot_dir, actual)
        pytest.skip(f"golden updated for {example}")
        return

    if not snapshot_dir.exists():
        pytest.fail(
            f"no golden snapshot for {example}. "
            f"Run `UPDATE_GOLDEN=1 pytest tests/test_golden.py` to create one."
        )

    expected = _read_golden(snapshot_dir)

    extra = sorted(set(actual) - set(expected))
    missing = sorted(set(expected) - set(actual))
    assert not extra, f"{example}: generated files not in golden: {extra}"
    assert not missing, f"{example}: golden files not generated: {missing}"

    for rel in sorted(actual):
        assert actual[rel] == expected[rel], (
            f"{example}: {rel} drifted from golden. "
            f"Run `UPDATE_GOLDEN=1 pytest tests/test_golden.py` if intentional."
        )
