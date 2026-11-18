"""Executed end-to-end check for a generated notebook.

This is the only test that runs a notebook in a real kernel: a MOCK model is
generated, loaded through ``nbformat`` (which validates the document schema) and
executed with ``nbclient``. The optional dependencies live in the ``test``
extra, so without them the test skips instead of failing, and it is marked
``slow`` so the default unit job can exclude it with ``-m "not slow"``.

Set ``SIGIL_SKIP_NOTEBOOK_E2E=1`` to skip it even when the dependencies exist;
``python -m pytest tests/test_notebook_exec.py -q -rs`` then reports one skip.
"""

from __future__ import annotations

import os
import re
import tempfile

import pytest

from sigil.language import parse_string
from sigil.notebook import generate_notebook

_SINK_TOPIC = "out/topic"

# MOCK broker, no window: the outputs cell drains nothing and the message count
# is final as soon as the inputs cell returns. FILTER keeps all three samples.
_SRC = """
BROKER b
    TRANSPORT: MOCK
    HOST: "mock"
    PORT: 0
;

PIPELINE relay
    SOURCE FROM b :: "in/topic"
    |> FILTER msg.value > 1
    |> MAP {
        doubled = msg.value * 2
    }
    |> SINK TO b :: "out/topic"
;
"""

_SINK_COUNT = re.compile(re.escape(_SINK_TOPIC) + r": (\d+) message\(s\)")


def _outputs_cell(cells) -> dict:
    """Return the executed outputs cell from an executed notebook."""
    for cell in cells:
        if cell["cell_type"] == "code" and "_msgs[-5:]" in "".join(cell["source"]):
            return cell
    raise AssertionError("executed notebook has no outputs cell")


@pytest.mark.slow
@pytest.mark.skipif(
    bool(os.environ.get("SIGIL_SKIP_NOTEBOOK_E2E")),
    reason="SIGIL_SKIP_NOTEBOOK_E2E is set",
)
def test_executed_notebook_captures_sink_output():
    nbformat = pytest.importorskip("nbformat")
    pytest.importorskip("ipykernel")
    nbclient = pytest.importorskip("nbclient")

    with tempfile.TemporaryDirectory() as tmp:
        path = generate_notebook(parse_string(_SRC), tmp, mode="mock", name="exec_e2e")
        notebook = nbformat.read(path, as_version=4)
        executed = nbclient.NotebookClient(notebook, timeout=120, kernel_name="python3").execute()

    for index, cell in enumerate(executed.cells):
        errors = [
            output for output in cell.get("outputs", []) if output.get("output_type") == "error"
        ]
        assert errors == [], (index, errors)

    outputs = _outputs_cell(executed.cells)
    stdout = "".join(
        output.get("text", "")
        for output in outputs["outputs"]
        if output.get("output_type") == "stream"
    )
    match = _SINK_COUNT.search(stdout)
    assert match is not None, stdout
    assert int(match.group(1)) > 0

    displayed = "".join(
        output["data"].get("text/plain", "")
        for output in outputs["outputs"]
        if output.get("output_type") in {"display_data", "execute_result"}
    )
    assert "doubled" in displayed
    assert "24.0" in displayed
