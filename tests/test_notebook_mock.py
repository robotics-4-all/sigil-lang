"""Driver-level tests for generated Sigil notebooks.

Every test builds a notebook from an inline DSL string, then executes the
driver cells in a plain namespace dict with ``exec``. No kernel, no ``nbformat``
and no optional dependency is needed: a notebook is JSON and the driver cells
are ordinary Python once the ``%pip`` magic cell is skipped.

The preamble patches ``commlib.transports.mock._MOCK_BUS_LOCK`` to an ``RLock``
for the whole process. That is intentional (the generated notebooks depend on
it) and is deliberately not restored, so these tests also prove the patch
survives repeated preamble execution.

Cell order is fixed: 0 title, 1 install, 2 preamble, 3..2+N pipeline loaders,
then instantiate, start, inputs, outputs and cleanup. ``_cell_index`` derives
the driver indices from the total cell count instead of hard-coding them.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading

import commlib.transports.mock as mock_transport

from sigil.language import parse
from sigil.notebook import generate_notebook
from tests.test_notebook import _nb

_MOCK_BROKER = """BROKER b
    TRANSPORT: MOCK
    HOST: "mock"
    PORT: 0
;
"""

# FILTER keeps all three samples (12, 25, 5), MAP derives ``doubled``; the
# mismatch between source and sink topics is what makes capture observable.
_RELAY = (
    _MOCK_BROKER
    + """
PIPELINE relay
    SOURCE FROM b :: "in/topic"
    |> FILTER msg.value > 1
    |> MAP {
        doubled = msg.value * 2
    }
    |> SINK TO b :: "out/topic"
;
"""
)

_RETRY_PAIR = (
    _MOCK_BROKER
    + """
PIPELINE one
    SOURCE FROM b :: "in/one"
    |> SINK TO b :: "out/one"
    RETRY: 1 WITH BACKOFF 100 ms
;

PIPELINE two
    SOURCE FROM b :: "in/two"
    |> SINK TO b :: "out/two"
    RETRY: 7 WITH BACKOFF 100 ms
;
"""
)

_ROUTED_AND_PLAIN = (
    _MOCK_BROKER
    + """
PIPELINE router
    SOURCE FROM b :: "in/all"
    |> ROUTE
        when msg.value > 10 => SINK TO b :: "high"
        when msg.value > 5 => SINK TO b :: "mid"
        DEFAULT => SINK TO b :: "low"
    ;
    ON_ERROR: SINK TO b :: "errors/dlq"
;

PIPELINE plain
    SOURCE FROM b :: "in/plain"
    |> SINK TO b :: "plain/out"
;
"""
)

_SELF_LOOP = (
    _MOCK_BROKER
    + """
PIPELINE loop
    SOURCE FROM b :: "same/topic"
    |> SINK TO b :: "same/topic"
;
"""
)

_EXAMPLES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples")

_DRIVER_OFFSETS = {
    "instantiate": 3,
    "start": 4,
    "inputs": 5,
    "outputs": 6,
    "cleanup": 7,
}


def _pipeline_count(doc: dict) -> int:
    """Return the number of per-pipeline loader cells in ``doc``."""
    return len(doc["cells"]) - 8


def _cell_index(doc: dict, role: str) -> int:
    """Return the index of the driver cell named ``role``."""
    if role == "preamble":
        return 2
    return _DRIVER_OFFSETS[role] + _pipeline_count(doc)


def _cell_text(doc: dict, role: str) -> str:
    """Return one cell's source joined back into a single string."""
    return "".join(doc["cells"][_cell_index(doc, role)]["source"])


def _pipeline_indices(doc: dict) -> list[int]:
    """Return the indices of the per-pipeline loader cells."""
    return list(range(3, 3 + _pipeline_count(doc)))


def _run(doc: dict, namespace: dict, *roles: str) -> dict:
    """Execute the requested cells in ``namespace``, in order, and return it.

    The same namespace is reused for every cell so later cells see the names
    defined by earlier ones, exactly as a kernel would.
    """
    for role in roles:
        indices = _pipeline_indices(doc) if role == "pipelines" else [_cell_index(doc, role)]
        for index in indices:
            source = "".join(doc["cells"][index]["source"])
            exec(compile(source, f"<{role}-cell-{index}>", "exec"), namespace)
    return namespace


def _collector_topics(doc: dict) -> list[str]:
    """Return the topic list the start cell subscribes a collector to."""
    prefix = "for _t in "
    for line in _cell_text(doc, "start").splitlines():
        if line.startswith(prefix + "["):
            return json.loads(line[len(prefix) :].rstrip(":"))
    raise AssertionError("start cell has no collector topic list")


def test_preamble_patches_mock_lock():
    doc, _ = _nb(_RELAY)
    namespace: dict = {}
    _run(doc, namespace, "preamble")
    patched = namespace["_mock_transport"]._MOCK_BUS_LOCK
    # Without the RLock a sink publish from inside _on_message deadlocks the
    # kernel on the stock non-reentrant bus lock.
    assert isinstance(patched, type(threading.RLock()))
    assert isinstance(mock_transport._MOCK_BUS_LOCK, type(threading.RLock()))


def test_passthrough_message_captured():
    doc, _ = _nb(_RELAY)
    namespace = _run(doc, {}, "preamble", "pipelines", "instantiate", "start", "inputs")
    collected = namespace["COLLECTED"]
    assert list(collected) == ["out/topic"]
    messages = collected["out/topic"]
    assert len(messages) == 3
    assert sorted(message["doubled"] for message in messages) == [10.0, 24.0, 50.0]
    namespace["_pub"].publish({"value": 30, "sensor": "relay", "ts": 0.0})
    namespace["time"].sleep(0.1)
    assert len(messages) == 4
    assert messages[-1]["doubled"] == 60.0
    _run(doc, namespace, "cleanup")


def test_multi_pipeline_module_isolation():
    doc, _ = _nb(_RETRY_PAIR)
    namespace = _run(doc, {}, "preamble", "pipelines")
    modules = namespace["_NOTEBOOK_MODULES"]
    assert set(modules) == {"one", "two"}
    # RETRY renders as the module-level _MAX_PUBLISH_RETRIES constant
    # (pipeline.py.j2), not as a per-instance attribute.
    assert modules["one"]._MAX_PUBLISH_RETRIES == 1
    assert modules["two"]._MAX_PUBLISH_RETRIES == 7


def test_start_cell_rerun_still_captures_one():
    doc, _ = _nb(_RELAY)
    namespace = _run(doc, {}, "preamble", "pipelines", "instantiate", "start", "inputs")
    assert len(namespace["COLLECTED"]["out/topic"]) == 3
    assert len(namespace["_collectors"]) == 1
    # Re-running start re-registers the collectors, so the _callback_registered
    # reset is what keeps every message from being delivered twice.
    _run(doc, namespace, "start", "inputs")
    assert len(namespace["COLLECTED"]["out/topic"]) == 3
    assert len(namespace["_collectors"]) == 1
    _run(doc, namespace, "cleanup")


def test_output_topics_cover_sink_route_and_dlq():
    doc, _ = _nb(_ROUTED_AND_PLAIN)
    topics = _collector_topics(doc)
    assert topics == ["high", "mid", "low", "errors/dlq", "plain/out"]


def test_self_loop_warning_cell():
    doc, path = _nb(_SELF_LOOP)
    # A self-feeding pipeline is legal: the guard stops it at runtime instead.
    assert os.path.isfile(path)
    start = _cell_text(doc, "start")
    assert "def _guard_recursion(pipe, limit=32):" in start
    assert "mock self-loop: sink topic feeds its own source" in start
    assert _collector_topics(doc) == ["same/topic"]


def test_cleanup_cell_redisplays_collected():
    doc, _ = _nb(_RELAY)
    namespace = _run(doc, {}, "preamble", "pipelines", "instantiate", "start", "inputs")
    assert len(namespace["COLLECTED"]["out/topic"]) == 3
    _run(doc, namespace, "cleanup")
    text = _cell_text(doc, "cleanup")
    order = [
        "_p.drain()",
        "_p.stop()",
        "clear_mock_bus()",
        "_clear_modules()",
        'print("mock bus cleared, pipeline modules dropped")',
        'print(f"final {_t}:',
    ]
    positions = [text.index(token) for token in order]
    assert positions == sorted(positions)
    # The final counts print after stop() so @finally publishes are included.
    assert len(namespace["COLLECTED"]["out/topic"]) == 3
    assert "pipelines" not in sys.modules


def test_notebook_size_multi_pipeline():
    with tempfile.TemporaryDirectory() as tmp:
        path = generate_notebook(parse(os.path.join(_EXAMPLES, "multi_pipeline.sigil")), tmp)
        assert os.path.getsize(path) < 1024 * 1024
