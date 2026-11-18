"""Tests for the notebook generator (``sigil.notebook``).

Each test parses an inline DSL string, writes the notebook into a temporary
directory, reloads it as plain JSON and asserts structural properties. Nothing
here needs a broker, and nothing imports ``nbformat``: a notebook is JSON.
"""

from __future__ import annotations

import ast
import json
import os
import tempfile
import threading

from sigil.cli import main
from sigil.codegen import build_contexts, render_pipeline_source
from sigil.language import parse_string
from sigil.notebook import (
    _MOCK_MIN_COMMLIB,
    _preamble_source,
    _strip_magics,
    generate_notebook,
)

# A local TemporaryDirectory would be garbage-collected, deleting the notebook
# before the assertions read it, so every directory is retained for the session.
_TEMP_DIRS: list[tempfile.TemporaryDirectory] = []


def _nb(src: str, **kwargs) -> tuple[dict, str]:
    """Generate a notebook from ``src`` and return ``(document, absolute path)``.

    Extra keyword arguments are forwarded to ``generate_notebook``. The
    temporary output directory is retained for the session so the returned path
    stays readable by the caller.
    """
    tmp = tempfile.TemporaryDirectory()
    _TEMP_DIRS.append(tmp)
    path = generate_notebook(parse_string(src), tmp.name, **kwargs)
    with open(path, encoding="utf-8") as handle:
        return json.load(handle), path


def _tmpdir() -> str:
    tmp = tempfile.TemporaryDirectory()
    _TEMP_DIRS.append(tmp)
    return tmp.name


def _text(cell: dict) -> str:
    return "".join(cell["source"])


def _code_cells(doc: dict) -> list[dict]:
    return [cell for cell in doc["cells"] if cell["cell_type"] == "code"]


def _expect_value_error(call) -> ValueError:
    try:
        call()
    except ValueError as exc:
        return exc
    raise AssertionError("expected ValueError")


_SINGLE = """
BROKER local
    TRANSPORT: MQTT
    HOST: "localhost"
    PORT: 1883
;

PIPELINE relay
    SOURCE FROM local :: "in/topic"
    |> SINK TO local :: "out/topic"
;
"""

_MIXED = """
BROKER upstream
    TRANSPORT: MQTT
    HOST: "mqtt.local"
    PORT: 1883
;

BROKER cache
    TRANSPORT: REDIS
    HOST: "redis.local"
    PORT: 6379
;

PIPELINE into_cache
    SOURCE FROM upstream :: "sensors/in"
    |> SINK TO cache :: "sensors/cached"
;

PIPELINE out_of_cache
    SOURCE FROM cache :: "sensors/cached"
    |> SINK TO upstream :: "sensors/out"
;
"""


def test_notebook_metadata():
    doc, _ = _nb(_SINGLE)
    assert doc["nbformat"] == 4
    assert doc["nbformat_minor"] == 5
    assert doc["metadata"]["kernelspec"]["name"] == "python3"
    assert doc["metadata"]["language_info"]["name"] == "python"


def test_cell_shapes():
    model = parse_string(_MIXED)
    doc, _ = _nb(_MIXED)
    cells = doc["cells"]
    pipeline_count = len(model.pipelines)
    assert pipeline_count == 2
    assert cells[0]["cell_type"] == "markdown"
    # Driver cells are appended after the per-pipeline block, so only the lower
    # bound is stable; pinning the total would break as drivers are added.
    assert len(cells) >= 3 + pipeline_count
    for cell in cells[1 : 3 + pipeline_count]:
        assert cell["cell_type"] == "code"
        assert cell["execution_count"] is None
        assert cell["outputs"] == []
        assert cell["metadata"] == {}
    for cell in cells[3 + pipeline_count :]:
        assert cell["cell_type"] == "code"


def test_cell_ids_deterministic():
    first, _ = _nb(_MIXED, mode="broker")
    second, _ = _nb(_MIXED, mode="broker")
    ids = [cell["id"] for cell in first["cells"]]
    assert ids == [f"cell-{index}" for index in range(1, len(ids) + 1)]
    assert ids == [cell["id"] for cell in second["cells"]]
    assert first["cells"][1] is not second["cells"][1]
    assert first["cells"][1]["source"] is not second["cells"][1]["source"]
    first["cells"][1]["metadata"]["mutated"] = True
    first["cells"][1]["source"].append("mutated\n")
    assert second["cells"][1]["metadata"] == {}
    assert "mutated\n" not in second["cells"][1]["source"]


def test_every_code_cell_compiles():
    doc, _ = _nb(_MIXED)
    code_cells = _code_cells(doc)
    assert code_cells
    for cell in code_cells:
        source = _strip_magics(_text(cell))
        if not source.strip():
            continue
        ast.parse(source)


def test_pipeline_source_embedded_verbatim():
    doc, _ = _nb(_MIXED, mode="broker")
    model = parse_string(_MIXED)
    ctxs = build_contexts(model)
    assert len(ctxs.pipeline_contexts) == len(model.pipelines)
    for offset, pctx in enumerate(ctxs.pipeline_contexts):
        expected = (
            f"_load_pipeline({json.dumps(pctx.name)}, {json.dumps(render_pipeline_source(pctx))})\n"
        )
        cell = doc["cells"][3 + offset]
        assert cell["cell_type"] == "code"
        assert _text(cell) == expected
        assert '"""' not in _text(cell)


def test_no_signal_or_blocking_loop():
    doc, _ = _nb(_MIXED)
    blob = "\n".join(_text(cell) for cell in doc["cells"])
    assert "signal.signal" not in blob
    assert "while True" not in blob


def test_install_cell_pins_mock_floor():
    doc, _ = _nb(_SINGLE)
    install = _text(doc["cells"][1])
    assert '%pip install "commlib-py>=0.13.2"' in install
    assert _MOCK_MIN_COMMLIB == ">=0.13.2"


def test_preamble_patches_mock_lock():
    source = _preamble_source()
    doc, _ = _nb(_SINGLE)
    assert _text(doc["cells"][2]) == source
    assert "_MOCK_BUS_LOCK = threading.RLock()" in source
    assert 'hasattr(_mock_transport, "_MOCK_BUS_LOCK")' in source
    namespace: dict = {}
    exec(compile(source, "<preamble>", "exec"), namespace)
    mock_transport = namespace["_mock_transport"]
    assert isinstance(mock_transport._MOCK_BUS_LOCK, type(threading.RLock()))


def test_mock_mode_overrides_mixed_transports():
    doc, _ = _nb(_MIXED, mode="mock")
    blob = json.dumps(doc)
    assert ".mqtt" not in blob
    assert ".redis" not in blob
    assert "overridden to mock" in _text(doc["cells"][0])


def test_broker_mode_keeps_declared_transports():
    doc, _ = _nb(_MIXED, mode="broker")
    blob = json.dumps(doc)
    assert ".mqtt" in blob
    assert ".redis" in blob
    assert "broker" in _text(doc["cells"][0])


def test_mode_validation_raises():
    model = parse_string(_SINGLE)
    exc = _expect_value_error(lambda: generate_notebook(model, _tmpdir(), mode="nonsense"))
    assert "nonsense" in str(exc)


def test_name_derivation():
    _, default_path = _nb(_SINGLE)
    assert os.path.basename(default_path) == "notebook.ipynb"
    _, custom_path = _nb(_SINGLE, name="custom")
    stem, extension = os.path.splitext(os.path.basename(custom_path))
    assert (stem, extension) == ("custom", ".ipynb")


def test_output_path_forms():
    model = parse_string(_SINGLE)
    nested = os.path.join(_tmpdir(), "nested", "x.ipynb")
    written = generate_notebook(model, nested, name="ignored")
    assert written == os.path.abspath(nested)
    assert os.path.isfile(nested)

    directory = _tmpdir()
    written = generate_notebook(model, directory, name="here")
    assert written == os.path.abspath(os.path.join(directory, "here.ipynb"))
    assert os.path.isabs(written)
    assert os.path.isfile(written)


def test_cli_notebook_subcommand():
    source_path = os.path.join(_tmpdir(), "cli_pipeline.sigil")
    with open(source_path, "w", encoding="utf-8") as handle:
        handle.write(_SINGLE)
    out_dir = _tmpdir()
    assert main(["notebook", source_path, "-o", out_dir]) == 0
    assert os.path.isfile(os.path.join(out_dir, "cli_pipeline.ipynb"))
    missing = os.path.join(_tmpdir(), "absent.sigil")
    assert main(["notebook", missing, "-o", out_dir]) != 0


def _driver_cell(doc: dict, offset: int) -> str:
    """Return a driver cell's source, counting back from the last cell."""
    return _text(doc["cells"][len(doc["cells"]) - offset])


def test_broker_mode_uses_dsl_transport():
    doc, _ = _nb(_SINGLE, mode="broker")
    inputs = _driver_cell(doc, 3)
    assert "from commlib.transports.mqtt import" in inputs
    assert "clear_mock_bus" not in inputs
    # The publisher carries the broker's real connection parameters, never the
    # mock placeholder, and no broker cell clears the mock bus.
    assert 'host="localhost"' in inputs
    assert "port=1883" in inputs
    driver_blob = "\n".join(_text(cell) for cell in _code_cells(doc)[3:])
    assert "clear_mock_bus" not in driver_blob
    assert "mock_transport" not in driver_blob

    mock_doc, _ = _nb(_SINGLE, mode="mock")
    assert "commlib.transports.mqtt" not in json.dumps(mock_doc)


def test_broker_mode_mixed_transports_use_each_broker():
    doc, _ = _nb(_MIXED, mode="broker")
    inputs = _driver_cell(doc, 3)
    start = _driver_cell(doc, 4)
    for cell in (inputs, start):
        assert "from commlib.transports.mqtt import" in cell
        assert "from commlib.transports.redis import" in cell

    # Each source publisher uses its own broker's connection parameters.
    assert 'MqttConnectionParameters(host="mqtt.local", port=1883)' in inputs
    assert 'RedisConnectionParameters(host="redis.local", port=6379)' in inputs
    # Each collector subscribes on the broker that publishes its topic.
    assert 'MqttConnectionParameters(host="mqtt.local", port=1883)' in start
    assert 'RedisConnectionParameters(host="redis.local", port=6379)' in start
    assert "clear_mock_bus" not in "\n".join((inputs, start))

    for cell in _code_cells(doc):
        ast.parse(_strip_magics(_text(cell)))
