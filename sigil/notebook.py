"""Notebook generation for Sigil models.

A generated notebook is an nbformat-4 document (``nbformat_minor`` 5) whose
code cells bootstrap a private ``pipelines`` package inside the kernel and load
one rendered pipeline module per pipeline. The M1 cell set is the title cell,
the install cell, the preamble cell and one ``_load_pipeline`` cell per
pipeline; driver cells are appended by later milestones.
"""

from __future__ import annotations

import json
import os
from typing import Any, Literal, TypedDict

from .codegen import (
    BrokerCtx,
    ContextBundle,
    PipelineCtx,
    _override_transports,
    build_contexts,
    ml_packages,
    render_pipeline_source,
)

# The notebook cell pins a higher commlib floor than ``requirements.txt.j2``
# (``>=0.13.0``) on purpose: 0.13.0 has no ``commlib.transports.mock`` module,
# 0.13.1 ships a no-op stub, and 0.13.2 is the first working in-process bus.
# Raising the template floor would change golden snapshots, so the two drift.
_MOCK_MIN_COMMLIB = ">=0.13.2"

_MODES = frozenset({"mock", "broker"})


class _CodeCell(TypedDict):
    cell_type: Literal["code"]
    execution_count: None
    id: str
    metadata: dict[str, Any]
    outputs: list[Any]
    source: list[str]


class _MarkdownCell(TypedDict):
    cell_type: Literal["markdown"]
    id: str
    metadata: dict[str, Any]
    source: list[str]


_Cell = _CodeCell | _MarkdownCell


def _source_lines(source: str) -> list[str]:
    """Split ``source`` into nbformat's newline-terminated line list."""
    return [line + "\n" for line in source.splitlines()]


def _code_cell(source: str) -> _CodeCell:
    """Build an unexecuted code cell; ``_notebook`` assigns its id."""
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": "",
        "metadata": {},
        "outputs": [],
        "source": _source_lines(source),
    }


def _markdown_cell(source: str) -> _MarkdownCell:
    """Build a markdown cell; ``_notebook`` assigns its id."""
    return {
        "cell_type": "markdown",
        "id": "",
        "metadata": {},
        "source": _source_lines(source),
    }


def _notebook(cells: list[_Cell]) -> dict[str, Any]:
    """Wrap ``cells`` in an nbformat-4 document with deterministic cell ids."""
    for index, cell in enumerate(cells, start=1):
        cell["id"] = f"cell-{index}"
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _strip_magics(source: str) -> str:
    """Drop ``%``/``!`` magic lines so a cell can be parsed with ``ast``."""
    kept = [line for line in source.splitlines() if not line.lstrip().startswith(("%", "!"))]
    if not kept:
        return ""
    return "\n".join(kept) + "\n"


_PREAMBLE_SOURCE = """import json, sys, threading, types, time
import commlib.transports.mock as _mock_transport
from commlib.transports.mock import clear_mock_bus, Publisher, Subscriber, ConnectionParameters

# R17: the stock bus lock is a non-reentrant threading.Lock held while subscriber
# callbacks run, and every pipeline publishes its sink from inside _on_message.
# Patch to an RLock before any endpoint exists; remove when upstream ships one.
if not hasattr(_mock_transport, "_MOCK_BUS_LOCK"):
    raise RuntimeError(
        "commlib.transports.mock._MOCK_BUS_LOCK is missing; the reentrant-lock "
        "workaround requires commlib-py>=0.13.2. Install it and restart the kernel."
    )
_mock_transport._MOCK_BUS_LOCK = threading.RLock()

_NOTEBOOK_MODULES = {}


def _reset_pipelines_package():
    for _name in [n for n in list(sys.modules) if n == "pipelines" or n.startswith("pipelines.")]:
        del sys.modules[_name]
    _pkg = types.ModuleType("pipelines")
    _pkg.__path__ = []
    sys.modules["pipelines"] = _pkg
    _NOTEBOOK_MODULES.clear()


def _load_pipeline(name, source):
    if "pipelines" not in sys.modules:
        _reset_pipelines_package()
    mod = types.ModuleType("pipelines." + name)
    mod.__package__ = "pipelines"
    mod.__file__ = "<notebook:pipelines/%s.py>" % name
    sys.modules["pipelines." + name] = mod
    setattr(sys.modules["pipelines"], name, mod)
    exec(compile(source, mod.__file__, "exec"), mod.__dict__)
    _NOTEBOOK_MODULES[name] = mod
    return mod


def _clear_modules():
    _NOTEBOOK_MODULES.clear()
    for _name in [n for n in list(sys.modules) if n == "pipelines" or n.startswith("pipelines.")]:
        del sys.modules[_name]
"""


def _preamble_source() -> str:
    """Return the preamble cell source shared by the generator and tests."""
    return _PREAMBLE_SOURCE


def _brokers(pctx: PipelineCtx) -> list[BrokerCtx]:
    """Return every broker context reachable from one pipeline context."""
    brokers: list[BrokerCtx] = [pctx.source_broker, pctx.sink_broker]
    if pctx.join is not None:
        brokers.append(pctx.join.broker)
    brokers.extend(arm.broker for arm in pctx.route_arms)
    if pctx.route_default is not None:
        brokers.append(pctx.route_default.broker)
    if pctx.on_error_broker is not None:
        brokers.append(pctx.on_error_broker)
    if pctx.validate_stage is not None and pctx.validate_stage.on_error_broker is not None:
        brokers.append(pctx.validate_stage.on_error_broker)
    brokers.extend(
        stage.on_error_broker for stage in pctx.enrich_stages if stage.on_error_broker is not None
    )
    return brokers


def _declared_transports(ctxs: ContextBundle) -> list[str]:
    """Return the sorted, uppercased transports declared by the model."""
    declared = {
        broker.transport.upper() for pctx in ctxs.pipeline_contexts for broker in _brokers(pctx)
    }
    return sorted(declared)


def _mode_label(mode: str) -> str:
    if mode == "mock":
        return "mock (offline, no broker)"
    return "broker (declared transports)"


def _transports_label(mode: str, declared: list[str]) -> str:
    joined = ", ".join(declared) if declared else "NONE"
    if mode == "mock":
        if declared and all(transport == "MOCK" for transport in declared):
            return f"{joined} -> mock (declared in source)"
        return f"{joined} -> overridden to mock (offline)"
    return f"{joined} -> {', '.join(transport.lower() for transport in declared)}"


def _pipeline_summary(pctx: PipelineCtx) -> str:
    """Describe one pipeline as ``name  (origin -> destination)``."""
    if pctx.has_pipe_source:
        origin = f"pipe {pctx.pipe_source_name}"
    else:
        origin = f"{pctx.source_broker.name} :: {pctx.source_topic}"
    if pctx.has_sink:
        destination = f"{pctx.sink_broker.name} :: {pctx.sink_topic}"
    elif pctx.has_route:
        destination = f"route ({len(pctx.route_arms)} arm(s))"
    else:
        destination = "no output"
    return f"{pctx.name}  ({origin} -> {destination})"


def _title_cell(
    ctxs: ContextBundle,
    *,
    name: str,
    source_path: str,
    mode: str,
    declared: list[str],
) -> _MarkdownCell:
    """Build the title cell: identity, transports and offline notes."""
    brokers = {broker.name: broker for pctx in ctxs.pipeline_contexts for broker in _brokers(pctx)}
    lines = [
        f"# Sigil notebook: {name}",
        f"# Mode: {_mode_label(mode)}  |  Source: {source_path}",
        f"# Transports: {_transports_label(mode, declared)}",
    ]
    if mode == "mock" and any(transport != "MOCK" for transport in declared):
        lines.append(
            f"# {len(brokers)} broker(s) declared; kernel reaches no network. "
            f"Use --mode broker to keep {'/'.join(declared)}."
        )
    for index, pctx in enumerate(ctxs.pipeline_contexts):
        prefix = "# Pipelines:" if index == 0 else "#           "
        lines.append(f"{prefix} {_pipeline_summary(pctx)}")
    if mode == "mock":
        lines.extend(
            [
                "#",
                "# Mock transport notes",
                "# - topics match exactly, so wildcard topics are published literally",
                "# - the bus is in-process; it is cleared at the start and end of a run",
                "# - window results appear only after the outputs cell drains the pipelines",
                "# - use --mode broker to keep the declared transports instead of mock",
            ]
        )
    return _markdown_cell("\n".join(lines) + "\n")


def _install_cell(runtimes: set[str]) -> _CodeCell:
    """Build the install cell: the commlib pin plus any ML runtime packages."""
    lines = [f'%pip install "commlib-py{_MOCK_MIN_COMMLIB}"']
    packages = ml_packages(runtimes)
    if packages:
        lines.append("%pip install " + " ".join(packages))
    return _code_cell("\n".join(lines) + "\n")


def _pipeline_cell(pctx: PipelineCtx) -> _CodeCell:
    """Build one cell that loads a rendered pipeline module into the kernel."""
    source = render_pipeline_source(pctx)
    return _code_cell(f"_load_pipeline({json.dumps(pctx.name)}, {json.dumps(source)})\n")


def _model_filename(model) -> str | None:
    """Return the real source filename of ``model``, or ``None`` for strings."""
    filename = getattr(model, "_tx_filename", None)
    if isinstance(filename, str) and filename and not filename.startswith("<"):
        return filename
    return None


def _default_name(model) -> str:
    """Derive the notebook name from the source stem, falling back to ``notebook``."""
    filename = _model_filename(model)
    if filename is None:
        return "notebook"
    stem = os.path.splitext(os.path.basename(filename))[0]
    return stem or "notebook"


def _source_display(model) -> str:
    """Return the source path shown in the title cell, relative when possible."""
    filename = _model_filename(model)
    if filename is None:
        return "<string>"
    try:
        relative = os.path.relpath(filename)
    except ValueError:
        return filename
    if relative.startswith(".."):
        return filename
    return relative


def _resolve_path(output_path_or_dir: str, name: str) -> str:
    """Resolve ``output_path_or_dir`` into a notebook file path (R3)."""
    if output_path_or_dir.lower().endswith(".ipynb"):
        parent = os.path.dirname(output_path_or_dir)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return output_path_or_dir
    os.makedirs(output_path_or_dir, exist_ok=True)
    return os.path.join(output_path_or_dir, f"{name}.ipynb")


def generate_notebook(
    model,
    output_path_or_dir: str,
    *,
    mode: str = "mock",
    name: str | None = None,
) -> str:
    """Write an nbformat-4 notebook for ``model`` and return its absolute path.

    ``mode="mock"`` rewrites every broker transport to the in-process mock
    transport after building the contexts, so the rendered modules import
    ``commlib.transports.mock`` without touching the source file. ``mode="broker"``
    keeps the declared transports verbatim.
    """
    if mode not in _MODES:
        valid = ", ".join(repr(item) for item in sorted(_MODES))
        raise ValueError(f"unsupported notebook mode {mode!r}; expected one of {valid}")

    ctxs = build_contexts(model)
    declared = _declared_transports(ctxs)
    if mode == "mock":
        _override_transports(ctxs, "mock")

    notebook_name = name or _default_name(model)
    path = _resolve_path(output_path_or_dir, notebook_name)

    cells: list[_Cell] = [
        _title_cell(
            ctxs,
            name=notebook_name,
            source_path=_source_display(model),
            mode=mode,
            declared=declared,
        ),
        _install_cell(ctxs.ml_runtimes),
        _code_cell(_preamble_source()),
    ]
    cells.extend(_pipeline_cell(pctx) for pctx in ctxs.pipeline_contexts)

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(_notebook(cells), handle, indent=1, ensure_ascii=False)
        handle.write("\n")
    return os.path.abspath(path)
