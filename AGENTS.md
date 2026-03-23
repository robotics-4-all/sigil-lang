# PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-15

## OVERVIEW

`sigil` is a DSL compiler for aggregation pipelines. Parses `.sigil` files (textX grammar) into an AST, validates semantics, and generates standalone Python applications using Jinja2 templates that consume/produce messages via `commlib-py` (supports MQTT, Redis, AMQP, Kafka transports). Supports pipeline chaining, conditional routing, stream joins, multi-file imports, stage annotations, named multi-window, schema validation, error handling with DLQ, user-defined functions, match expressions, pipeline-level let bindings, ML inference (predict stage), tee/fanout output, and pipeline explanation/visualization. The visual designer lives in the separate `sigil-designer` repository.

## STRUCTURE

```
sigil-lang/
+-- sigil/                 # Core package (see sigil/AGENTS.md)
|   +-- grammar.tx         # textX grammar (IS the metamodel, 537 lines)
|   +-- language.py        # Metamodel builder + parse + import resolution (69 lines)
|   +-- semantics.py       # Semantic validation (2169 lines, 50+ _check_* fns)
|   +-- expressions.py     # AST -> Python expression compiler (327 lines)
|   +-- codegen.py         # Model -> generated app (14 context dataclasses, 868 lines)
|   +-- explain.py         # Human-readable pipeline explanation (244 lines)
|   +-- visualize.py       # DOT/Mermaid diagram generation (742 lines)
|   +-- cli.py             # CLI: check / generate / explain / visualize (132 lines)
|   +-- templates/         # Jinja2 templates (app.py.j2, pipeline.py.j2, requirements.txt.j2)
+-- examples/              # 32 progressive .sigil examples (01-30 + simple + multi_pipeline)
+-- tests/                 # pytest suite (~718 unit + ~26 integration tests)
+-- docs/                  # language-reference, tutorial, architecture, examples walkthrough
+-- setup.py               # Package config, console_scripts entry point
+-- Makefile               # Docker-based build/test/run targets (+ validate-examples)
+-- Dockerfile             # App container (python:3.12-slim)
+-- docker-compose.yml     # Full dev stack (EMQX, Redis, RabbitMQ, Kafka)
+-- .github/workflows/     # CI: unit tests on 3.10/3.11/3.12, integration via Docker
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change DSL syntax | `sigil/grammar.tx` | textX notation. Grammar IS the metamodel |
| Add/modify validation rules | `sigil/semantics.py` | Each `_check_*` returns `list[SemanticError]` |
| Change generated Python code | `sigil/templates/*.j2` | Jinja2 with `PipelineCtx` dataclass |
| Modify expression compilation | `sigil/expressions.py` | `compile_expr()` walks AST, emits Python |
| Add new CLI subcommand | `sigil/cli.py` | argparse, entry point: `sigil` |
| Explain pipeline logic | `sigil/explain.py` | `explain_model()` returns human-readable text |
| Visualize pipeline diagrams | `sigil/visualize.py` | `visualize_dot(model)`, `visualize_mermaid(model)` |
| Add codegen context fields | `sigil/codegen.py` | 14 dataclasses: `BrokerCtx`, `PipelineCtx`, etc |
| Add new stage type | `grammar.tx` + `semantics.py` (STAGE_ORDER) + `codegen.py` (_build_pipeline_ctx) + `templates/pipeline.py.j2` + `explain.py` + `visualize.py` |
| Add new aggregation function | `grammar.tx` (AggFunc) + `semantics.py` (FUNCS_* sets) + `templates/pipeline.py.j2` |
| Add ML model runtime support | `sigil/grammar.tx` (Runtime rule) + `codegen.py` (ModelCtx/PredictCtx) + `templates/pipeline.py.j2` + `templates/requirements.txt.j2` |
| Visual designer (UI) | `sigil-designer` repo | Separate repository: React + React Flow + Zustand |
| See DSL syntax examples | `examples/*.sigil` | Progressive 01-30 (26-30 are ML predict examples) |
| Language reference | `docs/language-reference.md` | Full syntax documentation |

## CONVENTIONS

- **textX grammar = metamodel**: No separate metamodel class hierarchy. Grammar rules define AST classes directly.
- **Stage dispatch by class name**: `stage.__class__.__name__` string comparison (not `isinstance`) in `codegen.py`, `semantics.py`, `explain.py`, `visualize.py`. Consistent everywhere.
- **Expression compiler**: Recursive `compile_expr()` using class name dispatch, not visitor pattern. `ExprContext` enum (FILTER/MAP/AGGREGATE/HAVING/QUANTIFIER) controls FieldRef compilation.
- **Validation**: Returns `list[SemanticError]` (never raises). `validate_or_raise()` wraps for CLI. Warnings use `severity="warning"`.
- **Codegen context dataclasses**: Intermediate representation between parsed model and Jinja2 templates. `PipelineCtx` is sole interface to templates. All template variables come from these dataclasses.
- **Relative imports within sigil/**: `from .module import ...` (e.g., `from .expressions import ...`, `from .semantics import ...`). External imports use absolute paths.
- **Map assignment separators**: Multiple `map` assignments use `;` as separator. `AggregateStage` does NOT use semicolons (`}` provides unambiguous boundaries).
- **Python >=3.10**: Uses `str | None` union syntax throughout.
- **No linter/formatter config**: No ruff/flake8/mypy/black config files. No `# type: ignore`, `# noqa`, or `# pylint: disable` anywhere.
- **Setup.py only**: No pyproject.toml or setup.cfg. Uses `setuptools` with `package_data` for grammar.tx and templates.
- **AST walking**: `_AST_WALK_ATTRS` tuple in codegen.py defines attribute names for recursive AST traversal (`_has_temporal_ref`, `_has_env_ref`, etc.).
- **Duration handling**: `UNIT_MULTIPLIERS` dict defined in `semantics.py`, imported by `codegen.py` via `from .semantics import UNIT_MULTIPLIERS`.
- **Grammar changes cascade**: Any grammar.tx change requires checking semantics, codegen, expressions, explain, visualize, and templates.
- **Naming**: `snake_case` functions/variables, `PascalCase` classes, `_` prefix for private helpers. No TODO/FIXME/HACK comments in codebase.
- **No `__main__.py`**: Package not runnable via `python -m sigil`. Use `sigil` CLI entry point only.

## ANTI-PATTERNS (THIS PROJECT)

- No explicit anti-pattern comments (`DO NOT`, `NEVER`, `DEPRECATED`, etc.) in codebase.
- Warnings handled programmatically via `SemanticError(severity="warning")`, not inline comments.
- No type suppression (`# type: ignore`, `# noqa`) anywhere in the codebase.

## PIPELINE ARCHITECTURE

Fixed processing pipeline order: `source -> [join] -> [let bindings] -> deduplicate -> filter -> predict -> rate -> flatten -> distinct -> throttle -> sample -> group_by -> window -> aggregate -> accumulate -> having -> map -> sink`.

`PredictStage` runs at phase 0 (same as FilterStage). Output goes to `sink` (single), `route` (conditional fan-out), `tee` (broadcast copy), or `fanout` (parallel branches). Never more than one output type.

Pipeline-level let bindings (`let name = expr`) appear between join and stages. Computed per-message, available in all downstream stage expressions.

Stage order enforced by `STAGE_ORDER` dict in `semantics.py` (maps stage class names to integer phases -1 through 11). All stages except source/sink optional. Each stage type at most once (`_check_duplicate_stages`), except `WindowStage` (named multi-window).

Stages wrapped in `AnnotatedStage` (optional `@warn`/`@error` annotations). Use `_unwrap_stages(pipeline)` for raw stage objects.

Pipeline output: `sink` (single destination) OR `route` (conditional fan-out) OR `tee`/`fanout`. Never more than one.

Source: broker subscription (`source from BROKER :: "topic"`) or pipeline chain (`source from pipe NAME`). Pipe sources use in-memory `_downstream_listeners`.

Join: subscribes to second stream, matches by key within time window. `other.field` references joined message. Inner (buffers unmatched, retries on join arrival) / left (passes through with `other=None`). Incompatible with pipe sources. Sits at pipeline level (not in `Stage` abstract rule).

`import "path.sigil"` merges brokers, constants, pipelines, schemas. Circular imports detected via `_seen` set of absolute paths.

### Surface Syntax

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline example
    source from local :: "input/topic"
    |> filter msg.value > 10
    |> window tumbling(1 m)
    |> aggregate {
        total = sum(msg.value)
    }
    |> sink to local :: "output/topic"
end
```

Structural blocks use `end` keywords. Stages prefixed with `|>`. Data blocks (aggregate, map, accumulate, batch, case, hooks) use `{ }` braces.

## EXPRESSION SYSTEM

Precedence (low to high): `or -> and -> comparison -> +,- -> *,/,% -> ?? -> unary(-,not) -> atom`.

FieldRef compilation depends on context:
- **ALL contexts**: `other.x.y` -> `_get_nested(other, 'x', 'y')` (join-stream reference)
- **FILTER/AGGREGATE**: `msg.x.y` -> `_get_nested(msg, 'x', 'y')`
- **HAVING**: bare `id` -> `aggregated['id']`; `msg.x` -> `_get_nested(msg, 'x')`
- **MAP**: bare `id` -> `aggregated['id']` (or let binding); `msg.x` -> `_get_nested(msg, 'x')`

Built-in expression forms:
- Constants (`const NAME = value`): inlined at compile time via `const_map`
- `env("KEY", default)` -> `os.environ.get('KEY', default)`
- `fmt("pattern {}", arg1, arg2)` -> `"pattern {}".format(arg1, arg2)`
- `case { when c => r ... else => d }` -> nested Python ternaries
- `match subject { case p => r ... else => d }` -> nested Python ternaries (`r if subject == p else d`)
- `old(msg.x)` -> `_get_nested(self._prev_msg, 'x')`
- `changed(msg.x)` -> `(_get_nested(msg, 'x') != _get_nested(self._prev_msg, 'x'))`
- `forall(m => pred)` -> `all(pred for m in messages)`
- `exists(x in msg.field => pred)` -> `any(pred for x in (_get_nested(msg, 'field') or []))`
- User-defined functions: `func celsius(t) = (t - 32) * 5 / 9` -> inlined at call sites via `func_defs`

Lifecycle hooks: `@initially { publish { k: v } }` / `@finally { publish { k: v } }`.
Sink: `format: json|csv|raw`, `batch: { size: N, timeout: Ts }`.
Retry: `retry: N with backoff DURATION`.

## CODE GENERATION FLOW

1. `parse(path)` -> textX AST model (with imports resolved)
2. `validate(model)` -> `list[SemanticError]` (50+ checks)
3. `generate(model, output_dir)`:
   - `_build_const_map(model)` + `_build_func_defs(model)` -> lookup tables
   - `_build_model_map(model, const_map, func_defs)` -> `dict[str, ModelCtx]` (ML models)
   - Per pipeline: `_build_pipeline_ctx(pipeline, const_map, func_defs)` -> `PipelineCtx`
   - Stage dispatch via `stage.__class__.__name__` populates ctx fields
   - Expressions compiled via `compile_expr()` into Python code strings
   - Jinja2 renders: `app.py.j2` + `pipeline.py.j2` (per pipeline) + `requirements.txt.j2`

Generated app structure: `app.py` (imports, signal handling, event loop), `pipelines/<name>.py` (class per pipeline with `_on_message` chain), `pipelines/__init__.py`, `requirements.txt`.

## TESTING

~718 unit + ~26 integration tests across 5 files:

| File | LOC | Tests | Coverage |
|------|-----|-------|----------|
| `test_codegen.py` | 3625 | 208 | Generated code correctness, expression compilation, explain |
| `test_grammar.py` | 3550 | 187 | Parsing all DSL constructs into AST, file imports |
| `test_semantics.py` | 3056 | 194 | All `_check_*` validation rules, errors + warnings |
| `test_integration.py` | 1996 | 26 | End-to-end with real brokers (MQTT/Redis/AMQP/Kafka) |
| `test_visualize.py` | 1202 | 103 | DOT and Mermaid diagram output |

Conventions:
- No `conftest.py`. Helpers inline: `_model(src)`, `_errors(src)` in test_semantics.py; `_stages(pipeline)` in test_codegen.py and test_visualize.py.
- All tests use inline multi-line DSL strings (no external fixture files).
- Tests invoke Python functions directly (`parse_string`, `validate`, `generate`, `compile_expr`). No CLI testing.
- Integration tests: `pytest.mark.skipif` for broker availability (`mqtt_available`, `redis_available`, `amqp_available`, `kafka_available`). Docker required.
- Codegen tests: `tempfile.TemporaryDirectory()` + Python `compile()` for syntax verification.

## COMMANDS

```bash
# Install (local dev)
pip install -e .

# Run tests (local, skip integration)
pytest tests/ -v --ignore=tests/test_integration.py

# Docker-based (full stack)
make build              # Build Docker image
make up                 # Start services (EMQX, Redis, RabbitMQ, Kafka)
make test               # Full test suite in Docker
make test-unit          # Unit tests only
make test-integration   # Integration tests (real brokers)
make validate-examples  # Validate all examples/*.sigil
make clean              # Remove containers, images, caches

# CLI
sigil check examples/simple.sigil
sigil generate examples/simple.sigil -o generated/
sigil explain examples/simple.sigil
sigil visualize examples/multi_pipeline.sigil --format dot
sigil visualize examples/multi_pipeline.sigil --format mermaid
```

## CI

GitHub Actions (`.github/workflows/ci.yml`):
- **unit**: Python 3.10/3.11/3.12 matrix, `pytest --ignore=tests/test_integration.py`
- **integration**: Docker Compose (`make build` + `make test-integration`), depends on unit

## NOTES

- Generated apps depend on `commlib-py>=0.13.0` (unified pub/sub across MQTT, Redis, AMQP, Kafka).
- Broker `transport` defaults to `mqtt`. Valid: `mqtt`, `redis`, `amqp`, `kafka`.
- Cross-transport pipelines supported (e.g., source MQTT, sink Redis).
- Window flushing uses `threading.Thread` event loop.
- Generated apps support `LOG_FORMAT=json` and `LOG_LEVEL=DEBUG|INFO|WARNING|ERROR` env vars.
- Graceful shutdown: SIGTERM/SIGINT -> drain windows/batches -> stop pipelines.
- Publish retry with exponential backoff. Default: 3 attempts, 100ms base. Configurable via `retry:` clause.
- Incoming messages validated: non-dict payloads auto-parsed from JSON; invalid dropped.
- `build_metamodel()` cached via `@lru_cache(maxsize=1)`.
- Aggregate assignments support optional type annotations: `total: float = sum(msg.value)`.
- Broker deduplication in templates: shared broker = single client reused.
- Duplicate broker names caught by textX (`not unique`) before semantic validation.
- Named multi-window `|> window NAME tumbling(...)` with `over NAME` in aggregations. Each window gets own buffer, flush thread, `_process_window_<name>` method.
- Root-level utility scripts (`replace_colons.py`, `replace_colons_script.py`) are one-off migration scripts, not part of the core package.
- 32 examples in `examples/` progress from basic (01_hello_passthrough) to advanced (30_ml_custom_runtime). Examples 26-30 cover ML predict stage.
- `pipeline.py.j2` is the largest template at 1254 lines.
- 14 codegen context dataclasses (was 11): added `FanoutBranchCtx`, `ModelCtx`, `PredictCtx`.
- ML predict stage: `|> predict using MODEL_NAME`. Runtimes: `onnx`, `sklearn`, `torch`, `tflite`, custom Python module. ML deps conditionally included in `requirements.txt.j2`.
- Version: `0.1.0` (defined in both `setup.py` and `sigil/__init__.py`).
- Visual designer lives in the separate `sigil-designer` repository (React/Vite). Does NOT currently support predict stage in UI.
