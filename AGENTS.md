# PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-04

## OVERVIEW

`sigil` is a DSL compiler for aggregation pipelines. Parses `.sigil` files (textX grammar) into an AST, validates semantics, and generates standalone Python applications using Jinja2 templates that consume/produce messages via `commlib-py` (supports MQTT, Redis, AMQP, Kafka transports). Supports pipeline chaining, conditional routing, stream joins, multi-file imports, stage annotations, and pipeline explanation.

## STRUCTURE

```
aggr_app_dsl/
├── sigil/             # Core package — grammar, parser, validator, codegen
│   ├── grammar.tx     # textX grammar (IS the metamodel)
│   ├── language.py    # Metamodel builder + parse entrypoints + import resolution
│   ├── semantics.py   # Semantic validation (stage order, types, refs)
│   ├── expressions.py # AST → Python expression compiler
│   ├── codegen.py     # Model → generated app (Jinja2)
│   ├── explain.py     # Human-readable pipeline explanation
│   ├── visualize.py   # DOT/Mermaid diagram generation from models
│   ├── cli.py         # CLI entry point (check / generate / explain / visualize)
│   └── templates/     # Jinja2 templates for generated code
├── examples/          # Sample .sigil files (26 progressive examples)
├── tests/             # pytest suite (grammar, semantics, codegen, visualize, integration)
├── setup.py           # Package config, console_scripts entry point
└── requirements.txt   # textX, Jinja2, pytest
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change DSL syntax | `sigil/grammar.tx` | textX notation. Grammar IS the metamodel |
| Add/modify validation rules | `sigil/semantics.py` | Each `_check_*` returns `list[SemanticError]` |
| Change generated Python code | `sigil/templates/*.j2` | Jinja2 with `PipelineCtx` dataclass |
| Modify expression compilation | `sigil/expressions.py` | `compile_expr()` walks AST, emits Python |
| Add new CLI subcommand | `sigil/cli.py` | argparse, entry point: `sigil` (check/generate/explain/visualize) |
| Explain pipeline logic | `sigil/explain.py` | `explain_model()` returns human-readable text |
| Visualize pipeline diagrams | `sigil/visualize.py` | `visualize_dot(model)`, `visualize_mermaid(model)` |
| Add codegen context fields | `sigil/codegen.py` | Dataclasses: `BrokerCtx`, `PipelineCtx`, etc |
| See DSL syntax examples | `examples/*.sigil` | `simple.sigil`, `multi_pipeline.sigil` |

## CONVENTIONS

- **textX grammar = metamodel**: No separate metamodel class hierarchy. Grammar rules define AST classes directly.
- **Stage dispatch by class name**: `stage.__class__.__name__` string comparison (not `isinstance`) in `codegen.py` and `semantics.py`.
- **Expression compiler**: Recursive `compile_expr()` using class name dispatch, not visitor pattern. Context enum (`ExprContext`) controls FieldRef compilation.
- **Validation**: Returns `list[SemanticError]` (never raises). `validate_or_raise()` wraps for CLI. Warnings use `severity="warning"` (e.g., unused aggregations).
- **Codegen context dataclasses**: Intermediate representation between parsed model and Jinja2 templates. All template variables come from these dataclasses.
- **Map assignment separators**: Multiple `map:` assignments use `;` as separator (`mappings+=MapAssignment[';']`). Single assignments need no semicolon. Last assignment in a block needs no trailing `;`. Aggregate assignments do NOT use semicolons (`)` provides unambiguous boundaries).
- **Python >=3.10**: Uses `str | None` union syntax.
- **No linter/formatter config**: No ruff/flake8/mypy/black config files. Code uses ruff cache (`.ruff_cache/` exists).

## ANTI-PATTERNS (THIS PROJECT)

- No anti-pattern comments found in codebase.
- No explicit forbidden patterns documented.

## PIPELINE ARCHITECTURE

The DSL defines a fixed processing pipeline: `source → [join] → deduplicate → filter → rate → flatten → distinct → throttle → sample → group_by → window → aggregate → accumulate → having → map → sink`. Stage order is enforced semantically (`STAGE_ORDER` dict in `semantics.py`). All stages except source/sink are optional. Each stage type can appear at most once (enforced by `_check_duplicate_stages`), except `WindowStage` which supports named multi-window (multiple named windows per pipeline).

Stages are wrapped in `AnnotatedStage` in the grammar, which holds optional `@warn`/`@error` annotations. Use `_unwrap_stages(pipeline)` to get raw stage objects.

Pipeline output can be a `sink` (single destination) or `route` (conditional fan-out to multiple destinations). A pipeline has either `sink` or `route`, not both.

Source can be a broker subscription (`source from BROKER :: "topic"`) or a pipeline chain (`source from pipe NAME`). Pipe sources receive messages via in-memory callback from the upstream pipeline's `_downstream_listeners`.

Join (`|> join BROKER :: "topic" on msg.X == other.Y within DURATION [type: inner|left] end`) subscribes to a second stream and matches messages by key within a time window. After a join, `other.field` references the matched message from the joined stream. Inner joins buffer unmatched primary messages and retry when join-side messages arrive. Left joins pass primary messages through with `other=None` when no match is found. Join is incompatible with pipe sources. Join sits at pipeline level (not in the `Stage` abstract rule).

`import "path.sigil"` at top level merges brokers, constants, and pipelines from imported files. Circular imports are detected.

### Surface Syntax

Structural blocks use `end` keywords (brokers, pipelines, routes). Stages are prefixed with the `|>` pipe operator. Data blocks (aggregate, map, accumulate, auth, batch, case, hooks) retain `{ }` braces.

```sigil
broker local
    host: "localhost"
    port: 1883
end

pipeline example
    source from local :: "input/topic"
    |> filter: msg.value > 10
    |> window: tumbling(1 m)
    |> aggregate: {
        total = sum(msg.value)
    }
    |> sink to local :: "output/topic"
end
```

## EXPRESSION SYSTEM

Precedence (low to high): `or → and → comparison → +,- → *,/,% → ?? → unary(-,not) → atom`.
FieldRef compilation depends on context:
- **ALL contexts**: `other.x.y` → `_get_nested(other, 'x', 'y')` (join-stream reference, checked first)
- **FILTER/AGGREGATE**: `msg.x.y` → `_get_nested(msg, 'x', 'y')`
- **HAVING**: bare `id` → `aggregated['id']`; `msg.x` → `_get_nested(msg, 'x')`
- **MAP**: bare `id` → `aggregated['id']`; `msg.x` → `_get_nested(msg, 'x')`

Constants (`const NAME = value`) are inlined at compile time via `const_map` parameter.
`env("KEY", default)` compiles to `os.environ.get('KEY', default)`.
`fmt("pattern {}", arg1, arg2)` compiles to `"pattern {}".format(arg1, arg2)`.
`case { when c => r ... else => d }` compiles to nested Python ternaries.
`old(msg.x)` compiles to `_get_nested(self._prev_msg, 'x')`.
`changed(msg.x)` compiles to `(_get_nested(msg, 'x') != _get_nested(self._prev_msg, 'x'))`.
`forall(m => pred)` compiles to `all(pred for m in messages)` (window-implicit).
`exists(x in msg.field => pred)` compiles to `any(pred for x in (_get_nested(msg, 'field') or []))`.
`@initially { publish { k: v } }` publishes ObjectLiteral on pipeline start.
`@finally { publish { k: v } }` publishes ObjectLiteral on pipeline stop.
Sink `format: json|csv|raw` controls serialization; `batch: { size: N, timeout: Ts }` buffers output.

## COMMANDS

```bash
# Install
pip install -e .

# Validate a .sigil file
sigil check examples/simple.sigil

# Generate Python app from .sigil
sigil generate examples/simple.sigil -o generated/

# Explain a pipeline
sigil explain examples/simple.sigil

# Visualize a pipeline (DOT or Mermaid)
sigil visualize examples/multi_pipeline.sigil --format dot
sigil visualize examples/multi_pipeline.sigil --format mermaid

# Run tests
pytest tests/
```

## NOTES

- Generated apps depend on `commlib-py>=0.13.0` (unified pub/sub across MQTT, Redis, AMQP, Kafka).
- Broker `transport` field is optional; defaults to `mqtt`. Valid values: `mqtt`, `redis`, `amqp`, `kafka`.
- Cross-transport pipelines are supported (e.g., source from MQTT, sink to Redis).
- Window flushing uses `threading.Thread` event loop — adequate for moderate throughput.
- Generated apps support `LOG_FORMAT=json` and `LOG_LEVEL=DEBUG|INFO|WARNING|ERROR` env vars.
- Generated apps have graceful shutdown: SIGTERM/SIGINT → drain windows/batches → stop pipelines.
- Publish calls use retry with exponential backoff. Default: 3 attempts, 100ms base. Configurable via DSL `retry:` clause.
- Incoming messages are validated: non-dict payloads are auto-parsed from JSON; invalid messages are logged and dropped.
- `build_metamodel()` is cached via `@lru_cache(maxsize=1)` — safe since grammar is static.
- Aggregate assignments support optional type annotations: `total: float = sum(msg.value)` → runtime type coercion.
- Broker deduplication in templates: if source and sink share a broker, a single MQTT client is reused.
- Duplicate broker names are caught by textX (`not unique`) before semantic validation runs.
- Retry policy: `retry: N with backoff DURATION` configures publish retry per-pipeline. Populates `_MAX_PUBLISH_RETRIES` and `_PUBLISH_RETRY_BASE` in generated code.
- Named multi-window: `|> window NAME: tumbling(...)` allows multiple windows per pipeline. Aggregations use `over NAME` to bind to specific windows. Each window has its own buffer, flush thread, and `_process_window_<name>` method. Output includes `_window` field. When single window, name and `over` are optional (backward compatible).
