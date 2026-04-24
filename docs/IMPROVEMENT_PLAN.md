# Sigil DSL — Improvement Plan & Roadmap

> Audit conducted on branch `devel @ 1bae2d9` (Wave-2 features just landed).
> Methodology: five parallel specialist agents (semantics, codegen, tooling,
> examples/tests, prior-art) plus first-hand code reads of `grammar.tx`,
> `language.py`, `semantics.py`, `expressions.py`, `codegen.py`, all three
> templates, `explain.py`, `cli.py`, `ci.yml`, and a representative slice of
> the 39 examples.

This document is the canonical roadmap for evolving Sigil from a competent
prototype into a production-grade DSL appropriate for a Horizon Europe
deliverable. It is referenced by the README, CHANGELOG, and (where applicable)
the EU-project deliverables describing exploitation and tech-transfer plans.

---

## 1 · Executive Summary

Sigil is a **legitimately impressive DSL**: ~6,500 LOC of well-factored code,
763 tests, 39 progressive examples, four solid documentation files,
multi-transport (MQTT/Redis/AMQP/Kafka) over `commlib-py`, schemas, joins,
ML predict, routing, retries, DLQs, lifecycle hooks, named multi-window. The
textX grammar is clean, the semantic layer is conscientious (60 `_check_*`
functions), and the code generator produces runnable Python with structured
logging and graceful shutdown.

For a **professional, EU-funded** language, however, several gaps separate it
from production maturity used by industrial partners and from the level
expected by reviewers in a Horizon Europe context (FAIR data, exploitation/IP,
reproducibility, openness, interoperability, observability).

### Top-5 problems, ranked by impact on adoption

| # | Issue | Why it hurts |
|---|---|---|
| 1 | **No source positions on errors** (`_tx_position` is *never* read) | Users see `[ERROR] Pipeline 'p': ...` with no file, no line, no caret. Blocks IDE integration & frustrates pilots. |
| 2 | **Processing-time-only semantics** (`time.time()` flush threads) | No watermarks, late-data, session windows, event-time. Disqualifies Sigil from many real IoT/streaming use cases. |
| 3 | **Generated `pipeline.py` (1452 LOC) duplicates a 138-line aggregation block per named multi-window**; in-process only, lost on restart | Bloated artifacts; no persistent state; not exactly-once. |
| 4 | **Zero IDE support** (no tree-sitter, no LSP, no `tmLanguage`, no formatter, no linter) | Editors treat `.sigil` as plain text. Adoption barrier. |
| 5 | **Stage dispatch by `cls.__class__.__name__` string matching across 38 sites** in 4 modules | Refactor-fragile; one typo silently breaks a stage. |

### Strengths to preserve and market

- Unified multi-transport DSL (genuinely unique vs. Beam/Flink/Bytewax).
- Inline ML predict with 5 runtimes (ONNX/sklearn/torch/tflite/custom).
- Declarative pipe syntax — clean and readable.
- Comprehensive docs (~88K words across 4 markdown files).
- ~99% behavior covered by tests; no dead-code, no TODOs.

---

## 2 · Language & Semantics Improvements

### 2.1 Carry source positions through the entire pipeline (P0, ~1 week)

textX populates `_tx_position`, `_tx_position_end`, `_tx_filename` on **every**
AST node. The codebase reads these *zero* times. Concrete change:

```python
@dataclass
class SemanticError:
    message: str
    severity: str = "error"
    code: str | None = None              # "E0042" — assignable, suppressable
    category: str | None = None          # "reference" | "type" | "constraint" | "composition"
    file: str | None = None
    line: int | None = None
    column: int | None = None
    end_line: int | None = None
    end_column: int | None = None
    suggestion: str | None = None        # actionable fix hint
    node: object | None = None           # AST node for tooling (LSP)
```

Helper:

```python
def _err(node, msg, code=None, suggestion=None, severity="error"):
    pos = getattr(node, "_tx_position", None)
    line, col = (None, None)
    if pos is not None and getattr(node, "_tx_parser", None):
        line, col = node._tx_parser.pos_to_linecol(pos)
    return SemanticError(msg, severity, code=code, file=node._tx_filename,
                         line=line, column=col, suggestion=suggestion, node=node)
```

Render Rust/Elm-style:

```
error[E0042]: aggregate stage requires a window
   ┌─ examples/22_stream_join.sigil:14:8
   |
14 |     |> aggregate {
   |        ^^^^^^^^^ no preceding `window` stage
   = hint: insert `|> window tumbling(1 m)` before aggregate
```

This single change unblocks LSP, IDE diagnostics, useful CI output, and
reviewer-friendly error UX.

### 2.2 Fix the parse-error UX too

`sigil/cli.py:62` swallows textX exceptions as `Parse error: {exc}`. textX's
`TextXSyntaxError` already has `.line`, `.col`, `.context`, `.expected_rules`,
`.filename`. Render them with the same printer used for semantic errors. Show
a 3-line context window with a caret, deduplicate the giant "Expected"
alternative list (group keywords vs. operators).

### 2.3 Type inference & flow-aware checks (P1)

There is no type system beyond `int|float|str|bool` in schemas. Add a
lightweight flow:

- Infer aggregation result types (`avg → float`, `count → int`,
  `first/last → field type`).
- Check map assignments against optional sink schema.
- Warn when `numeric` scalar functions (`abs/floor/log/sqrt/...`) are applied
  to a field declared `str` in the source schema.
- Warn on potential null fan-out: if a schema field is `optional?`, its use in
  arithmetic should require `??` or `is not null`.

### 2.4 Reachability, dead-code, cycle detection (P1)

- **Unreachable route arms**: `when x > 10 => ... when x > 5 => ...` — the
  second is unreachable for `x > 10`. Use a SAT-lite check on numeric
  comparisons.
- **Unused symbols**: pipeline-let bindings, constants, user-defined functions
  (currently only unused aggregations are warned).
- **Pipeline-chain cycle detection**: `pipe A → pipe B → pipe A` is currently
  legal but deadlocks at runtime.

### 2.5 Performance / cardinality warnings (P2)

- `group_by msg.timestamp` (timestamp/UUID-like fields) → cardinality
  explosion.
- `accumulate { x = cumsum(msg.v) }` without bound → unbounded state growth.
- `window tumbling(1 ms)` on a real-time stream → CPU overload.
- `batch: { size: 1000000 }` → memory risk.

Emit `severity=warning` with `suggestion=` text.

### 2.6 Refactor 8+ duplicated AST walkers (P1, ~3 days)

Replace `_walk_expr_for_funcs / _check_map_expr_refs / _check_having_expr_refs
/ _walk_expr_for_temporal / _walk_expr_for_env / _walk_expr_for_quantifiers /
_walk_expr_for_schema / _walk_expr_for_user_func_calls / _walk_expr_for_match`
with a single visitor base:

```python
class ExprVisitor:
    def visit(self, node):
        method = getattr(self, f"visit_{node.__class__.__name__}", None)
        if method: yield from method(node) or ()
        for child in self._children(node):
            yield from self.visit(child)
```

Same opportunity for: 5+ field-path validators, 6+ duration validators, 4+
positive-int validators, 3+ "name in set" validators, 8+ uniqueness checks.
Extracting them shrinks `semantics.py` from ~2,200 LOC to ~1,200 with strictly
better consistency of error wording.

### 2.7 Event-time semantics, watermarks, session windows (P0/P1, big)

This is the **single biggest functional gap** vs. Beam/Flink/ksqlDB and the
area where reviewers will look hard. Proposed surface syntax (kept
Sigil-flavored):

```sigil
|> window tumbling(1 m)
       on event_time msg.ts
       grace 30 s
       late => sink to broker :: "events/late"

|> window session(gap = 5 m) on event_time msg.ts
```

Required runtime additions: per-key timestamp tracking, watermark generator
(max-seen-minus-grace), late-data side output. Extracting a `sigil_runtime`
library (see §3.1) is a precondition — embedding watermark logic in the
current 1,452-line template would be unmaintainable.

### 2.8 Persistent state & exactly-once (P1, big)

`accumulate` keeps state in `self._accum_state` and dies with the process.
Add a state-store abstraction:

```sigil
state
    backend: rocksdb              # memory | rocksdb | redis
    path: "/var/lib/sigil/state"
    changelog: "internal/_state"  # for replay
    checkpoint: 30 s
end
```

Generated code uses an injectable `StateStore` instead of dicts. Pair with
idempotency keys for exactly-once publish (`dedup msg.event_id within 5 m`
already exists for inputs — generalize to outputs).

### 2.9 Side outputs & richer error model (P1)

A general "tap" mechanism would unify late-data, validate-DLQ, enrich-DLQ,
on_error-DLQ, and debug-mirror — currently all four are separate ad-hoc
constructs. Proposal: every stage may emit `=> sink to ...` for a side
stream.

### 2.10 Smaller language polish

- Document **all 80+ reserved keywords** in `language-reference.md` (the
  grammar header lists them; the docs do not).
- Make `model.runtime` extensible (plugin/entry-point) rather than a closed
  `onnx|sklearn|torch|tflite|custom`.
- The `select` stage was just added — ensure consistent treatment across
  `explain.py` and `visualize.py` for non-aggregating pipelines (template path
  `_process_immediate` references `aggregated` even when there is none —
  read-around with a defensive default, but worth a regression test).
- `_compile_user_func_call` inlines via `const_map` substitution. **Recursive
  user functions will infinite-loop at compile time.** Add a recursion-depth
  guard and document the limitation.

---

## 3 · Generated Code, Runtime & Codegen Improvements

### 3.1 Extract a `sigil_runtime` package (P0, ~1.5 weeks)

The single most impactful refactor. `templates/pipeline.py.j2` is 1,452 lines
because nearly every stage's behavior is **inlined** into the template.
Replace with:

```
sigil_runtime/
├── __init__.py
├── aggregations.py          # compute_aggregation(func, values, param)
├── windows.py               # WindowProcessor, MultiWindowProcessor, SessionWindow
├── publish.py               # publish_with_retry(publisher, msg, ...)
├── rate_limit.py            # TokenBucket, ThrottleBuffer
├── dedup.py                 # TTLCache (used by deduplicate + distinct)
├── join.py                  # KeyedJoinBuffer (with index, not linear scan)
├── format.py                # MessageFormatter (json|csv|raw)
├── validate.py              # SchemaValidator (handles nested objects/lists)
├── enrich.py                # HttpEnricher
├── predict.py               # ONNX/sklearn/torch/tflite/custom adapters
├── state.py                 # StateStore protocol (memory/rocksdb/redis)
├── metrics.py               # Optional Prometheus/OTel hooks
└── pipeline_base.py         # PipelineBase ABC: lifecycle, signal, drain, listeners
```

Generated `pipeline.py` then shrinks to ~150 lines per pipeline: a class
composing the runtime objects. Adding a stage means writing a runtime class +
a template snippet, not extending an 1,452-line template.

This **also** eliminates the duplicated 138-line aggregation block currently
emitted *per named multi-window*.

### 3.2 Decompose `PipelineCtx` (87 fields → composition)

```python
@dataclass
class PipelineCtx:
    name: str
    class_name: str
    source: SourceCtx
    output: SinkCtx | RouteCtx                 # exactly one
    join: JoinCtx | None = None
    pipeline_lets: list[PipelineLetCtx] = ...
    stages: list[StageCtx] = ...               # heterogeneous, polymorphic
    on_error: OnErrorCtx | None = None
    retry: RetryCtx | None = None
    initially: ObjectLiteralCtx | None = None
    finally_: ObjectLiteralCtx | None = None
```

The template iterates `p.stages` and dispatches to a `render_<stage>.j2`
partial. Adding a stage = one new partial + one new context class.

### 3.3 Replace string dispatch with a Stage protocol (P1)

```python
class StageHandler(Protocol):
    name: str                                # "filter", "window", ...
    def validate(self, stage, ctx) -> list[SemanticError]: ...
    def explain(self, stage) -> str: ...
    def visualize(self, stage) -> VisNode: ...
    def emit_init(self, stage, ctx) -> str: ...
    def emit_handle(self, stage, ctx) -> str: ...

STAGE_HANDLERS: dict[type, StageHandler] = {...}
```

Eliminates the 38 `cls == "FooStage"` sites across `codegen / semantics /
explain / visualize`. Future Sigil stages register themselves; third parties
can register out-of-tree.

### 3.4 Observability (P1, ~1 week)

Generated app currently exposes only `LOG_FORMAT=json|text`. Add **opt-in**
(env-flagged, no hard dep):

- `SIGIL_METRICS=prometheus:9090` → exposes counters
  (`sigil_messages_received_total`, `sigil_messages_dropped_total`,
  `sigil_window_buffer_size`, `sigil_publish_failures_total`,
  `sigil_dlq_messages_total`) and histograms
  (`sigil_window_flush_duration_seconds`,
  `sigil_message_processing_duration_seconds`).
- `SIGIL_TRACING=otlp:http://collector:4318` → OpenTelemetry spans per stage,
  parent span per message.
- `SIGIL_HEALTHCHECK=8080` → `/livez` (process up), `/readyz` (subscriber
  connected, brokers reachable).

Wire via the runtime library (§3.1) — no template bloat.

### 3.5 Backpressure & loss visibility

`del _buf[:_dropped]` in the window template silently truncates buffers to
`_MAX_WINDOW_BUFFER = 10000` (hard-coded). Three changes:

1. Surface as a metric (`sigil_messages_dropped_total{reason="backpressure"}`).
2. Make the limit a per-window config:
   `|> window tumbling(1 m) max_buffer 50000`.
3. Optionally route dropped messages to a side output (see §2.9).

Same treatment for `throttle` overflow=buffer (drops silently when buffer
full) and `rate` (drops silently when tokens exhausted).

### 3.6 Join is O(N·M) linear scan

`templates/pipeline.py.j2:622-641`: each incoming message scans the join
buffer linearly and on first match. Fix: a per-key index keyed on
`right_key`. Also: `_join_pending` retries replay old messages on every join
arrival — under high-throughput skew this can multiply emission. Bound the
retry count or expire pending messages on watermark advance.

### 3.7 ValidateStage runtime is shallow

Grammar supports `SchemaObjectType` (nested `{ ... }`) and `SchemaListType`
(`[Type]`), but the generated `_validate_msg` only checks top-level scalars
(4× repeated `int/float/str/bool` blocks). Extract `SchemaValidator` (see
§3.1) and implement nested + list element validation.

### 3.8 Expression compiler hardening (P2)

`expressions.py` is mostly safe (literals via `repr()`, field segments via
`repr()`, args are pre-compiled), but:

- No runtime guard around `math.log/sqrt/log10/exp` — a single bad message
  crashes the stage handler. Wrap in try/except returning `None`.
- `extract(haystack, pattern, group)` and `matches(haystack, pattern)`
  re-compile the regex on every message. Cache compiled patterns via
  `re._cache` (already global) or precompile in `__init__` when both args are
  literal.
- `_compile_user_func_call` recursion guard (mentioned in §2.10).
- `_flatten_binary` is correctness-critical (works around right-recursion in
  the PEG grammar) — annotate with the test names that guard it.

### 3.9 Generated-code testability (P2)

The generated `Pipeline` class is monolithic with publishers built in
`__init__`. Add a hook:

```python
class ExamplePipeline(PipelineBase):
    def __init__(self, *, publisher_factory=None, subscriber_factory=None,
                 state_store=None, clock=None):
```

Lets users unit-test individual handlers with mock brokers and a controllable
clock.

---

## 4 · Tooling & Developer UX

### 4.1 IDE support — start with tree-sitter (P0, ~2 weeks)

Highest ROI single investment for adoption.

- **Tree-sitter grammar** → instant syntax highlighting in Neovim, Helix, Zed,
  GitHub, Sublime, and (via wasm) the web playground.
- **VS Code extension** wrapping tree-sitter for highlighting + textX-driven
  diagnostics from `sigil check --json`.
- **LSP server** (optional, Phase 2): `pygls`-based, reuses the parser &
  semantics from `sigil/`. Provides diagnostics, hover, go-to-definition
  (broker/schema/model/pipeline references), completion, code actions for the
  most common errors.

The tree-sitter grammar is mostly mechanical translation of `grammar.tx`;
expect ~600 lines of `grammar.js` + a `queries/highlights.scm`.

### 4.2 CLI overhaul (P1, ~3 days)

```
sigil --version                              # missing
sigil check FILE [--json]                    # JSON output for CI/IDE
sigil generate FILE -o DIR [--watch]
sigil explain FILE [--format text|markdown|json]
sigil visualize FILE [--format dot|mermaid|svg|png]
sigil fmt FILE [--check]                     # NEW — formatter
sigil lint FILE [--rules ...]                # NEW — style/perf rules
sigil run FILE [--input file.jsonl] [--dry-run]   # NEW — synthetic exec
sigil repl                                   # NEW — interactive playground
sigil completion {bash|zsh|fish}             # NEW — shell completion
```

Add `rich` for colored output (single optional dep, falls back to plain).
Document exit codes (`0`=ok, `1`=usage, `2`=parse error, `3`=semantic error,
`4`=I/O error).

### 4.3 Formatter `sigil fmt` (P1, ~1 week)

Operates on the textX AST → unparser. Stable canonical form (1-true-indent,
alphabetized broker fields, normalized duration units, consistent `;`
placement in map blocks). Solves the `replace_colons*.py` migration class of
problem permanently.

### 4.4 Linter `sigil lint` (P1, ~1 week)

Built atop the visitor pattern in §2.6. Rule packs:

- `style`: naming conventions, line length, ordering of broker fields.
- `perf`: high-cardinality `group_by`, unbounded `accumulate`, very small
  windows, very large batches.
- `safety`: missing `??` on `optional?` fields, `enrich` without `on_error`,
  `predict` without input null-checks.
- `compat`: deprecated stage syntax, transport-incompatible options.

Each rule has an ID (`SIGIL-S001`, `SIGIL-P003`, ...) suppressible via inline
comments: `// sigil-lint disable=P003`.

### 4.5 Dry-run / synthetic-event mode (P1)

`sigil run pipeline.sigil --input events.jsonl --dry-run` should:
instantiate the generated pipeline class with a no-op publisher, feed
JSON-line events through `_on_message`, and print emitted output to stdout.
Closes the loop for CI golden-file testing of pipeline behavior, not just
generated code.

### 4.6 Web/CLI playground

The separate `sigil-designer` repo (React) handles visual authoring. A
complementary text-mode playground (`sigil repl` or a small docs site running
pyodide-compiled `sigil`) would let reviewers/partners try Sigil from a
browser without install.

---

## 5 · Quality Engineering & CI

### 5.1 Add `make validate-examples` to CI (5 minutes, P0)

The Makefile target exists; `.github/workflows/ci.yml` does not call it.
One-line fix:

```yaml
- run: make validate-examples
```

### 5.2 Golden-file tests for generated code (P1, ~1 day)

Currently `test_codegen.py` asserts substrings (`assert "_get_nested(msg,
'x')" in code`). False-positive prone and silent on structural drift. Adopt
golden files:

```
tests/golden/02_filtering/app.py
tests/golden/02_filtering/pipelines/temperature_filter.py
tests/golden/02_filtering/requirements.txt
```

with `UPDATE_GOLDEN=1 pytest` to refresh on intentional changes. Pair with a
per-PR human-review of golden-file diffs.

### 5.3 Snapshot tests for `explain` and `visualize` (P1, ~half day)

Use `syrupy`. Each example yields one `.ambr` snapshot. Catches regressions
in pedagogically-important output.

### 5.4 Property-based tests with Hypothesis (P1, ~2-3 days)

Strategies for: random valid expressions (precedence stress), random valid
pipelines, random schemas. Properties:

- `parse(unparse(parse(src))) == parse(src)` — roundtrip stability (requires
  `sigil fmt`).
- `compile_expr(parse(expr))` always produces syntactically valid Python
  (`compile(code, "<test>", "eval")` succeeds).
- `validate(model)` is total (never crashes; always returns a list).
- For randomly-generated expressions in pure-arithmetic contexts:
  `eval(compile_expr(e))` matches a reference Python interpreter.

### 5.5 Parametrize the test families

Ten copy-pasted `test_<op>_expression` tests collapse to one
`@pytest.mark.parametrize` with 10 cases. Easier to extend, easier to read.

### 5.6 Benchmarks

`.benchmarks/` is empty. Add `pytest-benchmark` cases for: parser throughput,
`validate()` throughput, codegen throughput, generated-pipeline message
throughput per stage type. Track regressions across releases.

### 5.7 Coverage gating

Add `coverage` reporting in CI with a floor (e.g., 90%) — Sigil is already at
high coverage, codify it.

### 5.8 Lint/type-check job

Add a third CI job: `ruff check`, `ruff format --check`, optional `mypy
--strict sigil/`. Currently `.ruff_cache` exists but no rule config —
formalize it.

---

## 6 · Project Hygiene & Packaging

| Item | Current | Action |
|---|---|---|
| Build backend | `setup.py` (19 LOC) | Migrate to `pyproject.toml` (PEP 621), keep setup.py shim if needed |
| Version | Duplicated in `setup.py` + `sigil/__init__.py` | Single source of truth via `importlib.metadata` |
| Linter / formatter | Cache present, no config | Adopt `ruff` (one tool covers both); commit `ruff.toml` |
| Type checker | None | `mypy --strict sigil/` (codebase already type-annotated) |
| Pre-commit | None | `.pre-commit-config.yaml`: ruff + mypy + grammar-changed test trigger |
| Dependency pins | `requirements.txt` unpinned | Use `pyproject.toml` ranges for runtime, lock dev deps |
| Docs site | Markdown files only | MkDocs Material → ReadTheDocs / GitHub Pages with versioning |
| API docs | None (no docstrings on public API) | Add docstrings, generate via mkdocstrings |
| Changelog | None | Adopt Keep-a-Changelog + semantic versioning |
| Release automation | Manual | GitHub Action: tag → build → PyPI + GitHub Release |
| Citation | None | Add `CITATION.cff` (mandatory for academic/EU citability) |
| Reproducibility | Docker present | Pin Docker base image digest, publish multi-arch image |

---

## 7 · EU-Project Specific Recommendations

These items are often the difference between "interesting prototype" and
"credible deliverable" in a Horizon Europe context.

### 7.1 FAIR & Open Science

- Publish the package on PyPI **and** Zenodo (with DOI per release; trivially
  automated via GitHub-Zenodo integration). Reference the DOI in `README.md`
  and `CITATION.cff`.
- Register in [openaire.eu](https://www.openaire.eu/) and link the funding
  programme + grant ID in `README.md` and `pyproject.toml` metadata.
- Add a `data/` directory with FAIR-compliant **synthetic datasets** for each
  example (esp. ML predict examples 26-30, which currently rely on
  user-supplied model files).

### 7.2 GDPR-aware example pack

Add 3-4 examples relevant to typical EU project pilots:

- **GDPR data minimization**: source from `users/events`, `validate` against
  schema, `map { ... pseudonymized_id = sha256(msg.user_id, env("PEPPER")) ... }`
  dropping PII fields, sink to `analytics/events`.
- **Energy / smart-grid** (ENTSO-E-style data shapes) with `window` +
  `aggregate` for hour/day rollups.
- **Industrial OPC-UA gateway** pattern: source bridging from a transport
  carrying OPC-UA-shaped messages, `validate` schema, `enrich` from a CMMS
  HTTP endpoint, sink to a cloud broker. Excellent showcase for
  cross-transport.
- **Healthcare device telemetry**: anomaly detection (`predict`) + `route` to
  clinician alerts vs. archive vs. DLQ.

### 7.3 Compliance & exploitation

- License headers on source files (Apache-2.0 — the LICENSE file exists;
  headers do not).
- `SECURITY.md` with vulnerability disclosure contact.
- `EXPLOITATION.md` describing TRL, intended exploitation routes, partners
  using the DSL, links to spin-off / consortium agreements as relevant.
- `THIRD_PARTY_NOTICES.md` listing dependency licenses (commlib-py, textX,
  Jinja2, optional ML deps).

### 7.4 Interoperability

- Generate **CycloneDX SBOM** for each release (security expectation in
  EU-funded SW post-CRA).
- Provide a JSON Schema for the `.sigil` AST so external tools (designer,
  validators, deliverable evidence) can consume it.
- Stable JSON output for `sigil check --json`, `sigil explain --format json`,
  `sigil visualize --format json` — feeds dashboards & deliverables.

### 7.5 Reviewer-ready demo

A 5-minute `make demo` that: spins Docker stack → publishes synthetic events
→ runs an example pipeline → prints metrics → opens a Mermaid diagram of the
pipeline. Make it the first thing in `README.md`.

---

## 8 · Prioritized Roadmap

| Priority | Theme | Item | Effort |
|---|---|---|---|
| **P0** | Errors | Source positions on every `SemanticError` + Rust-style printer | 1 wk |
| **P0** | Errors | Render textX parse errors with file:line:col + caret | 1-2 d |
| **P0** | Tooling | Tree-sitter grammar + minimal VS Code extension | 2 wk |
| **P0** | CI | Add `validate-examples` to GH Actions | 5 min |
| **P0** | Codegen | Extract `sigil_runtime` package; shrink `pipeline.py.j2` from 1452→<400 lines | 1.5 wk |
| **P0** | Packaging | Migrate to `pyproject.toml` + `ruff` + `mypy` + pre-commit | 2-3 d |
| **P1** | Semantics | Visitor refactor (8+ duplicated walkers) + `_err()` helper rollout | 3-4 d |
| **P1** | Semantics | Type inference & flow checks + reachability/dead-code | 1 wk |
| **P1** | Language | Event-time + watermarks + grace + late side-output | 2-3 wk |
| **P1** | Language | Session windows | 1 wk |
| **P1** | Runtime | Persistent state store (memory→rocksdb→redis) + changelog | 2-3 wk |
| **P1** | Runtime | Observability (Prometheus + OTel + healthcheck) | 1 wk |
| **P1** | Runtime | Indexed join, schema-registry hook, nested validate | 1-2 wk |
| **P1** | CLI | `--version`, `--watch`, `--json`, colored output, exit codes | 2-3 d |
| **P1** | CLI | `sigil fmt` (formatter) | 1 wk |
| **P1** | CLI | `sigil lint` (rule engine on visitor) | 1 wk |
| **P1** | CLI | `sigil run --dry-run --input events.jsonl` | 4 d |
| **P1** | Tests | Golden-files + Hypothesis + snapshots + parametrize + benchmarks | 1 wk |
| **P2** | Tooling | Full LSP via `pygls` | 2 wk |
| **P2** | Language | Exactly-once / idempotent publish | 1-2 wk |
| **P2** | Language | Generalized side outputs replacing 4 ad-hoc DLQ constructs | 1 wk |
| **P2** | Project | Zenodo + CITATION.cff + SBOM + EXPLOITATION.md + GDPR examples | 3-4 d |
| **P2** | Docs | MkDocs Material site + API docs (mkdocstrings) | 3-4 d |

### Suggested first sprint (2 weeks)

To maximize visible progress with low risk:

1. P0 errors (source positions + Rust-style printer + parse-error fix)
2. P0 CI (`validate-examples`)
3. P0 packaging (`pyproject.toml`, ruff, mypy, pre-commit)
4. P1 visitor refactor + `_err()` rollout
5. P1 CLI: `--version`, colored output, exit codes
6. P1 tests: golden files for codegen + parametrize the duplicated test
   families
7. P2 hygiene: `CITATION.cff`, `SECURITY.md`, license headers, README EU/funder
   banner

This sprint is **strictly improvements, no language additions** — derisks the
bigger items by giving you a cleaner foundation to build watermarks, state
stores, IDE tooling, and exactly-once on top of.

---

## 9 · What Not to Change

- The pipe-operator surface syntax — clean, readable, distinctive.
- The decision to compile to standalone Python over `commlib-py` rather than
  embed in a runtime — strong fit for edge/IoT and EU industrial use cases.
- textX as the parser frontend — grammar is the metamodel, which is exactly
  the right choice at this scale.
- The 39 progressively-documented examples — preserve as the canonical
  teaching path; just add EU/GDPR/energy variants alongside.
- Inline ML predict (5 runtimes) — genuine differentiator vs. Beam, Flink,
  Bytewax.
- Multi-transport unified DSL — also a real differentiator; emphasize in
  marketing/papers.

---

## 10 · One-line Summary

Sigil is a fundamentally sound DSL **two well-targeted sprints** away from
being credibly production-grade for EU industrial pilots — the work is finite,
mostly mechanical, and concentrated in three places: **error UX (source
positions), codegen modularity (extract a runtime package), and tooling
(tree-sitter / LSP / pyproject)**. The bigger language items (event-time,
watermarks, persistent state, exactly-once) are real and important but become
easy once those three foundations land.
