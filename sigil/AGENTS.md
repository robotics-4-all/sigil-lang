# sigil -- Core Package

## OVERVIEW

DSL compiler core: textX grammar -> parse -> validate -> generate standalone Python apps via Jinja2 templates.

## MODULE MAP

| Module | LOC | Role | Key Exports |
|--------|-----|------|-------------|
| `grammar.tx` | 495 | textX grammar definition | AST classes: `Model`, `Broker`, `Pipeline`, `ConstDef`, `SchemaDef`, `FuncDef`, `Stage` variants, `Expression` tree, `ImportDecl`, `AnnotatedStage`, `RouteBlock`, `ObjectLiteral`, hooks |
| `language.py` | 67 | Parser entrypoints + import resolution | `build_metamodel()`, `parse(path)`, `parse_string(src)`, `CircularImportError` |
| `semantics.py` | 1883 | Semantic validation (50 checks) | `validate(model)`, `validate_or_raise(model)`, `SemanticError`, `SemanticValidationError`, `_unwrap_stages()`, `UNIT_MULTIPLIERS`, `STAGE_ORDER` |
| `expressions.py` | 327 | Expression -> Python compiler | `compile_expr(node, ctx, local_names, const_map, func_defs)`, `extract_field_keys(ref)`, `ExprContext` enum |
| `codegen.py` | 694 | Model -> file generation | `generate(model, output_dir)`, 11 `*Ctx` dataclasses |
| `explain.py` | 215 | Pipeline explanation | `explain_model(model)` -> human-readable string |
| `visualize.py` | 644 | Pipeline diagram generation | `visualize_dot(model)` -> DOT, `visualize_mermaid(model)` -> Mermaid |
| `cli.py` | 132 | CLI (`sigil`) | `main()` -- subcommands: `check`, `generate`, `explain`, `visualize` |

## WHERE TO LOOK

| Task | File | Function/Section |
|------|------|------------------|
| Add new grammar rule | `grammar.tx` | Add rule + wire into `Stage` or `Model` |
| Add new aggregation function | `grammar.tx` (`AggFunc`) + `semantics.py` (`FUNCS_*` sets) + `templates/pipeline.py.j2` |
| Add new stage type | `grammar.tx` (`Stage`) + `semantics.py` (`STAGE_ORDER`) + `codegen.py` (`_build_pipeline_ctx`) + `templates/pipeline.py.j2` |
| Add new validation check | `semantics.py` | New `_check_*` function, wire into `validate()` |
| Change expression compilation | `expressions.py` | `compile_expr()` -- add clause for new AST class name |
| Change generated output structure | `codegen.py` | `generate()` + add `PipelineCtx` field + update template |
| Change generated Python behavior | `templates/*.j2` | Template variables come from `PipelineCtx` |
| Change import resolution | `language.py` | `_resolve_imports()` -- recursive merge of brokers/constants/pipelines/schemas |

## CONVENTIONS

- **Grammar changes cascade**: Any `grammar.tx` change requires checking semantics, codegen, expressions, explain, visualize, and templates.
- **Stage dispatch**: Always `stage.__class__.__name__` string matching. If adding a stage, update all 4 locations above + `explain.py` + `visualize.py`.
- **Validation functions**: Private `_check_*` functions return `list[SemanticError]`. Never raise directly. Wire each into `validate()` via `errors.extend(_check_*(...))`.
- **Duration handling**: `UNIT_MULTIPLIERS` dict in `semantics.py`, imported by `codegen.py`.
- **Codegen dataclasses**: `PipelineCtx` is the sole interface between `codegen.py` and Jinja2 templates. Add fields to `PipelineCtx` before using in templates.
- **Map assignment separators**: `MapStage` uses `mappings+=MapAssignment[';']`. `AggregateStage` does NOT use semicolons. Regression tests in `test_grammar.py` guard this.
- **AST walking**: `_AST_WALK_ATTRS` tuple in `codegen.py` defines attribute names for recursive AST traversal. Used by `_has_temporal_ref()`, `_has_env_ref()`, `_has_quantifier_over_messages()`.
- **Const/func resolution**: `_build_const_map()` and `_build_func_defs()` run before pipeline context building. Passed through to `compile_expr()`.
- **Cross-references in grammar**: `broker=[Broker]` (Source/Sink), `schema=[SchemaDef]` (Source), `pipe=ID` (Source), `window_ref=ID` (AggAssignment) -- resolved by textX during parsing.
- **Atom rule ordering**: In grammar.tx `Atom` rule, `OldRef | ChangedRef` MUST precede `UserFuncCall` to prevent `old()`/`changed()` from being captured as user function calls. Order matters in textX ordered choice.
- **User func inlining**: `_compile_user_func_call()` in expressions.py inlines function body by substituting params into `const_map`. Not a true function call — parameter expressions become const-like entries.

## CODEGEN DATACLASSES

11 context dataclasses populate template variables:

| Dataclass | Purpose |
|-----------|---------|
| `BrokerCtx` | Broker connection (name, transport, host, port, ssl, auth) |
| `PipelineCtx` | Full pipeline state -- sole template interface (`p` in pipeline.py.j2) |
| `AggregationCtx` | Single aggregation (name, func, field_keys, type, window_ref) |
| `AccumCtx` | Running accumulator (name, func, field_name, param_code) |
| `WindowCtx` | Window config (name, type, size_seconds, step_seconds) |
| `JoinCtx` | Join details (broker, topic, keys, window, type) |
| `PipelineLetCtx` | Pipeline-level let binding (name, value_code) |
| `LetBindingCtx` | Map-stage let binding (name, value_code) |
| `MappingCtx` | Map assignment (target, value_code) |
| `ObjectFieldCtx` | ObjectLiteral field for hooks (name, value_code) |
| `RouteArmCtx` | Route condition (condition_code, broker, topic, qos) |

## TEMPLATES

```
templates/
+-- app.py.j2           # Main: import pipelines, signal handling, event loop, pipe wiring
+-- pipeline.py.j2      # Per-pipeline class: _on_message chain, window/batch/join logic (1053 lines)
+-- requirements.txt.j2 # commlib-py>=0.13.0
```

Template variables: `p` (`PipelineCtx`) in pipeline.py.j2, `pipelines` (list of `PipelineCtx`) in app.py.j2.

## TESTING

Tests mirror module structure: `test_grammar.py`, `test_semantics.py`, `test_codegen.py`, `test_visualize.py`, `test_integration.py`.
- Grammar tests: parse DSL strings via `parse_string()`, assert AST structure.
- Semantics tests: parse -> validate -> assert error messages/severity. Helpers: `_model(src)`, `_errors(src)`.
- Codegen tests: parse -> generate to `tempfile.TemporaryDirectory()` -> assert file contents + `compile()` for syntax. `TestExpressionCompiler` class tests `compile_expr()` directly.
- Visualize tests: parse -> `visualize_dot()`/`visualize_mermaid()` -> assert graph content.
- Integration tests: parse -> generate -> dynamic import -> start pipeline -> send/receive via `commlib-py` -> assert output. Uses `pytest.mark.skipif` for broker availability.
