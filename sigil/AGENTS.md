# sigil — Core Package

## OVERVIEW

DSL compiler core: textX grammar → parse → validate → generate Python MQTT apps.

## MODULE MAP

| Module | Role | Key Exports |
|--------|------|-------------|
| `grammar.tx` | textX grammar definition | AST classes: `Model`, `Broker`, `Pipeline`, `ConstDef`, `Stage` variants (incl. `DeduplicateStage`, `HavingStage`), `Expression` tree (incl. `CaseExpr`, `EnvRef`, `OldRef`, `ChangedRef`, `ForallExpr`, `ExistsExpr`), `InitiallyHook`, `FinallyHook`, `ObjectLiteral`, `ObjectField`, `BatchConfig`, `ImportDecl`, `AnnotatedStage`, `StageAnnotation`, `RouteBlock`, `RouteArm`, `RouteSink` |
| `language.py` | Parser entrypoints + import resolution | `build_metamodel()`, `parse(path)`, `parse_string(src)`, `CircularImportError` |
| `semantics.py` | Semantic validation | `validate(model)`, `validate_or_raise(model)`, `SemanticError`, `SemanticValidationError`, `_unwrap_stages(pipeline)` |
| `expressions.py` | Expression → Python compiler | `compile_expr(node, ctx, local_names, const_map)`, `extract_field_keys(ref)`, `ExprContext` enum (FILTER/MAP/AGGREGATE/HAVING/QUANTIFIER) |
| `codegen.py` | Model → file generation | `generate(model, output_dir)` → writes `app.py`, `pipelines/*.py`, `requirements.txt` |
| `explain.py` | Pipeline explanation | `explain_model(model)` → human-readable string |
| `visualize.py` | Pipeline diagram generation | `visualize_dot(model)` → DOT string, `visualize_mermaid(model)` → Mermaid string |
| `cli.py` | CLI (`sigil`) | `main()` — subcommands: `check`, `generate`, `explain`, `visualize` |

## WHERE TO LOOK

| Task | File | Function/Class |
|------|------|----------------|
| Add new grammar rule | `grammar.tx` | Add rule + wire into `Stage` or `Model` |
| Add new aggregation function | `grammar.tx` (`AggFunc`) + `semantics.py` (`FUNCS_*` sets) + `templates/pipeline.py.j2` |
| Add new stage type | `grammar.tx` (`Stage`) + `semantics.py` (`STAGE_ORDER`) + `codegen.py` (`_build_pipeline_ctx`) + `templates/pipeline.py.j2` |
| Add new validation check | `semantics.py` | New `_check_*` function, wire into `validate()` |
| Change how expressions compile | `expressions.py` | `compile_expr()` — add clause for new AST class name |
| Change generated output structure | `codegen.py` | `generate()` + dataclass fields |
| Change generated Python behavior | `templates/*.j2` | Template variables come from `PipelineCtx` |

## CONVENTIONS

- **Grammar changes cascade**: Any grammar.tx change may require updates in semantics, codegen, expressions, and templates.
- **Stage dispatch**: Always `stage.__class__.__name__` string matching. If adding a stage, add to all 4 locations above.
- **Validation functions**: Private `_check_*` functions return `list[SemanticError]`. Never raise directly.
- **Duration handling**: `UNIT_MULTIPLIERS` dict in `semantics.py`, reused by `codegen.py` via import.
- **Codegen dataclasses**: `PipelineCtx` is the sole interface between `codegen.py` and Jinja2 templates. Add fields here before using in templates.
- **Map assignment separators**: `MapStage` uses `mappings+=MapAssignment[';']` — semicolons separate multiple assignments. Single assignments need no `;`. `AggregateStage` does NOT use semicolons (`)` from function calls is an unambiguous boundary). Regression tests in `test_grammar.py` guard this.

## TEMPLATES

```
templates/
├── app.py.j2           # Main app: imports pipelines, signal handling, event loop
├── pipeline.py.j2      # Per-pipeline class: MQTT connect, window/aggregate logic
└── requirements.txt.j2 # Just paho-mqtt
```

Template variables: `p` (`PipelineCtx`), `pipelines` (list of `PipelineCtx` in app.py.j2).

## TESTING

Tests mirror module structure: `test_grammar.py`, `test_semantics.py`, `test_codegen.py`.
- Grammar tests: parse DSL strings, assert AST structure.
- Semantics tests: parse → validate → assert error messages.
- Codegen tests: parse → generate to tmpdir → assert file contents/compilability.
- Helper: `_model(src)` and `_errors(src)` in test_semantics.py.
- Expression tests: `TestExpressionCompiler` class in test_codegen.py.
