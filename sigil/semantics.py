from dataclasses import dataclass, field


@dataclass
class SemanticError:
    message: str
    severity: str = "error"
    code: str | None = None
    file: str | None = None
    line: int | None = None
    column: int | None = None
    end_line: int | None = None
    end_column: int | None = None
    suggestion: str | None = None
    node: object | None = field(default=None, repr=False, compare=False)

    def __str__(self):
        loc = ""
        if self.file or self.line:
            file_part = self.file or "<unknown>"
            line_part = f":{self.line}" if self.line else ""
            col_part = f":{self.column}" if self.column else ""
            loc = f" {file_part}{line_part}{col_part}"
        code_part = f" [{self.code}]" if self.code else ""
        head = f"[{self.severity.upper()}]{code_part}{loc} {self.message}"
        if self.suggestion:
            return f"{head}\n  hint: {self.suggestion}"
        return head


def _node_position(node):
    pos = getattr(node, "_tx_position", None)
    parser = getattr(node, "_tx_parser", None)
    if pos is None or parser is None:
        return None, None, None, None
    line, col = parser.pos_to_linecol(pos)
    end = getattr(node, "_tx_position_end", None)
    end_line, end_col = (None, None)
    if end is not None:
        end_line, end_col = parser.pos_to_linecol(end)
    return line, col, end_line, end_col


def _err(node, message, *, code=None, suggestion=None, severity="error"):
    line, col, end_line, end_col = _node_position(node)
    return SemanticError(
        message=message,
        severity=severity,
        code=code,
        file=getattr(node, "_tx_filename", None) if node is not None else None,
        line=line,
        column=col,
        end_line=end_line,
        end_column=end_col,
        suggestion=suggestion,
        node=node,
    )


FUNCS_REQUIRING_FIELD = {
    "avg",
    "sum",
    "min",
    "max",
    "stddev",
    "first",
    "last",
    "percentile",
    "distinct_count",
}
FUNCS_NO_FIELD = {"count"}
FUNCS_REQUIRING_PARAM = {"percentile"}
VALID_QOS = {0, 1, 2}
VALID_FORMATS = {"json", "csv", "raw"}
VALID_TRANSPORTS = {"MQTT", "REDIS", "AMQP", "KAFKA"}


def _unwrap_stages(pipeline):
    """Extract raw stage objects from AnnotatedStage wrappers."""
    stages = []
    for item in pipeline.stages:
        if item.__class__.__name__ == "AnnotatedStage":
            stages.append(item.stage)
        else:
            stages.append(item)
    return stages


SCALAR_FUNC_ARGS = {
    "abs": (1, 1),
    "round": (1, 2),
    "len": (1, 1),
    "lower": (1, 1),
    "upper": (1, 1),
    "floor": (1, 1),
    "ceil": (1, 1),
    "fmt": (1, 99),
    "trim": (1, 1),
    "contains": (2, 2),
    "starts_with": (2, 2),
    "ends_with": (2, 2),
    "split": (2, 2),
    "replace": (3, 3),
    "substring": (2, 3),
    "sqrt": (1, 1),
    "pow": (2, 2),
    "log": (1, 1),
    "log10": (1, 1),
    "exp": (1, 1),
    "to_int": (1, 1),
    "to_float": (1, 1),
    "to_str": (1, 1),
    "to_bool": (1, 1),
    "now": (0, 0),
    "timestamp_ms": (0, 0),
    "date_format": (2, 2),
    "date_diff": (2, 2),
    "matches": (2, 2),
    "extract": (3, 3),
}

STAGE_ORDER = {
    "DebugStage": -2,
    "DeduplicateStage": -1,
    "FilterStage": 0,
    "ValidateStage": 0,
    "PredictStage": 0,
    "RateStage": 1,
    "FlattenStage": 2,
    "DistinctStage": 3,
    "ThrottleStage": 4,
    "SampleStage": 5,
    "GroupByStage": 6,
    "WindowStage": 7,
    "AggregateStage": 8,
    "AccumulateStage": 9,
    "HavingStage": 10,
    "MapStage": 11,
    "SelectStage": 11,
    "EnrichStage": 11,
}

ACCUM_FUNCS_REQUIRING_PARAM = {"ema"}
ACCUM_FUNCS_NO_PARAM = {"cumsum", "delta"}

NUMERIC_TYPES = {"int", "float"}
STRING_TYPES = {"str"}
AGG_FUNCS_NUMERIC = {"avg", "sum", "min", "max", "stddev"}
SCALAR_FUNCS_STRING = {"lower", "upper"}
SCALAR_FUNCS_NUMERIC = {"abs", "round", "floor", "ceil"}


def validate(model) -> list[SemanticError]:
    errors: list[SemanticError] = []
    const_names = {c.name for c in model.constants}
    func_names = {f.name for f in model.functions} if model.functions else set()
    errors.extend(_check_unique_broker_names(model))
    errors.extend(_check_unique_pipeline_names(model))
    errors.extend(_check_unique_const_names(model))
    errors.extend(_check_unique_schema_names(model))
    errors.extend(_check_schema_fields(model))
    errors.extend(_check_brokers(model))
    errors.extend(_check_func_defs(model))
    errors.extend(_check_models(model))
    for pipeline in model.pipelines:
        errors.extend(_check_source(pipeline, model))
        errors.extend(_check_sink_or_route(pipeline))
        errors.extend(_check_stage_order(pipeline))
        errors.extend(_check_duplicate_stages(pipeline))
        errors.extend(_check_stage_dependencies(pipeline))
        errors.extend(_check_multi_window(pipeline))
        errors.extend(_check_aggregations(pipeline))
        errors.extend(_check_duplicate_agg_names(pipeline))
        errors.extend(_check_duplicate_map_targets(pipeline))
        errors.extend(_check_map_references(pipeline, const_names))
        errors.extend(_check_having_references(pipeline, const_names))
        errors.extend(_check_unused_aggregations(pipeline))
        errors.extend(_check_window_constraints(pipeline))
        errors.extend(_check_dedup_constraints(pipeline))
        errors.extend(_check_scalar_funcs(pipeline))
        errors.extend(_check_let_bindings(pipeline))
        errors.extend(_check_pipeline_let_bindings(pipeline, const_names))
        errors.extend(_check_case_exprs(pipeline))
        errors.extend(_check_match_exprs(pipeline))
        errors.extend(_check_user_func_calls(pipeline, func_names))
        errors.extend(_check_temporal_refs(pipeline, model))
        errors.extend(_check_temporal_field_validity(pipeline))
        errors.extend(_check_env_refs(pipeline))
        errors.extend(_check_quantifier_refs(pipeline))
        errors.extend(_check_object_literal_fields(pipeline))
        errors.extend(_check_sink_batch(pipeline))
        errors.extend(_check_rate(pipeline))
        errors.extend(_check_accumulate(pipeline))
        errors.extend(_check_flatten(pipeline))
        errors.extend(_check_distinct(pipeline))
        errors.extend(_check_throttle(pipeline))
        errors.extend(_check_sample(pipeline))
        errors.extend(_check_stage_annotations(pipeline))
        errors.extend(_check_route_block(pipeline))
        errors.extend(_check_pipeline_chaining(pipeline, model))
        errors.extend(_check_join(pipeline))
        errors.extend(_check_on_error(pipeline))
        errors.extend(_check_retry(pipeline))
        errors.extend(_check_schema_references(pipeline, model))
        errors.extend(_check_predict_stages(pipeline, model))
        errors.extend(_check_select_stage(pipeline))
        errors.extend(_check_debug_stage(pipeline))
        errors.extend(_check_subscript_keys(pipeline))
        errors.extend(_check_validate_stage(pipeline, model))
        errors.extend(_check_enrich_stage(pipeline, model))
    return errors


def validate_or_raise(model):
    errors = validate(model)
    real_errors = [e for e in errors if e.severity == "error"]
    if real_errors:
        msg = "Semantic validation failed:\n" + "\n".join(f"  - {e}" for e in real_errors)
        raise SemanticValidationError(msg, errors=real_errors)
    return errors


class SemanticValidationError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors or []


def _check_unique_broker_names(model):
    seen = {}
    errors = []
    for b in model.brokers:
        if b.name in seen:
            errors.append(
                SemanticError(f"Duplicate broker name '{b.name}'. Broker names must be unique.")
            )
        seen[b.name] = b
    return errors


def _check_unique_pipeline_names(model):
    seen = {}
    errors = []
    for p in model.pipelines:
        if p.name in seen:
            errors.append(
                SemanticError(f"Duplicate pipeline name '{p.name}'. Pipeline names must be unique.")
            )
        seen[p.name] = p
    return errors


def _check_brokers(model):
    errors = []
    for broker in model.brokers:
        if broker.port < 1 or broker.port > 65535:
            errors.append(
                SemanticError(f"Broker '{broker.name}': port must be 1-65535, got {broker.port}.")
            )
        if broker.transport and broker.transport not in VALID_TRANSPORTS:
            errors.append(
                SemanticError(
                    f"Broker '{broker.name}': transport must be one of "
                    f"{', '.join(sorted(VALID_TRANSPORTS))}, "
                    f"got '{broker.transport}'."
                )
            )
    return errors


def _check_source(pipeline, model=None):
    errors = []
    src = pipeline.source
    if src.pipe:
        return errors
    if src.qos is not None and src.qos not in VALID_QOS:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': source qos must be 0, 1, or 2, got {src.qos}."
            )
        )
    if not src.topic:
        errors.append(SemanticError(f"Pipeline '{pipeline.name}': source topic must not be empty."))
    return errors


def _check_sink_or_route(pipeline):
    errors = []
    if pipeline.sink:
        snk = pipeline.sink
        if snk.qos is not None and snk.qos not in VALID_QOS:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': sink qos must be 0, 1, or 2, got {snk.qos}."
                )
            )
        if not snk.topic:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': sink topic must not be empty.")
            )
    return errors


def _check_stage_order(pipeline):
    errors = []
    last_order = -2
    for stage in _unwrap_stages(pipeline):
        cls = stage.__class__.__name__
        if cls == "DebugStage":
            continue
        order = STAGE_ORDER.get(cls, -1)
        if order < last_order:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': stage '{cls}' "
                    f"appears after a later-phase stage. "
                    f"Required order: deduplicate → filter → rate → flatten → "
                    f"distinct → throttle → sample → group_by → "
                    f"window → aggregate → accumulate → having → map."
                )
            )
        last_order = max(last_order, order)
    return errors


def _check_aggregations(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "AggregateStage":
            continue
        for agg in stage.assignments:
            if agg.func in FUNCS_REQUIRING_FIELD and agg.field is None:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': aggregation function "
                        f"'{agg.func}' requires a field argument. "
                        f"Example: {agg.func}(msg.value)"
                    )
                )
            if agg.func in FUNCS_NO_FIELD and agg.field is not None:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': aggregation function "
                        f"'{agg.func}' takes no arguments. "
                        f"Use: {agg.func}()"
                    )
                )
            if (
                agg.func in FUNCS_REQUIRING_FIELD
                and agg.field is not None
                and agg.field.segments == ["msg"]
            ):
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': '{agg.func}(msg)' — "
                        f"'msg' alone is not a field. "
                        f"Use a nested path like msg.value.",
                        severity="warning",
                    )
                )
            if agg.func in FUNCS_REQUIRING_PARAM and agg.param is None:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': aggregation function "
                        f"'{agg.func}' requires a quantile parameter (0-100). "
                        f"Example: {agg.func}(msg.value, 95)"
                    )
                )
            if agg.func == "percentile" and agg.param is not None:
                quantile_val = _extract_literal_number(agg.param)
                if quantile_val is not None and not (0 <= quantile_val <= 100):
                    errors.append(
                        SemanticError(
                            f"Pipeline '{pipeline.name}': percentile quantile "
                            f"must be between 0 and 100, got {quantile_val}.",
                            severity="warning",
                        )
                    )
    return errors


def _extract_literal_number(expr_node):
    """Return the numeric value if expr_node is a simple integer or float literal, else None."""
    cls = expr_node.__class__.__name__
    if cls == "Expression":
        if len(expr_node.operands) == 1:
            return _extract_literal_number(expr_node.operands[0])
        return None
    if cls == "AndExpr":
        if len(expr_node.operands) == 1:
            return _extract_literal_number(expr_node.operands[0])
        return None
    if cls == "Comparison":
        if expr_node.op is None and expr_node.nullCheck is None and expr_node.inCheck is None:
            return _extract_literal_number(expr_node.left)
        return None
    if cls == "Addition":
        if expr_node.op is None:
            return _extract_literal_number(expr_node.left)
        return None
    if cls == "Multiplication":
        if expr_node.op is None:
            return _extract_literal_number(expr_node.left)
        return None
    if cls == "Coalesce":
        if expr_node.right is None:
            return _extract_literal_number(expr_node.left)
        return None
    if cls == "Unary":
        if expr_node.op is None:
            return _extract_literal_number(expr_node.atom)
        return None
    if cls == "IntLiteral":
        return expr_node.val
    if cls == "FloatLiteral":
        return expr_node.val
    return None


def _check_map_references(pipeline, const_names=None):
    errors = []
    agg_names = set()
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ == "AggregateStage":
            for agg in stage.assignments:
                agg_names.add(agg.name)
        elif stage.__class__.__name__ == "AccumulateStage":
            for acc in stage.assignments:
                agg_names.add(acc.name)

    pipeline_let_names = set()
    if pipeline.pipeline_lets:
        for plb in pipeline.pipeline_lets:
            pipeline_let_names.add(plb.name)

    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "MapStage":
            continue
        valid_names = set(agg_names)
        valid_names |= pipeline_let_names
        if const_names:
            valid_names |= const_names
        for binding in stage.bindings:
            _check_map_expr_refs(binding.value, valid_names, pipeline.name, errors)
            valid_names.add(binding.name)
        for mapping in stage.mappings:
            _check_map_expr_refs(mapping.value, valid_names, pipeline.name, errors)
    return errors


def _check_map_expr_refs(node, agg_names, pipeline_name, errors):
    cls = node.__class__.__name__
    if cls == "FieldRef":
        segments = node.segments
        if segments[0] != "msg" and len(segments) == 1 and segments[0] not in agg_names:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': map references "
                    f"undefined aggregation '{segments[0]}'. "
                    f"Available: {', '.join(sorted(agg_names)) or '(none)'}."
                )
            )
        return

    if cls in ("ForallExpr", "ExistsExpr"):
        inner_names = set(agg_names)
        inner_names.add(node.var)
        _check_map_expr_refs(node.predicate, inner_names, pipeline_name, errors)
        if node.collection:
            _check_map_expr_refs(node.collection, agg_names, pipeline_name, errors)
        return

    for child in _iter_expr_children(node):
        _check_map_expr_refs(child, agg_names, pipeline_name, errors)


def _check_duplicate_stages(pipeline):
    errors = []
    seen = set()
    for stage in _unwrap_stages(pipeline):
        cls = stage.__class__.__name__
        if cls in seen and cls not in ("WindowStage", "DebugStage", "EnrichStage"):
            # Map class name to DSL keyword for readable error messages
            stage_keyword = {
                "DeduplicateStage": "DEDUPLICATE",
                "FilterStage": "FILTER",
                "RateStage": "RATE",
                "FlattenStage": "FLATTEN",
                "DistinctStage": "DISTINCT",
                "ThrottleStage": "THROTTLE",
                "SampleStage": "SAMPLE",
                "GroupByStage": "GROUP_BY",
                "AggregateStage": "AGGREGATE",
                "AccumulateStage": "ACCUMULATE",
                "HavingStage": "HAVING",
                "MapStage": "MAP",
                "SelectStage": "SELECT",
                "PredictStage": "PREDICT",
            }.get(cls, cls)
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': duplicate '{stage_keyword}' "
                    f"stage. Each stage type can appear at most once."
                )
            )
        seen.add(cls)
    return errors


def _check_unused_aggregations(pipeline):
    errors = []
    agg_names = set()
    has_map = False
    has_having = False
    used_refs = set()

    has_accumulate = False
    for stage in _unwrap_stages(pipeline):
        cls = stage.__class__.__name__
        if cls == "AggregateStage":
            for agg in stage.assignments:
                agg_names.add(agg.name)
        elif cls == "AccumulateStage":
            has_accumulate = True
            for acc in stage.assignments:
                if len(acc.field.segments) == 1 and acc.field.segments[0] != "msg":
                    used_refs.add(acc.field.segments[0])
        elif cls == "HavingStage":
            has_having = True
            _collect_map_refs(stage.condition, used_refs)
        elif cls == "MapStage":
            has_map = True
            for binding in stage.bindings:
                _collect_map_refs(binding.value, used_refs)
            for mapping in stage.mappings:
                _collect_map_refs(mapping.value, used_refs)

    if not has_map and not has_having and not has_accumulate:
        return errors

    unused = agg_names - used_refs
    for name in sorted(unused):
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': aggregation '{name}' "
                f"is defined but never referenced in map or having.",
                severity="warning",
            )
        )
    return errors


def _collect_map_refs(node, refs):
    cls = node.__class__.__name__
    if cls == "FieldRef":
        segments = node.segments
        if segments[0] != "msg" and len(segments) == 1:
            refs.add(segments[0])
        return

    if cls in ("ForallExpr", "ExistsExpr"):
        quantifier_refs = set()
        _collect_map_refs(node.predicate, quantifier_refs)
        quantifier_refs.discard(node.var)
        refs.update(quantifier_refs)
        if node.collection:
            _collect_map_refs(node.collection, refs)
        return

    for child in _iter_expr_children(node):
        _collect_map_refs(child, refs)


def _check_window_constraints(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "WindowStage":
            continue
        if stage.size.value <= 0:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': window size must be positive.")
            )
        if stage.type == "sliding":
            if stage.step is None:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': sliding window requires "
                        f"a 'step' parameter. "
                        f"Example: sliding(10 m, step = 2 m)"
                    )
                )
            elif _duration_to_seconds(stage.step) > _duration_to_seconds(stage.size):
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': sliding window step must be <= window size."
                    )
                )
    return errors


def _check_multi_window(pipeline):
    errors = []
    windows = [s for s in _unwrap_stages(pipeline) if s.__class__.__name__ == "WindowStage"]
    if not windows:
        for stage in _unwrap_stages(pipeline):
            if stage.__class__.__name__ != "AggregateStage":
                continue
            for agg in stage.assignments:
                if agg.window_ref:
                    errors.append(
                        SemanticError(
                            f"Pipeline '{pipeline.name}': aggregation '{agg.name}' "
                            f"uses 'over' but no window is defined."
                        )
                    )
        return errors

    if len(windows) == 1:
        w = windows[0]
        window_name = w.wname if w.wname else None
        for stage in _unwrap_stages(pipeline):
            if stage.__class__.__name__ != "AggregateStage":
                continue
            for agg in stage.assignments:
                if agg.window_ref and not window_name:
                    errors.append(
                        SemanticError(
                            f"Pipeline '{pipeline.name}': aggregation '{agg.name}' "
                            f"uses 'over {agg.window_ref}' but the window has "
                            f"no name. Either name the window or remove 'over'."
                        )
                    )
                elif agg.window_ref and agg.window_ref != window_name:
                    errors.append(
                        SemanticError(
                            f"Pipeline '{pipeline.name}': aggregation '{agg.name}' "
                            f"references undefined window '{agg.window_ref}'. "
                            f"Available: {window_name}."
                        )
                    )
        return errors

    window_names = set()
    for w in windows:
        if not w.wname:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': when using multiple windows, "
                    f"all windows must have names. "
                    f"Example: window short tumbling(1 m)"
                )
            )
        elif w.wname in window_names:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': duplicate window name '{w.wname}'.")
            )
        else:
            window_names.add(w.wname)

    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "AggregateStage":
            continue
        for agg in stage.assignments:
            if not agg.window_ref:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': aggregation '{agg.name}' "
                        f"must specify 'over <window>' when multiple windows "
                        f"are defined. Available: "
                        f"{', '.join(sorted(window_names)) or '(none)'}."
                    )
                )
            elif agg.window_ref not in window_names:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': aggregation '{agg.name}' "
                        f"references undefined window '{agg.window_ref}'. "
                        f"Available: "
                        f"{', '.join(sorted(window_names)) or '(none)'}."
                    )
                )

    return errors


_EXPR_WALK_ATTRS = (
    "operands",
    "left",
    "right",
    "rights",
    "operand",
    "atom",
    "expr",
    "value",
    "condition",
    "thenExpr",
    "elseExpr",
    "args",
    "items",
    "arms",
    "result",
    "default",
    "field",
    "key",
    "predicate",
    "collection",
)

_EXTRA_BLOCK_ATTRS = (
    "stages",
    "assignments",
    "mappings",
    "bindings",
    "list",
)

_EXTRA_MATCH_ATTRS = ("subject", "pattern")


def _iter_expr_children(node, *, extra_attrs=()):
    for attr_name in _EXPR_WALK_ATTRS + extra_attrs:
        child = getattr(node, attr_name, None)
        if child is None:
            continue
        if isinstance(child, list):
            for item in child:
                if (
                    item is not None
                    and hasattr(item, "__class__")
                    and item.__class__.__module__ != "builtins"
                ):
                    yield item
        elif hasattr(child, "__class__") and child.__class__.__module__ != "builtins":
            yield child


UNIT_MULTIPLIERS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


def _duration_to_seconds(dur) -> float:
    return dur.value * UNIT_MULTIPLIERS.get(dur.unit, 1)


def _check_scalar_funcs(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        _walk_expr_for_funcs(stage, pipeline.name, errors)
    return errors


def _walk_expr_for_funcs(node, pipeline_name, errors):
    cls = node.__class__.__name__
    if cls == "FuncCall":
        nargs = len(node.args)
        bounds = SCALAR_FUNC_ARGS.get(node.name)
        if bounds:
            lo, hi = bounds
            if nargs < lo or nargs > hi:
                expect = f"exactly {lo}" if lo == hi else f"{lo}-{hi}"
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline_name}': function '{node.name}' "
                        f"expects {expect} argument(s), got {nargs}."
                    )
                )
    for child in _iter_expr_children(node, extra_attrs=_EXTRA_BLOCK_ATTRS):
        _walk_expr_for_funcs(child, pipeline_name, errors)


def _check_let_bindings(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "MapStage":
            continue
        agg_names = set()
        for s in _unwrap_stages(pipeline):
            if s.__class__.__name__ == "AggregateStage":
                for agg in s.assignments:
                    agg_names.add(agg.name)
            elif s.__class__.__name__ == "AccumulateStage":
                for acc in s.assignments:
                    agg_names.add(acc.name)

        seen_let = set()
        for binding in stage.bindings:
            if binding.name in agg_names:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': let-binding '{binding.name}' "
                        f"shadows aggregation name."
                    )
                )
            if binding.name in seen_let:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': duplicate let-binding '{binding.name}'."
                    )
                )
            seen_let.add(binding.name)
    return errors


def _check_stage_dependencies(pipeline):
    errors = []
    stage_names = {s.__class__.__name__ for s in _unwrap_stages(pipeline)}
    has_window = "WindowStage" in stage_names
    has_aggregate = "AggregateStage" in stage_names
    has_group_by = "GroupByStage" in stage_names
    has_having = "HavingStage" in stage_names
    has_accumulate = "AccumulateStage" in stage_names

    if has_aggregate and not has_window:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': aggregate stage requires a "
                f"window stage. Without a window, there is no batch of "
                f"messages to aggregate over."
            )
        )
    if has_window and not has_aggregate:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': window stage without aggregate "
                f"— messages are buffered but never aggregated.",
                severity="warning",
            )
        )
    if has_group_by and not has_window:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': group_by stage has no effect without a window stage.",
                severity="warning",
            )
        )
    if has_having and not has_aggregate:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': having stage requires an "
                f"aggregate stage. Having filters aggregation results."
            )
        )
    if has_accumulate and not has_aggregate:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': accumulate stage requires an "
                f"aggregate stage. Accumulate operates on aggregation results."
            )
        )
    return errors


def _check_duplicate_agg_names(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "AggregateStage":
            continue
        seen = set()
        for agg in stage.assignments:
            if agg.name in seen:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': duplicate aggregation name '{agg.name}'."
                    )
                )
            seen.add(agg.name)
    return errors


def _check_duplicate_map_targets(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "MapStage":
            continue
        seen = set()
        for mapping in stage.mappings:
            if mapping.target in seen:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': duplicate map target '{mapping.target}'."
                    )
                )
            seen.add(mapping.target)
    return errors


def _check_unique_const_names(model):
    errors = []
    seen = set()
    for c in model.constants:
        if c.name in seen:
            errors.append(
                SemanticError(f"Duplicate const name '{c.name}'. Const names must be unique.")
            )
        seen.add(c.name)
    return errors


def _check_having_references(pipeline, const_names=None):
    errors = []
    agg_names = set()
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ == "AggregateStage":
            for agg in stage.assignments:
                agg_names.add(agg.name)
        elif stage.__class__.__name__ == "AccumulateStage":
            for acc in stage.assignments:
                agg_names.add(acc.name)

    valid_names = set(agg_names)
    if pipeline.pipeline_lets:
        for plb in pipeline.pipeline_lets:
            valid_names.add(plb.name)
    if const_names:
        valid_names |= const_names

    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "HavingStage":
            continue
        _check_having_expr_refs(stage.condition, valid_names, pipeline.name, errors)
    return errors


def _check_having_expr_refs(node, agg_names, pipeline_name, errors):
    cls = node.__class__.__name__
    if cls == "FieldRef":
        segments = node.segments
        if segments[0] != "msg" and len(segments) == 1 and segments[0] not in agg_names:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': having references "
                    f"undefined aggregation '{segments[0]}'. "
                    f"Available: {', '.join(sorted(agg_names)) or '(none)'}."
                )
            )
        return

    if cls in ("ForallExpr", "ExistsExpr"):
        inner_names = set(agg_names)
        inner_names.add(node.var)
        _check_having_expr_refs(node.predicate, inner_names, pipeline_name, errors)
        if node.collection:
            _check_having_expr_refs(node.collection, agg_names, pipeline_name, errors)
        return

    for child in _iter_expr_children(node):
        _check_having_expr_refs(child, agg_names, pipeline_name, errors)


def _check_dedup_constraints(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "DeduplicateStage":
            continue
        if stage.key.segments[0] != "msg" or len(stage.key.segments) < 2:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': deduplicate key must be a "
                    f"message field (e.g. msg.event_id), got "
                    f"'{'.'.join(stage.key.segments)}'."
                )
            )
        if _duration_to_seconds(stage.ttl) <= 0:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': deduplicate ttl must be positive.")
            )
    return errors


def _check_case_exprs(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        _walk_expr_for_case(stage, pipeline.name, errors)
    return errors


def _walk_expr_for_case(node, pipeline_name, errors):
    cls = node.__class__.__name__
    if cls == "CaseExpr":
        if not node.arms:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': case expression must have "
                    f"at least one 'when' arm."
                )
            )
        if node.arms and not node.elseExpr:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': case expression without "
                    f"'else' arm may produce None at runtime.",
                    severity="warning",
                )
            )

    for child in _iter_expr_children(node, extra_attrs=_EXTRA_BLOCK_ATTRS):
        _walk_expr_for_case(child, pipeline_name, errors)


def _check_temporal_refs(pipeline, model):
    errors = []
    for stage in _unwrap_stages(pipeline):
        cls = stage.__class__.__name__
        if cls in ("FilterStage", "HavingStage"):
            continue
        if cls == "MapStage":
            for binding in stage.bindings:
                _walk_expr_for_temporal(binding.value, pipeline.name, cls, errors)
            for mapping in stage.mappings:
                _walk_expr_for_temporal(mapping.value, pipeline.name, cls, errors)
        elif cls == "AggregateStage":
            for agg in stage.assignments:
                if agg.field:
                    _walk_expr_for_temporal(agg.field, pipeline.name, cls, errors)
        else:
            _walk_expr_for_temporal(stage, pipeline.name, cls, errors)
    return errors


def _walk_expr_for_temporal(node, pipeline_name, stage_cls, errors):
    cls = node.__class__.__name__
    if cls in ("OldRef", "ChangedRef"):
        keyword = "old()" if cls == "OldRef" else "changed()"
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline_name}': {keyword} is only valid in filter or having stages."
            )
        )
        if cls == "OldRef" and (node.field.segments[0] != "msg" or len(node.field.segments) < 2):
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': old() requires a message "
                    f"field path (e.g. old(msg.status))."
                )
            )
        if cls == "ChangedRef" and (
            node.field.segments[0] != "msg" or len(node.field.segments) < 2
        ):
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': changed() requires a message "
                    f"field path (e.g. changed(msg.status))."
                )
            )
        return

    for child in _iter_expr_children(node):
        _walk_expr_for_temporal(child, pipeline_name, stage_cls, errors)


def _check_temporal_field_validity(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        cls = stage.__class__.__name__
        if cls not in ("FilterStage", "HavingStage"):
            continue
        _walk_expr_for_temporal_fields(stage, pipeline.name, errors)
    return errors


def _walk_expr_for_temporal_fields(node, pipeline_name, errors):
    cls = node.__class__.__name__
    if cls == "OldRef" and (node.field.segments[0] != "msg" or len(node.field.segments) < 2):
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline_name}': old() requires a message "
                f"field path (e.g. old(msg.status))."
            )
        )
    if cls == "ChangedRef" and (
        node.field.segments[0] != "msg" or len(node.field.segments) < 2
    ):
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline_name}': changed() requires a message "
                f"field path (e.g. changed(msg.status))."
            )
        )

    for child in _iter_expr_children(node):
        _walk_expr_for_temporal_fields(child, pipeline_name, errors)


def _check_env_refs(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        _walk_expr_for_env(stage, pipeline.name, errors)
    return errors


def _walk_expr_for_env(node, pipeline_name, errors):
    cls = node.__class__.__name__
    if cls == "EnvRef" and not node.key:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline_name}': env() requires a non-empty string key argument."
            )
        )

    for child in _iter_expr_children(node, extra_attrs=_EXTRA_BLOCK_ATTRS):
        _walk_expr_for_env(child, pipeline_name, errors)


def _check_quantifier_refs(pipeline):
    errors = []
    stages = _unwrap_stages(pipeline)
    stage_names = {s.__class__.__name__ for s in stages}
    has_window = "WindowStage" in stage_names

    for stage in stages:
        cls = stage.__class__.__name__
        if cls == "FilterStage":
            _walk_expr_for_quantifiers(stage.condition, pipeline.name, "filter", has_window, errors)
        elif cls == "HavingStage":
            _walk_expr_for_quantifiers(stage.condition, pipeline.name, "having", has_window, errors)
        elif cls == "MapStage":
            for binding in stage.bindings:
                _walk_expr_for_quantifiers(binding.value, pipeline.name, "map", has_window, errors)
            for mapping in stage.mappings:
                _walk_expr_for_quantifiers(mapping.value, pipeline.name, "map", has_window, errors)
    return errors


def _walk_expr_for_quantifiers(node, pipeline_name, stage_kind, has_window, errors):
    cls = node.__class__.__name__
    if cls in ("ForallExpr", "ExistsExpr"):
        keyword = "forall" if cls == "ForallExpr" else "exists"
        if not node.collection:
            if stage_kind != "having":
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline_name}': {keyword}() without "
                        f"'in' collection is only valid in having stage."
                    )
                )
            if not has_window:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline_name}': {keyword}() over window "
                        f"messages requires a window stage."
                    )
                )
        else:
            segs = node.collection.segments
            if segs[0] != "msg" or len(segs) < 2:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline_name}': {keyword}() collection "
                        f"must be a message field path (e.g. msg.items), "
                        f"got '{'.'.join(segs)}'."
                    )
                )

    for child in _iter_expr_children(node):
        _walk_expr_for_quantifiers(child, pipeline_name, stage_kind, has_window, errors)


def _check_object_literal_fields(pipeline):
    errors = []
    if pipeline.initially:
        _check_object_field_uniqueness(pipeline.initially.body, pipeline.name, "@initially", errors)
    if pipeline.finally_:
        _check_object_field_uniqueness(pipeline.finally_.body, pipeline.name, "@finally", errors)
    return errors


def _check_object_field_uniqueness(obj_literal, pipeline_name, hook_name, errors):
    seen = set()
    for obj_field in obj_literal.fields:
        if obj_field.name in seen:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': {hook_name} has duplicate "
                    f"field '{obj_field.name}' in object literal."
                )
            )
        seen.add(obj_field.name)


def _check_sink_batch(pipeline):
    errors = []
    if not pipeline.sink:
        return errors
    snk = pipeline.sink
    if snk.batch:
        batch = snk.batch
        if batch.size is not None and batch.size <= 0:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': sink batch size must be "
                    f"positive, got {batch.size}."
                )
            )
        if batch.timeout is not None and _duration_to_seconds(batch.timeout) <= 0:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': sink batch timeout must be positive.")
            )
        if batch.size is None and batch.timeout is None:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': sink batch requires at "
                    f"least one of 'size' or 'timeout'."
                )
            )
    if snk.format and snk.format not in VALID_FORMATS:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': sink format must be one of "
                f"{', '.join(sorted(VALID_FORMATS))}, got '{snk.format}'."
            )
        )
    return errors


def _check_stage_annotations(pipeline):
    errors = []
    for annotated in pipeline.stages:
        if annotated.__class__.__name__ != "AnnotatedStage":
            continue
        for ann in annotated.annotations:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': @{ann.severity} on "
                    f"{annotated.stage.__class__.__name__}: {ann.message}",
                    severity=("warning" if ann.severity == "WARN" else "error"),
                )
            )
    return errors


def _check_route_block(pipeline):
    errors = []
    if not pipeline.route:
        return errors
    route = pipeline.route
    if not route.arms:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': route block must have at least one 'when' arm."
            )
        )
    has_default = route.default_sink or route.default_topic
    if not has_default:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': route block requires a "
                f"'default' arm to handle unmatched messages.",
                severity="warning",
            )
        )
    for arm in route.arms:
        if arm.topic and not route.via_broker:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': route arm uses bare topic "
                    f"string but no 'via' broker is specified."
                )
            )
    if route.default_topic and not route.via_broker:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': route default uses bare topic "
                f"string but no 'via' broker is specified."
            )
        )
    return errors


def _check_rate(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "RateStage":
            continue
        if stage.limit <= 0:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': rate limit must be positive, got {stage.limit}."
                )
            )
        if _duration_to_seconds(stage.period) <= 0:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': rate period must be positive.")
            )
    return errors


def _check_accumulate(pipeline):
    errors = []
    agg_names = set()
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ == "AggregateStage":
            for agg in stage.assignments:
                agg_names.add(agg.name)

    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "AccumulateStage":
            continue
        seen = set()
        for acc in stage.assignments:
            if acc.name in seen:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': duplicate accumulate name '{acc.name}'."
                    )
                )
            seen.add(acc.name)

            if acc.name in agg_names:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': accumulate name "
                        f"'{acc.name}' conflicts with aggregation name."
                    )
                )

            field_name = acc.field.segments[0] if len(acc.field.segments) == 1 else None
            if field_name and field_name not in agg_names:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': accumulate references "
                        f"undefined aggregation '{field_name}'. "
                        f"Available: {', '.join(sorted(agg_names)) or '(none)'}."
                    )
                )

            if acc.func in ACCUM_FUNCS_REQUIRING_PARAM and acc.param is None:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': accumulate function "
                        f"'{acc.func}' requires a parameter. "
                        f"Example: {acc.func}(field, 0.3)"
                    )
                )
            if acc.func in ACCUM_FUNCS_NO_PARAM and acc.param is not None:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': accumulate function "
                        f"'{acc.func}' takes no extra parameter."
                    )
                )
    return errors


def _check_flatten(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "FlattenStage":
            continue
        segs = stage.field.segments
        if segs[0] != "msg" or len(segs) < 2:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': flatten field must be a "
                    f"message field path (e.g. msg.items), got "
                    f"'{'.'.join(segs)}'."
                )
            )
    return errors


def _check_distinct(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "DistinctStage":
            continue
        segs = stage.field.segments
        if segs[0] != "msg" or len(segs) < 2:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': distinct field must be a "
                    f"message field path (e.g. msg.id), got "
                    f"'{'.'.join(segs)}'."
                )
            )
        if _duration_to_seconds(stage.ttl) <= 0:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': distinct ttl must be positive.")
            )
    return errors


def _check_throttle(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "ThrottleStage":
            continue
        if stage.limit <= 0:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': throttle limit must be "
                    f"positive, got {stage.limit}."
                )
            )
        if _duration_to_seconds(stage.period) <= 0:
            errors.append(
                SemanticError(f"Pipeline '{pipeline.name}': throttle period must be positive.")
            )
        if stage.overflow == "BUFFER" and stage.buffer_size and stage.buffer_size <= 0:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': throttle buffer size must be "
                    f"positive, got {stage.buffer_size}."
                )
            )
    return errors


def _check_sample(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "SampleStage":
            continue
        if stage.percentage < 1 or stage.percentage > 100:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': sample percentage must be "
                    f"between 1 and 100, got {stage.percentage}."
                )
            )
    return errors


def _check_pipeline_chaining(pipeline, model):
    errors = []
    src = pipeline.source
    if not src.pipe:
        return errors
    pipeline_names = {p.name for p in model.pipelines}
    if src.pipe not in pipeline_names:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': pipe source references "
                f"undefined pipeline '{src.pipe}'. "
                f"Available: {', '.join(sorted(pipeline_names)) or '(none)'}."
            )
        )
    if src.pipe == pipeline.name:
        errors.append(
            SemanticError(f"Pipeline '{pipeline.name}': pipe source cannot reference itself.")
        )
    return errors


def _check_join(pipeline):
    errors = []
    join = pipeline.join
    if not join:
        return errors

    if pipeline.source.pipe:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': join cannot be used with a "
                f"pipe source. Join requires a broker source."
            )
        )

    left_segs = join.left_key.segments
    if left_segs[0] != "msg" or len(left_segs) < 2:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': join left key must be a "
                f"message field path (e.g. msg.customer_id), "
                f"got '{'.'.join(left_segs)}'."
            )
        )

    right_segs = join.right_key.segments
    if right_segs[0] != "other" or len(right_segs) < 2:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': join right key must be a "
                f"field from the joined stream (e.g. other.id), "
                f"got '{'.'.join(right_segs)}'."
            )
        )

    if _duration_to_seconds(join.window) <= 0:
        errors.append(
            SemanticError(f"Pipeline '{pipeline.name}': join window duration must be positive.")
        )

    return errors


def _check_on_error(pipeline):
    errors = []
    if not pipeline.on_error:
        return errors
    oe = pipeline.on_error
    if oe.broker and not oe.topic:
        errors.append(
            SemanticError(f"Pipeline '{pipeline.name}': on_error sink topic must not be empty.")
        )
    if not oe.broker and oe.topic:
        errors.append(
            SemanticError(f"Pipeline '{pipeline.name}': on_error has topic but no broker.")
        )
    return errors


def _check_retry(pipeline):
    errors = []
    if not pipeline.retry:
        return errors
    retry = pipeline.retry
    if retry.max_retries < 1:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': retry max_retries must be at "
                f"least 1, got {retry.max_retries}."
            )
        )
    if _duration_to_seconds(retry.base) <= 0:
        errors.append(
            SemanticError(f"Pipeline '{pipeline.name}': retry backoff duration must be positive.")
        )
    return errors


# ─── Pipeline-level let-bindings ────────────────────────────────────


def _check_pipeline_let_bindings(pipeline, const_names=None):
    errors = []
    if not pipeline.pipeline_lets:
        return errors

    seen = set()
    for lb in pipeline.pipeline_lets:
        if lb.name in seen:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': duplicate pipeline let-binding '{lb.name}'."
                )
            )
        if const_names and lb.name in const_names:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': pipeline let-binding "
                    f"'{lb.name}' shadows a constant."
                )
            )
        seen.add(lb.name)
    return errors


# ─── User-defined functions ─────────────────────────────────────────

_BUILTIN_FUNC_NAMES = {"abs", "round", "len", "lower", "upper", "floor", "ceil", "fmt"}


def _check_func_defs(model):
    errors = []
    if not model.functions:
        return errors

    seen = set()
    for func in model.functions:
        if func.name in seen:
            errors.append(
                SemanticError(
                    f"Duplicate function name '{func.name}'. Function names must be unique."
                )
            )
        if func.name in _BUILTIN_FUNC_NAMES:
            errors.append(SemanticError(f"Function '{func.name}' shadows a built-in function."))
        seen.add(func.name)

        param_seen = set()
        for param in func.params or []:
            if param in param_seen:
                errors.append(
                    SemanticError(f"Function '{func.name}': duplicate parameter '{param}'.")
                )
            param_seen.add(param)
    return errors


def _check_user_func_calls(pipeline, func_names):
    errors = []
    for stage in _unwrap_stages(pipeline):
        _walk_expr_for_user_func_calls(stage, pipeline.name, func_names, errors)
    if pipeline.pipeline_lets:
        for lb in pipeline.pipeline_lets:
            _walk_expr_for_user_func_calls(lb.value, pipeline.name, func_names, errors)
    return errors


def _walk_expr_for_user_func_calls(node, pipeline_name, func_names, errors):
    cls = node.__class__.__name__
    if cls == "UserFuncCall" and node.name not in func_names:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline_name}': call to undefined function '{node.name}'."
            )
        )

    for child in _iter_expr_children(node, extra_attrs=_EXTRA_BLOCK_ATTRS + _EXTRA_MATCH_ATTRS):
        _walk_expr_for_user_func_calls(child, pipeline_name, func_names, errors)


# ─── Match expression validation ────────────────────────────────────


def _check_match_exprs(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        _walk_expr_for_match(stage, pipeline.name, errors)
    if pipeline.pipeline_lets:
        for lb in pipeline.pipeline_lets:
            _walk_expr_for_match(lb.value, pipeline.name, errors)
    return errors


def _walk_expr_for_match(node, pipeline_name, errors):
    cls = node.__class__.__name__
    if cls == "MatchExpr":
        if not node.arms:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': match expression must have "
                    f"at least one 'case' arm."
                )
            )
        if node.arms and not node.elseExpr:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline_name}': match expression without "
                    f"'else' arm may produce None at runtime.",
                    severity="warning",
                )
            )

    for child in _iter_expr_children(node, extra_attrs=_EXTRA_BLOCK_ATTRS + _EXTRA_MATCH_ATTRS):
        _walk_expr_for_match(child, pipeline_name, errors)


# ─── Schema validation ──────────────────────────────────────────────


def _check_unique_schema_names(model):
    errors = []
    seen = set()
    for s in model.schemas:
        if s.name in seen:
            errors.append(
                SemanticError(f"Duplicate schema name '{s.name}'. Schema names must be unique.")
            )
        seen.add(s.name)
    return errors


def _check_schema_fields(model):
    errors = []
    for s in model.schemas:
        if not s.fields:
            errors.append(SemanticError(f"Schema '{s.name}' must have at least one field."))
        _check_schema_field_uniqueness(s.fields, s.name, [], errors)
    return errors


def _check_schema_field_uniqueness(fields, schema_name, path, errors):
    seen = set()
    for f in fields:
        if f.name in seen:
            location = ".".join([*path, f.name]) if path else f.name
            errors.append(
                SemanticError(f"Schema '{schema_name}': duplicate field name '{location}'.")
            )
        seen.add(f.name)
        if f.type.__class__.__name__ == "SchemaObjectType":
            _check_schema_field_uniqueness(f.type.fields, schema_name, [*path, f.name], errors)


def _resolve_schema_field(schema, field_segments):
    """Walk schema fields to validate a msg.x.y.z path.
    Returns (found: bool, field_type: str | None, is_optional: bool).
    field_segments should NOT include 'msg' prefix.
    """
    current_fields = schema.fields
    for i, seg in enumerate(field_segments):
        match = None
        for f in current_fields:
            if f.name == seg:
                match = f
                break
        if match is None:
            return (False, None, False)
        if i == len(field_segments) - 1:
            type_name = _schema_type_name(match.type)
            return (True, type_name, bool(match.optional))
        if match.type.__class__.__name__ == "SchemaObjectType":
            current_fields = match.type.fields
        else:
            return (False, None, False)
    return (False, None, False)


def _schema_type_name(schema_type):
    type_names = {
        "SchemaScalarType": schema_type.name,
        "SchemaObjectType": "object",
        "SchemaListType": "list",
    }
    return type_names.get(schema_type.__class__.__name__, "unknown")


def _check_schema_references(pipeline, model):
    errors = []
    src = pipeline.source
    if src.pipe or not src.schema:
        return errors

    schema = src.schema
    is_strict = schema.strictness != "LENIENT"

    for stage in _unwrap_stages(pipeline):
        _walk_expr_for_schema(stage, pipeline.name, schema, is_strict, errors)

    return errors


def _walk_expr_for_schema(node, pipeline_name, schema, is_strict, errors):
    cls = node.__class__.__name__

    if cls == "AggAssignment":
        if node.field and node.func in AGG_FUNCS_NUMERIC:
            segs = node.field.segments
            if segs[0] == "msg" and len(segs) >= 2:
                found, ftype, _ = _resolve_schema_field(schema, segs[1:])
                if found and ftype in STRING_TYPES:
                    errors.append(
                        SemanticError(
                            f"Pipeline '{pipeline_name}': {node.func}() expects "
                            f"numeric field, but '{'.'.join(segs)}' is '{ftype}'.",
                            severity="warning",
                        )
                    )
        if node.field:
            _walk_expr_for_schema(node.field, pipeline_name, schema, is_strict, errors)
        return

    if cls == "FuncCall":
        if node.name in SCALAR_FUNCS_STRING and node.args:
            arg = node.args[0]
            if arg.__class__.__name__ == "FieldRef":
                segs = arg.segments
                if segs[0] == "msg" and len(segs) >= 2:
                    found, ftype, _ = _resolve_schema_field(schema, segs[1:])
                    if found and ftype in NUMERIC_TYPES:
                        errors.append(
                            SemanticError(
                                f"Pipeline '{pipeline_name}': {node.name}() expects "
                                f"string field, but '{'.'.join(segs)}' is '{ftype}'.",
                                severity="warning",
                            )
                        )
        if node.name in SCALAR_FUNCS_NUMERIC and node.args:
            arg = node.args[0]
            if arg.__class__.__name__ == "FieldRef":
                segs = arg.segments
                if segs[0] == "msg" and len(segs) >= 2:
                    found, ftype, _ = _resolve_schema_field(schema, segs[1:])
                    if found and ftype in STRING_TYPES:
                        errors.append(
                            SemanticError(
                                f"Pipeline '{pipeline_name}': {node.name}() expects "
                                f"numeric field, but '{'.'.join(segs)}' is '{ftype}'.",
                                severity="warning",
                            )
                        )

    if cls == "FieldRef":
        segs = node.segments
        if segs[0] == "msg" and len(segs) >= 2:
            found, ftype, _ = _resolve_schema_field(schema, segs[1:])
            if not found:
                severity = "error" if is_strict else "warning"
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline_name}': field '{'.'.join(segs)}' "
                        f"not found in schema '{schema.name}'.",
                        severity=severity,
                    )
                )
        return

    for child in _iter_expr_children(node, extra_attrs=_EXTRA_BLOCK_ATTRS):
        _walk_expr_for_schema(child, pipeline_name, schema, is_strict, errors)


# ─── ML model validation ────────────────────────────────────────────


def _check_models(model):
    errors = []
    seen = set()
    for m in model.models:
        if m.name in seen:
            errors.append(
                SemanticError(f"Duplicate model name '{m.name}'. Model names must be unique.")
            )
        seen.add(m.name)

        if m.returns:
            output_seen = set()
            for o in m.returns:
                if o in output_seen:
                    errors.append(SemanticError(f"Model '{m.name}': duplicate output name '{o}'."))
                output_seen.add(o)

        if m.runtime.lower() != "custom":
            if not m.features:
                errors.append(
                    SemanticError(
                        f"Model '{m.name}': runtime '{m.runtime}' requires "
                        f"'features' specification.",
                        severity="warning",
                    )
                )
            if not m.returns:
                errors.append(
                    SemanticError(
                        f"Model '{m.name}': runtime '{m.runtime}' requires "
                        f"'returns' specification.",
                        severity="warning",
                    )
                )
    return errors


def _check_predict_stages(pipeline, model):
    errors = []
    model_names = {m.name for m in model.models}
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "PredictStage":
            continue
        if stage.model is None:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': predict stage references unknown model."
                )
            )
            continue
        if stage.model.name not in model_names:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': predict references model "
                    f"'{stage.model.name}' which is not defined."
                )
            )
    return errors


def _check_select_stage(pipeline):
    errors = []
    stage_names = {s.__class__.__name__ for s in _unwrap_stages(pipeline)}
    has_map = "MapStage" in stage_names
    has_select = "SelectStage" in stage_names
    if has_map and has_select:
        errors.append(
            SemanticError(
                f"Pipeline '{pipeline.name}': cannot use both 'map' and 'select' "
                f"stages in the same pipeline."
            )
        )
    return errors


def _check_debug_stage(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "DebugStage":
            continue
        for field_ref in stage.fields:
            if field_ref.subscript:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': debug field ref "
                        f"'{'.'.join(field_ref.segments)}[...]' uses a subscript "
                        f"which is not usable as a stable key.",
                        severity="warning",
                    )
                )
    return errors


def _check_subscript_keys(pipeline):
    errors = []
    for stage in _unwrap_stages(pipeline):
        cls = stage.__class__.__name__
        if cls == "DeduplicateStage":
            if stage.key.subscript:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': deduplicate key "
                        f"'{'.'.join(stage.key.segments)}[...]' uses a subscript "
                        f"which is not valid as a deduplication key.",
                        severity="warning",
                    )
                )
        elif cls == "DistinctStage":
            if stage.field.subscript:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': distinct field "
                        f"'{'.'.join(stage.field.segments)}[...]' uses a subscript "
                        f"which is not valid as a distinct key.",
                        severity="warning",
                    )
                )
        elif cls == "GroupByStage":
            for ref in stage.fields:
                if ref.subscript:
                    errors.append(
                        SemanticError(
                            f"Pipeline '{pipeline.name}': group_by field "
                            f"'{'.'.join(ref.segments)}[...]' uses a subscript "
                            f"which is not valid as a grouping key.",
                            severity="warning",
                        )
                    )
    return errors


# ─── ValidateStage validation ────────────────────────────────────────


def _check_validate_stage(pipeline, model):
    errors = []
    schema_names = {s.name for s in model.schemas}
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "ValidateStage":
            continue
        if stage.schema is None:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': validate stage references unknown schema."
                )
            )
            continue
        if stage.schema.name not in schema_names:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': validate references schema "
                    f"'{stage.schema.name}' which is not defined."
                )
            )
        if stage.validate_err is not None:
            ve = stage.validate_err
            if ve.broker is not None and not ve.topic:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': validate on_error sink "
                        f"topic must not be empty."
                    )
                )
    return errors


# ─── EnrichStage validation ──────────────────────────────────────────


def _check_enrich_stage(pipeline, model):
    errors = []
    for stage in _unwrap_stages(pipeline):
        if stage.__class__.__name__ != "EnrichStage":
            continue
        if not stage.bind:
            errors.append(
                SemanticError(
                    f"Pipeline '{pipeline.name}': enrich stage requires a 'bind' identifier."
                )
            )
        if stage.enrich_err is not None:
            ee = stage.enrich_err
            if ee.broker is not None and not ee.topic:
                errors.append(
                    SemanticError(
                        f"Pipeline '{pipeline.name}': enrich on_error sink topic must not be empty."
                    )
                )
    return errors
