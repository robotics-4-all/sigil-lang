import pytest

from sigil.language import parse_string
from sigil.semantics import SemanticValidationError, validate, validate_or_raise


def _model(src):
    return parse_string(src)


def _errors(src):
    return validate(_model(src))


VALID = """
BROKER b HOST: "h" PORT: 1883 ;
PIPELINE p
    SOURCE FROM b :: "in" QOS: 1
    |> FILTER msg.v > 0
    |> WINDOW tumbling(5 m)
    |> AGGREGATE { a = avg(msg.v), n = count() }
    |> MAP { result = a, total = n }
    |> SINK TO b :: "out" QOS: 1
;
"""


def test_valid_model_has_no_errors():
    assert _errors(VALID) == []


def test_duplicate_broker_name():
    src = """
    BROKER b HOST: "a" PORT: 1 ;
    BROKER b HOST: "b" PORT: 2 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> SINK TO b :: "y"
    ;
    """
    with pytest.raises(Exception, match="not unique"):
        _model(src)


def test_duplicate_pipeline_name():
    src = """
    BROKER b HOST: "a" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> SINK TO b :: "y"
    ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("Duplicate pipeline" in str(e) for e in errs)


def test_invalid_qos():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x" QOS: 3
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("qos" in str(e).lower() for e in errs)


def test_invalid_port():
    src = """
    BROKER b HOST: "h" PORT: 99999 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("port" in str(e).lower() for e in errs)


def test_stage_order_violation():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { n = count() }
        |> FILTER msg.v > 0
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("order" in str(e).lower() for e in errs)


def test_aggregate_func_missing_field():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { a = avg() }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("requires a field" in str(e) for e in errs)


def test_count_with_field_is_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { n = count(msg.v) }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("count" in str(e) and "no arguments" in str(e) for e in errs)


def test_map_references_undefined_aggregation():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { a = avg(msg.v) }
        |> MAP { out = nonexistent }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e) for e in errs)


def test_sliding_window_requires_step():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> WINDOW sliding(10 m)
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("step" in str(e) for e in errs)


def test_sliding_window_step_too_large():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> WINDOW sliding(5 m, STEP = 10 m)
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("step" in str(e).lower() and "size" in str(e).lower() for e in errs)


def test_validate_or_raise_on_errors():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x" QOS: 5
        |> SINK TO b :: "y"
    ;
    """
    with pytest.raises(SemanticValidationError):
        validate_or_raise(_model(src))


def test_validate_or_raise_passes_clean():
    validate_or_raise(_model(VALID))


def test_duplicate_filter_stage():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> FILTER msg.v > 0
        |> FILTER msg.v < 100
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "filter" in str(e).lower() for e in errs)


def test_duplicate_window_stage():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> WINDOW tumbling(5 m)
        |> WINDOW tumbling(10 m)
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("must have names" in str(e) for e in errs)


def test_no_duplicate_error_on_distinct_stages():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> FILTER msg.v > 0
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { n = count() }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert not any("duplicate" in str(e).lower() for e in errs)


def test_unused_aggregation_warning():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { a = avg(msg.v), b_val = sum(msg.v), n = count() }
        |> MAP { result = a }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert len(warnings) == 2
    warning_text = " ".join(str(w) for w in warnings)
    assert "b_val" in warning_text
    assert "n" in warning_text


def test_no_unused_warning_without_map():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { a = avg(msg.v), n = count() }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert len(warnings) == 0


def test_no_unused_warning_when_all_referenced():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { a = avg(msg.v), n = count() }
        |> MAP { average = a, total = n }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert len(warnings) == 0


def test_millisecond_window_validation():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> WINDOW sliding(500 ms, STEP = 100 ms)
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert not any("step" in str(e).lower() and "size" in str(e).lower() for e in errs)


def test_millisecond_step_exceeds_size():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> WINDOW sliding(100 ms, STEP = 500 ms)
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("step" in str(e).lower() and "size" in str(e).lower() for e in errs)


def test_scalar_func_abs_too_many_args():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> FILTER abs(msg.x, msg.y) > 10
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("expects" in str(e) and "argument" in str(e) for e in errs)


def test_scalar_func_round_too_many_args():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP { r = round(v, 2, 1) }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("expects" in str(e) and "argument" in str(e) for e in errs)


def test_duplicate_let_binding_names():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP {
            LET a = v * 2,
            LET a = v * 3,
            result = a
        }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "let-binding" in str(e).lower() for e in errs)


def test_let_binding_shadows_aggregation():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP {
            LET v = 42,
            result = v
        }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("shadows" in str(e) for e in errs)


def test_let_binding_references_undefined():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP {
            LET a = nonexistent,
            result = a
        }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e) for e in errs)


def test_valid_let_bindings_no_errors():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "x"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP {
            LET ratio = v * 100,
            LET doubled = ratio * 2,
            result = doubled
        }
        |> SINK TO b :: "y"
    ;
    """
    errs = _errors(src)
    assert errs == []


def test_duplicate_aggregate_names():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { a = avg(msg.x), a = sum(msg.y) }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "aggregation" in str(e).lower() for e in errs)


def test_duplicate_map_targets():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { x = avg(msg.v), y = sum(msg.v) }
        |> MAP { result = x, result = y }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "map target" in str(e).lower() for e in errs)


def test_empty_source_topic():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: ""
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("topic" in str(e).lower() and "empty" in str(e).lower() for e in errs)


def test_empty_sink_topic():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: ""
    ;
    """
    errs = _errors(src)
    assert any("topic" in str(e).lower() and "empty" in str(e).lower() for e in errs)


def test_aggregate_without_window():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> AGGREGATE { a = avg(msg.v) }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("aggregate" in str(e).lower() and "window" in str(e).lower() for e in errs)


def test_sink_broker_invalid_port():
    src = """
    BROKER src_broker HOST: "h" PORT: 1883 ;
    BROKER sink_broker HOST: "h" PORT: 99999 ;
    PIPELINE p
        SOURCE FROM src_broker :: "in"
        |> SINK TO sink_broker :: "out"
    ;
    """
    errs = _errors(src)
    assert any("port" in str(e).lower() for e in errs)


def test_window_without_aggregate_warning():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(5 m)
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    hard = [e for e in errs if e.severity == "error"]
    assert len(hard) == 0
    assert any("window" in str(w).lower() and "aggregate" in str(w).lower() for w in warnings)


def test_group_by_without_window_warning():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> GROUP_BY msg.sensor_id
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    hard = [e for e in errs if e.severity == "error"]
    assert len(hard) == 0
    assert any("group_by" in str(w).lower() and "window" in str(w).lower() for w in warnings)


def test_aggregate_bare_msg_warning():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { a = avg(msg) }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("msg" in str(w).lower() and "field" in str(w).lower() for w in warnings)


def test_map_undefined_ref_inside_if_then_else():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { a = avg(msg.v) }
        |> MAP { result = if undefined_var > 0 then 1 else 0 }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e).lower() for e in errs)


def test_complete_valid_pipeline_no_errors():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in" QOS: 1
        |> FILTER msg.v > 0
        |> GROUP_BY msg.sensor_id
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { a = avg(msg.v), n = count() }
        |> MAP { result = a, total = n }
        |> SINK TO b :: "out" QOS: 1
    ;
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: const definitions ---


def test_duplicate_const_name():
    src = """
    CONST X = 1
    CONST X = 2
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("Duplicate const" in str(e) for e in errs)


def test_const_in_filter_expression():
    src = """
    CONST THRESHOLD = 42
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER msg.v > THRESHOLD
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


def test_const_in_map_expression():
    src = """
    CONST FACTOR = 1.5
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP { result = v * FACTOR }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: env() references ---


def test_env_ref_empty_key():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER msg.x > env("")
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("non-empty" in str(e) for e in errs)


# --- Phase 2: case expressions ---


def test_case_no_else_warning():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP { out = case { when v > 0 => "pos" }}
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("case" in str(w).lower() and "none" in str(w).lower() for w in warnings)


def test_case_valid_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP { out = case { when v > 0 => "pos" else => "neg" }}
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    case_warnings = [e for e in errs if e.severity == "warning" and "case" in str(e).lower()]
    assert len(case_warnings) == 0


# --- Phase 2: having stage ---


def test_having_without_aggregate_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(5 m)
        |> HAVING msg.v > 10
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("having" in str(e).lower() and "aggregate" in str(e).lower() for e in errs)


def test_having_undefined_ref():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { n = count() }
        |> HAVING nonexistent > 10
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e) for e in errs)


def test_having_after_map_order_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { n = count() }
        |> MAP { result = n }
        |> HAVING n > 10
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("order" in str(e).lower() for e in errs)


def test_having_valid_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { n = count(), a = avg(msg.v) }
        |> HAVING n > 5
        |> MAP { result = a }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: deduplicate stage ---


def test_dedup_key_not_msg_field():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> DEDUPLICATE event_id WITHIN 30 s
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("msg" in str(e).lower() and "field" in str(e).lower() for e in errs)


def test_dedup_after_filter_order_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER msg.v > 0
        |> DEDUPLICATE msg.event_id WITHIN 30 s
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("order" in str(e).lower() for e in errs)


def test_dedup_valid_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> DEDUPLICATE msg.event_id WITHIN 30 s
        |> FILTER msg.v > 0
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: old() / changed() temporal references ---


def test_old_in_map_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP { result = old(msg.x) }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("old()" in str(e) and "only valid" in str(e) for e in errs)


def test_changed_in_map_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP { result = if changed(msg.x) then v else 0 }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("changed()" in str(e) and "only valid" in str(e) for e in errs)


def test_old_in_filter_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER old(msg.status) != msg.status
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    temporal_errs = [e for e in errs if "old()" in str(e) and "only valid" in str(e)]
    assert temporal_errs == []


def test_changed_in_filter_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER changed(msg.value)
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    temporal_errs = [e for e in errs if "changed()" in str(e) and "only valid" in str(e)]
    assert temporal_errs == []


def test_old_ref_bare_msg_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER old(msg) != msg
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("old()" in str(e) and "field path" in str(e) for e in errs)


def test_temporal_in_filter_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER old(msg.status) != msg.status and changed(msg.value)
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    temporal_errs = [e for e in errs if "old()" in str(e) or "changed()" in str(e)]
    assert temporal_errs == []


# --- Phase 3: forall/exists quantifier validation ---


def test_forall_without_collection_not_in_having():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER forall(m => m > 0)
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("forall()" in str(e) and "only valid in having" in str(e) for e in errs)


def test_exists_without_collection_not_in_having():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER exists(m => m > 0)
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("exists()" in str(e) and "only valid in having" in str(e) for e in errs)


def test_forall_without_window():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> AGGREGATE { n = count() }
        |> HAVING forall(m => m > 0)
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("forall()" in str(e) and "window" in str(e) for e in errs)


def test_quantifier_invalid_collection():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER forall(x in tags => x > 0)
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("forall()" in str(e) and "msg" in str(e) for e in errs)


def test_exists_with_valid_collection_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER exists(x in msg.tags => x == "critical")
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    quantifier_errs = [e for e in errs if "exists()" in str(e) or "forall()" in str(e)]
    assert quantifier_errs == []


def test_forall_in_having_with_window_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { n = count() }
        |> HAVING forall(m => m > 0)
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    quantifier_errs = [e for e in errs if "forall()" in str(e)]
    assert quantifier_errs == []


def test_quantifier_var_not_flagged_as_undefined_agg():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { n = count(), avg_val = avg(msg.value) }
        |> HAVING forall(m => m > 0) and n >= 5
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    ref_errs = [e for e in errs if "undefined aggregation" in str(e)]
    assert ref_errs == []


# --- Phase 3: @initially/@finally validation ---


def test_initially_duplicate_fields():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        @INITIALLY { PUBLISH { status: "on", status: "off" } }
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "@initially" in str(e) for e in errs)


def test_finally_duplicate_fields():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
        @FINALLY { PUBLISH { x: 1, x: 2 } }
    ;
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "@finally" in str(e) for e in errs)


def test_hooks_valid_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        @INITIALLY { PUBLISH { status: "online" } }
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
        @FINALLY { PUBLISH { status: "offline" } }
    ;
    """
    errs = _errors(src)
    hook_errs = [e for e in errs if "@initially" in str(e) or "@finally" in str(e)]
    assert hook_errs == []


# --- Phase 3: sink batch validation ---


def test_sink_batch_size_zero():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" BATCH: { SIZE: 0 }
    ;
    """
    errs = _errors(src)
    assert any("batch size" in str(e) and "positive" in str(e) for e in errs)


def test_sink_batch_negative_size():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" BATCH: { SIZE: -1 }
    ;
    """
    errs = _errors(src)
    assert any("batch size" in str(e) and "positive" in str(e) for e in errs)


def test_sink_batch_valid_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" BATCH: { SIZE: 100  TIMEOUT: 5s }
    ;
    """
    errs = _errors(src)
    batch_errs = [e for e in errs if "batch" in str(e).lower()]
    assert batch_errs == []


def test_sink_format_valid_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" FORMAT: csv
    ;
    """
    errs = _errors(src)
    format_errs = [e for e in errs if "format" in str(e).lower()]
    assert format_errs == []


# --- Phase 4: import validation ---


def test_import_declaration_no_semantic_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    import_errs = [e for e in errs if "import" in str(e).lower()]
    assert import_errs == []


# --- Phase 4: pipeline chaining validation ---


def test_pipe_source_undefined_pipeline():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE downstream
        SOURCE FROM PIPE nonexistent
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("undefined pipeline" in str(e) and "nonexistent" in str(e) for e in errs)


def test_pipe_source_self_reference():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE self_ref
        SOURCE FROM PIPE self_ref
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("cannot reference itself" in str(e) for e in errs)


def test_pipe_source_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE upstream
        SOURCE FROM b :: "in"
        |> SINK TO b :: "mid"
    ;
    PIPELINE downstream
        SOURCE FROM PIPE upstream
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    pipe_errs = [e for e in errs if "pipe" in str(e).lower() and e.severity == "error"]
    assert pipe_errs == []


def test_pipe_source_no_sink_validation():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE upstream
        SOURCE FROM b :: "in"
        |> SINK TO b :: "mid"
    ;
    PIPELINE downstream
        SOURCE FROM PIPE upstream
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    qos_errs = [e for e in errs if "qos" in str(e).lower() and "downstream" in str(e)]
    topic_errs = [
        e
        for e in errs
        if "topic" in str(e).lower() and "source" in str(e).lower() and "downstream" in str(e)
    ]
    assert qos_errs == []
    assert topic_errs == []


# --- Phase 4: route validation ---


def test_route_no_default_warning():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> ROUTE
            when msg.x > 0 => SINK TO b :: "pos"
        ;
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("route" in str(w).lower() and "default" in str(w).lower() for w in warnings)


def test_route_valid_no_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> ROUTE
            when msg.x > 0 => SINK TO b :: "pos"
            DEFAULT => SINK TO b :: "other"
        ;
    ;
    """
    errs = _errors(src)
    route_errs = [e for e in errs if "route" in str(e).lower() and e.severity == "error"]
    assert route_errs == []


def test_route_pipeline_no_sink_batch_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> ROUTE
            when msg.x > 0 => SINK TO b :: "pos"
            DEFAULT => SINK TO b :: "other"
        ;
    ;
    """
    errs = _errors(src)
    batch_errs = [e for e in errs if "batch" in str(e).lower()]
    assert batch_errs == []


# --- Phase 4: stage annotation validation ---


def test_warn_annotation_emits_warning():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> @WARN("slow stage") FILTER msg.x > 0
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("@warn" in str(w).lower() and "slow stage" in str(w) for w in warnings)


def test_error_annotation_emits_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> @ERROR("deprecated") FILTER msg.x > 0
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert any("@error" in str(e).lower() and "deprecated" in str(e) for e in hard)


def test_annotation_message_in_error():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> @WARN("custom message here") FILTER msg.x > 0
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("custom message here" in str(e) for e in errs)


def test_no_annotation_no_extra_errors():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER msg.x > 0
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    annotation_errs = [e for e in errs if "@warn" in str(e) or "@error" in str(e)]
    assert annotation_errs == []


def test_rate_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> RATE 100 PER 1s
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_rate_with_on_excess_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> RATE 50 PER 5s ON_EXCESS: WARN
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_rate_stage_order_before_group_by():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> GROUP_BY msg.k
        |> RATE 100 PER 1s
        |> WINDOW tumbling(5s)
        |> AGGREGATE { n = count() }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("appears after a later-phase stage" in str(e) for e in errs)


def test_accumulate_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { avg_val = avg(msg.v) }
        |> ACCUMULATE {
            smooth = ema(avg_val, 0.3)
        }
        |> MAP { result = smooth }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_accumulate_requires_aggregate():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> ACCUMULATE {
            smooth = ema(avg_val, 0.3)
        }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("accumulate stage requires an aggregate stage" in str(e) for e in errs)


def test_accumulate_undefined_field():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { total = sum(msg.v) }
        |> ACCUMULATE {
            smooth = ema(nonexistent, 0.3)
        }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("undefined aggregation 'nonexistent'" in str(e) for e in errs)


def test_accumulate_ema_requires_param():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { avg_val = avg(msg.v) }
        |> ACCUMULATE {
            smooth = ema(avg_val)
        }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("requires a parameter" in str(e) for e in errs)


def test_accumulate_cumsum_no_extra_param():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { total = sum(msg.v) }
        |> ACCUMULATE {
            running = cumsum(total, 0.5)
        }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("takes no extra parameter" in str(e) for e in errs)


def test_accumulate_duplicate_name():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { total = sum(msg.v) }
        |> ACCUMULATE {
            x = cumsum(total),
            x = delta(total)
        }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("duplicate accumulate name 'x'" in str(e) for e in errs)


def test_accumulate_name_conflicts_with_agg():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { total = sum(msg.v) }
        |> ACCUMULATE {
            total = cumsum(total)
        }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("conflicts with aggregation name" in str(e) for e in errs)


def test_having_can_reference_accumulate_names():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { avg_val = avg(msg.v) }
        |> ACCUMULATE {
            smooth = ema(avg_val, 0.3)
        }
        |> HAVING smooth > 10
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_map_can_reference_accumulate_names():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { avg_val = avg(msg.v) }
        |> ACCUMULATE {
            smooth = ema(avg_val, 0.3),
            running = cumsum(avg_val)
        }
        |> MAP { result = smooth, total = running }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_accumulate_used_suppresses_unused_agg_warning():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { avg_val = avg(msg.v) }
        |> ACCUMULATE {
            smooth = ema(avg_val, 0.3)
        }
        |> MAP { result = smooth }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    unused_warnings = [e for e in errs if "never referenced" in str(e)]
    assert unused_warnings == []


def test_duplicate_rate_stage():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> RATE 100 PER 1s
        |> RATE 50 PER 2s
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("duplicate 'rate'" in str(e).lower() for e in errs)


def test_duplicate_accumulate_stage():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(10s)
        |> AGGREGATE { v = avg(msg.v) }
        |> ACCUMULATE { s = ema(v, 0.3) }
        |> ACCUMULATE { r = cumsum(v) }
        |> SINK TO b :: "b"
    ;
    """
    errs = _errors(src)
    assert any("duplicate 'accumulate'" in str(e).lower() for e in errs)


def test_join_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "orders"
        |> JOIN b :: "customers"
            ON msg.customer_id == other.id
            WITHIN 10 s
        ;
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_join_left_key_not_msg():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "orders"
        |> JOIN b :: "customers"
            ON customer_id == other.id
            WITHIN 10 s
        ;
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("left key" in str(e) and "msg" in str(e) for e in errs)


def test_join_right_key_not_other():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "orders"
        |> JOIN b :: "customers"
            ON msg.customer_id == msg.id
            WITHIN 10 s
        ;
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("right key" in str(e) and "other" in str(e) for e in errs)


def test_join_with_pipe_source():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE upstream
        SOURCE FROM b :: "in"
        |> SINK TO b :: "mid"
    ;
    PIPELINE downstream
        SOURCE FROM PIPE upstream
        |> JOIN b :: "customers"
            ON msg.id == other.id
            WITHIN 10 s
        ;
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("join" in str(e).lower() and "pipe source" in str(e).lower() for e in errs)


def test_join_zero_window():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "orders"
        |> JOIN b :: "customers"
            ON msg.id == other.id
            WITHIN 0 s
        ;
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("join" in str(e).lower() and "positive" in str(e).lower() for e in errs)


def test_join_left_type_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "orders"
        |> JOIN b :: "customers"
            ON msg.customer_id == other.id
            WITHIN 10 s
            TYPE: left
        ;
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_fmt_valid_with_args():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> MAP {
            label = fmt("Value: {}", msg.value)
        }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert errs == []


def test_fmt_valid_single_arg():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> MAP {
            label = fmt("static text")
        }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert errs == []


def test_type_annotation_valid():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(10 s)
        |> AGGREGATE {
            total: float = sum(msg.value),
            n: int = count()
        }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert errs == []


def test_type_annotation_mixed():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(10 s)
        |> AGGREGATE {
            total: float = sum(msg.value),
            n = count()
        }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert errs == []


# ─── Schema validation ──────────────────────────────────────────────


def test_schema_valid():
    src = """
    SCHEMA Sensor
        temp: float
        humidity: float
        sensor_id: str
        active: bool
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Sensor
        |> FILTER msg.temp > 30.0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


def test_schema_duplicate_names():
    src = """
    SCHEMA Sensor
        temp: float
    ;
    SCHEMA Sensor
        humidity: float
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("Duplicate schema" in str(e) for e in errs)


def test_schema_empty_fields():
    src = """
    SCHEMA Empty
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    with pytest.raises(Exception):
        _model(src)


def test_schema_duplicate_field_names():
    src = """
    SCHEMA Bad
        temp: float
        temp: int
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("duplicate field" in str(e).lower() for e in errs)


def test_schema_nested_duplicate_fields():
    src = """
    SCHEMA Bad
        location: {
            lat: float,
            lat: float
        }
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("duplicate field" in str(e).lower() for e in errs)


def test_schema_ref_valid_field():
    src = """
    SCHEMA Sensor
        temp: float
        humidity: float
        sensor_id: str
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Sensor
        |> FILTER msg.temp > 30.0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


def test_schema_ref_unknown_field_strict():
    src = """
    SCHEMA Sensor STRICT
        temp: float
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Sensor
        |> FILTER msg.unknown > 0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1
    assert schema_errs[0].severity == "error"


def test_schema_ref_unknown_field_lenient():
    src = """
    SCHEMA Sensor LENIENT
        temp: float
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Sensor
        |> FILTER msg.unknown > 0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1
    assert schema_errs[0].severity == "warning"


def test_schema_ref_nested_field():
    src = """
    SCHEMA Reading
        value: float
        location: {
            lat: float,
            lon: float
        }
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Reading
        |> FILTER msg.location.lat > 0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


def test_schema_ref_nested_field_invalid():
    src = """
    SCHEMA Reading
        value: float
        location: {
            lat: float,
            lon: float
        }
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Reading
        |> FILTER msg.location.xyz > 0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1


def test_schema_type_mismatch_sum_string():
    src = """
    SCHEMA Sensor
        temp: float
        sensor_id: str
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Sensor
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { total = sum(msg.sensor_id) }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("sum()" in str(w) and "numeric" in str(w) for w in warnings)


def test_schema_type_mismatch_abs_string():
    src = """
    SCHEMA Sensor
        temp: float
        sensor_id: str
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Sensor
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { total = sum(msg.sensor_id) }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("numeric" in str(w) for w in warnings)


def test_schema_no_binding_no_validation():
    src = """
    SCHEMA Sensor
        temp: float
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> FILTER msg.nonexistent > 0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


# ─── on_error validation ────────────────────────────────────────────


def test_on_error_valid_sink():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
        ON_ERROR: SINK TO b :: "errors/p"
    ;
    """
    errs = _errors(src)
    on_error_errs = [e for e in errs if "on_error" in str(e) and e.severity == "error"]
    assert on_error_errs == []


def test_on_error_valid_drop():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
        ON_ERROR: DROP
    ;
    """
    errs = _errors(src)
    on_error_errs = [e for e in errs if "on_error" in str(e) and e.severity == "error"]
    assert on_error_errs == []


def test_on_error_empty_topic():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
        ON_ERROR: SINK TO b :: ""
    ;
    """
    errs = _errors(src)
    assert any("on_error" in str(e) and "topic" in str(e).lower() for e in errs)


class TestNewStagesSemantics:
    def test_flatten_bare_msg_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> FLATTEN msg
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("message field path" in str(e) for e in errs)

    def test_flatten_non_msg_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> FLATTEN data.items
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("message field path" in str(e) for e in errs)

    def test_flatten_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> FLATTEN msg.items
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        flatten_errs = [e for e in errs if "flatten" in str(e).lower() and e.severity == "error"]
        assert flatten_errs == []

    def test_distinct_bare_msg_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> DISTINCT msg WITHIN 5 m
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("message field path" in str(e) for e in errs)

    def test_distinct_zero_ttl_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> DISTINCT msg.id WITHIN 0 s
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("ttl must be positive" in str(e) for e in errs)

    def test_distinct_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> DISTINCT msg.id WITHIN 10 s
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        distinct_errs = [e for e in errs if "distinct" in str(e).lower() and e.severity == "error"]
        assert distinct_errs == []

    def test_throttle_zero_limit_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> THROTTLE 0 PER 1 s
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("limit must be positive" in str(e) for e in errs)

    def test_throttle_zero_period_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> THROTTLE 10 PER 0 s
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("period must be positive" in str(e) for e in errs)

    def test_throttle_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> THROTTLE 10 PER 1 s
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        throttle_errs = [e for e in errs if "throttle" in str(e).lower() and e.severity == "error"]
        assert throttle_errs == []

    def test_throttle_buffer_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> THROTTLE 10 PER 1 s OVERFLOW: BUFFER BUFFER: 100
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        throttle_errs = [e for e in errs if "throttle" in str(e).lower() and e.severity == "error"]
        assert throttle_errs == []

    def test_sample_zero_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> SAMPLE 0 %
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("between 1 and 100" in str(e) for e in errs)

    def test_sample_over_100_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> SAMPLE 101 %
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("between 1 and 100" in str(e) for e in errs)

    def test_sample_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> SAMPLE 50 %
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        sample_errs = [e for e in errs if "sample" in str(e).lower() and e.severity == "error"]
        assert sample_errs == []

    def test_flatten_after_window_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> WINDOW tumbling(1 m)
            |> FLATTEN msg.items
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("order" in str(e).lower() for e in errs)

    def test_distinct_after_aggregate_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { n = count() }
            |> DISTINCT msg.id WITHIN 5 m
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("order" in str(e).lower() for e in errs)

    def test_new_stages_duplicate_rejected(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "a"
            |> FLATTEN msg.items
            |> FLATTEN msg.data
            |> SINK TO b :: "b"
        ;
        """
        errs = _errors(src)
        assert any("duplicate" in str(e).lower() for e in errs)


class TestRetryPolicySemantics:
    def test_retry_valid_no_errors(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
            RETRY: 5 WITH BACKOFF 200 ms
        ;
        """
        errs = _errors(src)
        retry_errs = [e for e in errs if "retry" in str(e).lower() and e.severity == "error"]
        assert retry_errs == []

    def test_retry_zero_retries_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
            RETRY: 0 WITH BACKOFF 100 ms
        ;
        """
        errs = _errors(src)
        assert any("at least 1" in str(e) for e in errs)

    def test_retry_valid_BACKOFF(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
            RETRY: 3 WITH BACKOFF 500 ms
        ;
        """
        errs = _errors(src)
        retry_errs = [e for e in errs if "retry" in str(e).lower() and e.severity == "error"]
        assert retry_errs == []

    def test_retry_with_seconds_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
            RETRY: 10 WITH BACKOFF 2 s
        ;
        """
        errs = _errors(src)
        retry_errs = [e for e in errs if "retry" in str(e).lower() and e.severity == "error"]
        assert retry_errs == []

    def test_no_retry_no_errors(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        retry_errs = [e for e in errs if "retry" in str(e).lower()]
        assert retry_errs == []


class TestMultiWindowSemantics:
    def test_single_unnamed_window_no_over_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { n = count() }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        window_errs = [e for e in errs if "window" in str(e).lower() and "over" in str(e).lower()]
        assert window_errs == []

    def test_single_named_window_with_over_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW short tumbling(1 m)
            |> AGGREGATE { n = count() OVER short }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_single_unnamed_window_with_over_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { n = count() OVER mywin }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("no name" in str(e) for e in errs)

    def test_multi_window_all_named_all_over_valid(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW short tumbling(1 m)
            |> WINDOW long tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short,
                avg_5m = avg(msg.value) OVER long
            }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_multi_window_one_unnamed_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW short tumbling(1 m)
            |> WINDOW tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short
            }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("must have names" in str(e) for e in errs)

    def test_multi_window_agg_missing_over_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW short tumbling(1 m)
            |> WINDOW long tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value)
            }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("must specify" in str(e) and "over" in str(e) for e in errs)

    def test_multi_window_over_invalid_name_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW short tumbling(1 m)
            |> WINDOW long tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER nonexistent
            }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("undefined window" in str(e) for e in errs)

    def test_multi_window_duplicate_names_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW short tumbling(1 m)
            |> WINDOW short tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short
            }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("duplicate window name" in str(e) for e in errs)

    def test_no_window_but_over_used_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> AGGREGATE { n = count() OVER mywin }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("no window is defined" in str(e) for e in errs)

    def test_single_named_window_over_wrong_name_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW short tumbling(1 m)
            |> AGGREGATE { n = count() OVER wrong }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("undefined window" in str(e) for e in errs)


# ─── User-defined function validation (_check_func_defs) ────────────


class TestFuncDefsSemantics:
    def test_valid_func_def_no_errors(self):
        src = """
        FUNC double(x) = x * 2
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> FILTER double(msg.value) > 10
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []

    def test_valid_func_def_multiple_params_no_errors(self):
        src = """
        FUNC add(a, b) = a + b
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> FILTER add(msg.x, msg.y) > 0
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []

    def test_duplicate_func_name_error(self):
        src = """
        FUNC double(x) = x * 2
        FUNC double(y) = y * 3
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("Duplicate function name" in str(e) for e in errs)

    def test_func_shadows_builtin_abs_error(self):
        src = """
        FUNC abs(x) = x
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_shadows_builtin_round_error(self):
        src = """
        FUNC round(x) = x
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_shadows_builtin_len_error(self):
        src = """
        FUNC len(x) = x
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_shadows_builtin_fmt_error(self):
        src = """
        FUNC fmt(x) = x
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_duplicate_parameter_error(self):
        src = """
        FUNC bad(a, a) = a * 2
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("duplicate parameter" in str(e) for e in errs)

    def test_func_duplicate_parameter_three_params_error(self):
        src = """
        FUNC bad(x, y, x) = x + y
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("duplicate parameter" in str(e) for e in errs)

    def test_func_single_param_no_errors(self):
        src = """
        FUNC negate(x) = 0 - x
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> FILTER negate(msg.value) > 0
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []


# ─── Pipeline-level let binding validation (_check_pipeline_let_bindings) ────


class TestPipelineLetBindingsSemantics:
    def test_valid_pipeline_let_no_errors(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET threshold = 42
            |> FILTER msg.value > threshold
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []

    def test_valid_pipeline_let_multiple_no_errors(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET low = 10
            LET high = 100
            |> FILTER msg.value > low and msg.value < high
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []

    def test_duplicate_pipeline_let_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET threshold = 42
            LET threshold = 99
            |> FILTER msg.value > threshold
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("duplicate pipeline let-binding" in str(e) for e in errs)

    def test_pipeline_let_shadows_constant_error(self):
        src = """
        CONST LIMIT = 50
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET LIMIT = 99
            |> FILTER msg.value > LIMIT
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("shadows a constant" in str(e) for e in errs)

    def test_pipeline_let_does_not_shadow_different_constant(self):
        src = """
        CONST LIMIT = 50
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET threshold = 42
            |> FILTER msg.value > threshold
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []


# ─── Match expression validation (_check_match_exprs) ────────────────


class TestMatchExprsSemantics:
    def test_valid_match_with_else_no_errors(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> MAP {
                label = match total {
                    case 0 => "zero"
                    case 1 => "one"
                    else => "many"
                }
            }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_match_without_else_warning(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> MAP {
                label = match total {
                    case 0 => "zero"
                    case 1 => "one"
                }
            }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        warnings = [e for e in errs if e.severity == "warning"]
        assert any("without 'else' arm" in str(w) for w in warnings)

    def test_match_in_filter_without_else_warning(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> FILTER match msg.status {
                case "active" => true
                case "pending" => true
            } == true
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        warnings = [e for e in errs if e.severity == "warning"]
        assert any("without 'else' arm" in str(w) for w in warnings)

    def test_match_with_else_no_warning(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> FILTER match msg.status {
                case "active" => true
                else => false
            } == true
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        warnings = [e for e in errs if e.severity == "warning"]
        match_warnings = [w for w in warnings if "match" in str(w).lower()]
        assert match_warnings == []


# ─── User function call validation (_check_user_func_calls) ─────────


class TestUserFuncCallsSemantics:
    def test_valid_call_to_defined_func_no_errors(self):
        src = """
        FUNC double(x) = x * 2
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> FILTER double(msg.value) > 10
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []

    def test_call_to_undefined_func_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> FILTER unknown(msg.value) > 10
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("undefined function" in str(e) for e in errs)

    def test_call_to_undefined_func_in_map_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> MAP { result = transform(total) }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("undefined function" in str(e) for e in errs)

    def test_call_defined_func_in_map_no_errors(self):
        src = """
        FUNC scale(x) = x * 100
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> MAP { result = scale(total) }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []

    def test_call_to_undefined_func_in_pipeline_let_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET val = missing_fn(42)
            |> FILTER msg.value > val
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("undefined function" in str(e) for e in errs)

    def test_call_defined_func_in_pipeline_let_no_errors(self):
        src = """
        FUNC double(x) = x * 2
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET val = double(21)
            |> FILTER msg.value > val
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert errs == []


# ─── Pipeline lets visible in map/having references ─────────────────


class TestPipelineLetsInReferences:
    def test_pipeline_let_visible_in_map(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET factor = 10
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> MAP { result = total * factor }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_pipeline_let_visible_in_having(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET min_total = 100
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> HAVING total > min_total
            |> MAP { result = total }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_pipeline_let_not_confused_with_undefined_in_map(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET ratio = 0
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> MAP { result = ratio }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        # ratio is a pipeline let, should NOT produce "undefined aggregation"
        assert not any("undefined aggregation" in str(e) and "ratio" in str(e) for e in errs)

    def test_pipeline_let_not_confused_with_undefined_in_having(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET limit = 50
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> HAVING total > limit
            |> MAP { result = total }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        # limit is a pipeline let, should NOT produce "undefined aggregation"
        assert not any("undefined aggregation" in str(e) and "limit" in str(e) for e in errs)

    def test_undefined_ref_in_map_still_caught(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET factor = 10
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> MAP { result = nonexistent }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("undefined aggregation" in str(e) and "nonexistent" in str(e) for e in errs)

    def test_undefined_ref_in_having_still_caught(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "x"
            LET factor = 10
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.value) }
            |> HAVING nonexistent > 0
            |> MAP { result = total }
            |> SINK TO b :: "y"
        ;
        """
        errs = _errors(src)
        assert any("undefined aggregation" in str(e) and "nonexistent" in str(e) for e in errs)


# ─── Coverage: semantics.py lines 334-339 (ForallExpr/ExistsExpr in map) ──


def test_map_with_forall_expr():
    """_check_map_expr_refs with ForallExpr in map (semantics.py lines 333-339)."""
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "t"
        |> WINDOW tumbling(1 m)
        |> AGGREGATE { total = sum(msg.v) }
        |> MAP { all_positive = forall(x => x > 0) }
        |> SINK TO b :: "o"
    ;
    """
    errs = _errors(src)
    # forall with quantifier var should not produce false-positive errors
    # about undefined aggregation for the quantifier variable
    undef_errs = [e for e in errs if "undefined aggregation" in str(e) and "'x'" in str(e)]
    assert undef_errs == []


def test_map_with_exists_collection():
    """_check_map_expr_refs with ExistsExpr + collection in map (semantics.py line 338)."""
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "t"
        |> MAP { has_big = exists(x in msg.items => x > 100) }
        |> SINK TO b :: "o"
    ;
    """
    errs = _errors(src)
    undef_errs = [e for e in errs if "undefined aggregation" in str(e) and "'x'" in str(e)]
    assert undef_errs == []


# ─── Coverage: semantics.py lines 836-841 (ForallExpr/ExistsExpr in having) ──


def test_having_with_exists_collection():
    """_check_having_expr_refs with ExistsExpr + collection (semantics.py lines 836-841)."""
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "t"
        |> WINDOW tumbling(1 m)
        |> AGGREGATE { total = sum(msg.v) }
        |> HAVING exists(x in msg.tags => x > 0)
        |> MAP { result = total }
        |> SINK TO b :: "o"
    ;
    """
    errs = _errors(src)
    undef_errs = [e for e in errs if "undefined aggregation" in str(e) and "'x'" in str(e)]
    assert undef_errs == []


def test_having_with_pipeline_let():
    """_check_having_references with pipeline lets available (semantics.py line 808-810)."""
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "t"
        LET threshold = 50
        |> WINDOW tumbling(1 m)
        |> AGGREGATE { total = sum(msg.v) }
        |> HAVING total > threshold
        |> MAP { result = total }
        |> SINK TO b :: "o"
    ;
    """
    errs = _errors(src)
    undef_errs = [e for e in errs if "undefined aggregation" in str(e)]
    assert undef_errs == []


# ─── Coverage: semantics.py lines 1776-1780 (_schema_type_name object/list) ──


def test_schema_object_field_type_name():
    """_schema_type_name for SchemaObjectType (semantics.py lines 1776-1777)."""
    src = """
    SCHEMA Reading
        value: float
        meta: {
            SOURCE: str,
            quality: int
        }
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Reading
        |> FILTER msg.meta.SOURCE == "sensor"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


def test_schema_list_field_traversal():
    """_schema_type_name for SchemaListType returns 'list' (semantics.py lines 1778-1779).
    Traversing into a list field should stop resolution (not an object)."""
    src = """
    SCHEMA Reading
        value: float
        tags: [str]
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Reading
        |> FILTER msg.tags.first > 0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    # tags is a list, so tags.first can't be resolved -> not found
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1


# ─── Coverage: semantics.py lines 1819-1840 (_walk_expr_for_schema FuncCall) ──


def test_schema_agg_numeric_func_on_string_field():
    """_walk_expr_for_schema: aggregate numeric func (avg) on string field
    (semantics.py lines 1801-1813)."""
    src = """
    SCHEMA Sensor
        temp: float
        label: str
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Sensor
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { total = avg(msg.label) }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("avg()" in str(w) and "numeric" in str(w) for w in warnings)


def test_schema_object_field_not_traversable_as_scalar():
    """_schema_type_name returns 'object' for SchemaObjectType, and traversal
    into non-object field returns not-found (semantics.py lines 1765-1768)."""
    src = """
    SCHEMA Reading STRICT
        value: float
        meta: {
            SOURCE: str,
            quality: int
        }
    ;
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in" SCHEMA: Reading
        |> FILTER msg.value.nested > 0
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1


def test_valid_model_and_predict():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL det
        SOURCE: "m.ONNX"
        RUNTIME: ONNX
        FEATURES: [msg.x, msg.y]
        RETURNS: [score, flag]
    ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT det
        |> SINK TO b :: "out"
    ;
    """
    assert _errors(src) == []


def test_duplicate_model_names():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m SOURCE: "a" RUNTIME: ONNX FEATURES: [msg.x] RETURNS: [y] ;
    MODEL m SOURCE: "b" RUNTIME: SKLEARN FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("Duplicate model" in str(e) for e in errs)


def test_duplicate_model_outputs():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m
        SOURCE: "m.ONNX"
        RUNTIME: ONNX
        FEATURES: [msg.x]
        RETURNS: [score, score]
    ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("duplicate output" in str(e) for e in errs)


def test_model_non_custom_no_features_warning():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m
        SOURCE: "m.ONNX"
        RUNTIME: ONNX
    ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("features" in str(w) for w in warnings)


def test_model_non_custom_no_returns_warning():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m
        SOURCE: "m.ONNX"
        RUNTIME: ONNX
    ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("returns" in str(w) for w in warnings)


def test_custom_runtime_no_features_no_warning():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m
        SOURCE: "my_module.py"
        RUNTIME: CUSTOM
    ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert errs == []


def test_predict_stage_order_after_filter():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m SOURCE: "m.ONNX" RUNTIME: ONNX FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> FILTER msg.x > 0
        |> PREDICT m
        |> SINK TO b :: "out"
    ;
    """
    assert _errors(src) == []


def test_predict_stage_order_before_filter():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m SOURCE: "m.ONNX" RUNTIME: ONNX FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> FILTER msg.x > 0
        |> SINK TO b :: "out"
    ;
    """
    assert _errors(src) == []


def test_predict_after_aggregate_error():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m SOURCE: "m.ONNX" RUNTIME: ONNX FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> WINDOW tumbling(1 m)
        |> AGGREGATE { a = avg(msg.x) }
        |> PREDICT m
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    real_errs = [e for e in errs if e.severity == "error"]
    assert any("order" in str(e).lower() or "stage" in str(e).lower() for e in real_errs)


def test_duplicate_predict_stage():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m SOURCE: "m.ONNX" RUNTIME: ONNX FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> PREDICT m
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "predict" in str(e).lower() for e in errs)


def test_predict_with_window_and_aggregate():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m SOURCE: "m.ONNX" RUNTIME: ONNX FEATURES: [msg.x] RETURNS: [score] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> WINDOW tumbling(1 m)
        |> AGGREGATE { avg_score = avg(msg.score) }
        |> SINK TO b :: "out"
    ;
    """
    assert _errors(src) == []


def test_predict_with_downstream_map():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    MODEL m SOURCE: "m.ONNX" RUNTIME: ONNX FEATURES: [msg.x] RETURNS: [score] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> MAP { result = msg.score }
        |> SINK TO b :: "out"
    ;
    """
    assert _errors(src) == []


def test_select_stage_valid():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SELECT [id, value, ts]
        |> SINK TO b :: "out"
    ;
    """
    assert _errors(src) == []


def test_select_stage_with_rename_valid():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SELECT [out_id: raw_id, value]
        |> SINK TO b :: "out"
    ;
    """
    assert _errors(src) == []


def test_select_and_map_conflict():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SELECT [id, value]
        |> MAP { result = msg.value }
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("cannot use both" in str(e) for e in errs)


def test_select_duplicate_stage():
    src = """
    BROKER b HOST: "h" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> SELECT [id]
        |> SELECT [value]
        |> SINK TO b :: "out"
    ;
    """
    errs = _errors(src)
    assert any("select" in str(e).lower() for e in errs)


# ── Array indexing ──────────────────────────────────────────────────────────

BASE_SUB = 'BROKER b HOST: "h" PORT: 1883;\n'


def test_subscript_no_error_in_filter():
    src = (
        BASE_SUB
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> FILTER msg.items[0] > 0
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_subscript_warning_in_group_by():
    src = (
        BASE_SUB
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> GROUP_BY msg.items[0]
    |> AGGREGATE { n = count() }
    |> SINK TO b :: "out"
;
"""
    )
    warnings = [e for e in _errors(src) if e.severity == "warning"]
    assert any("subscript" in e.message.lower() or "index" in e.message.lower() for e in warnings)


def test_subscript_warning_in_dedup():
    src = (
        BASE_SUB
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> DEDUPLICATE msg.items[0] WITHIN 60 s
    |> SINK TO b :: "out"
;
"""
    )
    warnings = [e for e in _errors(src) if e.severity == "warning"]
    assert any("subscript" in e.message.lower() or "index" in e.message.lower() for e in warnings)


# ── DebugStage ───────────────────────────────────────────────────────────────

BASE_DBG = 'BROKER b HOST: "h" PORT: 1883;\n'


def test_debug_no_errors():
    src = (
        BASE_DBG
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> DEBUG LABEL: "test"
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_multiple_debug_no_duplicate_error():
    src = (
        BASE_DBG
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> DEBUG
    |> FILTER msg.v > 0
    |> DEBUG
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


# ── percentile / distinct_count ──────────────────────────────────────────────

BASE_PCT = 'BROKER b HOST: "h" PORT: 1883;\n'


def test_percentile_no_errors():
    src = (
        BASE_PCT
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> WINDOW tumbling(1 m)
    |> AGGREGATE { p95 = percentile(msg.value, 95) }
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_percentile_missing_param_error():
    src = (
        BASE_PCT
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> WINDOW tumbling(1 m)
    |> AGGREGATE { p95 = percentile(msg.value) }
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert any("percentile" in e.message.lower() or "param" in e.message.lower() for e in errors)


def test_distinct_count_no_errors():
    src = (
        BASE_PCT
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> WINDOW tumbling(1 m)
    |> AGGREGATE { n = distinct_count(msg.device_id) }
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


# ── ValidateStage ─────────────────────────────────────────────────────────────

BASE_VAL = """
BROKER b HOST: "h" PORT: 1883;
SCHEMA S STRICT id: str value: float active: bool?;
"""


def test_validate_no_errors():
    src = (
        BASE_VAL
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> VALIDATE SCHEMA: S ON_ERROR: DROP
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_validate_no_on_error_ok():
    src = (
        BASE_VAL
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> VALIDATE SCHEMA: S
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


# ── EnrichStage ──────────────────────────────────────────────────────────────

BASE_ENR = 'BROKER b HOST: "h" PORT: 1883;\n'


def test_enrich_no_errors():
    src = (
        BASE_ENR
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> ENRICH VIA HTTP
        URL: "https://api.example.com/"
        BIND: meta
        ON_ERROR: SKIP
    ;
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_multiple_enrich_no_duplicate_error():
    src = (
        BASE_ENR
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> ENRICH VIA HTTP
        URL: "https://api.example.com/a"
        BIND: a_data
    ;
    |> ENRICH VIA HTTP
        URL: "https://api.example.com/b"
        BIND: b_data
    ;
    |> SINK TO b :: "out"
;
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []
