import pytest
from sigil.language import parse_string
from sigil.semantics import validate, validate_or_raise, SemanticValidationError


def _model(src):
    return parse_string(src)


def _errors(src):
    return validate(_model(src))


VALID = """
broker b host: "h" port: 1883 end
pipeline p
    source from b :: "in" qos: 1
    |> filter msg.v > 0
    |> window tumbling(5 m)
    |> aggregate { a = avg(msg.v)  n = count() }
    |> map { result = a;  total = n }
    |> sink to b :: "out" qos: 1
end
"""


def test_valid_model_has_no_errors():
    assert _errors(VALID) == []


def test_duplicate_broker_name():
    src = """
    broker b host: "a" port: 1 end
    broker b host: "b" port: 2 end
    pipeline p
        source from b :: "x"
        |> sink to b :: "y"
    end
    """
    with pytest.raises(Exception, match="not unique"):
        _model(src)


def test_duplicate_pipeline_name():
    src = """
    broker b host: "a" port: 1 end
    pipeline p
        source from b :: "x"
        |> sink to b :: "y"
    end
    pipeline p
        source from b :: "x"
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("Duplicate pipeline" in str(e) for e in errs)


def test_invalid_qos():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x" qos: 3
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("qos" in str(e).lower() for e in errs)


def test_invalid_port():
    src = """
    broker b host: "h" port: 99999 end
    pipeline p
        source from b :: "x"
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("port" in str(e).lower() for e in errs)


def test_stage_order_violation():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { n = count() }
        |> filter msg.v > 0
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("order" in str(e).lower() for e in errs)


def test_aggregate_func_missing_field():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { a = avg() }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("requires a field" in str(e) for e in errs)


def test_count_with_field_is_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { n = count(msg.v) }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("count" in str(e) and "no arguments" in str(e) for e in errs)


def test_map_references_undefined_aggregation():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { a = avg(msg.v) }
        |> map { out = nonexistent }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e) for e in errs)


def test_sliding_window_requires_step():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> window sliding(10 m)
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("step" in str(e) for e in errs)


def test_sliding_window_step_too_large():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> window sliding(5 m, step = 10 m)
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("step" in str(e).lower() and "size" in str(e).lower() for e in errs)


def test_validate_or_raise_on_errors():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x" qos: 5
        |> sink to b :: "y"
    end
    """
    with pytest.raises(SemanticValidationError):
        validate_or_raise(_model(src))


def test_validate_or_raise_passes_clean():
    validate_or_raise(_model(VALID))


def test_duplicate_filter_stage():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> filter msg.v > 0
        |> filter msg.v < 100
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "filter" in str(e) for e in errs)


def test_duplicate_window_stage():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> window tumbling(5 m)
        |> window tumbling(10 m)
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("must have names" in str(e) for e in errs)


def test_no_duplicate_error_on_distinct_stages():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> filter msg.v > 0
        |> window tumbling(5 m)
        |> aggregate { n = count() }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert not any("duplicate" in str(e).lower() for e in errs)


def test_unused_aggregation_warning():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { a = avg(msg.v)  b_val = sum(msg.v)  n = count() }
        |> map { result = a }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert len(warnings) == 2
    warning_text = " ".join(str(w) for w in warnings)
    assert "b_val" in warning_text
    assert "n" in warning_text


def test_no_unused_warning_without_map():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { a = avg(msg.v)  n = count() }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert len(warnings) == 0


def test_no_unused_warning_when_all_referenced():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { a = avg(msg.v)  n = count() }
        |> map { average = a;  total = n }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert len(warnings) == 0


def test_millisecond_window_validation():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> window sliding(500 ms, step = 100 ms)
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert not any("step" in str(e).lower() and "size" in str(e).lower() for e in errs)


def test_millisecond_step_exceeds_size():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> window sliding(100 ms, step = 500 ms)
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("step" in str(e).lower() and "size" in str(e).lower() for e in errs)


def test_scalar_func_abs_too_many_args():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> filter abs(msg.x, msg.y) > 10
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("expects" in str(e) and "argument" in str(e) for e in errs)


def test_scalar_func_round_too_many_args():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { v = avg(msg.x) }
        |> map { r = round(v, 2, 1) }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("expects" in str(e) and "argument" in str(e) for e in errs)


def test_duplicate_let_binding_names():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { v = avg(msg.x) }
        |> map {
            let a = v * 2;
            let a = v * 3;
            result = a
        }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any(
        "duplicate" in str(e).lower() and "let-binding" in str(e).lower() for e in errs
    )


def test_let_binding_shadows_aggregation():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { v = avg(msg.x) }
        |> map {
            let v = 42;
            result = v
        }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("shadows" in str(e) for e in errs)


def test_let_binding_references_undefined():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> aggregate { v = avg(msg.x) }
        |> map {
            let a = nonexistent;
            result = a
        }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e) for e in errs)


def test_valid_let_bindings_no_errors():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> window tumbling(5 m)
        |> aggregate { v = avg(msg.x) }
        |> map {
            let ratio = v * 100;
            let doubled = ratio * 2;
            result = doubled
        }
        |> sink to b :: "y"
    end
    """
    errs = _errors(src)
    assert errs == []


def test_duplicate_aggregate_names():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> window tumbling(5 m)
        |> aggregate { a = avg(msg.x)  a = sum(msg.y) }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any(
        "duplicate" in str(e).lower() and "aggregation" in str(e).lower() for e in errs
    )


def test_duplicate_map_targets():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> window tumbling(5 m)
        |> aggregate { x = avg(msg.v)  y = sum(msg.v) }
        |> map { result = x;  result = y }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any(
        "duplicate" in str(e).lower() and "map target" in str(e).lower() for e in errs
    )


def test_empty_source_topic():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: ""
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("topic" in str(e).lower() and "empty" in str(e).lower() for e in errs)


def test_empty_sink_topic():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> sink to b :: ""
    end
    """
    errs = _errors(src)
    assert any("topic" in str(e).lower() and "empty" in str(e).lower() for e in errs)


def test_aggregate_without_window():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> aggregate { a = avg(msg.v) }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any(
        "aggregate" in str(e).lower() and "window" in str(e).lower() for e in errs
    )


def test_sink_broker_invalid_port():
    src = """
    broker src_broker host: "h" port: 1883 end
    broker sink_broker host: "h" port: 99999 end
    pipeline p
        source from src_broker :: "in"
        |> sink to sink_broker :: "out"
    end
    """
    errs = _errors(src)
    assert any("port" in str(e).lower() for e in errs)


def test_window_without_aggregate_warning():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> window tumbling(5 m)
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    hard = [e for e in errs if e.severity == "error"]
    assert len(hard) == 0
    assert any(
        "window" in str(w).lower() and "aggregate" in str(w).lower() for w in warnings
    )


def test_group_by_without_window_warning():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> group_by msg.sensor_id
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    hard = [e for e in errs if e.severity == "error"]
    assert len(hard) == 0
    assert any(
        "group_by" in str(w).lower() and "window" in str(w).lower() for w in warnings
    )


def test_aggregate_bare_msg_warning():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> window tumbling(5 m)
        |> aggregate { a = avg(msg) }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("msg" in str(w).lower() and "field" in str(w).lower() for w in warnings)


def test_map_undefined_ref_inside_if_then_else():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> window tumbling(5 m)
        |> aggregate { a = avg(msg.v) }
        |> map { result = if undefined_var > 0 then 1 else 0 }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e).lower() for e in errs)


def test_complete_valid_pipeline_no_errors():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in" qos: 1
        |> filter msg.v > 0
        |> group_by msg.sensor_id
        |> window tumbling(5 m)
        |> aggregate { a = avg(msg.v)  n = count() }
        |> map { result = a;  total = n }
        |> sink to b :: "out" qos: 1
    end
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: const definitions ---


def test_duplicate_const_name():
    src = """
    const X = 1
    const X = 2
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("Duplicate const" in str(e) for e in errs)


def test_const_in_filter_expression():
    src = """
    const THRESHOLD = 42
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter msg.v > THRESHOLD
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


def test_const_in_map_expression():
    src = """
    const FACTOR = 1.5
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(5 m)
        |> aggregate { v = avg(msg.x) }
        |> map { result = v * FACTOR }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: env() references ---


def test_env_ref_empty_key():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter msg.x > env("")
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("non-empty" in str(e) for e in errs)


# --- Phase 2: case expressions ---


def test_case_no_else_warning():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate { v = avg(msg.x) }
        |> map { out = case { when v > 0 => "pos" } }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("case" in str(w).lower() and "none" in str(w).lower() for w in warnings)


def test_case_valid_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(5 m)
        |> aggregate { v = avg(msg.x) }
        |> map { out = case { when v > 0 => "pos" else => "neg" } }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    case_warnings = [
        e for e in errs if e.severity == "warning" and "case" in str(e).lower()
    ]
    assert len(case_warnings) == 0


# --- Phase 2: having stage ---


def test_having_without_aggregate_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(5 m)
        |> having msg.v > 10
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any(
        "having" in str(e).lower() and "aggregate" in str(e).lower() for e in errs
    )


def test_having_undefined_ref():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(5 m)
        |> aggregate { n = count() }
        |> having nonexistent > 10
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("undefined aggregation" in str(e) for e in errs)


def test_having_after_map_order_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(5 m)
        |> aggregate { n = count() }
        |> map { result = n }
        |> having n > 10
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("order" in str(e).lower() for e in errs)


def test_having_valid_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(5 m)
        |> aggregate { n = count()  a = avg(msg.v) }
        |> having n > 5
        |> map { result = a }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: deduplicate stage ---


def test_dedup_key_not_msg_field():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> deduplicate event_id within 30 s
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("msg" in str(e).lower() and "field" in str(e).lower() for e in errs)


def test_dedup_after_filter_order_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter msg.v > 0
        |> deduplicate msg.event_id within 30 s
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("order" in str(e).lower() for e in errs)


def test_dedup_valid_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> deduplicate msg.event_id within 30 s
        |> filter msg.v > 0
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


# --- Phase 2: old() / changed() temporal references ---


def test_old_in_map_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate { v = avg(msg.x) }
        |> map { result = old(msg.x) }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("old()" in str(e) and "only valid" in str(e) for e in errs)


def test_changed_in_map_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate { v = avg(msg.x) }
        |> map { result = if changed(msg.x) then v else 0 }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("changed()" in str(e) and "only valid" in str(e) for e in errs)


def test_old_in_filter_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter old(msg.status) != msg.status
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    temporal_errs = [e for e in errs if "old()" in str(e) and "only valid" in str(e)]
    assert temporal_errs == []


def test_changed_in_filter_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter changed(msg.value)
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    temporal_errs = [
        e for e in errs if "changed()" in str(e) and "only valid" in str(e)
    ]
    assert temporal_errs == []


def test_old_ref_bare_msg_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter old(msg) != msg
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("old()" in str(e) and "field path" in str(e) for e in errs)


def test_temporal_in_filter_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter old(msg.status) != msg.status and changed(msg.value)
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    temporal_errs = [e for e in errs if "old()" in str(e) or "changed()" in str(e)]
    assert temporal_errs == []


# --- Phase 3: forall/exists quantifier validation ---


def test_forall_without_collection_not_in_having():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter forall(m => m > 0)
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("forall()" in str(e) and "only valid in having" in str(e) for e in errs)


def test_exists_without_collection_not_in_having():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter exists(m => m > 0)
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("exists()" in str(e) and "only valid in having" in str(e) for e in errs)


def test_forall_without_window():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate { n = count() }
        |> having forall(m => m > 0)
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("forall()" in str(e) and "window" in str(e) for e in errs)


def test_quantifier_invalid_collection():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter forall(x in tags => x > 0)
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("forall()" in str(e) and "msg" in str(e) for e in errs)


def test_exists_with_valid_collection_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter exists(x in msg.tags => x == "critical")
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    quantifier_errs = [e for e in errs if "exists()" in str(e) or "forall()" in str(e)]
    assert quantifier_errs == []


def test_forall_in_having_with_window_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { n = count() }
        |> having forall(m => m > 0)
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    quantifier_errs = [e for e in errs if "forall()" in str(e)]
    assert quantifier_errs == []


def test_quantifier_var_not_flagged_as_undefined_agg():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { n = count()  avg_val = avg(msg.value) }
        |> having forall(m => m > 0) and n >= 5
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    ref_errs = [e for e in errs if "undefined aggregation" in str(e)]
    assert ref_errs == []


# --- Phase 3: @initially/@finally validation ---


def test_initially_duplicate_fields():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        @initially { publish { status: "on", status: "off" } }
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "@initially" in str(e) for e in errs)


def test_finally_duplicate_fields():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
        @finally { publish { x: 1, x: 2 } }
    end
    """
    errs = _errors(src)
    assert any("duplicate" in str(e).lower() and "@finally" in str(e) for e in errs)


def test_hooks_valid_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        @initially { publish { status: "online" } }
        source from b :: "a"
        |> sink to b :: "b"
        @finally { publish { status: "offline" } }
    end
    """
    errs = _errors(src)
    hook_errs = [e for e in errs if "@initially" in str(e) or "@finally" in str(e)]
    assert hook_errs == []


# --- Phase 3: sink batch validation ---


def test_sink_batch_size_zero():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" batch: { size: 0 }
    end
    """
    errs = _errors(src)
    assert any("batch size" in str(e) and "positive" in str(e) for e in errs)


def test_sink_batch_negative_size():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" batch: { size: -1 }
    end
    """
    errs = _errors(src)
    assert any("batch size" in str(e) and "positive" in str(e) for e in errs)


def test_sink_batch_valid_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" batch: { size: 100  timeout: 5s }
    end
    """
    errs = _errors(src)
    batch_errs = [e for e in errs if "batch" in str(e).lower()]
    assert batch_errs == []


def test_sink_format_valid_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" format: csv
    end
    """
    errs = _errors(src)
    format_errs = [e for e in errs if "format" in str(e).lower()]
    assert format_errs == []


# --- Phase 4: import validation ---


def test_import_declaration_no_semantic_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    import_errs = [e for e in errs if "import" in str(e).lower()]
    assert import_errs == []


# --- Phase 4: pipeline chaining validation ---


def test_pipe_source_undefined_pipeline():
    src = """
    broker b host: "h" port: 1 end
    pipeline downstream
        source from pipe nonexistent
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("undefined pipeline" in str(e) and "nonexistent" in str(e) for e in errs)


def test_pipe_source_self_reference():
    src = """
    broker b host: "h" port: 1 end
    pipeline self_ref
        source from pipe self_ref
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("cannot reference itself" in str(e) for e in errs)


def test_pipe_source_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline upstream
        source from b :: "in"
        |> sink to b :: "mid"
    end
    pipeline downstream
        source from pipe upstream
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    pipe_errs = [e for e in errs if "pipe" in str(e).lower() and e.severity == "error"]
    assert pipe_errs == []


def test_pipe_source_no_sink_validation():
    src = """
    broker b host: "h" port: 1 end
    pipeline upstream
        source from b :: "in"
        |> sink to b :: "mid"
    end
    pipeline downstream
        source from pipe upstream
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    qos_errs = [e for e in errs if "qos" in str(e).lower() and "downstream" in str(e)]
    topic_errs = [
        e
        for e in errs
        if "topic" in str(e).lower()
        and "source" in str(e).lower()
        and "downstream" in str(e)
    ]
    assert qos_errs == []
    assert topic_errs == []


# --- Phase 4: route validation ---


def test_route_no_default_warning():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> route
            when msg.x > 0 => sink to b :: "pos"
        end
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any(
        "route" in str(w).lower() and "default" in str(w).lower() for w in warnings
    )


def test_route_valid_no_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> route
            when msg.x > 0 => sink to b :: "pos"
            default => sink to b :: "other"
        end
    end
    """
    errs = _errors(src)
    route_errs = [
        e for e in errs if "route" in str(e).lower() and e.severity == "error"
    ]
    assert route_errs == []


def test_route_pipeline_no_sink_batch_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> route
            when msg.x > 0 => sink to b :: "pos"
            default => sink to b :: "other"
        end
    end
    """
    errs = _errors(src)
    batch_errs = [e for e in errs if "batch" in str(e).lower()]
    assert batch_errs == []


# --- Phase 4: stage annotation validation ---


def test_warn_annotation_emits_warning():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> @warn("slow stage") filter msg.x > 0
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("@warn" in str(w) and "slow stage" in str(w) for w in warnings)


def test_error_annotation_emits_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> @error("deprecated") filter msg.x > 0
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert any("@error" in str(e) and "deprecated" in str(e) for e in hard)


def test_annotation_message_in_error():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> @warn("custom message here") filter msg.x > 0
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("custom message here" in str(e) for e in errs)


def test_no_annotation_no_extra_errors():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter msg.x > 0
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    annotation_errs = [e for e in errs if "@warn" in str(e) or "@error" in str(e)]
    assert annotation_errs == []


def test_rate_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> rate 100 per 1s
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_rate_with_on_excess_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> rate 50 per 5s on_excess: warn
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_rate_stage_order_before_group_by():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> group_by msg.k
        |> rate 100 per 1s
        |> window tumbling(5s)
        |> aggregate { n = count() }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("appears after a later-phase stage" in str(e) for e in errs)


def test_accumulate_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { avg_val = avg(msg.v) }
        |> accumulate {
            smooth = ema(avg_val, 0.3)
        }
        |> map { result = smooth }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_accumulate_requires_aggregate():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> accumulate {
            smooth = ema(avg_val, 0.3)
        }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("accumulate stage requires an aggregate stage" in str(e) for e in errs)


def test_accumulate_undefined_field():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { total = sum(msg.v) }
        |> accumulate {
            smooth = ema(nonexistent, 0.3)
        }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("undefined aggregation 'nonexistent'" in str(e) for e in errs)


def test_accumulate_ema_requires_param():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { avg_val = avg(msg.v) }
        |> accumulate {
            smooth = ema(avg_val)
        }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("requires a parameter" in str(e) for e in errs)


def test_accumulate_cumsum_no_extra_param():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { total = sum(msg.v) }
        |> accumulate {
            running = cumsum(total, 0.5)
        }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("takes no extra parameter" in str(e) for e in errs)


def test_accumulate_duplicate_name():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { total = sum(msg.v) }
        |> accumulate {
            x = cumsum(total)
            x = delta(total)
        }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("duplicate accumulate name 'x'" in str(e) for e in errs)


def test_accumulate_name_conflicts_with_agg():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { total = sum(msg.v) }
        |> accumulate {
            total = cumsum(total)
        }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("conflicts with aggregation name" in str(e) for e in errs)


def test_having_can_reference_accumulate_names():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { avg_val = avg(msg.v) }
        |> accumulate {
            smooth = ema(avg_val, 0.3)
        }
        |> having smooth > 10
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_map_can_reference_accumulate_names():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { avg_val = avg(msg.v) }
        |> accumulate {
            smooth = ema(avg_val, 0.3)
            running = cumsum(avg_val)
        }
        |> map { result = smooth; total = running }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_accumulate_used_suppresses_unused_agg_warning():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { avg_val = avg(msg.v) }
        |> accumulate {
            smooth = ema(avg_val, 0.3)
        }
        |> map { result = smooth }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    unused_warnings = [e for e in errs if "never referenced" in str(e)]
    assert unused_warnings == []


def test_duplicate_rate_stage():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> rate 100 per 1s
        |> rate 50 per 2s
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("duplicate 'rate'" in str(e) for e in errs)


def test_duplicate_accumulate_stage():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window tumbling(10s)
        |> aggregate { v = avg(msg.v) }
        |> accumulate { s = ema(v, 0.3) }
        |> accumulate { r = cumsum(v) }
        |> sink to b :: "b"
    end
    """
    errs = _errors(src)
    assert any("duplicate 'accumulate'" in str(e) for e in errs)


def test_join_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "orders"
        |> join b :: "customers"
            on msg.customer_id == other.id
            within 10 s
        end
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_join_left_key_not_msg():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "orders"
        |> join b :: "customers"
            on customer_id == other.id
            within 10 s
        end
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("left key" in str(e) and "msg" in str(e) for e in errs)


def test_join_right_key_not_other():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "orders"
        |> join b :: "customers"
            on msg.customer_id == msg.id
            within 10 s
        end
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("right key" in str(e) and "other" in str(e) for e in errs)


def test_join_with_pipe_source():
    src = """
    broker b host: "h" port: 1 end
    pipeline upstream
        source from b :: "in"
        |> sink to b :: "mid"
    end
    pipeline downstream
        source from pipe upstream
        |> join b :: "customers"
            on msg.id == other.id
            within 10 s
        end
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any(
        "join" in str(e).lower() and "pipe source" in str(e).lower() for e in errs
    )


def test_join_zero_window():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "orders"
        |> join b :: "customers"
            on msg.id == other.id
            within 0 s
        end
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("join" in str(e).lower() and "positive" in str(e).lower() for e in errs)


def test_join_left_type_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "orders"
        |> join b :: "customers"
            on msg.customer_id == other.id
            within 10 s
            type: left
        end
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    real = [e for e in errs if e.severity == "error"]
    assert real == []


def test_fmt_valid_with_args():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> map {
            label = fmt("Value: {}", msg.value)
        }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert errs == []


def test_fmt_valid_single_arg():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> map {
            label = fmt("static text")
        }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert errs == []


def test_type_annotation_valid():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> window tumbling(10 s)
        |> aggregate {
            total: float = sum(msg.value)
            n: int = count()
        }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert errs == []


def test_type_annotation_mixed():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> window tumbling(10 s)
        |> aggregate {
            total: float = sum(msg.value)
            n = count()
        }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert errs == []


# ─── Schema validation ──────────────────────────────────────────────


def test_schema_valid():
    src = """
    schema Sensor
        temp: float
        humidity: float
        sensor_id: str
        active: bool
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Sensor
        |> filter msg.temp > 30.0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    hard = [e for e in errs if e.severity == "error"]
    assert hard == []


def test_schema_duplicate_names():
    src = """
    schema Sensor
        temp: float
    end
    schema Sensor
        humidity: float
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("Duplicate schema" in str(e) for e in errs)


def test_schema_empty_fields():
    src = """
    schema Empty
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    with pytest.raises(Exception):
        _model(src)


def test_schema_duplicate_field_names():
    src = """
    schema Bad
        temp: float
        temp: int
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("duplicate field" in str(e).lower() for e in errs)


def test_schema_nested_duplicate_fields():
    src = """
    schema Bad
        location: {
            lat: float,
            lat: float
        }
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("duplicate field" in str(e).lower() for e in errs)


def test_schema_ref_valid_field():
    src = """
    schema Sensor
        temp: float
        humidity: float
        sensor_id: str
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Sensor
        |> filter msg.temp > 30.0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


def test_schema_ref_unknown_field_strict():
    src = """
    schema Sensor strict
        temp: float
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Sensor
        |> filter msg.unknown > 0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1
    assert schema_errs[0].severity == "error"


def test_schema_ref_unknown_field_lenient():
    src = """
    schema Sensor lenient
        temp: float
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Sensor
        |> filter msg.unknown > 0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1
    assert schema_errs[0].severity == "warning"


def test_schema_ref_nested_field():
    src = """
    schema Reading
        value: float
        location: {
            lat: float,
            lon: float
        }
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Reading
        |> filter msg.location.lat > 0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


def test_schema_ref_nested_field_invalid():
    src = """
    schema Reading
        value: float
        location: {
            lat: float,
            lon: float
        }
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Reading
        |> filter msg.location.xyz > 0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1


def test_schema_type_mismatch_sum_string():
    src = """
    schema Sensor
        temp: float
        sensor_id: str
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Sensor
        |> window tumbling(5 m)
        |> aggregate { total = sum(msg.sensor_id) }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("sum()" in str(w) and "numeric" in str(w) for w in warnings)


def test_schema_type_mismatch_abs_string():
    src = """
    schema Sensor
        temp: float
        sensor_id: str
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Sensor
        |> window tumbling(5 m)
        |> aggregate { total = sum(msg.sensor_id) }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("numeric" in str(w) for w in warnings)


def test_schema_no_binding_no_validation():
    src = """
    schema Sensor
        temp: float
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> filter msg.nonexistent > 0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


# ─── on_error validation ────────────────────────────────────────────


def test_on_error_valid_sink():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
        on_error: sink to b :: "errors/p"
    end
    """
    errs = _errors(src)
    on_error_errs = [e for e in errs if "on_error" in str(e) and e.severity == "error"]
    assert on_error_errs == []


def test_on_error_valid_drop():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
        on_error: drop
    end
    """
    errs = _errors(src)
    on_error_errs = [e for e in errs if "on_error" in str(e) and e.severity == "error"]
    assert on_error_errs == []


def test_on_error_empty_topic():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
        on_error: sink to b :: ""
    end
    """
    errs = _errors(src)
    assert any("on_error" in str(e) and "topic" in str(e).lower() for e in errs)


class TestNewStagesSemantics:
    def test_flatten_bare_msg_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> flatten msg
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("message field path" in str(e) for e in errs)

    def test_flatten_non_msg_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> flatten data.items
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("message field path" in str(e) for e in errs)

    def test_flatten_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> flatten msg.items
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        flatten_errs = [
            e for e in errs if "flatten" in str(e).lower() and e.severity == "error"
        ]
        assert flatten_errs == []

    def test_distinct_bare_msg_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> distinct msg within 5 m
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("message field path" in str(e) for e in errs)

    def test_distinct_zero_ttl_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> distinct msg.id within 0 s
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("ttl must be positive" in str(e) for e in errs)

    def test_distinct_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> distinct msg.id within 10 s
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        distinct_errs = [
            e for e in errs if "distinct" in str(e).lower() and e.severity == "error"
        ]
        assert distinct_errs == []

    def test_throttle_zero_limit_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> throttle 0 per 1 s
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("limit must be positive" in str(e) for e in errs)

    def test_throttle_zero_period_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> throttle 10 per 0 s
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("period must be positive" in str(e) for e in errs)

    def test_throttle_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> throttle 10 per 1 s
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        throttle_errs = [
            e for e in errs if "throttle" in str(e).lower() and e.severity == "error"
        ]
        assert throttle_errs == []

    def test_throttle_buffer_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> throttle 10 per 1 s overflow: buffer buffer: 100
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        throttle_errs = [
            e for e in errs if "throttle" in str(e).lower() and e.severity == "error"
        ]
        assert throttle_errs == []

    def test_sample_zero_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sample 0 %
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("between 1 and 100" in str(e) for e in errs)

    def test_sample_over_100_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sample 101 %
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("between 1 and 100" in str(e) for e in errs)

    def test_sample_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sample 50 %
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        sample_errs = [
            e for e in errs if "sample" in str(e).lower() and e.severity == "error"
        ]
        assert sample_errs == []

    def test_flatten_after_window_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> window tumbling(1 m)
            |> flatten msg.items
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("order" in str(e).lower() for e in errs)

    def test_distinct_after_aggregate_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> window tumbling(1 m)
            |> aggregate { n = count() }
            |> distinct msg.id within 5 m
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("order" in str(e).lower() for e in errs)

    def test_new_stages_duplicate_rejected(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> flatten msg.items
            |> flatten msg.data
            |> sink to b :: "b"
        end
        """
        errs = _errors(src)
        assert any("duplicate" in str(e).lower() for e in errs)


class TestRetryPolicySemantics:
    def test_retry_valid_no_errors(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            retry: 5 with backoff 200 ms
        end
        """
        errs = _errors(src)
        retry_errs = [
            e for e in errs if "retry" in str(e).lower() and e.severity == "error"
        ]
        assert retry_errs == []

    def test_retry_zero_retries_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            retry: 0 with backoff 100 ms
        end
        """
        errs = _errors(src)
        assert any("at least 1" in str(e) for e in errs)

    def test_retry_valid_backoff(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            retry: 3 with backoff 500 ms
        end
        """
        errs = _errors(src)
        retry_errs = [
            e for e in errs if "retry" in str(e).lower() and e.severity == "error"
        ]
        assert retry_errs == []

    def test_retry_with_seconds_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            retry: 10 with backoff 2 s
        end
        """
        errs = _errors(src)
        retry_errs = [
            e for e in errs if "retry" in str(e).lower() and e.severity == "error"
        ]
        assert retry_errs == []

    def test_no_retry_no_errors(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        retry_errs = [e for e in errs if "retry" in str(e).lower()]
        assert retry_errs == []


class TestMultiWindowSemantics:
    def test_single_unnamed_window_no_over_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window tumbling(1 m)
            |> aggregate { n = count() }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        window_errs = [
            e for e in errs if "window" in str(e).lower() and "over" in str(e).lower()
        ]
        assert window_errs == []

    def test_single_named_window_with_over_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short tumbling(1 m)
            |> aggregate { n = count() over short }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_single_unnamed_window_with_over_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window tumbling(1 m)
            |> aggregate { n = count() over mywin }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("no name" in str(e) for e in errs)

    def test_multi_window_all_named_all_over_valid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short tumbling(1 m)
            |> window long tumbling(5 m)
            |> aggregate {
                avg_1m = avg(msg.value) over short
                avg_5m = avg(msg.value) over long
            }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_multi_window_one_unnamed_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short tumbling(1 m)
            |> window tumbling(5 m)
            |> aggregate {
                avg_1m = avg(msg.value) over short
            }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("must have names" in str(e) for e in errs)

    def test_multi_window_agg_missing_over_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short tumbling(1 m)
            |> window long tumbling(5 m)
            |> aggregate {
                avg_1m = avg(msg.value)
            }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("must specify" in str(e) and "over" in str(e) for e in errs)

    def test_multi_window_over_invalid_name_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short tumbling(1 m)
            |> window long tumbling(5 m)
            |> aggregate {
                avg_1m = avg(msg.value) over nonexistent
            }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("undefined window" in str(e) for e in errs)

    def test_multi_window_duplicate_names_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short tumbling(1 m)
            |> window short tumbling(5 m)
            |> aggregate {
                avg_1m = avg(msg.value) over short
            }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("duplicate window name" in str(e) for e in errs)

    def test_no_window_but_over_used_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> aggregate { n = count() over mywin }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("no window is defined" in str(e) for e in errs)

    def test_single_named_window_over_wrong_name_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short tumbling(1 m)
            |> aggregate { n = count() over wrong }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("undefined window" in str(e) for e in errs)


# ─── User-defined function validation (_check_func_defs) ────────────


class TestFuncDefsSemantics:
    def test_valid_func_def_no_errors(self):
        src = """
        func double(x) = x * 2
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> filter double(msg.value) > 10
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []

    def test_valid_func_def_multiple_params_no_errors(self):
        src = """
        func add(a, b) = a + b
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> filter add(msg.x, msg.y) > 0
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []

    def test_duplicate_func_name_error(self):
        src = """
        func double(x) = x * 2
        func double(y) = y * 3
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("Duplicate function name" in str(e) for e in errs)

    def test_func_shadows_builtin_abs_error(self):
        src = """
        func abs(x) = x
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_shadows_builtin_round_error(self):
        src = """
        func round(x) = x
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_shadows_builtin_len_error(self):
        src = """
        func len(x) = x
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_shadows_builtin_fmt_error(self):
        src = """
        func fmt(x) = x
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("shadows a built-in" in str(e) for e in errs)

    def test_func_duplicate_parameter_error(self):
        src = """
        func bad(a, a) = a * 2
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("duplicate parameter" in str(e) for e in errs)

    def test_func_duplicate_parameter_three_params_error(self):
        src = """
        func bad(x, y, x) = x + y
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("duplicate parameter" in str(e) for e in errs)

    def test_func_single_param_no_errors(self):
        src = """
        func negate(x) = 0 - x
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> filter negate(msg.value) > 0
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []


# ─── Pipeline-level let binding validation (_check_pipeline_let_bindings) ────


class TestPipelineLetBindingsSemantics:
    def test_valid_pipeline_let_no_errors(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let threshold = 42
            |> filter msg.value > threshold
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []

    def test_valid_pipeline_let_multiple_no_errors(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let low = 10
            let high = 100
            |> filter msg.value > low and msg.value < high
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []

    def test_duplicate_pipeline_let_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let threshold = 42
            let threshold = 99
            |> filter msg.value > threshold
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("duplicate pipeline let-binding" in str(e) for e in errs)

    def test_pipeline_let_shadows_constant_error(self):
        src = """
        const LIMIT = 50
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let LIMIT = 99
            |> filter msg.value > LIMIT
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("shadows a constant" in str(e) for e in errs)

    def test_pipeline_let_does_not_shadow_different_constant(self):
        src = """
        const LIMIT = 50
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let threshold = 42
            |> filter msg.value > threshold
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []


# ─── Match expression validation (_check_match_exprs) ────────────────


class TestMatchExprsSemantics:
    def test_valid_match_with_else_no_errors(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> map {
                label = match total {
                    case 0 => "zero"
                    case 1 => "one"
                    else => "many"
                }
            }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_match_without_else_warning(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> map {
                label = match total {
                    case 0 => "zero"
                    case 1 => "one"
                }
            }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        warnings = [e for e in errs if e.severity == "warning"]
        assert any("without 'else' arm" in str(w) for w in warnings)

    def test_match_in_filter_without_else_warning(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> filter match msg.status {
                case "active" => true
                case "pending" => true
            } == true
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        warnings = [e for e in errs if e.severity == "warning"]
        assert any("without 'else' arm" in str(w) for w in warnings)

    def test_match_with_else_no_warning(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> filter match msg.status {
                case "active" => true
                else => false
            } == true
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        warnings = [e for e in errs if e.severity == "warning"]
        match_warnings = [w for w in warnings if "match" in str(w).lower()]
        assert match_warnings == []


# ─── User function call validation (_check_user_func_calls) ─────────


class TestUserFuncCallsSemantics:
    def test_valid_call_to_defined_func_no_errors(self):
        src = """
        func double(x) = x * 2
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> filter double(msg.value) > 10
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []

    def test_call_to_undefined_func_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> filter unknown(msg.value) > 10
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("undefined function" in str(e) for e in errs)

    def test_call_to_undefined_func_in_map_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> map { result = transform(total) }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("undefined function" in str(e) for e in errs)

    def test_call_defined_func_in_map_no_errors(self):
        src = """
        func scale(x) = x * 100
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> map { result = scale(total) }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []

    def test_call_to_undefined_func_in_pipeline_let_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let val = missing_fn(42)
            |> filter msg.value > val
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any("undefined function" in str(e) for e in errs)

    def test_call_defined_func_in_pipeline_let_no_errors(self):
        src = """
        func double(x) = x * 2
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let val = double(21)
            |> filter msg.value > val
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert errs == []


# ─── Pipeline lets visible in map/having references ─────────────────


class TestPipelineLetsInReferences:
    def test_pipeline_let_visible_in_map(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let factor = 10
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> map { result = total * factor }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_pipeline_let_visible_in_having(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let min_total = 100
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> having total > min_total
            |> map { result = total }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        hard = [e for e in errs if e.severity == "error"]
        assert hard == []

    def test_pipeline_let_not_confused_with_undefined_in_map(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let ratio = 0
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> map { result = ratio }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        # ratio is a pipeline let, should NOT produce "undefined aggregation"
        assert not any(
            "undefined aggregation" in str(e) and "ratio" in str(e) for e in errs
        )

    def test_pipeline_let_not_confused_with_undefined_in_having(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let limit = 50
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> having total > limit
            |> map { result = total }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        # limit is a pipeline let, should NOT produce "undefined aggregation"
        assert not any(
            "undefined aggregation" in str(e) and "limit" in str(e) for e in errs
        )

    def test_undefined_ref_in_map_still_caught(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let factor = 10
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> map { result = nonexistent }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any(
            "undefined aggregation" in str(e) and "nonexistent" in str(e) for e in errs
        )

    def test_undefined_ref_in_having_still_caught(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            let factor = 10
            |> window tumbling(1 m)
            |> aggregate { total = sum(msg.value) }
            |> having nonexistent > 0
            |> map { result = total }
            |> sink to b :: "y"
        end
        """
        errs = _errors(src)
        assert any(
            "undefined aggregation" in str(e) and "nonexistent" in str(e) for e in errs
        )


# ─── Coverage: semantics.py lines 334-339 (ForallExpr/ExistsExpr in map) ──


def test_map_with_forall_expr():
    """_check_map_expr_refs with ForallExpr in map (semantics.py lines 333-339)."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "t"
        |> window tumbling(1 m)
        |> aggregate { total = sum(msg.v) }
        |> map { all_positive = forall(x => x > 0) }
        |> sink to b :: "o"
    end
    """
    errs = _errors(src)
    # forall with quantifier var should not produce false-positive errors
    # about undefined aggregation for the quantifier variable
    undef_errs = [
        e for e in errs if "undefined aggregation" in str(e) and "'x'" in str(e)
    ]
    assert undef_errs == []


def test_map_with_exists_collection():
    """_check_map_expr_refs with ExistsExpr + collection in map (semantics.py line 338)."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "t"
        |> map { has_big = exists(x in msg.items => x > 100) }
        |> sink to b :: "o"
    end
    """
    errs = _errors(src)
    undef_errs = [
        e for e in errs if "undefined aggregation" in str(e) and "'x'" in str(e)
    ]
    assert undef_errs == []


# ─── Coverage: semantics.py lines 836-841 (ForallExpr/ExistsExpr in having) ──


def test_having_with_exists_collection():
    """_check_having_expr_refs with ExistsExpr + collection (semantics.py lines 836-841)."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "t"
        |> window tumbling(1 m)
        |> aggregate { total = sum(msg.v) }
        |> having exists(x in msg.tags => x > 0)
        |> map { result = total }
        |> sink to b :: "o"
    end
    """
    errs = _errors(src)
    undef_errs = [
        e for e in errs if "undefined aggregation" in str(e) and "'x'" in str(e)
    ]
    assert undef_errs == []


def test_having_with_pipeline_let():
    """_check_having_references with pipeline lets available (semantics.py line 808-810)."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "t"
        let threshold = 50
        |> window tumbling(1 m)
        |> aggregate { total = sum(msg.v) }
        |> having total > threshold
        |> map { result = total }
        |> sink to b :: "o"
    end
    """
    errs = _errors(src)
    undef_errs = [e for e in errs if "undefined aggregation" in str(e)]
    assert undef_errs == []


# ─── Coverage: semantics.py lines 1776-1780 (_schema_type_name object/list) ──


def test_schema_object_field_type_name():
    """_schema_type_name for SchemaObjectType (semantics.py lines 1776-1777)."""
    src = """
    schema Reading
        value: float
        meta: {
            source: str,
            quality: int
        }
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Reading
        |> filter msg.meta.source == "sensor"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert schema_errs == []


def test_schema_list_field_traversal():
    """_schema_type_name for SchemaListType returns 'list' (semantics.py lines 1778-1779).
    Traversing into a list field should stop resolution (not an object)."""
    src = """
    schema Reading
        value: float
        tags: [str]
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Reading
        |> filter msg.tags.first > 0
        |> sink to b :: "out"
    end
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
    schema Sensor
        temp: float
        label: str
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Sensor
        |> window tumbling(5 m)
        |> aggregate { total = avg(msg.label) }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("avg()" in str(w) and "numeric" in str(w) for w in warnings)


def test_schema_object_field_not_traversable_as_scalar():
    """_schema_type_name returns 'object' for SchemaObjectType, and traversal
    into non-object field returns not-found (semantics.py lines 1765-1768)."""
    src = """
    schema Reading strict
        value: float
        meta: {
            source: str,
            quality: int
        }
    end
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in" schema: Reading
        |> filter msg.value.nested > 0
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    schema_errs = [e for e in errs if "not found in schema" in str(e)]
    assert len(schema_errs) >= 1


def test_valid_model_and_predict():
    src = """
    broker b host: "h" port: 1883 end
    model det
        source: "m.onnx"
        runtime: onnx
        features: [msg.x, msg.y]
        returns: [score, flag]
    end
    pipeline p
        source from b :: "in"
        |> predict det
        |> sink to b :: "out"
    end
    """
    assert _errors(src) == []


def test_duplicate_model_names():
    src = """
    broker b host: "h" port: 1883 end
    model m source: "a" runtime: onnx features: [msg.x] returns: [y] end
    model m source: "b" runtime: sklearn features: [msg.x] returns: [y] end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("Duplicate model" in str(e) for e in errs)


def test_duplicate_model_outputs():
    src = """
    broker b host: "h" port: 1883 end
    model m
        source: "m.onnx"
        runtime: onnx
        features: [msg.x]
        returns: [score, score]
    end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("duplicate output" in str(e) for e in errs)


def test_model_non_custom_no_features_warning():
    src = """
    broker b host: "h" port: 1883 end
    model m
        source: "m.onnx"
        runtime: onnx
    end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("features" in str(w) for w in warnings)


def test_model_non_custom_no_returns_warning():
    src = """
    broker b host: "h" port: 1883 end
    model m
        source: "m.onnx"
        runtime: onnx
    end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    warnings = [e for e in errs if e.severity == "warning"]
    assert any("returns" in str(w) for w in warnings)


def test_custom_runtime_no_features_no_warning():
    src = """
    broker b host: "h" port: 1883 end
    model m
        source: "my_module.py"
        runtime: custom
    end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert errs == []


def test_predict_stage_order_after_filter():
    src = """
    broker b host: "h" port: 1883 end
    model m source: "m.onnx" runtime: onnx features: [msg.x] returns: [y] end
    pipeline p
        source from b :: "in"
        |> filter msg.x > 0
        |> predict m
        |> sink to b :: "out"
    end
    """
    assert _errors(src) == []


def test_predict_stage_order_before_filter():
    src = """
    broker b host: "h" port: 1883 end
    model m source: "m.onnx" runtime: onnx features: [msg.x] returns: [y] end
    pipeline p
        source from b :: "in"
        |> predict m
        |> filter msg.x > 0
        |> sink to b :: "out"
    end
    """
    assert _errors(src) == []


def test_predict_after_aggregate_error():
    src = """
    broker b host: "h" port: 1883 end
    model m source: "m.onnx" runtime: onnx features: [msg.x] returns: [y] end
    pipeline p
        source from b :: "in"
        |> window tumbling(1 m)
        |> aggregate { a = avg(msg.x) }
        |> predict m
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    real_errs = [e for e in errs if e.severity == "error"]
    assert any(
        "order" in str(e).lower() or "stage" in str(e).lower() for e in real_errs
    )


def test_duplicate_predict_stage():
    src = """
    broker b host: "h" port: 1883 end
    model m source: "m.onnx" runtime: onnx features: [msg.x] returns: [y] end
    pipeline p
        source from b :: "in"
        |> predict m
        |> predict m
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any(
        "duplicate" in str(e).lower() and "predict" in str(e).lower() for e in errs
    )


def test_predict_with_window_and_aggregate():
    src = """
    broker b host: "h" port: 1883 end
    model m source: "m.onnx" runtime: onnx features: [msg.x] returns: [score] end
    pipeline p
        source from b :: "in"
        |> predict m
        |> window tumbling(1 m)
        |> aggregate { avg_score = avg(msg.score) }
        |> sink to b :: "out"
    end
    """
    assert _errors(src) == []


def test_predict_with_downstream_map():
    src = """
    broker b host: "h" port: 1883 end
    model m source: "m.onnx" runtime: onnx features: [msg.x] returns: [score] end
    pipeline p
        source from b :: "in"
        |> predict m
        |> map { result = msg.score }
        |> sink to b :: "out"
    end
    """
    assert _errors(src) == []


def test_select_stage_valid():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> select [id, value, ts]
        |> sink to b :: "out"
    end
    """
    assert _errors(src) == []


def test_select_stage_with_rename_valid():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> select [out_id: raw_id, value]
        |> sink to b :: "out"
    end
    """
    assert _errors(src) == []


def test_select_and_map_conflict():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> select [id, value]
        |> map { result = msg.value }
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("cannot use both" in str(e) for e in errs)


def test_select_duplicate_stage():
    src = """
    broker b host: "h" port: 1883 end
    pipeline p
        source from b :: "in"
        |> select [id]
        |> select [value]
        |> sink to b :: "out"
    end
    """
    errs = _errors(src)
    assert any("select" in str(e).lower() for e in errs)


# ── Array indexing ──────────────────────────────────────────────────────────

BASE_SUB = 'broker b host: "h" port: 1883 end\n'


def test_subscript_no_error_in_filter():
    src = (
        BASE_SUB
        + """
pipeline p
    source from b :: "in"
    |> filter msg.items[0] > 0
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_subscript_warning_in_group_by():
    src = (
        BASE_SUB
        + """
pipeline p
    source from b :: "in"
    |> group_by msg.items[0]
    |> aggregate { n = count() }
    |> sink to b :: "out"
end
"""
    )
    warnings = [e for e in _errors(src) if e.severity == "warning"]
    assert any(
        "subscript" in e.message.lower() or "index" in e.message.lower()
        for e in warnings
    )


def test_subscript_warning_in_dedup():
    src = (
        BASE_SUB
        + """
pipeline p
    source from b :: "in"
    |> deduplicate msg.items[0] within 60 s
    |> sink to b :: "out"
end
"""
    )
    warnings = [e for e in _errors(src) if e.severity == "warning"]
    assert any(
        "subscript" in e.message.lower() or "index" in e.message.lower()
        for e in warnings
    )


# ── DebugStage ───────────────────────────────────────────────────────────────

BASE_DBG = 'broker b host: "h" port: 1883 end\n'


def test_debug_no_errors():
    src = (
        BASE_DBG
        + """
pipeline p
    source from b :: "in"
    |> debug label: "test"
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_multiple_debug_no_duplicate_error():
    src = (
        BASE_DBG
        + """
pipeline p
    source from b :: "in"
    |> debug
    |> filter msg.v > 0
    |> debug
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


# ── percentile / distinct_count ──────────────────────────────────────────────

BASE_PCT = 'broker b host: "h" port: 1883 end\n'


def test_percentile_no_errors():
    src = (
        BASE_PCT
        + """
pipeline p
    source from b :: "in"
    |> window tumbling(1 m)
    |> aggregate { p95 = percentile(msg.value, 95) }
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_percentile_missing_param_error():
    src = (
        BASE_PCT
        + """
pipeline p
    source from b :: "in"
    |> window tumbling(1 m)
    |> aggregate { p95 = percentile(msg.value) }
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert any(
        "percentile" in e.message.lower() or "param" in e.message.lower()
        for e in errors
    )


def test_distinct_count_no_errors():
    src = (
        BASE_PCT
        + """
pipeline p
    source from b :: "in"
    |> window tumbling(1 m)
    |> aggregate { n = distinct_count(msg.device_id) }
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


# ── ValidateStage ─────────────────────────────────────────────────────────────

BASE_VAL = """
broker b host: "h" port: 1883 end
schema S strict id: str value: float active: bool? end
"""


def test_validate_no_errors():
    src = (
        BASE_VAL
        + """
pipeline p
    source from b :: "in"
    |> validate schema: S on_error: drop
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_validate_no_on_error_ok():
    src = (
        BASE_VAL
        + """
pipeline p
    source from b :: "in"
    |> validate schema: S
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


# ── EnrichStage ──────────────────────────────────────────────────────────────

BASE_ENR = 'broker b host: "h" port: 1883 end\n'


def test_enrich_no_errors():
    src = (
        BASE_ENR
        + """
pipeline p
    source from b :: "in"
    |> enrich via http
        url: "https://api.example.com/"
        bind: meta
        on_error: skip
    end
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []


def test_multiple_enrich_no_duplicate_error():
    src = (
        BASE_ENR
        + """
pipeline p
    source from b :: "in"
    |> enrich via http
        url: "https://api.example.com/a"
        bind: a_data
    end
    |> enrich via http
        url: "https://api.example.com/b"
        bind: b_data
    end
    |> sink to b :: "out"
end
"""
    )
    errors = [e for e in _errors(src) if e.severity == "error"]
    assert errors == []
