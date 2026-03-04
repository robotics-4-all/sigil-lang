import pytest
from sigil.language import parse_string, build_metamodel


def _stages(pipeline):
    """Unwrap AnnotatedStage wrappers to get raw stage objects."""
    return [
        s.stage if s.__class__.__name__ == "AnnotatedStage" else s
        for s in pipeline.stages
    ]


MINIMAL = """
broker b1
    host: "localhost"
    port: 1883
end

pipeline p1
    source from b1 :: "t/in"
    |> sink to b1 :: "t/out"
end
    """


def test_parse_minimal():
    model = parse_string(MINIMAL)
    assert len(model.brokers) == 1
    assert len(model.pipelines) == 1
    assert model.brokers[0].name == "b1"
    assert model.brokers[0].host == "localhost"
    assert model.brokers[0].port == 1883


def test_broker_with_auth():
    src = """
    broker secure
        host: "mqtt.io"
        port: 8883
        ssl: true
        auth: {
            username: "admin"
            password: "pass"
        }
    end
    pipeline p
        source from secure :: "a"
        |> sink to secure :: "b"
    end
    """
    model = parse_string(src)
    b = model.brokers[0]
    assert b.ssl is True
    assert b.username == "admin"
    assert b.password == "pass"


def test_pipeline_with_all_stages():
    src = """
    broker b
        host: "localhost"
        port: 1883
    end
    pipeline full
        source from b :: "in" qos: 1 format: json

        |> filter: msg.value > 0 and msg.value < 100

        |> group_by: msg.sensor_id

        |> window: tumbling(5 m)

        |> aggregate: {
            avg_val = avg(msg.value)
            total   = sum(msg.value)
            n       = count()
        }

        |> map: {
            average = avg_val;
            sum_val = total;
            count   = n
        }

        |> sink to b :: "out" qos: 1
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert p.name == "full"
    assert p.source.topic == "in"
    assert p.source.qos == 1

    stage_names = [s.__class__.__name__ for s in _stages(p)]
    assert stage_names == [
        "FilterStage",
        "GroupByStage",
        "WindowStage",
        "AggregateStage",
        "MapStage",
    ]


def test_sliding_window():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: sliding(10 m, step = 2 m)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    w = _stages(model.pipelines[0])[0]
    assert w.__class__.__name__ == "WindowStage"
    assert w.type == "sliding"
    assert w.size.value == 10
    assert w.size.unit == "m"
    assert w.step.value == 2
    assert w.step.unit == "m"


def test_expression_comparison_operators():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.x >= 10
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_expression_arithmetic():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { r = avg(msg.v) }
        |> map: { fahrenheit = r * 1.8 + 32 }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "AggregateStage"
    assert stages[1].__class__.__name__ == "MapStage"


def test_parse_error_gives_message():
    with pytest.raises(Exception):
        parse_string("broker { }")


def test_multiple_brokers_and_pipelines():
    src = """
    broker a host: "a" port: 1 end
    broker b host: "b" port: 2 end
    pipeline p1
        source from a :: "x"
        |> sink to b :: "y"
    end
    pipeline p2
        source from b :: "y"
        |> sink to a :: "z"
    end
    """
    model = parse_string(src)
    assert len(model.brokers) == 2
    assert len(model.pipelines) == 2
    assert model.pipelines[0].source.broker.name == "a"
    assert model.pipelines[0].sink.broker.name == "b"


def test_line_comments():
    src = """
    // This is a line comment
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        // comment between stages
        |> sink to b :: "y"
    end
    """
    model = parse_string(src)
    assert len(model.pipelines) == 1


def test_block_comments():
    src = """
    /* Multi-line
       block comment */
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "x"
        |> sink to b :: "y"
    end
    """
    model = parse_string(src)
    assert len(model.pipelines) == 1


def test_inline_comment():
    src = """
    broker b host: "h" port: 1 end  // inline
    pipeline p
        source from b :: "x"
        |> sink to b :: "y"
    end
    """
    model = parse_string(src)
    assert model.brokers[0].name == "b"


def test_millisecond_duration():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(500 ms)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    w = _stages(model.pipelines[0])[0]
    assert w.__class__.__name__ == "WindowStage"
    assert w.size.value == 500
    assert w.size.unit == "ms"


def test_millisecond_does_not_break_minutes():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(5 m)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    w = _stages(model.pipelines[0])[0]
    assert w.size.value == 5
    assert w.size.unit == "m"


def test_metamodel_caching():
    mm1 = build_metamodel()
    mm2 = build_metamodel()
    assert mm1 is mm2


def test_broker_with_transport():
    src = """
    broker edge
        transport: mqtt
        host: "localhost"
        port: 1883
    end
    pipeline p
        source from edge :: "a"
        |> sink to edge :: "b"
    end
    """
    model = parse_string(src)
    assert model.brokers[0].transport == "mqtt"


def test_broker_with_redis_transport():
    src = """
    broker cache
        transport: redis
        host: "localhost"
        port: 6379
    end
    pipeline p
        source from cache :: "a"
        |> sink to cache :: "b"
    end
    """
    model = parse_string(src)
    assert model.brokers[0].transport == "redis"
    assert model.brokers[0].port == 6379


def test_broker_with_amqp_transport():
    src = """
    broker rabbit
        transport: amqp
        host: "rabbitmq.local"
        port: 5672
        auth: {
            username: "guest"
            password: "guest"
        }
    end
    pipeline p
        source from rabbit :: "a"
        |> sink to rabbit :: "b"
    end
    """
    model = parse_string(src)
    assert model.brokers[0].transport == "amqp"


def test_broker_with_kafka_transport():
    src = """
    broker stream
        transport: kafka
        host: "kafka.local"
        port: 9092
    end
    pipeline p
        source from stream :: "a"
        |> sink to stream :: "b"
    end
    """
    model = parse_string(src)
    assert model.brokers[0].transport == "kafka"


def test_broker_without_transport_defaults():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert not model.brokers[0].transport


def test_cross_transport_pipeline():
    src = """
    broker edge
        transport: mqtt
        host: "localhost"
        port: 1883
    end
    broker cloud
        transport: redis
        host: "redis.cloud.io"
        port: 6379
    end
    pipeline bridge
        source from edge :: "sensors/temp"
        |> sink to cloud :: "cloud.temp"
    end
    """
    model = parse_string(src)
    assert model.brokers[0].transport == "mqtt"
    assert model.brokers[1].transport == "redis"
    p = model.pipelines[0]
    assert p.source.broker.transport == "mqtt"
    assert p.sink.broker.transport == "redis"


# --- Regression tests: multi-assignment map blocks with arithmetic ---
# These guard against the PEG parser greedily consuming bare IDs across
# assignment boundaries.  The fix was MapAssignment[';'] separators.


def test_map_multi_assignment_with_arithmetic():
    """Multiple map assignments using arithmetic — the exact pattern that
    triggered the grammar bug before semicolons were added."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { s = sum(msg.v)  c = count() }
        |> map: {
            total = s * 1.0;
            samples = c
        }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert len(m.mappings) == 2
    assert m.mappings[0].target == "total"
    assert m.mappings[1].target == "samples"


def test_map_three_assignments_with_mixed_arithmetic():
    """Three map assignments mixing multiplication, addition, and bare refs."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: {
            a = avg(msg.x)
            s = sum(msg.x)
            n = count()
        }
        |> map: {
            mean   = a;
            total  = s * 2 + 1;
            count  = n
        }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert len(m.mappings) == 3
    assert [ma.target for ma in m.mappings] == ["mean", "total", "count"]


def test_map_single_assignment_no_semicolon():
    """Single map assignment needs no trailing semicolon."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { result = v * 100 }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert len(m.mappings) == 1
    assert m.mappings[0].target == "result"


def test_map_assignment_with_field_ref_arithmetic():
    """Map assignment mixing aggregated IDs and msg.field arithmetic."""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { t = sum(msg.price) }
        |> map: {
            cost  = t * 0.12;
            tax   = msg.rate * t;
            label = t
        }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert len(m.mappings) == 3
    assert [ma.target for ma in m.mappings] == ["cost", "tax", "label"]


def test_if_then_else_in_filter():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: if msg.x > 0 then 1 else 0
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_if_then_else_in_map():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { out = if v > 0 then v else 0 }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert m.__class__.__name__ == "MapStage"


def test_is_null():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.x is null
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_is_not_null():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.x is not null
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_null_coalesce_in_map():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { v = msg.x ?? 0 }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert m.__class__.__name__ == "MapStage"


def test_in_operator():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.status in ["active", "pending"]
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_not_in_operator():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.code not in [1, 2, 3]
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_scalar_func_abs_in_filter():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: abs(msg.x) > 10
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_scalar_func_round_in_map():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { r = round(v, 2) }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert m.__class__.__name__ == "MapStage"


def test_scalar_func_floor_in_map():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { f = floor(v) }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert m.__class__.__name__ == "MapStage"


def test_let_binding_single():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: {
            let ratio = v * 100;
            result = ratio
        }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert len(m.bindings) == 1
    assert len(m.mappings) == 1
    assert m.bindings[0].name == "ratio"


def test_let_binding_chained():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: {
            let a = v * 2;
            let b = a + 1;
            result = b
        }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert len(m.bindings) == 2


def test_let_binding_none():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { result = v }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert len(m.bindings) == 0


# --- Phase 2: const definitions ---


def test_const_int():
    src = """
    const THRESHOLD = 42
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert len(model.constants) == 1
    assert model.constants[0].name == "THRESHOLD"
    assert model.constants[0].value.val == 42.0


def test_const_float():
    src = """
    const RATIO = 3.14
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert model.constants[0].name == "RATIO"
    assert model.constants[0].value.val == 3.14


def test_const_string():
    src = """
    const LABEL = "high"
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert model.constants[0].name == "LABEL"
    assert model.constants[0].value.__class__.__name__ == "StringLiteral"
    assert model.constants[0].value.val == "high"


def test_const_bool():
    src = """
    const ENABLED = true
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert model.constants[0].name == "ENABLED"
    assert model.constants[0].value.__class__.__name__ == "BoolLiteral"
    assert model.constants[0].value.val is True


def test_const_multiple():
    src = """
    const A = 1
    const B = 2
    const C = "x"
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert len(model.constants) == 3
    assert [c.name for c in model.constants] == ["A", "B", "C"]


# --- Phase 2: env() references ---


def test_env_ref_no_default():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.x > env("THRESHOLD")
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_env_ref_with_default():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.x > env("THRESHOLD", 100)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


# --- Phase 2: case expressions ---


def test_case_single_arm():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { out = case { when v > 0 => "pos" else => "neg" } }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert m.__class__.__name__ == "MapStage"


def test_case_multiple_arms():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: {
            out = case {
                when v > 100 => "high"
                when v > 50  => "mid"
                when v > 0   => "low"
                else => "none"
            }
        }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert m.__class__.__name__ == "MapStage"


def test_case_no_else():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> aggregate: { v = avg(msg.x) }
        |> map: { out = case { when v > 0 => "pos" } }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    m = [s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"][
        0
    ]
    assert m.__class__.__name__ == "MapStage"


# --- Phase 2: having stage ---


def test_having_stage_parses():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(5 m)
        |> aggregate: { n = count() }
        |> having: n > 10
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    having = [s for s in stages if s.__class__.__name__ == "HavingStage"]
    assert len(having) == 1


def test_having_with_aggregate_and_map():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(5 m)
        |> aggregate: { n = count()  a = avg(msg.v) }
        |> having: n > 5
        |> map: { result = a }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    stage_names = [s.__class__.__name__ for s in _stages(model.pipelines[0])]
    assert "AggregateStage" in stage_names
    assert "HavingStage" in stage_names
    assert "MapStage" in stage_names


# --- Phase 2: deduplicate stage ---


def test_dedup_parses():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> deduplicate: msg.event_id within 30 s
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    d = _stages(model.pipelines[0])[0]
    assert d.__class__.__name__ == "DeduplicateStage"
    assert d.key.segments == ["msg", "event_id"]
    assert d.ttl.value == 30
    assert d.ttl.unit == "s"


def test_dedup_with_ms_ttl():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> deduplicate: msg.id within 500 ms
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    d = _stages(model.pipelines[0])[0]
    assert d.__class__.__name__ == "DeduplicateStage"
    assert d.ttl.value == 500
    assert d.ttl.unit == "ms"


# --- Phase 2: old() / changed() temporal references ---


def test_old_ref_in_filter():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.status != old(msg.status)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_changed_ref_in_filter():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: changed(msg.status)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


# --- Phase 3: forall/exists quantifiers ---


def test_forall_window_implicit():
    """forall(m => m > 0) — window-implicit quantifier"""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(10s)
        |> aggregate: { n = count() }
        |> having: forall(m => m > 0)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    having = [
        s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "HavingStage"
    ][0]
    assert having.__class__.__name__ == "HavingStage"


def test_exists_with_collection():
    """exists(x in msg.tags => x == 'critical') — collection-based quantifier"""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: exists(x in msg.tags => x == "critical")
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_forall_with_collection():
    """forall(item in msg.items => item > 0) — iterate array field"""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: forall(item in msg.values => item > 0)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    f = _stages(model.pipelines[0])[0]
    assert f.__class__.__name__ == "FilterStage"


def test_exists_window_implicit():
    """exists(m => m > 100) — window-implicit exists"""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(10s)
        |> aggregate: { n = count() }
        |> having: exists(m => m > 100)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    having = [
        s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "HavingStage"
    ][0]
    assert having.__class__.__name__ == "HavingStage"


def test_forall_and_exists_combined():
    """forall(...) and exists(...) combined in having"""
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(10s)
        |> aggregate: { n = count() }
        |> having: forall(m => m > 0) and exists(m => m > 100)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    having = [
        s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "HavingStage"
    ][0]
    assert having.__class__.__name__ == "HavingStage"


# --- Phase 3: @initially / @finally lifecycle hooks ---


def test_initially_hook():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        @initially { publish { status: "online" } }
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert p.initially is not None
    assert len(p.initially.body.fields) == 1
    assert p.initially.body.fields[0].name == "status"


def test_finally_hook():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
        @finally { publish { status: "offline" } }
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert p.finally_ is not None
    assert len(p.finally_.body.fields) == 1
    assert p.finally_.body.fields[0].name == "status"


def test_both_hooks():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        @initially { publish { status: "online", version: "1.0" } }
        source from b :: "a"
        |> sink to b :: "b"
        @finally { publish { status: "offline" } }
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert p.initially is not None
    assert p.finally_ is not None
    assert len(p.initially.body.fields) == 2


def test_no_hooks():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert not p.initially
    assert not p.finally_


def test_initially_with_env_ref():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        @initially { publish { host: env("HOSTNAME", "unknown"), status: "online" } }
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert p.initially is not None
    assert len(p.initially.body.fields) == 2


# --- Phase 3: sink format + batch ---


def test_sink_format():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" format: csv
    end
    """
    model = parse_string(src)
    assert model.pipelines[0].sink.format == "csv"


def test_sink_batch_size_only():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" batch: { size: 100 }
    end
    """
    model = parse_string(src)
    batch = model.pipelines[0].sink.batch
    assert batch is not None
    assert batch.size == 100


def test_sink_batch_timeout_only():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" batch: { timeout: 5s  }
    end
    """
    model = parse_string(src)
    batch = model.pipelines[0].sink.batch
    assert batch is not None
    assert batch.timeout.value == 5
    assert batch.timeout.unit == "s"


def test_sink_batch_both():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" format: json  batch: { size: 50  timeout: 10s  }
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert p.sink.format == "json"
    batch = p.sink.batch
    assert batch.size == 50
    assert batch.timeout.value == 10


def test_sink_no_batch():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert not model.pipelines[0].sink.batch
    assert not model.pipelines[0].sink.format


# --- Phase 4: import declarations ---


def test_import_declaration():
    src = """
    import "shared.sigil"
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert len(model.imports) == 1
    assert model.imports[0].path == "shared.sigil"


def test_multiple_imports():
    src = """
    import "types.sigil"
    import "utils.sigil"
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert len(model.imports) == 2
    assert model.imports[0].path == "types.sigil"
    assert model.imports[1].path == "utils.sigil"


def test_import_with_brokers():
    src = """
    import "shared.sigil"
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert len(model.imports) == 1
    assert len(model.brokers) == 1
    assert len(model.pipelines) == 1


# --- Phase 4: pipeline chaining (pipe source) ---


def test_pipe_source_parses():
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
    model = parse_string(src)
    ds = model.pipelines[1]
    assert ds.source.pipe == "upstream"
    assert not ds.source.broker


def test_pipe_source_no_topic():
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
    model = parse_string(src)
    ds = model.pipelines[1]
    assert not ds.source.topic


def test_broker_source_no_pipe():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    assert not model.pipelines[0].source.pipe
    assert model.pipelines[0].source.broker.name == "b"


# --- Phase 4: conditional routing ---


def test_route_single_arm():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> route
            when msg.x > 0 => sink to b :: "pos"
        end
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert p.route is not None
    assert len(p.route.arms) == 1
    assert p.route.default is None


def test_route_multiple_arms():
    src = """
    broker b host: "h" port: 1 end
    broker b2 host: "h2" port: 2 end
    pipeline p
        source from b :: "a"
        |> route
            when msg.x > 100 => sink to b :: "high"
            default => sink to b :: "other"
        end
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert len(p.route.arms) == 1
    assert p.route.default is not None
    assert p.route.default.topic == "other"


def test_route_without_default():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> route 
            when msg.x > 0 => sink to b :: "pos"
        end
    end
    """
    model = parse_string(src)
    arm = model.pipelines[0].route.arms[0]
    assert arm.condition is not None
    assert arm.sink.topic == "pos"


def test_route_no_sink():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> route 
            when msg.x > 0 => sink to b :: "pos"
        end
    end
    """
    model = parse_string(src)
    p = model.pipelines[0]
    assert not p.sink


# --- Phase 4: stage annotations ---


def test_warn_annotation():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> @warn("slow stage") filter: msg.x > 0
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    annotated = model.pipelines[0].stages[0]
    assert annotated.__class__.__name__ == "AnnotatedStage"
    assert len(annotated.annotations) == 1
    assert annotated.annotations[0].severity == "warn"
    assert annotated.annotations[0].message == "slow stage"


def test_error_annotation():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> @error("deprecated") filter: msg.x > 0
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    annotated = model.pipelines[0].stages[0]
    assert len(annotated.annotations) == 1
    assert annotated.annotations[0].severity == "error"
    assert annotated.annotations[0].message == "deprecated"


def test_multiple_annotations():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> @warn("slow") @error("deprecated") filter: msg.x > 0
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    annotated = model.pipelines[0].stages[0]
    assert len(annotated.annotations) == 2
    assert annotated.annotations[0].severity == "warn"
    assert annotated.annotations[1].severity == "error"


def test_no_annotations():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.x > 0
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    annotated = model.pipelines[0].stages[0]
    assert annotated.__class__.__name__ == "AnnotatedStage"
    assert len(annotated.annotations) == 0


# ---------------------------------------------------------------------------
# Parser regression: right-side arithmetic in comparisons (PEG fix)
# ---------------------------------------------------------------------------
# textX/Arpeggio PEG parser had a bug where ZeroOrMore repetition in
# Addition/Multiplication rules failed when Atom contained ID-matching
# alternatives (FieldRef). These tests guard the right-recursive grammar fix.


class TestRightSideArithmetic:
    """Regression tests for arithmetic on the right side of comparisons."""

    def test_multiply_right_of_comparison(self):
        """msg.x > 2 * 2 must parse (previously failed)."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x > 2 * 2
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_add_right_of_comparison(self):
        """msg.x > 2 + 3 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x > 2 + 3
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_complex_right_side_arithmetic(self):
        """msg.x > 2 * 3 + 1 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x > 2 * 3 + 1
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_field_ref_both_sides(self):
        """msg.x > msg.y * 2 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x > msg.y * 2
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_arithmetic_both_sides(self):
        """msg.x + 1 > msg.y * 2 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x + 1 > msg.y * 2
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_division_right_of_comparison(self):
        """msg.x < 100 / 3 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x < 100 / 3
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_modulo_right_of_comparison(self):
        """msg.x == 10 % 3 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x == 10 % 3
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_subtraction_right_of_comparison(self):
        """msg.x >= 10 - 3 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x >= 10 - 3
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_nested_arithmetic_in_and(self):
        """msg.x > 2 * 2 and msg.y < 10 + 5 must parse."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x > 2 * 2 and msg.y < 10 + 5
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None

    def test_parenthesized_still_works(self):
        """msg.x > (2 * 2) must still work (was the old workaround)."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x > (2 * 2)
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        assert model is not None


# ─── RateStage parse tests ──────────────────────────────────────────


class TestRateStage:
    def test_parse_rate_basic(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate: 100 per 1s
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        assert len(stages) == 1
        rate = stages[0]
        assert rate.__class__.__name__ == "RateStage"
        assert rate.limit == 100
        assert rate.period.value == 1
        assert rate.period.unit == "s"
        assert rate.on_excess is None

    def test_parse_rate_with_on_excess_drop(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate: 50 per 5m on_excess: drop
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        rate = _stages(model.pipelines[0])[0]
        assert rate.limit == 50
        assert rate.period.value == 5
        assert rate.period.unit == "m"
        assert rate.on_excess == "drop"

    def test_parse_rate_with_on_excess_warn(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate: 10 per 1s on_excess: warn
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        rate = _stages(model.pipelines[0])[0]
        assert rate.on_excess == "warn"

    def test_parse_rate_with_on_excess_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate: 1000 per 1h on_excess: error
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        rate = _stages(model.pipelines[0])[0]
        assert rate.on_excess == "error"
        assert rate.period.unit == "h"

    def test_rate_before_filter(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate: 100 per 1s
            |> filter: msg.x > 0
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        assert stages[0].__class__.__name__ == "RateStage"
        assert stages[1].__class__.__name__ == "FilterStage"

    def test_rate_with_milliseconds(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate: 10 per 500ms
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        rate = _stages(model.pipelines[0])[0]
        assert rate.period.value == 500
        assert rate.period.unit == "ms"


# ─── AccumulateStage parse tests ────────────────────────────────────


class TestAccumulateStage:
    def test_parse_accumulate_ema(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> window: tumbling(10s)
            |> aggregate: { avg_val = avg(msg.v) }
            |> accumulate: {
                smooth = ema(avg_val, 0.3)
            }
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        accum = [s for s in stages if s.__class__.__name__ == "AccumulateStage"][0]
        assert len(accum.assignments) == 1
        a = accum.assignments[0]
        assert a.name == "smooth"
        assert a.func == "ema"
        assert a.field.segments == ["avg_val"]
        assert a.param is not None

    def test_parse_accumulate_cumsum(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> window: tumbling(10s)
            |> aggregate: { total = sum(msg.v) }
            |> accumulate: {
                running = cumsum(total)
            }
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        accum = [
            s
            for s in _stages(model.pipelines[0])
            if s.__class__.__name__ == "AccumulateStage"
        ][0]
        a = accum.assignments[0]
        assert a.name == "running"
        assert a.func == "cumsum"
        assert a.param is None

    def test_parse_accumulate_delta(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> window: tumbling(10s)
            |> aggregate: { total = sum(msg.v) }
            |> accumulate: {
                change = delta(total)
            }
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        accum = [
            s
            for s in _stages(model.pipelines[0])
            if s.__class__.__name__ == "AccumulateStage"
        ][0]
        a = accum.assignments[0]
        assert a.name == "change"
        assert a.func == "delta"

    def test_parse_multiple_accumulations(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> window: tumbling(10s)
            |> aggregate: {
                avg_val = avg(msg.v)
                total = sum(msg.v)
            }
            |> accumulate: {
                smooth = ema(avg_val, 0.3)
                running = cumsum(total)
                change = delta(total)
            }
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        accum = [
            s
            for s in _stages(model.pipelines[0])
            if s.__class__.__name__ == "AccumulateStage"
        ][0]
        assert len(accum.assignments) == 3
        names = [a.name for a in accum.assignments]
        assert names == ["smooth", "running", "change"]

    def test_accumulate_after_aggregate(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> window: tumbling(10s)
            |> aggregate: { v = avg(msg.v) }
            |> accumulate: { s = ema(v, 0.5) }
            |> having: s > 10
            |> map: { result = s }
            |> sink to b :: "o"
        end
    """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        names = [s.__class__.__name__ for s in stages]
        assert names == [
            "WindowStage",
            "AggregateStage",
            "AccumulateStage",
            "HavingStage",
            "MapStage",
        ]


# ─── JoinStage parse tests ─────────────────────────────────────────


class TestJoinStage:
    def test_parse_inner_join(self):
        src = """
        broker local
            host: "localhost"
            port: 1883
        end
        broker remote
            host: "remote.io"
            port: 1883
        end
        pipeline p
            source from local :: "orders"
            |> join remote :: "customers"
                on msg.customer_id == other.id
                within 30 s
                type: inner
            end
            |> sink to local :: "enriched"
        end
    """
        model = parse_string(src)
        p = model.pipelines[0]
        join = p.join
        assert join is not None
        assert join.broker.name == "remote"
        assert join.topic == "customers"
        assert join.left_key.segments == ["msg", "customer_id"]
        assert join.right_key.segments == ["other", "id"]
        assert join.window.value == 30
        assert join.window.unit == "s"
        assert join.type == "inner"

    def test_parse_left_join(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "orders"
            |> join b :: "customers"
                on msg.cid == other.id
                within 10 s
                type: left
            end
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        join = model.pipelines[0].join
        assert join.type == "left"

    def test_parse_join_default_type(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> join b :: "b"
                on msg.id == other.id
                within 5 s
            end
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        join = model.pipelines[0].join
        assert join.type is None

    def test_parse_join_nested_keys(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> join b :: "b"
                on msg.user.address.id == other.profile.id
                within 10 s
            end
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        join = model.pipelines[0].join
        assert join.left_key.segments == ["msg", "user", "address", "id"]
        assert join.right_key.segments == ["other", "profile", "id"]

    def test_parse_join_with_filter(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> join b :: "b"
                on msg.id == other.id
                within 5 s
            end
            |> filter: msg.value > 0
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.join is not None
        stages = _stages(p)
        assert len(stages) == 1
        assert stages[0].__class__.__name__ == "FilterStage"

    def test_parse_join_with_map(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> join b :: "b"
                on msg.id == other.id
                within 5 s
            end
            |> aggregate: { n = count() }
            |> map: { total = n }
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.join is not None
        stage_names = [s.__class__.__name__ for s in _stages(p)]
        assert "AggregateStage" in stage_names
        assert "MapStage" in stage_names

    def test_parse_join_window_minutes(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> join b :: "b"
                on msg.id == other.id
                within 5 m
            end
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        join = model.pipelines[0].join
        assert join.window.value == 5
        assert join.window.unit == "m"

    def test_parse_join_window_milliseconds(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> join b :: "b"
                on msg.id == other.id
                within 500 ms
            end
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        join = model.pipelines[0].join
        assert join.window.value == 500
        assert join.window.unit == "ms"

    def test_join_no_stages(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> join b :: "b"
                on msg.id == other.id
                within 10 s
            end
            |> sink to b :: "out"
        end
    """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.join is not None
        assert _stages(p) == []
        assert p.sink is not None


class TestFmtFunction:
    """Tests for the fmt() string interpolation function."""

    def test_fmt_single_arg(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> map: {
                label = fmt("hello {}", msg.name)
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        map_stage = stages[0]
        func_call = map_stage.mappings[0].value
        # Navigate through expression tree to find the FuncCall
        atom = func_call.operands[0].operands[0].left.left.left.left.atom
        assert atom.__class__.__name__ == "FuncCall"
        assert atom.name == "fmt"
        assert len(atom.args) == 2

    def test_fmt_multiple_args(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> map: {
                label = fmt("{} is {} degrees", msg.sensor, msg.value)
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        map_stage = stages[0]
        atom = (
            map_stage.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        )
        assert atom.name == "fmt"
        assert len(atom.args) == 3

    def test_fmt_no_args(self):
        """fmt() with just a format string (no substitution args) is valid."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> map: {
                label = fmt("static text")
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        map_stage = stages[0]
        atom = (
            map_stage.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        )
        assert atom.name == "fmt"
        assert len(atom.args) == 1


class TestTypeAnnotations:
    """Tests for optional type annotations in aggregate assignments."""

    def test_aggregate_with_float_annotation(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10 s)
            |> aggregate: {
                total: float = sum(msg.value)
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = stages[1]
        assert agg.__class__.__name__ == "AggregateStage"
        assert agg.assignments[0].type == "float"
        assert agg.assignments[0].name == "total"

    def test_aggregate_with_int_annotation(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10 s)
            |> aggregate: {
                n: int = count()
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = stages[1]
        assert agg.assignments[0].type == "int"

    def test_aggregate_with_str_annotation(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10 s)
            |> aggregate: {
                first_name: str = first(msg.name)
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = stages[1]
        assert agg.assignments[0].type == "str"

    def test_aggregate_without_annotation(self):
        """Type annotation is optional — no annotation should still work."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10 s)
            |> aggregate: {
                total = sum(msg.value)
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = stages[1]
        assert agg.assignments[0].type is None or agg.assignments[0].type == ""

    def test_aggregate_mixed_annotated_and_not(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10 s)
            |> aggregate: {
                total: float = sum(msg.value)
                n = count()
                avg_val: float = avg(msg.value)
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = stages[1]
        assert len(agg.assignments) == 3
        assert agg.assignments[0].type == "float"
        assert agg.assignments[1].type is None or agg.assignments[1].type == ""
        assert agg.assignments[2].type == "float"

    def test_aggregate_bool_annotation(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10 s)
            |> aggregate: {
                has_data: bool = count()
            }
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = stages[1]
        assert agg.assignments[0].type == "bool"


# ─── Schema declaration parse tests ────────────────────────────────


class TestSchemaDeclaration:
    def test_parse_simple_schema(self):
        """Schema with scalar fields (int, float, str, bool)."""
        src = """
        schema Sensor
            temp: float
            humidity: float
            sensor_id: str
            active: bool
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        assert len(model.schemas) == 1
        s = model.schemas[0]
        assert s.name == "Sensor"
        assert len(s.fields) == 4
        assert s.fields[0].name == "temp"
        assert s.fields[0].type.__class__.__name__ == "SchemaScalarType"
        assert s.fields[0].type.name == "float"
        assert s.fields[1].name == "humidity"
        assert s.fields[2].name == "sensor_id"
        assert s.fields[2].type.name == "str"
        assert s.fields[3].name == "active"
        assert s.fields[3].type.name == "bool"

    def test_parse_schema_with_optional_fields(self):
        """Fields with ? modifier."""
        src = """
        schema Event
            id: int
            label: str?
            score: float?
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        s = model.schemas[0]
        assert not s.fields[0].optional
        assert s.fields[1].optional
        assert s.fields[2].optional

    def test_parse_schema_with_nested_object(self):
        """Schema containing { ... } nested object type."""
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
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        s = model.schemas[0]
        assert len(s.fields) == 2
        loc = s.fields[1]
        assert loc.name == "location"
        assert loc.type.__class__.__name__ == "SchemaObjectType"
        assert len(loc.type.fields) == 2
        assert loc.type.fields[0].name == "lat"
        assert loc.type.fields[1].name == "lon"

    def test_parse_schema_with_list_type(self):
        """Schema containing [str], [int] etc."""
        src = """
        schema Tagged
            name: str
            tags: [str]
            scores: [int]
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        s = model.schemas[0]
        tags = s.fields[1]
        assert tags.name == "tags"
        assert tags.type.__class__.__name__ == "SchemaListType"
        assert tags.type.element.__class__.__name__ == "SchemaScalarType"
        assert tags.type.element.name == "str"
        scores = s.fields[2]
        assert scores.type.__class__.__name__ == "SchemaListType"
        assert scores.type.element.name == "int"

    def test_parse_schema_strict(self):
        """Schema with explicit strict keyword."""
        src = """
        schema StrictSensor strict
            temp: float
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        assert model.schemas[0].strictness == "strict"

    def test_parse_schema_lenient(self):
        """Schema with explicit lenient keyword."""
        src = """
        schema LenientSensor lenient
            temp: float
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        assert model.schemas[0].strictness == "lenient"

    def test_parse_schema_default_strictness(self):
        """Schema without strictness keyword (defaults to None/empty)."""
        src = """
        schema DefaultSensor
            temp: float
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        s = model.schemas[0]
        assert not s.strictness or s.strictness is None

    def test_parse_source_with_schema_binding(self):
        """Source with schema: SchemaName."""
        src = """
        schema Sensor
            temp: float
            humidity: float
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in" schema: Sensor
            |> filter: msg.temp > 30.0
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.source.schema is not None
        assert p.source.schema.name == "Sensor"

    def test_parse_schema_complex_nested(self):
        """Schema with nested object containing list and optional fields."""
        src = """
        schema Reading
            value: float
            location: {
                lat: float,
                lon: float
            }
            tags: [str]
            metadata: {
                source: str,
                version: int
            }?
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        s = model.schemas[0]
        assert s.name == "Reading"
        assert len(s.fields) == 4
        assert s.fields[0].name == "value"
        assert s.fields[1].type.__class__.__name__ == "SchemaObjectType"
        assert s.fields[2].type.__class__.__name__ == "SchemaListType"
        assert s.fields[3].name == "metadata"
        assert s.fields[3].optional
        assert s.fields[3].type.__class__.__name__ == "SchemaObjectType"
        assert len(s.fields[3].type.fields) == 2


# ─── OnError parse tests ───────────────────────────────────────────


class TestOnError:
    def test_parse_on_error_sink(self):
        """Pipeline with on_error: sink to broker :: 'topic'."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> filter: msg.value > 0
            |> sink to b :: "out"
            on_error: sink to b :: "errors/p"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.on_error is not None
        assert p.on_error.__class__.__name__ == "OnErrorClause"
        assert p.on_error.broker is not None
        assert p.on_error.broker.name == "b"
        assert p.on_error.topic == "errors/p"

    def test_parse_on_error_drop(self):
        """Pipeline with on_error: drop."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> filter: msg.value > 0
            |> sink to b :: "out"
            on_error: drop
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.on_error is not None
        assert not p.on_error.broker
        assert not p.on_error.topic

    def test_parse_pipeline_without_on_error(self):
        """Pipeline without on_error (verify it's None/falsy)."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert not p.on_error

    def test_parse_on_error_with_finally(self):
        """Both on_error and @finally in same pipeline."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            on_error: sink to b :: "errors/p"
            @finally { publish { status: "offline" } }
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.on_error is not None
        assert p.on_error.broker.name == "b"
        assert p.on_error.topic == "errors/p"
        assert p.finally_ is not None
        assert p.finally_.body.fields[0].name == "status"


# ─── New stages: flatten, distinct, throttle, sample ────────────────


class TestNewStages:
    """Parse tests for FlattenStage, DistinctStage, ThrottleStage, SampleStage."""

    def test_flatten_basic(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> flatten: msg.items
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "FlattenStage"
        assert stage.field.segments == ["msg", "items"]

    def test_flatten_nested_field(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> flatten: msg.data.items
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "FlattenStage"
        assert stage.field.segments == ["msg", "data", "items"]

    def test_distinct_basic(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> distinct: msg.id within 5 m
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "DistinctStage"
        assert stage.field.segments == ["msg", "id"]
        assert stage.ttl.value == 5
        assert stage.ttl.unit == "m"

    def test_distinct_with_seconds(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> distinct: msg.event_id within 30 s
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "DistinctStage"
        assert stage.field.segments == ["msg", "event_id"]
        assert stage.ttl.value == 30
        assert stage.ttl.unit == "s"

    def test_throttle_basic(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> throttle: 100 per 1 s
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "ThrottleStage"
        assert stage.limit == 100
        assert stage.period.value == 1
        assert stage.period.unit == "s"
        assert stage.overflow is None
        assert stage.buffer_size == 0

    def test_throttle_with_overflow_drop(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> throttle: 50 per 10 s overflow: drop
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "ThrottleStage"
        assert stage.limit == 50
        assert stage.overflow == "drop"

    def test_throttle_with_buffer(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> throttle: 10 per 1 s overflow: buffer buffer: 500
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "ThrottleStage"
        assert stage.limit == 10
        assert stage.overflow == "buffer"
        assert stage.buffer_size == 500

    def test_sample_basic(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> sample: 50 %
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "SampleStage"
        assert stage.percentage == 50

    def test_sample_full(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> sample: 100 %
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "SampleStage"
        assert stage.percentage == 100

    def test_sample_minimal(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> sample: 1 %
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage = _stages(model.pipelines[0])[0]
        assert stage.__class__.__name__ == "SampleStage"
        assert stage.percentage == 1

    def test_new_stages_correct_order(self):
        """Parse pipeline with filter → flatten → distinct → throttle → sample → window → aggregate."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: msg.x > 0
            |> flatten: msg.items
            |> distinct: msg.id within 5 m
            |> throttle: 100 per 1 s
            |> sample: 50 %
            |> window: tumbling(5 m)
            |> aggregate: { n = count() }
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage_names = [s.__class__.__name__ for s in _stages(model.pipelines[0])]
        assert stage_names == [
            "FilterStage",
            "FlattenStage",
            "DistinctStage",
            "ThrottleStage",
            "SampleStage",
            "WindowStage",
            "AggregateStage",
        ]

    def test_all_stages_combined(self):
        """Parse pipeline with ALL stage types including the 4 new ones."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> deduplicate: msg.uid within 10 s
            |> filter: msg.x > 0
            |> rate: 100 per 1s
            |> flatten: msg.items
            |> distinct: msg.id within 5 m
            |> throttle: 50 per 10 s overflow: drop
            |> sample: 25 %
            |> group_by: msg.sensor_id
            |> window: tumbling(5 m)
            |> aggregate: { n = count() }
            |> map: { total = n }
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stage_names = [s.__class__.__name__ for s in _stages(model.pipelines[0])]
        assert stage_names == [
            "DeduplicateStage",
            "FilterStage",
            "RateStage",
            "FlattenStage",
            "DistinctStage",
            "ThrottleStage",
            "SampleStage",
            "GroupByStage",
            "WindowStage",
            "AggregateStage",
            "MapStage",
        ]


# ─── RetryPolicy parse tests ───────────────────────────────────────


class TestRetryPolicy:
    """Tests for the retry policy clause on pipelines."""

    def test_parse_retry_clause(self):
        """Parse retry clause with explicit values."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            retry: 5 with backoff 200 ms
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.retry is not None
        assert p.retry.max_retries == 5
        assert p.retry.base.value == 200
        assert p.retry.base.unit == "ms"

    def test_parse_pipeline_without_retry(self):
        """Pipeline without retry clause — backward compat."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert not p.retry

    def test_parse_retry_with_seconds(self):
        """Retry with seconds duration unit."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            retry: 3 with backoff 1 s
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.retry is not None
        assert p.retry.max_retries == 3
        assert p.retry.base.value == 1
        assert p.retry.base.unit == "s"

    def test_parse_retry_with_on_error(self):
        """Retry alongside on_error clause."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            on_error: drop
            retry: 5 with backoff 200 ms
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.on_error is not None
        assert p.retry is not None
        assert p.retry.max_retries == 5

    def test_parse_retry_with_finally(self):
        """Retry alongside @finally hook."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> sink to b :: "y"
            retry: 2 with backoff 500 ms
            @finally { publish { status: "offline" } }
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert p.retry is not None
        assert p.retry.max_retries == 2
        assert p.retry.base.value == 500
        assert p.retry.base.unit == "ms"
        assert p.finally_ is not None


# ─── MultiWindow parse tests ───────────────────────────────────────


class TestMultiWindow:
    """Tests for named windows and the 'over' clause in aggregations."""

    def test_parse_single_unnamed_window(self):
        """Single unnamed window — backward compat."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window: tumbling(1 m)
            |> aggregate: { n = count() }
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        w = [s for s in stages if s.__class__.__name__ == "WindowStage"][0]
        assert w.wname is None or w.wname == ""

    def test_parse_single_named_window(self):
        """Single named window."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short: tumbling(1 m)
            |> aggregate: { n = count() }
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        w = [s for s in stages if s.__class__.__name__ == "WindowStage"][0]
        assert w.wname == "short"

    def test_parse_multiple_named_windows(self):
        """Multiple named windows."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short: tumbling(1 m)
            |> window long: tumbling(5 m)
            |> aggregate: {
                avg_1m = avg(msg.value) over short
                avg_5m = avg(msg.value) over long
            }
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        windows = [s for s in stages if s.__class__.__name__ == "WindowStage"]
        assert len(windows) == 2
        assert windows[0].wname == "short"
        assert windows[1].wname == "long"

    def test_parse_over_in_agg_assignment(self):
        """Aggregate assignment with 'over' clause."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short: tumbling(1 m)
            |> aggregate: {
                avg_val = avg(msg.value) over short
            }
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = [s for s in stages if s.__class__.__name__ == "AggregateStage"][0]
        assert agg.assignments[0].window_ref == "short"

    def test_parse_agg_without_over(self):
        """Aggregate assignment without 'over' — backward compat."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window: tumbling(1 m)
            |> aggregate: { n = count() }
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = [s for s in stages if s.__class__.__name__ == "AggregateStage"][0]
        assert (
            agg.assignments[0].window_ref is None or agg.assignments[0].window_ref == ""
        )

    def test_parse_multi_window_mixed_types(self):
        """Multiple windows with different types (tumbling + sliding)."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window fast: tumbling(30 s)
            |> window slow: sliding(10 m, step = 2 m)
            |> aggregate: {
                avg_fast = avg(msg.value) over fast
                avg_slow = avg(msg.value) over slow
            }
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        windows = [s for s in stages if s.__class__.__name__ == "WindowStage"]
        assert len(windows) == 2
        assert windows[0].wname == "fast"
        assert windows[0].type == "tumbling"
        assert windows[1].wname == "slow"
        assert windows[1].type == "sliding"
        assert windows[1].step is not None

    def test_parse_multi_window_all_aggs_have_over(self):
        """All aggregate assignments reference a window via 'over'."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "x"
            |> window short: tumbling(1 m)
            |> window long: tumbling(5 m)
            |> aggregate: {
                n_short = count() over short
                n_long = count() over long
                avg_short = avg(msg.value) over short
            }
            |> sink to b :: "y"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        agg = [s for s in stages if s.__class__.__name__ == "AggregateStage"][0]
        assert len(agg.assignments) == 3
        assert agg.assignments[0].window_ref == "short"
        assert agg.assignments[1].window_ref == "long"
        assert agg.assignments[2].window_ref == "short"
