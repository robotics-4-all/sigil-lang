import pytest
from sigil.language import parse_string, build_metamodel, parse, CircularImportError


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

        |> filter msg.value > 0 and msg.value < 100

        |> group_by msg.sensor_id

        |> window tumbling(5 m)

        |> aggregate {
            avg_val = avg(msg.value)
            total   = sum(msg.value)
            n       = count()
        }

        |> map {
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
        |> window sliding(10 m, step = 2 m)
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
        |> filter msg.x >= 10
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
        |> aggregate { r = avg(msg.v) }
        |> map { fahrenheit = r * 1.8 + 32 }
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
        |> window tumbling(500 ms)
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
        |> window tumbling(5 m)
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
        |> aggregate { s = sum(msg.v)  c = count() }
        |> map {
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
        |> aggregate {
            a = avg(msg.x)
            s = sum(msg.x)
            n = count()
        }
        |> map {
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
        |> aggregate { v = avg(msg.x) }
        |> map { result = v * 100 }
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
        |> aggregate { t = sum(msg.price) }
        |> map {
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
        |> filter if msg.x > 0 then 1 else 0
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
        |> aggregate { v = avg(msg.x) }
        |> map { out = if v > 0 then v else 0 }
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
        |> filter msg.x is null
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
        |> filter msg.x is not null
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
        |> aggregate { v = avg(msg.x) }
        |> map { v = msg.x ?? 0 }
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
        |> filter msg.status in ["active", "pending"]
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
        |> filter msg.code not in [1, 2, 3]
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
        |> filter abs(msg.x) > 10
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
        |> aggregate { v = avg(msg.x) }
        |> map { r = round(v, 2) }
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
        |> aggregate { v = avg(msg.x) }
        |> map { f = floor(v) }
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
        |> aggregate { v = avg(msg.x) }
        |> map {
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
        |> aggregate { v = avg(msg.x) }
        |> map {
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
        |> aggregate { v = avg(msg.x) }
        |> map { result = v }
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
        |> filter msg.x > env("THRESHOLD")
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
        |> filter msg.x > env("THRESHOLD", 100)
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
        |> aggregate { v = avg(msg.x) }
        |> map { out = case { when v > 0 => "pos" else => "neg" } }
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
        |> aggregate { v = avg(msg.x) }
        |> map {
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
        |> aggregate { v = avg(msg.x) }
        |> map { out = case { when v > 0 => "pos" } }
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
        |> window tumbling(5 m)
        |> aggregate { n = count() }
        |> having n > 10
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
        |> window tumbling(5 m)
        |> aggregate { n = count()  a = avg(msg.v) }
        |> having n > 5
        |> map { result = a }
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
        |> deduplicate msg.event_id within 30 s
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
        |> deduplicate msg.id within 500 ms
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
        |> filter msg.status != old(msg.status)
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
        |> filter changed(msg.status)
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
        |> window tumbling(10s)
        |> aggregate { n = count() }
        |> having forall(m => m > 0)
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
        |> filter exists(x in msg.tags => x == "critical")
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
        |> filter forall(item in msg.values => item > 0)
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
        |> window tumbling(10s)
        |> aggregate { n = count() }
        |> having exists(m => m > 100)
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
        |> window tumbling(10s)
        |> aggregate { n = count() }
        |> having forall(m => m > 0) and exists(m => m > 100)
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
    assert p.route.default_sink is None


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
    assert p.route.default_sink is not None
    assert p.route.default_sink.topic == "other"


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
        |> @warn("slow stage") filter msg.x > 0
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
        |> @error("deprecated") filter msg.x > 0
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
        |> @warn("slow") @error("deprecated") filter msg.x > 0
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
        |> filter msg.x > 0
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
            |> filter msg.x > 2 * 2
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
            |> filter msg.x > 2 + 3
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
            |> filter msg.x > 2 * 3 + 1
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
            |> filter msg.x > msg.y * 2
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
            |> filter msg.x + 1 > msg.y * 2
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
            |> filter msg.x < 100 / 3
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
            |> filter msg.x == 10 % 3
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
            |> filter msg.x >= 10 - 3
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
            |> filter msg.x > 2 * 2 and msg.y < 10 + 5
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
            |> filter msg.x > (2 * 2)
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
            |> rate 100 per 1s
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
            |> rate 50 per 5m on_excess: drop
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
            |> rate 10 per 1s on_excess: warn
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
            |> rate 1000 per 1h on_excess: error
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
            |> rate 100 per 1s
            |> filter msg.x > 0
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
            |> rate 10 per 500ms
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
            |> window tumbling(10s)
            |> aggregate { avg_val = avg(msg.v) }
            |> accumulate {
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
            |> window tumbling(10s)
            |> aggregate { total = sum(msg.v) }
            |> accumulate {
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
            |> window tumbling(10s)
            |> aggregate { total = sum(msg.v) }
            |> accumulate {
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
            |> window tumbling(10s)
            |> aggregate {
                avg_val = avg(msg.v)
                total = sum(msg.v)
            }
            |> accumulate {
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
            |> window tumbling(10s)
            |> aggregate { v = avg(msg.v) }
            |> accumulate { s = ema(v, 0.5) }
            |> having s > 10
            |> map { result = s }
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
            |> filter msg.value > 0
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
            |> aggregate { n = count() }
            |> map { total = n }
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
            |> map {
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
            |> map {
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
            |> map {
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
            |> window tumbling(10 s)
            |> aggregate {
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
            |> window tumbling(10 s)
            |> aggregate {
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
            |> window tumbling(10 s)
            |> aggregate {
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
            |> window tumbling(10 s)
            |> aggregate {
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
            |> window tumbling(10 s)
            |> aggregate {
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
            |> window tumbling(10 s)
            |> aggregate {
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
            |> filter msg.temp > 30.0
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
            |> filter msg.value > 0
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
            |> filter msg.value > 0
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
            |> flatten msg.items
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
            |> flatten msg.data.items
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
            |> distinct msg.id within 5 m
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
            |> distinct msg.event_id within 30 s
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
            |> throttle 100 per 1 s
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
            |> throttle 50 per 10 s overflow: drop
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
            |> throttle 10 per 1 s overflow: buffer buffer: 500
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
            |> sample 50 %
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
            |> sample 100 %
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
            |> sample 1 %
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
            |> filter msg.x > 0
            |> flatten msg.items
            |> distinct msg.id within 5 m
            |> throttle 100 per 1 s
            |> sample 50 %
            |> window tumbling(5 m)
            |> aggregate { n = count() }
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
            |> deduplicate msg.uid within 10 s
            |> filter msg.x > 0
            |> rate 100 per 1s
            |> flatten msg.items
            |> distinct msg.id within 5 m
            |> throttle 50 per 10 s overflow: drop
            |> sample 25 %
            |> group_by msg.sensor_id
            |> window tumbling(5 m)
            |> aggregate { n = count() }
            |> map { total = n }
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
            |> window tumbling(1 m)
            |> aggregate { n = count() }
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
            |> window short tumbling(1 m)
            |> aggregate { n = count() }
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
            |> window short tumbling(1 m)
            |> window long tumbling(5 m)
            |> aggregate {
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
            |> window short tumbling(1 m)
            |> aggregate {
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
            |> window tumbling(1 m)
            |> aggregate { n = count() }
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
            |> window fast tumbling(30 s)
            |> window slow sliding(10 m, step = 2 m)
            |> aggregate {
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
            |> window short tumbling(1 m)
            |> window long tumbling(5 m)
            |> aggregate {
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


# ─── FuncDef (top-level user-defined functions) ─────────────────────


class TestFuncDef:
    def test_parse_func_single_param(self):
        """func with one parameter parses correctly."""
        src = """
        broker b host: "h" port: 1 end
        func celsius(t) = (t - 32) * 5 / 9
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        assert len(model.functions) == 1
        f = model.functions[0]
        assert f.name == "celsius"
        assert f.params == ["t"]
        assert f.body is not None

    def test_parse_func_multi_param(self):
        """func with multiple parameters."""
        src = """
        broker b host: "h" port: 1 end
        func add(a, b) = a + b
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        assert len(model.functions) == 1
        f = model.functions[0]
        assert f.name == "add"
        assert f.params == ["a", "b"]
        assert f.body is not None

    def test_parse_func_no_params(self):
        """func with zero parameters."""
        src = """
        broker b host: "h" port: 1 end
        func pi() = 3.14159
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        assert len(model.functions) == 1
        f = model.functions[0]
        assert f.name == "pi"
        assert f.params == []
        assert f.body is not None

    def test_parse_multiple_func_defs(self):
        """Multiple function definitions in one model."""
        src = """
        broker b host: "h" port: 1 end
        func celsius(t) = (t - 32) * 5 / 9
        func add(a, b) = a + b
        func pi() = 3.14159
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        assert len(model.functions) == 3
        assert model.functions[0].name == "celsius"
        assert model.functions[1].name == "add"
        assert model.functions[2].name == "pi"

    def test_parse_func_body_is_expression(self):
        """func body is a full expression (comparison)."""
        src = """
        broker b host: "h" port: 1 end
        func is_hot(t) = t > 100
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        f = model.functions[0]
        assert f.name == "is_hot"
        assert f.params == ["t"]
        # body is an Expression node (top of precedence chain)
        assert f.body.__class__.__name__ == "Expression"


# ─── PipelineLetBinding ─────────────────────────────────────────────


class TestPipelineLetBinding:
    def test_parse_single_let(self):
        """Single let binding before stages."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            let threshold = msg.value * 2
            |> filter msg.value > 10
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert len(p.pipeline_lets) == 1
        lb = p.pipeline_lets[0]
        assert lb.name == "threshold"
        assert lb.value is not None

    def test_parse_multiple_lets(self):
        """Multiple let bindings before stages."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            let low = 10
            let high = 100
            let mid = 50
            |> filter msg.value > 0
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert len(p.pipeline_lets) == 3
        assert p.pipeline_lets[0].name == "low"
        assert p.pipeline_lets[1].name == "high"
        assert p.pipeline_lets[2].name == "mid"

    def test_parse_let_with_field_ref(self):
        """Let binding using msg field reference."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            let x = msg.temperature
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert len(p.pipeline_lets) == 1
        lb = p.pipeline_lets[0]
        assert lb.name == "x"
        assert lb.value is not None

    def test_parse_let_with_complex_expression(self):
        """Let binding with arithmetic expression."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            let offset = (msg.value - 32) * 5 / 9
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert len(p.pipeline_lets) == 1
        assert p.pipeline_lets[0].name == "offset"
        assert p.pipeline_lets[0].value is not None

    def test_parse_pipeline_no_lets(self):
        """Pipeline without let bindings has empty list."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        p = model.pipelines[0]
        assert len(p.pipeline_lets) == 0


# ─── MatchExpr ──────────────────────────────────────────────────────


class TestMatchExpr:
    def test_parse_match_basic(self):
        """Basic match expression with two arms and else."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> aggregate { v = avg(msg.x) }
            |> map {
                out = match msg.type {
                    case "A" => 1
                    case "B" => 2
                    else => 0
                }
            }
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        m = [
            s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"
        ][0]
        # Navigate expression tree to the MatchExpr atom
        match_node = (
            m.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        )
        assert match_node.__class__.__name__ == "MatchExpr"
        assert len(match_node.arms) == 2
        assert match_node.elseExpr is not None

    def test_parse_match_without_else(self):
        """Match expression without else clause."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> aggregate { v = avg(msg.x) }
            |> map {
                out = match msg.type {
                    case "A" => 1
                    case "B" => 2
                }
            }
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        m = [
            s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"
        ][0]
        match_node = (
            m.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        )
        assert match_node.__class__.__name__ == "MatchExpr"
        assert len(match_node.arms) == 2
        assert match_node.elseExpr is None

    def test_parse_match_single_arm(self):
        """Match expression with a single arm."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> aggregate { v = avg(msg.x) }
            |> map {
                out = match msg.status {
                    case "ok" => 1
                    else => 0
                }
            }
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        m = [
            s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"
        ][0]
        match_node = (
            m.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        )
        assert match_node.__class__.__name__ == "MatchExpr"
        assert len(match_node.arms) == 1
        assert match_node.elseExpr is not None

    def test_parse_match_arm_structure(self):
        """MatchArm has pattern and result attributes."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> aggregate { v = avg(msg.x) }
            |> map {
                out = match msg.level {
                    case "high" => 100
                    case "low" => 10
                    else => 50
                }
            }
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        m = [
            s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"
        ][0]
        match_node = (
            m.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        )
        assert match_node.__class__.__name__ == "MatchExpr"
        arm0 = match_node.arms[0]
        assert arm0.__class__.__name__ == "MatchArm"
        assert arm0.pattern is not None
        assert arm0.result is not None

    def test_parse_match_subject_is_expression(self):
        """Match subject can be a complex expression."""
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "a"
            |> aggregate { v = avg(msg.x) }
            |> map {
                out = match msg.a + msg.b {
                    case 10 => "ten"
                    else => "other"
                }
            }
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        m = [
            s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"
        ][0]
        match_node = (
            m.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        )
        assert match_node.__class__.__name__ == "MatchExpr"
        assert match_node.subject is not None


# ─── UserFuncCall ───────────────────────────────────────────────────


class TestUserFuncCall:
    def test_parse_user_func_call_in_filter(self):
        """User function call in filter expression."""
        src = """
        broker b host: "h" port: 1 end
        func celsius(t) = (t - 32) * 5 / 9
        pipeline p
            source from b :: "a"
            |> filter celsius(msg.temp) > 0
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        f = _stages(model.pipelines[0])[0]
        assert f.__class__.__name__ == "FilterStage"
        # The filter condition is a Comparison: celsius(msg.temp) > 0
        # Left side: Addition -> Multiplication -> Coalesce -> Unary -> atom
        atom = f.condition.operands[0].operands[0].left.left.left.left.atom
        assert atom.__class__.__name__ == "UserFuncCall"
        assert atom.name == "celsius"
        assert len(atom.args) == 1

    def test_parse_user_func_call_multi_args(self):
        """User function call with multiple arguments."""
        src = """
        broker b host: "h" port: 1 end
        func add(a, b) = a + b
        pipeline p
            source from b :: "a"
            |> filter add(msg.x, msg.y) > 0
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        f = _stages(model.pipelines[0])[0]
        assert f.__class__.__name__ == "FilterStage"
        atom = f.condition.operands[0].operands[0].left.left.left.left.atom
        assert atom.__class__.__name__ == "UserFuncCall"
        assert atom.name == "add"
        assert len(atom.args) == 2

    def test_parse_user_func_call_in_map(self):
        """User function call in map expression."""
        src = """
        broker b host: "h" port: 1 end
        func double(x) = x * 2
        pipeline p
            source from b :: "a"
            |> map { result = double(msg.value) }
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        m = [
            s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"
        ][0]
        atom = m.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        assert atom.__class__.__name__ == "UserFuncCall"
        assert atom.name == "double"
        assert len(atom.args) == 1

    def test_parse_user_func_call_three_args(self):
        """User function call with three arguments."""
        src = """
        broker b host: "h" port: 1 end
        func clamp(val, lo, hi) = if val < lo then lo else if val > hi then hi else val
        pipeline p
            source from b :: "a"
            |> map { clamped = clamp(msg.value, 0, 100) }
            |> sink to b :: "b"
        end
        """
        model = parse_string(src)
        m = [
            s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage"
        ][0]
        atom = m.mappings[0].value.operands[0].operands[0].left.left.left.left.atom
        assert atom.__class__.__name__ == "UserFuncCall"
        assert atom.name == "clamp"
        assert len(atom.args) == 3


# ─── parse() and _resolve_imports() (language.py) ───────────────────


class TestParseFile:
    """Tests for parse(path) and _resolve_imports() in language.py."""

    def test_parse_file(self):
        """parse(path) reads a .sigil file and returns a model."""
        import tempfile, os

        src = 'broker b host: "h" port: 1 end\npipeline p\n    source from b :: "t"\n    |> sink to b :: "o"\nend\n'
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sigil", delete=False) as f:
            f.write(src)
            f.flush()
            path = f.name
        try:
            model = parse(path)
            assert len(model.brokers) == 1
            assert model.brokers[0].name == "b"
            assert len(model.pipelines) == 1
            assert model.pipelines[0].name == "p"
        finally:
            os.unlink(path)

    def test_parse_file_nonexistent_raises(self):
        """parse() with a non-existent path raises an error."""
        with pytest.raises(Exception):
            parse("/tmp/_sigil_nonexistent_file_12345.sigil")

    def test_import_resolves_broker_from_helper(self):
        """import merges brokers from the imported file into the model."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            helper = os.path.join(td, "helper.sigil")
            with open(helper, "w") as f:
                f.write('broker helper_broker host: "helper" port: 2 end\n')

            main = os.path.join(td, "main.sigil")
            with open(main, "w") as f:
                f.write(
                    'import "helper.sigil"\n'
                    'broker b host: "h" port: 1 end\n'
                    "pipeline p\n"
                    '    source from b :: "t"\n'
                    '    |> sink to b :: "o"\n'
                    "end\n"
                )

            model = parse(main)
            broker_names = {br.name for br in model.brokers}
            assert "b" in broker_names
            assert "helper_broker" in broker_names
            assert len(model.pipelines) == 1

    def test_import_file_not_found(self):
        """Importing a non-existent file raises FileNotFoundError."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            main = os.path.join(td, "main.sigil")
            with open(main, "w") as f:
                f.write(
                    'import "nonexistent.sigil"\n'
                    'broker b host: "h" port: 1 end\n'
                    "pipeline p\n"
                    '    source from b :: "t"\n'
                    '    |> sink to b :: "o"\n'
                    "end\n"
                )

            with pytest.raises(FileNotFoundError, match="nonexistent.sigil"):
                parse(main)

    def test_circular_import_direct(self):
        """Two files importing each other raises CircularImportError."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            a_path = os.path.join(td, "a.sigil")
            b_path = os.path.join(td, "b.sigil")

            with open(a_path, "w") as f:
                f.write(
                    'import "b.sigil"\n'
                    'broker ba host: "h" port: 1 end\n'
                    "pipeline pa\n"
                    '    source from ba :: "t"\n'
                    '    |> sink to ba :: "o"\n'
                    "end\n"
                )

            with open(b_path, "w") as f:
                f.write(
                    'import "a.sigil"\n'
                    'broker bb host: "h" port: 2 end\n'
                    "pipeline pb\n"
                    '    source from bb :: "t2"\n'
                    '    |> sink to bb :: "o2"\n'
                    "end\n"
                )

            with pytest.raises(CircularImportError, match="Circular import"):
                parse(a_path)

    def test_circular_import_self(self):
        """A file importing itself raises CircularImportError."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            self_path = os.path.join(td, "self.sigil")
            with open(self_path, "w") as f:
                f.write(
                    'import "self.sigil"\n'
                    'broker b host: "h" port: 1 end\n'
                    "pipeline p\n"
                    '    source from b :: "t"\n'
                    '    |> sink to b :: "o"\n'
                    "end\n"
                )

            with pytest.raises(CircularImportError, match="Circular import"):
                parse(self_path)

    def test_import_merges_constants_and_schemas(self):
        """Import merges brokers, constants, and schemas from helper."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            helper = os.path.join(td, "helper.sigil")
            with open(helper, "w") as f:
                f.write(
                    'broker helper_b host: "h2" port: 2 end\n'
                    "const THRESHOLD = 42\n"
                    "schema sensor\n"
                    "    value: float\n"
                    "end\n"
                )

            main = os.path.join(td, "main.sigil")
            with open(main, "w") as f:
                f.write(
                    'import "helper.sigil"\n'
                    'broker b host: "h" port: 1 end\n'
                    "pipeline p\n"
                    '    source from b :: "t"\n'
                    '    |> sink to b :: "o"\n'
                    "end\n"
                )

            model = parse(main)
            broker_names = {br.name for br in model.brokers}
            assert "b" in broker_names
            assert "helper_b" in broker_names
            assert len(model.constants) == 1
            assert model.constants[0].name == "THRESHOLD"
            assert len(model.schemas) == 1
            assert model.schemas[0].name == "sensor"
            assert len(model.pipelines) == 1

    def test_import_merges_pipelines(self):
        """Imported pipelines are merged into the main model."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            helper = os.path.join(td, "helper.sigil")
            with open(helper, "w") as f:
                f.write(
                    'broker hb host: "h" port: 1 end\n'
                    "pipeline helper_pipe\n"
                    '    source from hb :: "t"\n'
                    '    |> sink to hb :: "o"\n'
                    "end\n"
                )

            main = os.path.join(td, "main.sigil")
            with open(main, "w") as f:
                f.write(
                    'import "helper.sigil"\n'
                    'broker mb host: "h" port: 2 end\n'
                    "pipeline main_pipe\n"
                    '    source from mb :: "t2"\n'
                    '    |> sink to mb :: "o2"\n'
                    "end\n"
                )

            model = parse(main)
            pipe_names = {p.name for p in model.pipelines}
            assert pipe_names == {"main_pipe", "helper_pipe"}

    def test_transitive_imports(self):
        """a imports b, b imports c — transitive merge works."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            c_path = os.path.join(td, "c.sigil")
            with open(c_path, "w") as f:
                f.write('broker cb host: "h" port: 1 end\nconst BASE = 10\n')

            b_path = os.path.join(td, "b.sigil")
            with open(b_path, "w") as f:
                f.write(
                    'import "c.sigil"\n'
                    'broker bb host: "h" port: 2 end\n'
                    "const MULTIPLIER = 5\n"
                    "pipeline bp\n"
                    '    source from bb :: "t"\n'
                    '    |> sink to bb :: "o"\n'
                    "end\n"
                )

            a_path = os.path.join(td, "a.sigil")
            with open(a_path, "w") as f:
                f.write(
                    'import "b.sigil"\n'
                    'broker ab host: "h" port: 3 end\n'
                    "pipeline ap\n"
                    '    source from ab :: "t2"\n'
                    '    |> sink to ab :: "o2"\n'
                    "end\n"
                )

            model = parse(a_path)
            # Broker from c.sigil should be transitively available
            broker_names = {br.name for br in model.brokers}
            assert "cb" in broker_names
            assert "bb" in broker_names
            assert "ab" in broker_names
            # Constants from both c and b
            const_names = {c.name for c in model.constants}
            assert "BASE" in const_names
            assert "MULTIPLIER" in const_names
            # Pipelines from both b and a
            pipe_names = {p.name for p in model.pipelines}
            assert pipe_names == {"bp", "ap"}

    def test_import_no_imports_is_noop(self):
        """File with no import declarations — _resolve_imports is a no-op."""
        import tempfile, os

        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "standalone.sigil")
            with open(path, "w") as f:
                f.write(
                    'broker b host: "h" port: 1 end\n'
                    "pipeline p\n"
                    '    source from b :: "t"\n'
                    '    |> sink to b :: "o"\n'
                    "end\n"
                )

            model = parse(path)
            assert len(model.brokers) == 1
            assert len(model.pipelines) == 1
            assert len(model.constants) == 0
            assert len(model.schemas) == 0


PREDICT_BASE = """
broker b host: "localhost" port: 1883 end
"""


def test_parse_model_def_full():
    src = (
        PREDICT_BASE
        + """
model det
    source: "models/anomaly.onnx"
    runtime: onnx
    features: [msg.temperature, msg.humidity, msg.pressure]
    returns: [anomaly_score, is_anomaly]
end
pipeline p
    source from b :: "in"
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    assert len(model.models) == 1
    m = model.models[0]
    assert m.name == "det"
    assert m.runtime == "onnx"
    assert len(m.features) == 3
    assert m.features[0].segments == ["msg", "temperature"]
    assert m.features[1].segments == ["msg", "humidity"]
    assert m.features[2].segments == ["msg", "pressure"]
    assert m.returns == ["anomaly_score", "is_anomaly"]


def test_parse_model_def_minimal():
    src = (
        PREDICT_BASE
        + """
model m
    source: "model.pkl"
    runtime: sklearn
end
pipeline p
    source from b :: "in"
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    assert len(model.models) == 1
    m = model.models[0]
    assert m.name == "m"
    assert m.runtime == "sklearn"
    assert m.features == []
    assert m.returns == []


def test_parse_model_runtime_onnx():
    src = (
        PREDICT_BASE
        + """
model m source: "x" runtime: onnx end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    assert parse_string(src).models[0].runtime == "onnx"


def test_parse_model_runtime_sklearn():
    src = (
        PREDICT_BASE
        + """
model m source: "x" runtime: sklearn end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    assert parse_string(src).models[0].runtime == "sklearn"


def test_parse_model_runtime_torch():
    src = (
        PREDICT_BASE
        + """
model m source: "x" runtime: torch end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    assert parse_string(src).models[0].runtime == "torch"


def test_parse_model_runtime_tflite():
    src = (
        PREDICT_BASE
        + """
model m source: "x" runtime: tflite end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    assert parse_string(src).models[0].runtime == "tflite"


def test_parse_model_runtime_custom():
    src = (
        PREDICT_BASE
        + """
model m source: "x" runtime: custom end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    assert parse_string(src).models[0].runtime == "custom"


def test_parse_predict_stage():
    src = (
        PREDICT_BASE
        + """
model det source: "m.onnx" runtime: onnx features: [msg.x] returns: [y] end
pipeline p
    source from b :: "in"
    |> predict det
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert len(stages) == 1
    assert stages[0].__class__.__name__ == "PredictStage"
    assert stages[0].model.name == "det"


def test_parse_predict_with_filter():
    src = (
        PREDICT_BASE
        + """
model det source: "m.onnx" runtime: onnx features: [msg.x] returns: [y] end
pipeline p
    source from b :: "in"
    |> filter msg.x > 0
    |> predict det
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert len(stages) == 2
    assert stages[0].__class__.__name__ == "FilterStage"
    assert stages[1].__class__.__name__ == "PredictStage"


def test_predict_model_cross_reference():
    src = (
        PREDICT_BASE
        + """
model my_model source: "m.bin" runtime: sklearn features: [msg.a] returns: [b] end
pipeline p
    source from b :: "in"
    |> predict my_model
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stage = _stages(model.pipelines[0])[0]
    assert stage.model is model.models[0]
    assert stage.model.name == "my_model"


def test_model_source_as_env():
    src = (
        PREDICT_BASE
        + """
model m source: env("MODEL_PATH", "default.onnx") runtime: onnx features: [msg.x] returns: [y] end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    model = parse_string(src)
    m = model.models[0]
    assert m.source is not None


def test_model_multiple_inputs():
    src = (
        PREDICT_BASE
        + """
model m
    source: "m.onnx"
    runtime: onnx
    features: [msg.a, msg.b.c, msg.d]
    returns: [out]
end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    model = parse_string(src)
    m = model.models[0]
    assert len(m.features) == 3
    assert m.features[1].segments == ["msg", "b", "c"]


def test_model_multiple_outputs():
    src = (
        PREDICT_BASE
        + """
model m
    source: "m.onnx"
    runtime: onnx
    features: [msg.x]
    returns: [a, b, c, d]
end
pipeline p source from b :: "in" |> sink to b :: "out" end
"""
    )
    model = parse_string(src)
    assert model.models[0].returns == ["a", "b", "c", "d"]


def test_predict_and_downstream_map():
    src = (
        PREDICT_BASE
        + """
model det source: "m.onnx" runtime: onnx features: [msg.x] returns: [score] end
pipeline p
    source from b :: "in"
    |> predict det
    |> map {
        result = msg.score
    }
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert len(stages) == 2
    assert stages[0].__class__.__name__ == "PredictStage"
    assert stages[1].__class__.__name__ == "MapStage"


def test_multiple_models():
    src = (
        PREDICT_BASE
        + """
model m1 source: "a.onnx" runtime: onnx features: [msg.x] returns: [y] end
model m2 source: "b.pkl" runtime: sklearn features: [msg.a] returns: [b] end
pipeline p
    source from b :: "in"
    |> predict m1
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    assert len(model.models) == 2
    assert model.models[0].name == "m1"
    assert model.models[1].name == "m2"


def test_pipeline_no_sink():
    m = parse_string("""
        broker local
            host: "localhost"
            port: 1883
        end
        pipeline hub
            source from local :: "input/data"
            |> filter msg.value > 0
        end
    """)
    assert m.pipelines[0].sink is None
    assert m.pipelines[0].route is None


def test_cross_file_broker_reference():
    import tempfile, os

    with tempfile.TemporaryDirectory() as td:
        brokers_file = os.path.join(td, "brokers.sigil")
        with open(brokers_file, "w") as f:
            f.write('broker remote host: "remote.host" port: 1883 end\n')

        main_file = os.path.join(td, "main.sigil")
        with open(main_file, "w") as f:
            f.write(
                'import "brokers.sigil"\n'
                'broker local host: "localhost" port: 1883 end\n'
                "pipeline p\n"
                '    source from remote :: "input/topic"\n'
                '    |> sink to local :: "output/topic"\n'
                "end\n"
            )

        model = parse(main_file)
        assert len(model.pipelines) == 1
        p = model.pipelines[0]
        assert p.source.broker.name == "remote"
        assert p.sink.broker.name == "local"


SELECT_BASE = """
broker b host: "localhost" port: 1883 end
"""


def test_parse_select_passthrough_fields():
    src = (
        SELECT_BASE
        + """
pipeline p
    source from b :: "in"
    |> select [field1, field2, field3]
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert len(stages) == 1
    assert stages[0].__class__.__name__ == "SelectStage"
    fields = stages[0].fields
    assert len(fields) == 3
    assert fields[0].name == "field1"
    assert fields[0].alias == ""
    assert fields[1].name == "field2"
    assert fields[2].name == "field3"


def test_parse_select_rename_fields():
    src = (
        SELECT_BASE
        + """
pipeline p
    source from b :: "in"
    |> select [out_name: source_name, other: orig]
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "SelectStage"
    fields = stages[0].fields
    assert len(fields) == 2
    assert fields[0].alias == "out_name"
    assert fields[0].source == "source_name"
    assert fields[1].alias == "other"
    assert fields[1].source == "orig"


def test_parse_select_mixed_fields():
    src = (
        SELECT_BASE
        + """
pipeline p
    source from b :: "in"
    |> select [id, value: raw_value, ts]
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    fields = stages[0].fields
    assert len(fields) == 3
    assert fields[0].name == "id"
    assert fields[1].alias == "value"
    assert fields[1].source == "raw_value"
    assert fields[2].name == "ts"


def test_parse_select_single_field():
    src = (
        SELECT_BASE
        + """
pipeline p
    source from b :: "in"
    |> select [value]
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "SelectStage"
    assert len(stages[0].fields) == 1


# ── Array indexing ──────────────────────────────────────────────────────────

BASE_SUB = """broker b host: "h" port: 1883 end\n"""


def test_parse_field_subscript_positive():
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
    model = parse_string(src)
    assert model is not None


def test_parse_field_subscript_negative():
    src = (
        BASE_SUB
        + """
pipeline p
    source from b :: "in"
    |> filter msg.readings[-1] > 0
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    assert model is not None


def test_parse_field_subscript_nested():
    src = (
        BASE_SUB
        + """
pipeline p
    source from b :: "in"
    |> filter msg.data.items[2] > 0
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    assert model is not None


def test_parse_field_subscript_in_map():
    src = (
        BASE_SUB
        + """
pipeline p
    source from b :: "in"
    |> map { first = msg.items[0]; last = msg.items[-1] }
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    assert model is not None


# ── DebugStage ───────────────────────────────────────────────────────────────

BASE_DBG = """broker b host: "h" port: 1883 end\n"""


def test_parse_debug_bare():
    src = (
        BASE_DBG
        + """
pipeline p
    source from b :: "in"
    |> debug
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "DebugStage"


def test_parse_debug_with_label():
    src = (
        BASE_DBG
        + """
pipeline p
    source from b :: "in"
    |> debug label: "checkpoint"
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].label == "checkpoint"


def test_parse_debug_with_fields():
    src = (
        BASE_DBG
        + """
pipeline p
    source from b :: "in"
    |> debug label: "arrived", fields: [msg.id, msg.value]
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert len(stages[0].fields) == 2


def test_parse_multiple_debug_stages():
    src = (
        BASE_DBG
        + """
pipeline p
    source from b :: "in"
    |> debug label: "pre"
    |> filter msg.v > 0
    |> debug label: "post"
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    debug_stages = [s for s in stages if s.__class__.__name__ == "DebugStage"]
    assert len(debug_stages) == 2


# ── percentile / distinct_count ──────────────────────────────────────────────

BASE_PCT = """broker b host: "h" port: 1883 end\n"""


def test_parse_percentile_aggregation():
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
    model = parse_string(src)
    assert model is not None


def test_parse_distinct_count_aggregation():
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
    model = parse_string(src)
    assert model is not None


def test_parse_percentile_and_regular_aggs():
    src = (
        BASE_PCT
        + """
pipeline p
    source from b :: "in"
    |> window tumbling(1 m)
    |> aggregate {
        p95 = percentile(msg.value, 95)
        p50 = percentile(msg.value, 50)
        n = distinct_count(msg.device_id)
        total = sum(msg.value)
    }
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    assert model is not None


# ── ValidateStage ─────────────────────────────────────────────────────────────

BASE_VAL = """
broker b host: "h" port: 1883 end
schema S strict id: str value: float active: bool? end
"""


def test_parse_validate_drop():
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
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "ValidateStage"
    assert stages[0].schema.name == "S"


def test_parse_validate_skip():
    src = (
        BASE_VAL
        + """
pipeline p
    source from b :: "in"
    |> validate schema: S on_error: skip
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "ValidateStage"


def test_parse_validate_no_on_error():
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
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "ValidateStage"


def test_parse_validate_sink_to_broker():
    src = (
        BASE_VAL
        + """
pipeline p
    source from b :: "in"
    |> validate schema: S on_error: sink to b :: "dlq/errors"
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "ValidateStage"


# ── EnrichStage ──────────────────────────────────────────────────────────────

BASE_ENR = """broker b host: "h" port: 1883 end\n"""


def test_parse_enrich_minimal():
    src = (
        BASE_ENR
        + """
pipeline p
    source from b :: "in"
    |> enrich via http
        url: "https://api.example.com/data"
        bind: info
    end
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "EnrichStage"
    assert stages[0].bind == "info"


def test_parse_enrich_with_method_and_timeout():
    src = (
        BASE_ENR
        + """
pipeline p
    source from b :: "in"
    |> enrich via http
        url: "https://api.example.com/data"
        method: GET
        timeout: 2 s
        bind: meta
        on_error: skip
    end
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "EnrichStage"


def test_parse_enrich_on_error_default():
    src = (
        BASE_ENR
        + """
pipeline p
    source from b :: "in"
    |> enrich via http
        url: "https://api.example.com/data"
        bind: meta
        on_error: default { name: "unknown" }
    end
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    assert stages[0].__class__.__name__ == "EnrichStage"


def test_parse_multiple_enrich_stages():
    src = (
        BASE_ENR
        + """
pipeline p
    source from b :: "in"
    |> enrich via http
        url: "https://api.example.com/a"
        bind: meta_a
    end
    |> enrich via http
        url: "https://api.example.com/b"
        bind: meta_b
    end
    |> sink to b :: "out"
end
"""
    )
    model = parse_string(src)
    stages = _stages(model.pipelines[0])
    enrich_stages = [s for s in stages if s.__class__.__name__ == "EnrichStage"]
    assert len(enrich_stages) == 2
