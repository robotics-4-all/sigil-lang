import pytest

from sigil.language import parse_string
from sigil.visualize import visualize_dot, visualize_mermaid


# ─── Helpers ────────────────────────────────────────────────────────

MINIMAL = """
broker b host: "localhost" port: 1883 end

pipeline passthrough
    source from b :: "in"
    |> sink to b :: "out"
end
"""

FULL_PIPELINE = """
broker b host: "localhost" port: 1883 end

pipeline full
    source from b :: "sensors/raw" format: json

    |> deduplicate msg.event_id within 10 s

    |> filter msg.value > 0

    |> group_by msg.sensor_id

    |> window tumbling(5 m)

    |> aggregate {
        avg_val = avg(msg.value)
        n = count()
    }

    |> having n > 3

    |> map {
        average = avg_val;
        samples = n
    }

    |> sink to b :: "sensors/clean"
end
"""

MULTI_PIPELINE = """
broker b1 host: "localhost" port: 1883 end

broker b2 host: "remote" port: 1884 end

pipeline first
    source from b1 :: "a"
    |> sink to b1 :: "b"
end

pipeline second
    source from b2 :: "c"
    |> sink to b2 :: "d"
end
"""

CHAINED = """
broker b host: "localhost" port: 1883 end

pipeline upstream
    source from b :: "raw"
    |> sink to b :: "clean"
end

pipeline downstream
    source from pipe upstream
    |> sink to b :: "final"
end
"""

ROUTING = """
broker local host: "localhost" port: 1883 end

broker alerts
    transport: redis
    host: "redis.local"
    port: 6379
end

pipeline classify
    source from local :: "sensors/all"

    |> filter msg.value is not null

    |> route
        when msg.value > 90 => sink to alerts :: "priority.high"
        when msg.value > 50 => sink to local :: "priority/medium"
        default => sink to local :: "priority/low"
    end
end
"""

CONSTANTS = """
const THRESHOLD = 42
const NAME = "sensor"
const ENABLED = true

broker b host: "localhost" port: 1883 end

pipeline p
    source from b :: "in"
    |> filter msg.value > THRESHOLD
    |> sink to b :: "out"
end
"""

ANNOTATIONS = """
broker b host: "localhost" port: 1883 end

pipeline annotated
    source from b :: "in"

    |> @warn("large window may cause memory pressure") window tumbling(1 h)

    |> aggregate {
        avg_val = avg(msg.value)
        n = count()
    }

    |> map {
        average = avg_val;
        samples = n
    }

    |> sink to b :: "out"
end
"""

HOOKS = """
broker b host: "localhost" port: 1883 end

pipeline hooked
    @initially { publish { status: "online", version: "1.0" } }

    source from b :: "in"

    |> sink to b :: "out"

    @finally { publish { status: "offline" } }
end
"""

MULTI_TRANSPORT = """
broker mqtt_broker
    transport: mqtt
    host: "mqtt.local"
    port: 1883
end

broker redis_broker
    transport: redis
    host: "redis.local"
    port: 6379
end

pipeline cross
    source from mqtt_broker :: "input"
    |> sink to redis_broker :: "output"
end
"""

SLIDING_WINDOW = """
broker b host: "localhost" port: 1883 end

pipeline sliding
    source from b :: "in"

    |> window sliding(10 m, step = 2 m)

    |> aggregate {
        total = sum(msg.value)
    }

    |> map {
        result = total
    }

    |> sink to b :: "out"
end
"""

JOIN_PIPELINE = """
broker local host: "localhost" port: 1883 end

pipeline enrich
    source from local :: "orders/new"
    |> join local :: "customers/profiles"
        on msg.customer_id == other.id
        within 30 s
        type: inner
    end
    |> filter other.active == true
    |> map {
        order_id = msg.order_id;
        customer_name = other.name
    }
    |> sink to local :: "orders/enriched"
end
"""


# ─── DOT tests ──────────────────────────────────────────────────────


class TestVisualizeDot:
    def test_simple_passthrough(self):
        model = parse_string(MINIMAL)
        dot = visualize_dot(model)
        assert "digraph" in dot
        assert "rankdir=LR" in dot
        assert "passthrough" in dot
        assert "source" in dot
        assert "sink" in dot
        assert "in" in dot
        assert "out" in dot

    def test_simple_passthrough_edges(self):
        model = parse_string(MINIMAL)
        dot = visualize_dot(model)
        # Source and sink should be connected
        assert "p_passthrough_source" in dot
        assert "p_passthrough_sink" in dot
        assert "p_passthrough_source -> p_passthrough_sink" in dot

    def test_broker_node(self):
        model = parse_string(MINIMAL)
        dot = visualize_dot(model)
        assert "broker_b" in dot
        assert "shape=cylinder" in dot
        assert "localhost" in dot
        assert "1883" in dot

    def test_full_pipeline_all_stages(self):
        model = parse_string(FULL_PIPELINE)
        dot = visualize_dot(model)
        assert "deduplicate" in dot
        assert "filter" in dot
        assert "group_by" in dot
        assert "window" in dot
        assert "aggregate" in dot
        assert "having" in dot
        assert "map" in dot

    def test_full_pipeline_stage_details(self):
        model = parse_string(FULL_PIPELINE)
        dot = visualize_dot(model)
        assert "event_id" in dot
        assert "10s" in dot
        assert "sensor_id" in dot
        assert "tumbling" in dot
        assert "5m" in dot
        assert "avg" in dot
        assert "count" in dot
        assert "average" in dot
        assert "samples" in dot

    def test_full_pipeline_stage_shapes(self):
        model = parse_string(FULL_PIPELINE)
        dot = visualize_dot(model)
        assert "shape=box" in dot
        assert "shape=cds" in dot

    def test_multi_pipeline_subgraphs(self):
        model = parse_string(MULTI_PIPELINE)
        dot = visualize_dot(model)
        assert "subgraph cluster_first" in dot
        assert "subgraph cluster_second" in dot
        assert "broker_b1" in dot
        assert "broker_b2" in dot

    def test_pipeline_chaining_dashed_edge(self):
        model = parse_string(CHAINED)
        dot = visualize_dot(model)
        assert "style=dashed" in dot
        assert "pipe" in dot
        assert "p_upstream_sink" in dot
        assert "p_downstream_source" in dot

    def test_pipeline_chaining_pipe_source_label(self):
        model = parse_string(CHAINED)
        dot = visualize_dot(model)
        assert "pipe: upstream" in dot

    def test_conditional_routing_diamond(self):
        model = parse_string(ROUTING)
        dot = visualize_dot(model)
        assert "shape=diamond" in dot
        assert "route" in dot

    def test_conditional_routing_arms(self):
        model = parse_string(ROUTING)
        dot = visualize_dot(model)
        assert "priority.high" in dot
        assert "priority/medium" in dot
        assert "priority/low" in dot

    def test_conditional_routing_default_label(self):
        model = parse_string(ROUTING)
        dot = visualize_dot(model)
        assert 'label="default"' in dot

    def test_conditional_routing_when_label(self):
        model = parse_string(ROUTING)
        dot = visualize_dot(model)
        assert 'label="when"' in dot

    def test_constants_node(self):
        model = parse_string(CONSTANTS)
        dot = visualize_dot(model)
        assert "shape=note" in dot
        assert "constants" in dot
        assert "THRESHOLD" in dot
        assert "42" in dot
        assert "NAME" in dot
        assert "sensor" in dot
        assert "ENABLED" in dot
        assert "true" in dot

    def test_annotations_in_label(self):
        model = parse_string(ANNOTATIONS)
        dot = visualize_dot(model)
        assert "@warn" in dot
        assert "large window may cause memory pressure" in dot

    def test_initially_hook(self):
        model = parse_string(HOOKS)
        dot = visualize_dot(model)
        assert "@initially" in dot
        assert "status" in dot
        assert "version" in dot

    def test_finally_hook(self):
        model = parse_string(HOOKS)
        dot = visualize_dot(model)
        assert "@finally" in dot
        assert "status" in dot

    def test_hook_nodes_shape(self):
        model = parse_string(HOOKS)
        dot = visualize_dot(model)
        # Hook nodes should use note shape
        assert "shape=note" in dot

    def test_multi_transport_labels(self):
        model = parse_string(MULTI_TRANSPORT)
        dot = visualize_dot(model)
        assert "mqtt" in dot
        assert "redis" in dot
        assert "mqtt.local" in dot
        assert "redis.local" in dot
        assert "1883" in dot
        assert "6379" in dot

    def test_sliding_window(self):
        model = parse_string(SLIDING_WINDOW)
        dot = visualize_dot(model)
        assert "sliding" in dot
        assert "10m" in dot
        assert "step=2m" in dot

    def test_source_format(self):
        model = parse_string(FULL_PIPELINE)
        dot = visualize_dot(model)
        assert "format: json" in dot

    def test_route_broker_edges(self):
        model = parse_string(ROUTING)
        dot = visualize_dot(model)
        # Route arms should connect to their brokers
        assert "broker_alerts" in dot
        assert "broker_local" in dot

    def test_join_node_in_dot(self):
        model = parse_string(JOIN_PIPELINE)
        dot = visualize_dot(model)
        assert "join (inner)" in dot
        assert "within 30s" in dot
        assert "customer_id" in dot
        assert "other.id" in dot
        assert "shape=box" in dot
        assert "style=bold" in dot

    def test_join_broker_edge_in_dot(self):
        model = parse_string(JOIN_PIPELINE)
        dot = visualize_dot(model)
        # Join broker edge should be dashed
        assert "broker_local" in dot
        assert "p_enrich_join" in dot
        assert "style=dashed" in dot


# ─── Mermaid tests ──────────────────────────────────────────────────


class TestVisualizeMermaid:
    def test_simple_passthrough(self):
        model = parse_string(MINIMAL)
        mmd = visualize_mermaid(model)
        assert "graph LR" in mmd
        assert "passthrough" in mmd
        assert "source" in mmd
        assert "sink" in mmd

    def test_simple_passthrough_edges(self):
        model = parse_string(MINIMAL)
        mmd = visualize_mermaid(model)
        assert "p_passthrough_source" in mmd
        assert "p_passthrough_sink" in mmd
        assert "p_passthrough_source --> p_passthrough_sink" in mmd

    def test_broker_cylinder(self):
        model = parse_string(MINIMAL)
        mmd = visualize_mermaid(model)
        # Mermaid cylinder syntax: [("label")]
        assert "broker_b" in mmd
        assert "[(" in mmd
        assert "localhost" in mmd

    def test_full_pipeline_all_stages(self):
        model = parse_string(FULL_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "deduplicate" in mmd
        assert "filter" in mmd
        assert "group_by" in mmd
        assert "window" in mmd
        assert "aggregate" in mmd
        assert "having" in mmd
        assert "map" in mmd

    def test_full_pipeline_stage_details(self):
        model = parse_string(FULL_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "event_id" in mmd
        assert "sensor_id" in mmd
        assert "tumbling" in mmd
        assert "avg" in mmd
        assert "count" in mmd

    def test_multi_pipeline_subgraphs(self):
        model = parse_string(MULTI_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "subgraph p_first" in mmd
        assert "subgraph p_second" in mmd
        assert "end" in mmd

    def test_pipeline_chaining_dotted_edge(self):
        model = parse_string(CHAINED)
        mmd = visualize_mermaid(model)
        assert "-.->|" in mmd
        assert "pipe" in mmd

    def test_pipeline_chaining_pipe_label(self):
        model = parse_string(CHAINED)
        mmd = visualize_mermaid(model)
        assert "pipe: upstream" in mmd

    def test_conditional_routing_diamond(self):
        model = parse_string(ROUTING)
        mmd = visualize_mermaid(model)
        # Mermaid diamond syntax: {"label"}
        assert "route" in mmd
        assert "{" in mmd

    def test_conditional_routing_arms(self):
        model = parse_string(ROUTING)
        mmd = visualize_mermaid(model)
        assert "priority.high" in mmd
        assert "priority/medium" in mmd
        assert "priority/low" in mmd

    def test_conditional_routing_default_label(self):
        model = parse_string(ROUTING)
        mmd = visualize_mermaid(model)
        assert "default" in mmd

    def test_constants_node(self):
        model = parse_string(CONSTANTS)
        mmd = visualize_mermaid(model)
        assert "constants" in mmd
        assert "THRESHOLD" in mmd
        assert "42" in mmd

    def test_annotations_in_label(self):
        model = parse_string(ANNOTATIONS)
        mmd = visualize_mermaid(model)
        assert "@warn" in mmd
        assert "large window may cause memory pressure" in mmd

    def test_initially_hook(self):
        model = parse_string(HOOKS)
        mmd = visualize_mermaid(model)
        assert "@initially" in mmd
        assert "status" in mmd

    def test_finally_hook(self):
        model = parse_string(HOOKS)
        mmd = visualize_mermaid(model)
        assert "@finally" in mmd

    def test_multi_transport_labels(self):
        model = parse_string(MULTI_TRANSPORT)
        mmd = visualize_mermaid(model)
        assert "mqtt" in mmd
        assert "redis" in mmd
        assert "mqtt.local" in mmd
        assert "redis.local" in mmd

    def test_sliding_window(self):
        model = parse_string(SLIDING_WINDOW)
        mmd = visualize_mermaid(model)
        assert "sliding" in mmd
        assert "10m" in mmd
        assert "step=2m" in mmd

    def test_source_trapezoid(self):
        model = parse_string(MINIMAL)
        mmd = visualize_mermaid(model)
        # Mermaid trapezoid syntax: [/"label"/]
        assert "[/" in mmd

    def test_route_when_edge_label(self):
        model = parse_string(ROUTING)
        mmd = visualize_mermaid(model)
        assert '-->|"when"|' in mmd

    def test_route_default_edge_label(self):
        model = parse_string(ROUTING)
        mmd = visualize_mermaid(model)
        assert '-->|"default"|' in mmd

    def test_join_node_in_mermaid(self):
        model = parse_string(JOIN_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "join (inner)" in mmd
        assert "within 30s" in mmd
        assert "customer_id" in mmd
        assert "other.id" in mmd

    def test_join_broker_edge_in_mermaid(self):
        model = parse_string(JOIN_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "broker_local" in mmd
        assert "p_enrich_join" in mmd
        assert "-.-> p_enrich_join" in mmd


# ─── Cross-format consistency ───────────────────────────────────────


class TestCrossFormat:
    """Verify both formats contain the same key information."""

    def test_both_contain_broker_info(self):
        model = parse_string(MULTI_TRANSPORT)
        dot = visualize_dot(model)
        mmd = visualize_mermaid(model)
        for keyword in ["mqtt_broker", "redis_broker", "mqtt.local", "redis.local"]:
            assert keyword in dot, f"DOT missing: {keyword}"
            assert keyword in mmd, f"Mermaid missing: {keyword}"

    def test_both_contain_stage_info(self):
        model = parse_string(FULL_PIPELINE)
        dot = visualize_dot(model)
        mmd = visualize_mermaid(model)
        for keyword in [
            "deduplicate",
            "filter",
            "group_by",
            "window",
            "aggregate",
            "having",
            "map",
        ]:
            assert keyword in dot, f"DOT missing: {keyword}"
            assert keyword in mmd, f"Mermaid missing: {keyword}"

    def test_both_contain_route_info(self):
        model = parse_string(ROUTING)
        dot = visualize_dot(model)
        mmd = visualize_mermaid(model)
        for keyword in ["route", "priority.high", "priority/medium", "priority/low"]:
            assert keyword in dot, f"DOT missing: {keyword}"
            assert keyword in mmd, f"Mermaid missing: {keyword}"

    def test_both_contain_hook_info(self):
        model = parse_string(HOOKS)
        dot = visualize_dot(model)
        mmd = visualize_mermaid(model)
        for keyword in ["@initially", "@finally", "status"]:
            assert keyword in dot, f"DOT missing: {keyword}"
            assert keyword in mmd, f"Mermaid missing: {keyword}"

    def test_both_contain_pipe_info(self):
        model = parse_string(CHAINED)
        dot = visualize_dot(model)
        mmd = visualize_mermaid(model)
        for keyword in ["pipe", "upstream", "downstream"]:
            assert keyword in dot, f"DOT missing: {keyword}"
            assert keyword in mmd, f"Mermaid missing: {keyword}"

    def test_rate_stage_label_in_dot(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate 100 per 1s on_excess: warn
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        dot = visualize_dot(model)
        assert "rate" in dot
        assert "100 per 1s" in dot
        assert "on_excess: warn" in dot

    def test_rate_stage_label_in_mermaid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> rate 100 per 1s on_excess: warn
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        mmd = visualize_mermaid(model)
        assert "rate" in mmd
        assert "100 per 1s" in mmd

    def test_accumulate_stage_label_in_dot(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> window tumbling(10s)
            |> aggregate { avg_val = avg(msg.v) }
            |> accumulate { smooth = ema(avg_val, 0.3) }
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        dot = visualize_dot(model)
        assert "accumulate" in dot
        assert "ema(avg_val)" in dot

    def test_accumulate_stage_label_in_mermaid(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> window tumbling(10s)
            |> aggregate { avg_val = avg(msg.v) }
            |> accumulate { smooth = ema(avg_val, 0.3) }
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        mmd = visualize_mermaid(model)
        assert "accumulate" in mmd
        assert "ema(avg_val)" in mmd

    def test_both_contain_join_info(self):
        model = parse_string(JOIN_PIPELINE)
        dot = visualize_dot(model)
        mmd = visualize_mermaid(model)
        for keyword in ["join (inner)", "customer_id", "other.id", "within 30s"]:
            assert keyword in dot, f"DOT missing: {keyword}"
            assert keyword in mmd, f"Mermaid missing: {keyword}"


SCHEMA_PIPELINE = """
schema Sensor
    temp: float
    humidity: float
    sensor_id: str
end

broker b host: "localhost" port: 1883 end

pipeline sensor_check
    source from b :: "sensors/raw" schema: Sensor
    |> filter msg.temp > 30.0
    |> sink to b :: "sensors/hot"
end
"""

DLQ_PIPELINE = """
broker b host: "localhost" port: 1883 end

pipeline with_dlq
    source from b :: "input"
    |> filter msg.value > 0
    |> sink to b :: "output"
    on_error: sink to b :: "errors/with_dlq"
end
"""


class TestSchemaVisualize:
    def test_dot_schema_binding(self):
        model = parse_string(SCHEMA_PIPELINE)
        dot = visualize_dot(model)
        assert "schema: Sensor" in dot

    def test_mermaid_schema_binding(self):
        model = parse_string(SCHEMA_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "schema: Sensor" in mmd


class TestOnErrorVisualize:
    def test_dot_on_error_dlq(self):
        model = parse_string(DLQ_PIPELINE)
        dot = visualize_dot(model)
        assert "DLQ" in dot
        assert "errors/with_dlq" in dot
        assert "on_error" in dot
        assert "hexagon" in dot

    def test_mermaid_on_error_dlq(self):
        model = parse_string(DLQ_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "DLQ" in mmd
        assert "errors/with_dlq" in mmd
        assert "on_error" in mmd

    def test_dot_on_error_dlq_edge(self):
        model = parse_string(DLQ_PIPELINE)
        dot = visualize_dot(model)
        assert "p_with_dlq_dlq" in dot
        assert "style=dashed" in dot
        assert "color=red" in dot

    def test_mermaid_on_error_dlq_edge(self):
        model = parse_string(DLQ_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "p_with_dlq_dlq" in mmd
        assert "on_error" in mmd


NEW_STAGES_FLATTEN = """
broker b host: "localhost" port: 1883 end

pipeline flat
    source from b :: "in"
    |> flatten msg.items
    |> sink to b :: "out"
end
"""

NEW_STAGES_DISTINCT = """
broker b host: "localhost" port: 1883 end

pipeline dedup
    source from b :: "in"
    |> distinct msg.id within 5 m
    |> sink to b :: "out"
end
"""

NEW_STAGES_THROTTLE = """
broker b host: "localhost" port: 1883 end

pipeline throttled
    source from b :: "in"
    |> throttle 100 per 1 s overflow: drop
    |> sink to b :: "out"
end
"""

NEW_STAGES_SAMPLE = """
broker b host: "localhost" port: 1883 end

pipeline sampled
    source from b :: "in"
    |> sample 50 %
    |> sink to b :: "out"
end
"""


class TestNewStagesVisualize:
    def test_flatten_dot(self):
        model = parse_string(NEW_STAGES_FLATTEN)
        dot = visualize_dot(model)
        assert "flatten\\nmsg.items" in dot

    def test_distinct_dot(self):
        model = parse_string(NEW_STAGES_DISTINCT)
        dot = visualize_dot(model)
        assert "distinct\\nmsg.id within 5m" in dot

    def test_throttle_dot(self):
        model = parse_string(NEW_STAGES_THROTTLE)
        dot = visualize_dot(model)
        assert "throttle\\n" in dot
        assert "overflow:" in dot

    def test_sample_dot(self):
        model = parse_string(NEW_STAGES_SAMPLE)
        dot = visualize_dot(model)
        assert "sample\\n50%" in dot

    def test_flatten_mermaid(self):
        model = parse_string(NEW_STAGES_FLATTEN)
        mmd = visualize_mermaid(model)
        assert "flatten" in mmd
        assert "msg.items" in mmd

    def test_distinct_mermaid(self):
        model = parse_string(NEW_STAGES_DISTINCT)
        mmd = visualize_mermaid(model)
        assert "distinct" in mmd
        assert "msg.id" in mmd

    def test_throttle_mermaid(self):
        model = parse_string(NEW_STAGES_THROTTLE)
        mmd = visualize_mermaid(model)
        assert "throttle" in mmd
        assert "100 per 1s" in mmd

    def test_sample_mermaid(self):
        model = parse_string(NEW_STAGES_SAMPLE)
        mmd = visualize_mermaid(model)
        assert "sample" in mmd
        assert "50%" in mmd


RETRY_PIPELINE = """
broker b host: "localhost" port: 1883 end

pipeline retried
    source from b :: "in"
    |> sink to b :: "out"
    retry: 5 with backoff 200 ms
end
"""

MULTI_WINDOW_PIPELINE = """
broker b host: "localhost" port: 1883 end

pipeline multi_win
    source from b :: "in"
    |> window short tumbling(1 m)
    |> window long tumbling(5 m)
    |> aggregate {
        avg_1m = avg(msg.value) over short
        avg_5m = avg(msg.value) over long
    }
    |> sink to b :: "out"
end
"""


class TestRetryPolicyVisualize:
    def test_dot_retry_in_subgraph_label(self):
        model = parse_string(RETRY_PIPELINE)
        dot = visualize_dot(model)
        assert "retry" in dot
        assert "5x" in dot
        assert "backoff" in dot
        assert "200ms" in dot

    def test_mermaid_retry_in_subgraph_label(self):
        model = parse_string(RETRY_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "retry" in mmd
        assert "5x" in mmd
        assert "backoff" in mmd
        assert "200ms" in mmd

    def test_dot_no_retry_no_label(self):
        model = parse_string(MINIMAL)
        dot = visualize_dot(model)
        assert "retry" not in dot

    def test_mermaid_no_retry_no_label(self):
        model = parse_string(MINIMAL)
        mmd = visualize_mermaid(model)
        assert "retry" not in mmd


class TestMultiWindowVisualize:
    def test_dot_named_window_label(self):
        model = parse_string(MULTI_WINDOW_PIPELINE)
        dot = visualize_dot(model)
        assert "window 'short'" in dot
        assert "window 'long'" in dot
        assert "tumbling(1m)" in dot
        assert "tumbling(5m)" in dot

    def test_mermaid_named_window_label(self):
        model = parse_string(MULTI_WINDOW_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "window 'short'" in mmd
        assert "window 'long'" in mmd
        assert "tumbling(1m)" in mmd
        assert "tumbling(5m)" in mmd

    def test_dot_aggregate_over_label(self):
        model = parse_string(MULTI_WINDOW_PIPELINE)
        dot = visualize_dot(model)
        assert "avg(msg.value) over short" in dot
        assert "avg(msg.value) over long" in dot

    def test_mermaid_aggregate_over_label(self):
        model = parse_string(MULTI_WINDOW_PIPELINE)
        mmd = visualize_mermaid(model)
        assert "avg(msg.value) over short" in mmd
        assert "avg(msg.value) over long" in mmd

    def test_dot_both_formats_consistent(self):
        model = parse_string(MULTI_WINDOW_PIPELINE)
        dot = visualize_dot(model)
        mmd = visualize_mermaid(model)
        for keyword in ["window 'short'", "window 'long'", "over short", "over long"]:
            assert keyword in dot, f"DOT missing: {keyword}"
            assert keyword in mmd, f"Mermaid missing: {keyword}"


class TestFunctionsVisualize:
    """Tests for user-defined function visualization in DOT and Mermaid."""

    FUNC_SINGLE = """
    func celsius(t) = (t - 32) * 5 / 9
    broker b host: "localhost" port: 1883 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """

    FUNC_MULTIPLE = """
    func celsius(t) = (t - 32) * 5 / 9
    func add(a, b) = a + b
    func pi() = 3.14159
    broker b host: "localhost" port: 1883 end
    pipeline p
        source from b :: "in"
        |> sink to b :: "out"
    end
    """

    def test_dot_functions_node_present(self):
        model = parse_string(self.FUNC_SINGLE)
        dot = visualize_dot(model)
        assert "functions" in dot

    def test_dot_functions_shape_note(self):
        model = parse_string(self.FUNC_SINGLE)
        dot = visualize_dot(model)
        assert "shape=note" in dot

    def test_dot_function_name_in_label(self):
        model = parse_string(self.FUNC_SINGLE)
        dot = visualize_dot(model)
        assert "celsius" in dot

    def test_dot_function_params_in_label(self):
        model = parse_string(self.FUNC_MULTIPLE)
        dot = visualize_dot(model)
        assert "func celsius(t)" in dot
        assert "func add(a, b)" in dot
        assert "func pi()" in dot

    def test_mermaid_functions_node_present(self):
        model = parse_string(self.FUNC_SINGLE)
        mmd = visualize_mermaid(model)
        assert "functions" in mmd

    def test_mermaid_function_name_in_label(self):
        model = parse_string(self.FUNC_SINGLE)
        mmd = visualize_mermaid(model)
        assert "celsius" in mmd

    def test_mermaid_multiple_functions(self):
        model = parse_string(self.FUNC_MULTIPLE)
        mmd = visualize_mermaid(model)
        assert "func celsius(t)" in mmd
        assert "func add(a, b)" in mmd
        assert "func pi()" in mmd


class TestPipelineLetVisualize:
    """Tests for pipeline-level let binding visualization in DOT and Mermaid."""

    LET_SINGLE = """
    broker b host: "localhost" port: 1883 end
    pipeline p
        source from b :: "in"
        let threshold = 42
        |> filter msg.value > 10
        |> sink to b :: "out"
    end
    """

    LET_MULTIPLE = """
    broker b host: "localhost" port: 1883 end
    pipeline p
        source from b :: "in"
        let low = 10
        let high = 100
        |> filter msg.value > 0
        |> sink to b :: "out"
    end
    """

    def test_dot_let_node_present(self):
        model = parse_string(self.LET_SINGLE)
        dot = visualize_dot(model)
        assert "p_p_lets" in dot

    def test_dot_let_shape_note(self):
        model = parse_string(self.LET_SINGLE)
        dot = visualize_dot(model)
        assert "shape=note" in dot

    def test_dot_let_name_in_label(self):
        model = parse_string(self.LET_SINGLE)
        dot = visualize_dot(model)
        assert "threshold" in dot

    def test_dot_let_in_node_chain(self):
        """Let node should be connected to other nodes via edges."""
        model = parse_string(self.LET_SINGLE)
        dot = visualize_dot(model)
        assert "p_p_lets" in dot
        # The let node should appear in an edge (connected in the chain)
        assert "-> p_p_lets" in dot or "p_p_lets ->" in dot

    def test_dot_multiple_let_names(self):
        model = parse_string(self.LET_MULTIPLE)
        dot = visualize_dot(model)
        assert "low" in dot
        assert "high" in dot

    def test_mermaid_let_node_present(self):
        model = parse_string(self.LET_SINGLE)
        mmd = visualize_mermaid(model)
        assert "p_p_lets" in mmd

    def test_mermaid_let_name_in_label(self):
        model = parse_string(self.LET_SINGLE)
        mmd = visualize_mermaid(model)
        assert "threshold" in mmd

    def test_mermaid_multiple_let_names(self):
        model = parse_string(self.LET_MULTIPLE)
        mmd = visualize_mermaid(model)
        assert "low" in mmd
        assert "high" in mmd


# ─── Coverage: visualize.py lines 133, 135-140 (DOT sink format + batch) ──


class TestDotSinkFormatBatch:
    SINK_FORMAT_BATCH = """
    broker b host: "localhost" port: 1883 end

    pipeline p
        source from b :: "in"
        |> sink to b :: "out" format: json batch: { size: 50 timeout: 10 s }
    end
    """

    def test_dot_sink_format_label(self):
        """DOT sink node includes format (visualize.py line 133)."""
        model = parse_string(self.SINK_FORMAT_BATCH)
        dot = visualize_dot(model)
        assert "format: json" in dot

    def test_dot_sink_batch_label(self):
        """DOT sink node includes batch info (visualize.py lines 135-140)."""
        model = parse_string(self.SINK_FORMAT_BATCH)
        dot = visualize_dot(model)
        assert "batch:" in dot
        assert "size=50" in dot
        assert "timeout=10s" in dot

    def test_mermaid_sink_format_label(self):
        """Mermaid sink node includes format (visualize.py line 466)."""
        model = parse_string(self.SINK_FORMAT_BATCH)
        mmd = visualize_mermaid(model)
        assert "format: json" in mmd


# ─── Coverage: visualize.py lines 245-247, 563-565 (pipe source from route) ──


class TestPipeSourceFromRoute:
    ROUTE_THEN_PIPE = """
    broker b host: "localhost" port: 1883 end

    pipeline upstream
        source from b :: "in"
        |> route
            when msg.value > 50 => sink to b :: "high"
            default => sink to b :: "low"
        end
    end

    pipeline downstream
        source from pipe upstream
        |> sink to b :: "final"
    end
    """

    def test_dot_pipe_from_route_upstream(self):
        """DOT pipe source from upstream with route (visualize.py lines 245-247)."""
        model = parse_string(self.ROUTE_THEN_PIPE)
        dot = visualize_dot(model)
        assert "p_upstream_route" in dot
        assert "p_downstream_source" in dot
        assert "pipe" in dot
        assert "style=dashed" in dot

    def test_mermaid_pipe_from_route_upstream(self):
        """Mermaid pipe source from upstream with route (visualize.py lines 563-565)."""
        model = parse_string(self.ROUTE_THEN_PIPE)
        mmd = visualize_mermaid(model)
        assert "p_upstream_route" in mmd
        assert "p_downstream_source" in mmd
        assert "pipe" in mmd


# ─── Coverage: visualize.py line 323, 639 (stage label fallthrough) ──
# Lines 323 and 639 are the fallthrough returns in _dot_stage_label and
# _mermaid_stage_label for unknown stage types. These are unreachable in
# normal usage since all stage types are handled. We test the accumulate
# stage which is the last known stage before the fallthrough to ensure
# coverage of the path leading up to it.


# ─── Coverage: visualize.py annotations in DOT/Mermaid ──


class TestAnnotationsVisualize:
    ANNOTATED_ERROR = """
    broker b host: "localhost" port: 1883 end

    pipeline p
        source from b :: "in"
        |> @error("critical filter") filter msg.value > 0
        |> sink to b :: "out"
    end
    """

    def test_dot_error_annotation_in_label(self):
        """DOT annotation with @error appears in stage label."""
        model = parse_string(self.ANNOTATED_ERROR)
        dot = visualize_dot(model)
        assert "@error" in dot
        assert "critical filter" in dot

    def test_mermaid_error_annotation_in_label(self):
        """Mermaid annotation with @error appears in stage label (visualize.py line 453-455)."""
        model = parse_string(self.ANNOTATED_ERROR)
        mmd = visualize_mermaid(model)
        assert "@error" in mmd
        assert "critical filter" in mmd


# ─── Coverage: visualize.py route with default in DOT and Mermaid ──


class TestRouteDefaultVisualize:
    ROUTE_WITH_DEFAULT = """
    broker b host: "localhost" port: 1883 end

    pipeline p
        source from b :: "in"
        |> filter msg.value is not null
        |> route
            when msg.value > 100 => sink to b :: "high"
            default => sink to b :: "normal"
        end
    end
    """

    def test_dot_route_default_node(self):
        """DOT route with default arm creates default node (visualize.py lines 155-159)."""
        model = parse_string(self.ROUTE_WITH_DEFAULT)
        dot = visualize_dot(model)
        assert "route_default" in dot
        assert "normal" in dot
        assert 'label="default"' in dot

    def test_mermaid_route_default_node(self):
        """Mermaid route with default arm (visualize.py lines 481-485)."""
        model = parse_string(self.ROUTE_WITH_DEFAULT)
        mmd = visualize_mermaid(model)
        assert "route_default" in mmd
        assert "normal" in mmd
        assert '"default"' in mmd
