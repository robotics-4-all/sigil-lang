import os
import tempfile
import pytest

from sigil.language import parse_string
from sigil.codegen import generate
from sigil.expressions import compile_expr, ExprContext
from sigil.explain import explain_model


def _stages(pipeline):
    return [
        s.stage if s.__class__.__name__ == "AnnotatedStage" else s
        for s in pipeline.stages
    ]


SRC = """
broker local host: "localhost" port: 1883 end

broker remote
    host: "mqtt.cloud.io"
    port: 8883
    ssl: true
    auth: {
        username: "user"
        password: "pass"
    }
end

pipeline temp_avg
    source from local :: "sensors/+/temperature" qos: 1 format: json

    |> filter: msg.value > -40 and msg.value < 85

    |> window: tumbling(5 m)

    |> aggregate: {
        avg_val = avg(msg.value)
        n       = count()
    }

    |> map: {
        average = avg_val;
        samples = n
    }

    |> sink to remote :: "analytics/temp" qos: 1
end
"""


def test_generate_creates_files():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)

        assert os.path.isfile(os.path.join(tmpdir, "app.py"))
        assert os.path.isfile(os.path.join(tmpdir, "requirements.txt"))
        assert os.path.isfile(os.path.join(tmpdir, "pipelines", "temp_avg.py"))
        assert os.path.isfile(os.path.join(tmpdir, "pipelines", "__init__.py"))


def test_generated_app_imports_pipeline():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        app_code = open(os.path.join(tmpdir, "app.py")).read()
        assert "from pipelines.temp_avg import TempAvgPipeline" in app_code
        assert "TempAvgPipeline()" in app_code


def test_generated_pipeline_has_class():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "temp_avg.py")).read()
        assert "class TempAvgPipeline:" in code
        assert "def start(self):" in code
        assert "def stop(self" in code
        assert "def _on_message(" in code


def test_generated_pipeline_contains_filter():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "temp_avg.py")).read()
        assert "_get_nested(msg, 'value')" in code
        assert "(-40" in code
        assert "85" in code


def test_generated_pipeline_contains_aggregation():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "temp_avg.py")).read()
        assert 'aggregated["avg_val"]' in code
        assert 'aggregated["n"]' in code
        assert "statistics.mean" in code
        assert "len(messages)" in code


def test_generated_pipeline_contains_mapping():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "temp_avg.py")).read()
        assert 'result["average"]' in code
        assert 'result["samples"]' in code


def test_generated_pipeline_broker_config():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "temp_avg.py")).read()
        assert '"localhost"' in code
        assert "1883" in code
        assert '"mqtt.cloud.io"' in code
        assert "8883" in code
        assert "ssl=True" in code
        assert 'username="user"' in code
        assert 'password="pass"' in code
        assert "from commlib.transports.mqtt import" in code


def test_generated_pipeline_compiles():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "temp_avg.py")).read()
        compile(code, "<test>", "exec")


def test_generated_app_compiles():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "app.py")).read()
        compile(code, "<test>", "exec")


class TestExpressionCompiler:
    def _parse_filter_expr(self, expr_src):
        src = f"""
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: {expr_src}
            |> sink to b :: "t"
        end
        """
        model = parse_string(src)
        return _stages(model.pipelines[0])[0].condition

    def test_simple_comparison(self):
        expr = self._parse_filter_expr("msg.x > 10")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_get_nested(msg, 'x') > " in code
        assert "10" in code

    def test_and_expression(self):
        expr = self._parse_filter_expr("msg.a > 0 and msg.b < 100")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "and" in code

    def test_or_expression(self):
        expr = self._parse_filter_expr("msg.a > 0 or msg.b < 100")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "or" in code

    def test_nested_field_ref(self):
        expr = self._parse_filter_expr("msg.location.lat > 0")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_get_nested(msg, 'location', 'lat')" in code

    def test_string_literal(self):
        expr = self._parse_filter_expr('msg.status == "ok"')
        code = compile_expr(expr, ExprContext.FILTER)
        assert "'ok'" in code

    def test_not_expression(self):
        expr = self._parse_filter_expr("not msg.disabled")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "not" in code

    def test_arithmetic_in_map(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> aggregate: { r = avg(msg.v) }
            |> map: { f = r * 1.8 + 32 }
            |> sink to b :: "t"
        end
        """
        model = parse_string(src)
        m = _stages(model.pipelines[0])[1].mappings[0]
        code = compile_expr(m.value, ExprContext.MAP)
        assert "aggregated['r']" in code
        assert "1.8" in code
        assert "32" in code

    def test_if_then_else(self):
        expr = self._parse_filter_expr("if msg.x > 0 then 1 else 0")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "if" in code
        assert "else" in code

    def test_is_null(self):
        expr = self._parse_filter_expr("msg.x is null")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "is None" in code

    def test_is_not_null(self):
        expr = self._parse_filter_expr("msg.x is not null")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "is not None" in code

    def test_coalesce(self):
        expr = self._parse_filter_expr("msg.x ?? 0")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_coalesce" in code

    def test_in_operator(self):
        expr = self._parse_filter_expr('msg.status in ["a", "b"]')
        code = compile_expr(expr, ExprContext.FILTER)
        assert "in [" in code

    def test_not_in_operator(self):
        expr = self._parse_filter_expr("msg.x not in [1, 2]")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "not in [" in code

    def test_scalar_func_abs(self):
        expr = self._parse_filter_expr("abs(msg.x)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "abs(" in code

    def test_scalar_func_round(self):
        expr = self._parse_filter_expr("round(msg.x, 2)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "round(" in code

    def test_scalar_func_floor(self):
        expr = self._parse_filter_expr("floor(msg.x)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "math.floor(" in code

    def test_scalar_func_lower(self):
        expr = self._parse_filter_expr("lower(msg.x)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert ".lower()" in code

    def test_env_ref_compiles_no_default(self):
        expr = self._parse_filter_expr('msg.x > env("THRESHOLD")')
        code = compile_expr(expr, ExprContext.FILTER)
        assert "os.environ['THRESHOLD']" in code

    def test_env_ref_compiles_with_default(self):
        expr = self._parse_filter_expr('msg.x > env("HOST", "localhost")')
        code = compile_expr(expr, ExprContext.FILTER)
        assert "os.environ.get('HOST', 'localhost')" in code

    def test_case_compiles_to_ternary(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> aggregate: { v = avg(msg.x) }
            |> map: { out = case { when v > 0 => "pos" when v == 0 => "zero" else => "neg" } }
            |> sink to b :: "t"
        end
        """
        model = parse_string(src)
        m = _stages(model.pipelines[0])[1].mappings[0]
        code = compile_expr(m.value, ExprContext.MAP)
        assert "if" in code
        assert "else" in code
        assert "'pos'" in code
        assert "'neg'" in code

    def test_old_compiles(self):
        expr = self._parse_filter_expr("old(msg.x)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_get_nested(self._prev_msg, 'x')" in code

    def test_changed_compiles(self):
        expr = self._parse_filter_expr("changed(msg.x)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_get_nested(msg, 'x')" in code
        assert "_get_nested(self._prev_msg, 'x')" in code
        assert "!=" in code


def test_let_binding_codegen():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "ratio = " in code
        ratio_pos = code.index("ratio = ")
        result_pos = code.index('result["result"]')
        assert ratio_pos < result_pos
        compile(code, "<test>", "exec")


def test_generated_pipeline_uses_commlib():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "temp_avg.py")).read()
        assert "from commlib.transports.mqtt import" in code
        assert "Subscriber" in code
        assert "Publisher" in code
        assert "ConnectionParameters" in code
        assert "self._subscriber" in code
        assert "self._publisher" in code


def test_generated_requirements_uses_commlib():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        reqs = open(os.path.join(tmpdir, "requirements.txt")).read()
        assert "commlib-py" in reqs
        assert "paho-mqtt" not in reqs


def test_generated_pipeline_redis_transport():
    src = """
    broker cache
        transport: redis
        host: "redis.local"
        port: 6379
    end
    pipeline redis_pipe
        source from cache :: "in"
        |> sink to cache :: "out"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "redis_pipe.py")).read()
        assert "from commlib.transports.redis import" in code
        assert '"redis.local"' in code
        assert "6379" in code
        compile(code, "<test>", "exec")


def test_generated_pipeline_cross_transport():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "bridge.py")).read()
        assert "from commlib.transports.mqtt import" in code
        assert "from commlib.transports.redis import" in code
        assert "SourceConnParams" in code
        assert "SinkConnParams" in code
        compile(code, "<test>", "exec")


def test_generated_pipeline_default_transport():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "from commlib.transports.mqtt import" in code
        compile(code, "<test>", "exec")


def test_const_inlined_in_filter():
    src = """
    const THRESHOLD = 42
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.v > THRESHOLD
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "42" in code
        compile(code, "<test>", "exec")


def test_const_inlined_in_map():
    src = """
    const FACTOR = 1.5
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(5 m)
        |> aggregate: { v = avg(msg.x) }
        |> map: { result = v * FACTOR }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "1.5" in code
        compile(code, "<test>", "exec")


def test_env_ref_generates_import_os():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.v > env("THRESHOLD", 100)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "import os" in code
        compile(code, "<test>", "exec")


def test_case_in_map_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> window: tumbling(5 m)
        |> aggregate: { v = avg(msg.x) }
        |> map: { out = case { when v > 100 => "high" when v > 0 => "low" else => "none" } }
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "'high'" in code
        assert "'low'" in code
        compile(code, "<test>", "exec")


def test_having_codegen():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "aggregated['n']" in code
        assert "5" in code
        compile(code, "<test>", "exec")


def test_dedup_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> deduplicate: msg.event_id within 30 s
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "_dedup_seen" in code
        assert "_dedup_ttl" in code
        assert "30" in code
        compile(code, "<test>", "exec")


def test_temporal_codegen_has_prev_msg():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: old(msg.status) != msg.status
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "self._prev_msg = {}" in code
        assert "_get_nested(self._prev_msg" in code
        compile(code, "<test>", "exec")


# --- Phase 3: forall/exists codegen ---


def test_forall_compiles_to_all():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "all(" in code
        assert "for m in messages" in code
        compile(code, "<test>", "exec")


def test_exists_with_collection_compiles_to_any():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: exists(x in msg.tags => x == "critical")
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "any(" in code
        assert "_get_nested(msg, 'tags')" in code
        compile(code, "<test>", "exec")


def test_forall_expression_compiler():
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
    code = compile_expr(having.condition, ExprContext.HAVING)
    assert "all(m > 0" in code
    assert "for m in messages" in code


def test_exists_expression_compiler():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: exists(x in msg.items => x > 10)
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    filter_stage = _stages(model.pipelines[0])[0]
    code = compile_expr(filter_stage.condition, ExprContext.FILTER)
    assert "any(x > 10" in code
    assert "_get_nested(msg, 'items')" in code


# --- Phase 3: @initially/@finally codegen ---


def test_initially_hook_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        @initially { publish { status: "online" } }
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert '"status"' in code
        assert "'online'" in code
        compile(code, "<test>", "exec")


def test_finally_hook_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
        @finally { publish { status: "offline" } }
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert '"status"' in code
        assert "'offline'" in code
        compile(code, "<test>", "exec")


def test_both_hooks_codegen():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "'online'" in code
        assert "'offline'" in code
        assert "'1.0'" in code
        compile(code, "<test>", "exec")


def test_initially_with_env_ref_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        @initially { publish { host: env("HOSTNAME", "unknown") } }
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "import os" in code
        assert "os.environ.get" in code
        compile(code, "<test>", "exec")


# --- Phase 3: sink format + batch codegen ---


def test_sink_batch_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" batch: { size: 50  timeout: 5s  }
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "_batch_buffer" in code
        assert "_batch_size" in code
        assert "50" in code
        assert "_flush_batch" in code
        compile(code, "<test>", "exec")


def test_sink_batch_size_only_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" batch: { size: 100  }
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "_batch_buffer" in code
        assert "_batch_size" in code
        assert "_schedule_batch_flush" not in code
        compile(code, "<test>", "exec")


def test_sink_csv_format_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" format: csv 
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "import csv" in code
        assert "import io" in code
        assert "DictWriter" in code
        compile(code, "<test>", "exec")


def test_sink_raw_format_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b" format: raw 
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "str(msg)" in code
        compile(code, "<test>", "exec")


def test_sink_no_batch_no_format_codegen():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> sink to b :: "b"
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "_batch_buffer" not in code
        assert "DictWriter" not in code
        compile(code, "<test>", "exec")


# --- Phase 4: pipeline chaining codegen ---


def test_pipe_source_codegen():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "downstream.py")).read()
        assert "_downstream_listeners" in code
        assert "Subscriber" not in code
        compile(code, "<test>", "exec")


def test_pipe_source_app_wiring():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        app_code = open(os.path.join(tmpdir, "app.py")).read()
        assert "_downstream_listeners" in app_code
        assert "_on_message" in app_code
        compile(app_code, "<test>", "exec")


def test_pipe_source_no_subscriber():
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
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "downstream.py")).read()
        assert "self._subscriber" not in code


# --- Phase 4: route codegen ---


def test_route_codegen_compiles():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        compile(code, "<test>", "exec")


def test_route_codegen_has_route_publishers():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "_route_pub_1" in code
        assert "_route_pub_default" in code


def test_route_codegen_has_condition():
    src = """
    broker b host: "h" port: 1 end
    broker b2 host: "h2" port: 2 end
    pipeline p
        source from b :: "a"
        |> route
            when msg.x > 100 => sink to b :: "high"
            when msg.x > 0 => sink to b2 :: "low"
            default => sink to b :: "other"
        end
    end
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "if " in code
        assert "elif " in code
        compile(code, "<test>", "exec")


def test_route_no_single_publisher():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        code = open(os.path.join(tmpdir, "pipelines", "p.py")).read()
        assert "self._publisher" not in code


# --- Phase 4: stage annotation codegen ---


def test_annotation_does_not_affect_codegen():
    base_src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> filter: msg.x > 0
        |> sink to b :: "b"
    end
    """
    annotated_src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "a"
        |> @warn("slow") filter: msg.x > 0
        |> sink to b :: "b"
    end
    """
    model_base = parse_string(base_src)
    model_ann = parse_string(annotated_src)
    with tempfile.TemporaryDirectory() as tmpdir_base:
        generate(model_base, tmpdir_base)
        code_base = open(os.path.join(tmpdir_base, "pipelines", "p.py")).read()
    with tempfile.TemporaryDirectory() as tmpdir_ann:
        generate(model_ann, tmpdir_ann)
        code_ann = open(os.path.join(tmpdir_ann, "pipelines", "p.py")).read()
    assert code_base == code_ann


# --- Phase 4: explain ---


def test_explain_basic_pipeline():
    src = """
    broker b host: "h" port: 1 end
    pipeline sensor_avg
        source from b :: "sensors/in"
        |> sink to b :: "sensors/out"
    end
    """
    model = parse_string(src)
    output = explain_model(model)
    assert "sensor_avg" in output
    assert "sensors/in" in output
    assert "sensors/out" in output


def test_explain_stages():
    src = """
    broker b host: "h" port: 1 end
    pipeline p
        source from b :: "in"
        |> filter: msg.x > 0
        |> window: tumbling(5 m)
        |> aggregate: { n = count() }
        |> sink to b :: "out"
    end
    """
    model = parse_string(src)
    output = explain_model(model)
    assert "filter" in output
    assert "window" in output
    assert "aggregate" in output


def test_explain_pipe_source():
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
    output = explain_model(model)
    assert "receives from pipeline" in output


class TestRightSideArithmeticCompilation:
    """Verify compiled expressions produce correct left-associative Python."""

    def _filter_code(self, expr_src):
        src = f"""
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "t"
            |> filter: {expr_src}
            |> sink to b :: "o"
        end
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        return compile_expr(stages[0].condition, ExprContext.FILTER)

    def test_multiply_right_compiles(self):
        code = self._filter_code("msg.x > 2 * 2")
        ns = {
            "msg": {"x": 5},
            "_get_nested": lambda d, *k: d.get(k[0]) if len(k) == 1 else None,
            "_coalesce": lambda a, b: a if a is not None else b,
        }
        assert eval(code, ns) is True  # 5 > 4
        ns["msg"] = {"x": 3}
        assert eval(code, ns) is False  # 3 > 4

    def test_add_right_compiles(self):
        code = self._filter_code("msg.x > 2 + 3")
        ns = {
            "msg": {"x": 6},
            "_get_nested": lambda d, *k: d.get(k[0]) if len(k) == 1 else None,
            "_coalesce": lambda a, b: a if a is not None else b,
        }
        assert eval(code, ns) is True  # 6 > 5
        ns["msg"] = {"x": 4}
        assert eval(code, ns) is False  # 4 > 5

    def test_subtraction_left_associative(self):
        code = self._filter_code("msg.x > 10 - 3 - 2")
        ns = {
            "msg": {"x": 6},
            "_get_nested": lambda d, *k: d.get(k[0]) if len(k) == 1 else None,
            "_coalesce": lambda a, b: a if a is not None else b,
        }
        assert eval(code, ns) is True  # 6 > 5 (10-3-2=5, left-assoc)
        ns["msg"] = {"x": 4}
        assert eval(code, ns) is False

    def test_division_left_associative(self):
        code = self._filter_code("msg.x > 100 / 5 / 2")
        ns = {
            "msg": {"x": 11},
            "_get_nested": lambda d, *k: d.get(k[0]) if len(k) == 1 else None,
            "_coalesce": lambda a, b: a if a is not None else b,
        }
        assert eval(code, ns) is True  # 11 > 10.0 (100/5/2=10, left-assoc)
        ns["msg"] = {"x": 9}
        assert eval(code, ns) is False

    def test_mixed_arithmetic_right(self):
        code = self._filter_code("msg.x > 2 * 3 + 1")
        ns = {
            "msg": {"x": 8},
            "_get_nested": lambda d, *k: d.get(k[0]) if len(k) == 1 else None,
            "_coalesce": lambda a, b: a if a is not None else b,
        }
        assert eval(code, ns) is True  # 8 > 7 (2*3+1=7)
        ns["msg"] = {"x": 6}
        assert eval(code, ns) is False


class TestRateCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_rate_generates_token_bucket(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> rate: 100 per 1s
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_rate_limit = 100" in code
        assert "_rate_period = 1" in code
        assert "_rate_tokens" in code
        assert "_rate_lock" in code
        compile(code, "rate_test.py", "exec")

    def test_rate_on_excess_warn(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> rate: 50 per 5s on_excess: warn
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "logger.warning" in code
        assert "Rate limit exceeded" in code
        compile(code, "rate_warn.py", "exec")

    def test_rate_on_excess_error(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> rate: 10 per 1s on_excess: error
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "logger.error" in code
        compile(code, "rate_error.py", "exec")

    def test_rate_on_excess_drop_no_log(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> rate: 10 per 1s on_excess: drop
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "self._rate_tokens < 1.0" in code
        assert "Rate limit exceeded" not in code
        compile(code, "rate_drop.py", "exec")


class TestJoinCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_join_generates_subscriber(self):
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
        code = self._gen_pipeline_code(src)
        assert "JoinSubscriber" in code
        assert "_join_subscriber" in code
        compile(code, "join_sub.py", "exec")

    def test_join_inner_pending_buffer(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "orders"
            |> join b :: "customers"
                on msg.id == other.id
                within 10 s
                type: inner
            end
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "join_pending" in code
        compile(code, "join_inner.py", "exec")

    def test_join_left_no_pending(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "orders"
            |> join b :: "customers"
                on msg.id == other.id
                within 10 s
                type: left
            end
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "join_pending.append" not in code
        compile(code, "join_left.py", "exec")

    def test_join_other_in_map(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "orders"
            |> join b :: "customers"
                on msg.id == other.id
                within 10 s
            end
            |> map: { name = other.name }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_get_nested(other, 'name')" in code
        compile(code, "join_map.py", "exec")

    def test_join_compiles(self):
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
            |> filter: msg.value > 0
            |> map: { customer = other.name }
            |> sink to local :: "enriched"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "JoinConnParams" in code
        assert "_join_buffer" in code
        assert "_join_window" in code
        compile(code, "join_full.py", "exec")


class TestAccumulateCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_accumulate_ema_generates_state(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10s)
            |> aggregate: { avg_val = avg(msg.v) }
            |> accumulate: { smooth = ema(avg_val, 0.3) }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_accum_state" in code
        assert '"smooth"' in code
        assert "0.3" in code
        compile(code, "accum_ema.py", "exec")

    def test_accumulate_cumsum_generates_state(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10s)
            |> aggregate: { total = sum(msg.v) }
            |> accumulate: { running = cumsum(total) }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_accum_state" in code
        assert '"running"' in code
        compile(code, "accum_cumsum.py", "exec")

    def test_accumulate_delta_generates_state(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10s)
            |> aggregate: { total = sum(msg.v) }
            |> accumulate: { change = delta(total) }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_accum_state" in code
        assert '"change"' in code
        assert '"change_prev"' in code
        compile(code, "accum_delta.py", "exec")

    def test_accumulate_injected_before_having(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10s)
            |> aggregate: { avg_val = avg(msg.v) }
            |> accumulate: { smooth = ema(avg_val, 0.3) }
            |> having: smooth > 10
            |> map: { result = smooth }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        accum_pos = code.index("_accum_state")
        having_pos = code.index("aggregated['smooth'] > 10")
        assert accum_pos < having_pos
        compile(code, "accum_before_having.py", "exec")

    def test_full_pipeline_with_both_rate_and_accumulate(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> rate: 100 per 1s on_excess: warn
            |> filter: msg.v > 0
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
            |> having: smooth > 5
            |> map: {
                smoothed = smooth;
                cumulative = running;
                delta_val = change
            }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_rate_limit = 100" in code
        assert "_accum_state" in code
        assert '"smooth"' in code
        assert '"running"' in code
        assert '"change"' in code
        compile(code, "full_rate_accum.py", "exec")


class TestFmtCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_fmt_generates_str_format(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> map: {
                label = fmt("Temperature: {}C", msg.temp)
            }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "'Temperature: {}C'.format" in code
        assert "_get_nested(msg, 'temp')" in code
        compile(code, "fmt_test.py", "exec")

    def test_fmt_multiple_args(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> map: {
                label = fmt("{} = {}", msg.key, msg.value)
            }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert (
            "'{} = {}'.format(_get_nested(msg, 'key'), _get_nested(msg, 'value'))"
            in code
        )
        compile(code, "fmt_multi.py", "exec")


class TestTypeAnnotationCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_float_annotation_generates_coercion(self):
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
        code = self._gen_pipeline_code(src)
        assert 'float(aggregated["total"])' in code
        compile(code, "type_float.py", "exec")

    def test_int_annotation_generates_coercion(self):
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
        code = self._gen_pipeline_code(src)
        assert 'int(aggregated["n"])' in code
        compile(code, "type_int.py", "exec")

    def test_no_annotation_no_coercion(self):
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
        code = self._gen_pipeline_code(src)
        assert "float(aggregated" not in code
        assert "int(aggregated" not in code
        compile(code, "no_type.py", "exec")

    def test_mixed_annotations(self):
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
        code = self._gen_pipeline_code(src)
        assert 'float(aggregated["total"])' in code
        assert 'float(aggregated["avg_val"])' in code
        assert 'int(aggregated["n"])' not in code
        compile(code, "mixed_type.py", "exec")


class TestHardeningCodegen:
    def _gen_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                pipe_code = f.read()
            with open(os.path.join(tmpdir, "app.py")) as f:
                app_code = f.read()
            return app_code, pipe_code

    def test_drain_method_exists(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(10 s)
            |> aggregate: { total = sum(msg.value) }
            |> sink to b :: "out"
        end
        """
        app_code, pipe_code = self._gen_code(src)
        assert "def drain(self):" in pipe_code
        assert "_flush_window()" in pipe_code
        compile(pipe_code, "drain_test.py", "exec")

    def test_drain_no_window_no_batch(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        _, pipe_code = self._gen_code(src)
        assert "def drain(self):" in pipe_code
        assert "_flush_window" not in pipe_code
        assert "_flush_batch" not in pipe_code
        compile(pipe_code, "drain_empty.py", "exec")

    def test_publish_with_retry_exists(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        _, pipe_code = self._gen_code(src)
        assert "def _publish_with_retry(self, publisher, payload):" in pipe_code
        assert "_MAX_PUBLISH_RETRIES" in pipe_code
        assert "_PUBLISH_RETRY_BASE" in pipe_code
        compile(pipe_code, "retry_test.py", "exec")

    def test_message_validation_exists(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        _, pipe_code = self._gen_code(src)
        assert "json.loads" in pipe_code
        assert "json.JSONDecodeError" in pipe_code
        assert "Invalid message" in pipe_code
        compile(pipe_code, "msg_validation.py", "exec")

    def test_app_json_logging_support(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        app_code, _ = self._gen_code(src)
        assert "_configure_logging" in app_code
        assert "LOG_FORMAT" in app_code
        assert "LOG_LEVEL" in app_code
        assert "_JsonFormatter" in app_code
        compile(app_code, "app_logging.py", "exec")

    def test_app_graceful_shutdown(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        app_code, _ = self._gen_code(src)
        assert "p.drain()" in app_code
        assert "_shutdown_in_progress" in app_code
        assert "Forced shutdown" in app_code
        compile(app_code, "app_shutdown.py", "exec")


class TestSchemaCodegen:
    def _gen_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_schema_pipeline_ctx_has_name(self):
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
        from sigil.codegen import _build_pipeline_ctx, _build_const_map

        const_map = _build_const_map(model)
        ctx = _build_pipeline_ctx(model.pipelines[0], const_map)
        assert ctx.schema_name == "Sensor"

    def test_schema_no_binding_ctx_none(self):
        src = """
        schema Sensor
            temp: float
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        model = parse_string(src)
        from sigil.codegen import _build_pipeline_ctx, _build_const_map

        const_map = _build_const_map(model)
        ctx = _build_pipeline_ctx(model.pipelines[0], const_map)
        assert ctx.schema_name is None

    def test_generated_code_compiles_with_schema(self):
        src = """
        schema Sensor
            temp: float
            humidity: float
            sensor_id: str
        end
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in" schema: Sensor
            |> filter: msg.temp > 30.0
            |> sink to b :: "out"
        end
        """
        code = self._gen_code(src)
        compile(code, "schema_test.py", "exec")


class TestOnErrorCodegen:
    def _gen_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_on_error_sink_generates_dlq_publisher(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            on_error: sink to b :: "errors/p"
        end
        """
        code = self._gen_code(src)
        assert "DlqPublisher" in code
        assert "DlqConnParams" in code

    def test_on_error_sink_generates_publish_to_dlq(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            on_error: sink to b :: "errors/p"
        end
        """
        code = self._gen_code(src)
        assert "_publish_to_dlq" in code
        assert "_dlq_publisher" in code

    def test_on_error_drop_no_dlq_publisher(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            on_error: drop
        end
        """
        code = self._gen_code(src)
        assert "DlqPublisher" not in code
        assert "_dlq_publisher" not in code

    def test_on_error_drop_debug_logging(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            on_error: drop
        end
        """
        code = self._gen_code(src)
        assert "logger.debug" in code
        assert "on_error: drop" in code

    def test_no_on_error_keeps_existing_behavior(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        code = self._gen_code(src)
        assert "DlqPublisher" not in code
        assert "_publish_to_dlq" not in code
        assert "on_error: drop" not in code

    def test_on_error_sink_code_compiles(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> filter: msg.value > 0
            |> window: tumbling(5 m)
            |> aggregate: { n = count() }
            |> sink to b :: "out"
            on_error: sink to b :: "errors/p"
        end
        """
        code = self._gen_code(src)
        compile(code, "on_error_sink.py", "exec")

    def test_on_error_drop_code_compiles(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> filter: msg.value > 0
            |> sink to b :: "out"
            on_error: drop
        end
        """
        code = self._gen_code(src)
        compile(code, "on_error_drop.py", "exec")

    def test_on_error_sink_has_dlq_topic(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            on_error: sink to b :: "dead-letters/p"
        end
        """
        code = self._gen_code(src)
        assert "dead-letters/p" in code

    def test_on_error_sink_has_error_envelope(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            on_error: sink to b :: "errors/p"
        end
        """
        code = self._gen_code(src)
        assert "envelope" in code or "DLQ" in code


class TestNewStagesCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_flatten_codegen(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> flatten: msg.items
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_get_nested(msg," in code
        assert "_process_flattened" in code
        assert "isinstance(" in code and "list" in code
        compile(code, "flatten_test.py", "exec")

    def test_distinct_codegen(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> distinct: msg.event_id within 30 s
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_distinct_seen" in code
        assert "_distinct_ttl" in code
        compile(code, "distinct_test.py", "exec")

    def test_throttle_drop_codegen(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> throttle: 100 per 1 s overflow: drop
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_throttle_tokens" in code
        assert "_throttle_limit" in code
        compile(code, "throttle_drop_test.py", "exec")

    def test_throttle_buffer_codegen(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> throttle: 10 per 1 s overflow: buffer buffer: 500
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_throttle_buffer" in code
        assert "_schedule_throttle_drain" in code
        assert "_throttle_drain_loop" in code
        compile(code, "throttle_buffer_test.py", "exec")

    def test_sample_codegen(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sample: 50 %
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "random.randint(1, 100)" in code
        compile(code, "sample_test.py", "exec")

    def test_all_new_stages_compile(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> flatten: msg.items
            |> distinct: msg.id within 10 s
            |> throttle: 50 per 5 s overflow: buffer buffer: 200
            |> sample: 75 %
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        compile(code, "all_new_stages.py", "exec")


class TestRetryPolicyCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_retry_generates_custom_constants(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            retry: 5 with backoff 200 ms
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_MAX_PUBLISH_RETRIES = 5" in code
        assert "_PUBLISH_RETRY_BASE = 0.2" in code
        compile(code, "retry_custom.py", "exec")

    def test_no_retry_generates_defaults(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_MAX_PUBLISH_RETRIES = 3" in code
        assert "_PUBLISH_RETRY_BASE = 0.1" in code
        compile(code, "retry_default.py", "exec")

    def test_retry_with_seconds_backoff(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> sink to b :: "out"
            retry: 10 with backoff 2 s
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_MAX_PUBLISH_RETRIES = 10" in code
        assert "_PUBLISH_RETRY_BASE = 2" in code
        compile(code, "retry_seconds.py", "exec")

    def test_retry_code_compiles_with_full_pipeline(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> filter: msg.value > 0
            |> window: tumbling(5 m)
            |> aggregate: { n = count() }
            |> sink to b :: "out"
            retry: 7 with backoff 300 ms
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_MAX_PUBLISH_RETRIES = 7" in code
        compile(code, "retry_full.py", "exec")


class TestMultiWindowCodegen:
    def _gen_pipeline_code(self, src):
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            pname = model.pipelines[0].name
            with open(os.path.join(tmpdir, "pipelines", f"{pname}.py")) as f:
                return f.read()

    def test_single_window_backward_compat(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window: tumbling(1 m)
            |> aggregate: { n = count() }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_window_data" in code
        assert "_schedule_window_flush" in code
        assert "_window_data_short" not in code
        compile(code, "single_window.py", "exec")

    def test_multi_window_per_window_data(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window short: tumbling(1 m)
            |> window long: tumbling(5 m)
            |> aggregate: {
                avg_1m = avg(msg.value) over short
                avg_5m = avg(msg.value) over long
            }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_window_data_short" in code
        assert "_window_data_long" in code
        assert "_schedule_window_flush_short" in code
        assert "_schedule_window_flush_long" in code
        compile(code, "multi_window.py", "exec")

    def test_multi_window_per_window_process(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window short: tumbling(1 m)
            |> window long: tumbling(5 m)
            |> aggregate: {
                avg_1m = avg(msg.value) over short
                avg_5m = avg(msg.value) over long
            }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert "_process_window_short" in code
        assert "_process_window_long" in code
        compile(code, "multi_window_process.py", "exec")

    def test_multi_window_result_has_window_field(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window short: tumbling(1 m)
            |> window long: tumbling(5 m)
            |> aggregate: {
                avg_1m = avg(msg.value) over short
                avg_5m = avg(msg.value) over long
            }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        assert '"_window"' in code
        compile(code, "multi_window_field.py", "exec")

    def test_multi_window_compiles(self):
        src = """
        broker b host: "h" port: 1 end
        pipeline p
            source from b :: "in"
            |> window short: tumbling(30 s)
            |> window long: sliding(10 m, step = 2 m)
            |> aggregate: {
                avg_short = avg(msg.value) over short
                n_long = count() over long
            }
            |> map: {
                fast_avg = avg_short;
                slow_count = n_long
            }
            |> sink to b :: "out"
        end
        """
        code = self._gen_pipeline_code(src)
        compile(code, "multi_window_full.py", "exec")
