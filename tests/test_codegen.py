import os
import tempfile

import pytest

from sigil.codegen import generate
from sigil.explain import explain_model
from sigil.expressions import ExprContext, compile_expr
from sigil.language import parse_string


def _stages(pipeline):
    return [s.stage if s.__class__.__name__ == "AnnotatedStage" else s for s in pipeline.stages]


SRC = """
BROKER local HOST: "localhost" PORT: 1883 ;

BROKER remote
    HOST: "mqtt.cloud.io"
    PORT: 8883
    SSL: true
    AUTH: {
        USERNAME: "user"
        PASSWORD: "pass"
    }
;

PIPELINE temp_avg
    SOURCE FROM local :: "sensors/+/temperature" QOS: 1 FORMAT: json

    |> FILTER msg.value > -40 and msg.value < 85

    |> WINDOW tumbling(5 m)

    |> AGGREGATE {
        avg_val = avg(msg.value),
        n       = count()
    }

    |> MAP {
        average = avg_val,
        samples = n
    }

    |> SINK TO remote :: "analytics/temp" QOS: 1
;
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
        with open(os.path.join(tmpdir, "app.py")) as f:
            app_code = f.read()
        assert "from pipelines.temp_avg import TempAvgPipeline" in app_code
        assert "TempAvgPipeline()" in app_code


def test_generated_pipeline_has_class():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "temp_avg.py")) as f:
            code = f.read()
        assert "class TempAvgPipeline:" in code
        assert "def start(self):" in code
        assert "def stop(self" in code
        assert "def _on_message(" in code


def test_generated_pipeline_contains_filter():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "temp_avg.py")) as f:
            code = f.read()
        assert "_get_nested(msg, 'value')" in code
        assert "(-40" in code
        assert "85" in code


def test_generated_pipeline_contains_aggregation():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "temp_avg.py")) as f:
            code = f.read()
        assert 'aggregated["avg_val"]' in code
        assert 'aggregated["n"]' in code
        assert "statistics.mean" in code
        assert "len(messages)" in code


def test_generated_pipeline_contains_mapping():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "temp_avg.py")) as f:
            code = f.read()
        assert 'result["average"]' in code
        assert 'result["samples"]' in code


def test_generated_pipeline_broker_config():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "temp_avg.py")) as f:
            code = f.read()
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
        with open(os.path.join(tmpdir, "pipelines", "temp_avg.py")) as f:
            code = f.read()
        compile(code, "<test>", "exec")


def test_generated_app_compiles():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "app.py")) as f:
            code = f.read()
        compile(code, "<test>", "exec")


_FILTER_EXPR_CASES = [
    ("simple_comparison", "msg.x > 10", ["_get_nested(msg, 'x') > ", "10"]),
    ("and_expression", "msg.a > 0 and msg.b < 100", ["and"]),
    ("or_expression", "msg.a > 0 or msg.b < 100", ["or"]),
    ("not_expression", "not msg.disabled", ["not"]),
    ("nested_field_ref", "msg.location.lat > 0", ["_get_nested(msg, 'location', 'lat')"]),
    ("string_literal", 'msg.status == "ok"', ["'ok'"]),
    ("if_then_else", "if msg.x > 0 then 1 else 0", ["if", "else"]),
    ("is_null", "msg.x is null", ["is None"]),
    ("is_not_null", "msg.x is not null", ["is not None"]),
    ("coalesce", "msg.x ?? 0", ["_coalesce"]),
    ("in_operator", 'msg.status in ["a", "b"]', ["in ["]),
    ("not_in_operator", "msg.x not in [1, 2]", ["not in ["]),
    ("scalar_func_abs", "abs(msg.x)", ["abs("]),
    ("scalar_func_len", "len(msg.items) > 0", ["len("]),
    ("scalar_func_lower", 'lower(msg.name) == "a"', ["lower("]),
    ("scalar_func_upper", 'upper(msg.name) == "A"', ["upper("]),
]


@pytest.mark.parametrize(
    "expr_src,substrings",
    [(case[1], case[2]) for case in _FILTER_EXPR_CASES],
    ids=[case[0] for case in _FILTER_EXPR_CASES],
)
def test_filter_expression_compilation_table(expr_src, substrings):
    src = f"""
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "t"
        |> FILTER {expr_src}
        |> SINK TO b :: "t"
    ;
    """
    model = parse_string(src)
    expr = _stages(model.pipelines[0])[0].condition
    code = compile_expr(expr, ExprContext.FILTER)
    for needle in substrings:
        assert needle in code, f"expected {needle!r} in compiled code: {code!r}"


class TestExpressionCompiler:
    def _parse_filter_expr(self, expr_src):
        src = f"""
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER {expr_src}
            |> SINK TO b :: "t"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> AGGREGATE { r = avg(msg.v) }
            |> MAP { f = r * 1.8 + 32 }
            |> SINK TO b :: "t"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> AGGREGATE { v = avg(msg.x) }
            |> MAP { out = case { when v > 0 => "pos" when v == 0 => "zero" else => "neg" }}
            |> SINK TO b :: "t"
        ;
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
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP {
            LET ratio = v * 100,
            result = ratio
        }
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "ratio = " in code
        ratio_pos = code.index("ratio = ")
        result_pos = code.index('result["result"]')
        assert ratio_pos < result_pos
        compile(code, "<test>", "exec")


def test_generated_pipeline_uses_commlib():
    model = parse_string(SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "temp_avg.py")) as f:
            code = f.read()
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
        with open(os.path.join(tmpdir, "requirements.txt")) as f:
            reqs = f.read()
        assert "commlib-py" in reqs
        assert "paho-mqtt" not in reqs


def test_generated_pipeline_redis_transport():
    src = """
    BROKER cache
        TRANSPORT: REDIS
        HOST: "redis.local"
        PORT: 6379
    ;
    PIPELINE redis_pipe
        SOURCE FROM cache :: "in"
        |> SINK TO cache :: "out"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "redis_pipe.py")) as f:
            code = f.read()
        assert "from commlib.transports.redis import" in code
        assert '"redis.local"' in code
        assert "6379" in code
        compile(code, "<test>", "exec")


def test_generated_pipeline_cross_transport():
    src = """
    BROKER edge
        TRANSPORT: MQTT
        HOST: "localhost"
        PORT: 1883
    ;
    BROKER cloud
        TRANSPORT: REDIS
        HOST: "redis.cloud.io"
        PORT: 6379
    ;
    PIPELINE bridge
        SOURCE FROM edge :: "sensors/temp"
        |> SINK TO cloud :: "cloud.temp"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "bridge.py")) as f:
            code = f.read()
        assert "from commlib.transports.mqtt import" in code
        assert "from commlib.transports.redis import" in code
        assert "SourceConnParams" in code
        assert "SinkConnParams" in code
        compile(code, "<test>", "exec")


def test_generated_pipeline_default_transport():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "from commlib.transports.mqtt import" in code
        compile(code, "<test>", "exec")


def test_const_inlined_in_filter():
    src = """
    CONST THRESHOLD = 42
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER msg.v > THRESHOLD
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "42" in code
        compile(code, "<test>", "exec")


def test_const_inlined_in_map():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "1.5" in code
        compile(code, "<test>", "exec")


def test_env_ref_generates_import_os():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER msg.v > env("THRESHOLD", 100)
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "import os" in code
        compile(code, "<test>", "exec")


def test_case_in_map_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { v = avg(msg.x) }
        |> MAP { out = case { when v > 100 => "high" when v > 0 => "low" else => "none" }}
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "'high'" in code
        assert "'low'" in code
        compile(code, "<test>", "exec")


def test_having_codegen():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "aggregated['n']" in code
        assert "5" in code
        compile(code, "<test>", "exec")


def test_dedup_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> DEDUPLICATE msg.event_id WITHIN 30 s
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_dedup_seen" in code
        assert "_dedup_ttl" in code
        assert "30" in code
        compile(code, "<test>", "exec")


def test_temporal_codegen_has_prev_msg():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER old(msg.status) != msg.status
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "self._prev_msg = {}" in code
        assert "_get_nested(self._prev_msg" in code
        compile(code, "<test>", "exec")


# --- Phase 3: forall/exists codegen ---


def test_forall_compiles_to_all():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "all(" in code
        assert "for m in messages" in code
        compile(code, "<test>", "exec")


def test_exists_with_collection_compiles_to_any():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER exists(x in msg.tags => x == "critical")
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "any(" in code
        assert "_get_nested(msg, 'tags')" in code
        compile(code, "<test>", "exec")


def test_forall_expression_compiler():
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
    model = parse_string(src)
    having = next(s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "HavingStage")
    code = compile_expr(having.condition, ExprContext.HAVING)
    assert "all(m > 0" in code
    assert "for m in messages" in code


def test_exists_expression_compiler():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER exists(x in msg.items => x > 10)
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    filter_stage = _stages(model.pipelines[0])[0]
    code = compile_expr(filter_stage.condition, ExprContext.FILTER)
    assert "any(x > 10" in code
    assert "_get_nested(msg, 'items')" in code


# --- Phase 3: @initially/@finally codegen ---


def test_initially_hook_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        @INITIALLY { PUBLISH { status: "online" } }
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert '"status"' in code
        assert "'online'" in code
        compile(code, "<test>", "exec")


def test_finally_hook_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
        @FINALLY { PUBLISH { status: "offline" } }
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert '"status"' in code
        assert "'offline'" in code
        compile(code, "<test>", "exec")


def test_both_hooks_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        @INITIALLY { PUBLISH { status: "online", version: "1.0" } }
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
        @FINALLY { PUBLISH { status: "offline" } }
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "'online'" in code
        assert "'offline'" in code
        assert "'1.0'" in code
        compile(code, "<test>", "exec")


def test_initially_with_env_ref_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        @INITIALLY { PUBLISH { HOST: env("HOSTNAME", "unknown") } }
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "import os" in code
        assert "os.environ.get" in code
        compile(code, "<test>", "exec")


# --- Phase 3: sink format + batch codegen ---


def test_sink_batch_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" BATCH: { SIZE: 50  TIMEOUT: 5s  }
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_batch_buffer" in code
        assert "_batch_size" in code
        assert "50" in code
        assert "_flush_batch" in code
        compile(code, "<test>", "exec")


def test_sink_batch_size_only_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" BATCH: { SIZE: 100  }
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_batch_buffer" in code
        assert "_batch_size" in code
        assert "_schedule_batch_flush" not in code
        compile(code, "<test>", "exec")


def test_sink_csv_format_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" FORMAT: csv
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "import csv" in code
        assert "import io" in code
        assert "DictWriter" in code
        compile(code, "<test>", "exec")


def test_sink_raw_format_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b" FORMAT: raw
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "str(msg)" in code
        compile(code, "<test>", "exec")


def test_sink_no_batch_no_format_codegen():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> SINK TO b :: "b"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_batch_buffer" not in code
        assert "DictWriter" not in code
        compile(code, "<test>", "exec")


# --- Phase 4: pipeline chaining codegen ---


def test_pipe_source_codegen():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "downstream.py")) as f:
            code = f.read()
        assert "_downstream_listeners" in code
        assert "Subscriber" not in code
        compile(code, "<test>", "exec")


def test_pipe_source_app_wiring():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "app.py")) as f:
            app_code = f.read()
        assert "_downstream_listeners" in app_code
        assert "_on_message" in app_code
        compile(app_code, "<test>", "exec")


def test_pipe_source_no_subscriber():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "downstream.py")) as f:
            code = f.read()
        assert "self._subscriber" not in code


# --- Phase 4: route codegen ---


def test_route_codegen_compiles():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        compile(code, "<test>", "exec")


def test_route_codegen_has_route_publishers():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_route_pub_1" in code
        assert "_route_pub_default" in code


def test_route_codegen_has_condition():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    BROKER b2 HOST: "h2" PORT: 2 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> ROUTE
            when msg.x > 100 => SINK TO b :: "high"
            when msg.x > 0 => SINK TO b2 :: "low"
            DEFAULT => SINK TO b :: "other"
        ;
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "if " in code
        assert "elif " in code
        compile(code, "<test>", "exec")


def test_route_no_single_publisher():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        assert "self._publisher" not in code


# --- Phase 4: stage annotation codegen ---


def test_annotation_does_not_affect_codegen():
    base_src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> FILTER msg.x > 0
        |> SINK TO b :: "b"
    ;
    """
    annotated_src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "a"
        |> @WARN("slow") FILTER msg.x > 0
        |> SINK TO b :: "b"
    ;
    """
    model_base = parse_string(base_src)
    model_ann = parse_string(annotated_src)
    with tempfile.TemporaryDirectory() as tmpdir_base:
        generate(model_base, tmpdir_base)
        with open(os.path.join(tmpdir_base, "pipelines", "p.py")) as f:
            code_base = f.read()
    with tempfile.TemporaryDirectory() as tmpdir_ann:
        generate(model_ann, tmpdir_ann)
        with open(os.path.join(tmpdir_ann, "pipelines", "p.py")) as f:
            code_ann = f.read()
    assert code_base == code_ann


# --- Phase 4: explain ---


def test_explain_basic_pipeline():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE sensor_avg
        SOURCE FROM b :: "sensors/in"
        |> SINK TO b :: "sensors/out"
    ;
    """
    model = parse_string(src)
    output = explain_model(model)
    assert "sensor_avg" in output
    assert "sensors/in" in output
    assert "sensors/out" in output


def test_explain_stages():
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> FILTER msg.x > 0
        |> WINDOW tumbling(5 m)
        |> AGGREGATE { n = count() }
        |> SINK TO b :: "out"
    ;
    """
    model = parse_string(src)
    output = explain_model(model)
    assert "filter" in output
    assert "window" in output
    assert "aggregate" in output


def test_explain_pipe_source():
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
    model = parse_string(src)
    output = explain_model(model)
    assert "receives from pipeline" in output


class TestRightSideArithmeticCompilation:
    """Verify compiled expressions produce correct left-associative Python."""

    def _filter_code(self, expr_src):
        src = f"""
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER {expr_src}
            |> SINK TO b :: "o"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> RATE 100 PER 1s
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_rate_limit = 100" in code
        assert "_rate_period = 1" in code
        assert "_rate_tokens" in code
        assert "_rate_lock" in code
        compile(code, "rate_test.py", "exec")

    def test_rate_on_excess_warn(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> RATE 50 PER 5s ON_EXCESS: WARN
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "logger.warning" in code
        assert "Rate limit exceeded" in code
        compile(code, "rate_warn.py", "exec")

    def test_rate_on_excess_error(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> RATE 10 PER 1s ON_EXCESS: ERROR
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "logger.error" in code
        compile(code, "rate_error.py", "exec")

    def test_rate_on_excess_drop_no_log(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> RATE 10 PER 1s ON_EXCESS: DROP
            |> SINK TO b :: "out"
        ;
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
        code = self._gen_pipeline_code(src)
        assert "JoinSubscriber" in code
        assert "_join_subscriber" in code
        compile(code, "join_sub.py", "exec")

    def test_join_inner_pending_buffer(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "orders"
            |> JOIN b :: "customers"
                ON msg.id == other.id
                WITHIN 10 s
                TYPE: inner
            ;
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "join_pending" in code
        compile(code, "join_inner.py", "exec")

    def test_join_left_no_pending(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "orders"
            |> JOIN b :: "customers"
                ON msg.id == other.id
                WITHIN 10 s
                TYPE: left
            ;
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "join_pending.append" not in code
        compile(code, "join_left.py", "exec")

    def test_join_other_in_map(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "orders"
            |> JOIN b :: "customers"
                ON msg.id == other.id
                WITHIN 10 s
            ;
            |> MAP { name = other.name }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_get_nested(other, 'name')" in code
        compile(code, "join_map.py", "exec")

    def test_join_compiles(self):
        src = """
        BROKER local
            HOST: "localhost"
            PORT: 1883
        ;
        BROKER remote
            HOST: "remote.io"
            PORT: 1883
        ;
        PIPELINE p
            SOURCE FROM local :: "orders"
            |> JOIN remote :: "customers"
                ON msg.customer_id == other.id
                WITHIN 30 s
                TYPE: inner
            ;
            |> FILTER msg.value > 0
            |> MAP { customer = other.name }
            |> SINK TO local :: "enriched"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10s)
            |> AGGREGATE { avg_val = avg(msg.v) }
            |> ACCUMULATE { smooth = ema(avg_val, 0.3) }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_accum_state" in code
        assert '"smooth"' in code
        assert "0.3" in code
        compile(code, "accum_ema.py", "exec")

    def test_accumulate_cumsum_generates_state(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10s)
            |> AGGREGATE { total = sum(msg.v) }
            |> ACCUMULATE { running = cumsum(total) }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_accum_state" in code
        assert '"running"' in code
        compile(code, "accum_cumsum.py", "exec")

    def test_accumulate_delta_generates_state(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10s)
            |> AGGREGATE { total = sum(msg.v) }
            |> ACCUMULATE { change = delta(total) }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_accum_state" in code
        assert '"change"' in code
        assert '"change_prev"' in code
        compile(code, "accum_delta.py", "exec")

    def test_accumulate_injected_before_having(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10s)
            |> AGGREGATE { avg_val = avg(msg.v) }
            |> ACCUMULATE { smooth = ema(avg_val, 0.3) }
            |> HAVING smooth > 10
            |> MAP { result = smooth }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        accum_pos = code.index("_accum_state")
        having_pos = code.index("aggregated['smooth'] > 10")
        assert accum_pos < having_pos
        compile(code, "accum_before_having.py", "exec")

    def test_full_pipeline_with_both_rate_and_accumulate(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> RATE 100 PER 1s ON_EXCESS: WARN
            |> FILTER msg.v > 0
            |> WINDOW tumbling(10s)
            |> AGGREGATE {
                avg_val = avg(msg.v),
                total = sum(msg.v)
            }
            |> ACCUMULATE {
                smooth = ema(avg_val, 0.3),
                running = cumsum(total),
                change = delta(total)
            }
            |> HAVING smooth > 5
            |> MAP {
                smoothed = smooth,
                cumulative = running,
                delta_val = change
            }
            |> SINK TO b :: "out"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> MAP {
                label = fmt("Temperature: {}C", msg.temp)
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "'Temperature: {}C'.format" in code
        assert "_get_nested(msg, 'temp')" in code
        compile(code, "fmt_test.py", "exec")

    def test_fmt_multiple_args(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> MAP {
                label = fmt("{} = {}", msg.key, msg.value)
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "'{} = {}'.format(_get_nested(msg, 'key'), _get_nested(msg, 'value'))" in code
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10 s)
            |> AGGREGATE {
                total: float = sum(msg.value)
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert 'float(aggregated["total"])' in code
        compile(code, "type_float.py", "exec")

    def test_int_annotation_generates_coercion(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10 s)
            |> AGGREGATE {
                n: int = count()
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert 'int(aggregated["n"])' in code
        compile(code, "type_int.py", "exec")

    def test_no_annotation_no_coercion(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10 s)
            |> AGGREGATE {
                total = sum(msg.value)
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "float(aggregated" not in code
        assert "int(aggregated" not in code
        compile(code, "no_type.py", "exec")

    def test_mixed_annotations(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10 s)
            |> AGGREGATE {
                total: float = sum(msg.value),
                n = count(),
                avg_val: float = avg(msg.value)
            }
            |> SINK TO b :: "out"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10 s)
            |> AGGREGATE { total = sum(msg.value) }
            |> SINK TO b :: "out"
        ;
        """
        _app_code, pipe_code = self._gen_code(src)
        assert "def drain(self):" in pipe_code
        assert "_flush_window()" in pipe_code
        compile(pipe_code, "drain_test.py", "exec")

    def test_drain_no_window_no_batch(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
        """
        _, pipe_code = self._gen_code(src)
        assert "def drain(self):" in pipe_code
        assert "_flush_window" not in pipe_code
        assert "_flush_batch" not in pipe_code
        compile(pipe_code, "drain_empty.py", "exec")

    def test_publish_with_retry_exists(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
        """
        _, pipe_code = self._gen_code(src)
        assert "def _publish_with_retry(self, publisher, payload):" in pipe_code
        assert "_MAX_PUBLISH_RETRIES" in pipe_code
        assert "_PUBLISH_RETRY_BASE" in pipe_code
        compile(pipe_code, "retry_test.py", "exec")

    def test_message_validation_exists(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
        """
        _, pipe_code = self._gen_code(src)
        assert "json.loads" in pipe_code
        assert "json.JSONDecodeError" in pipe_code
        assert "Invalid message" in pipe_code
        compile(pipe_code, "msg_validation.py", "exec")

    def test_app_json_logging_support(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
        """
        app_code, _ = self._gen_code(src)
        assert "_configure_logging" in app_code
        assert "LOG_FORMAT" in app_code
        assert "LOG_LEVEL" in app_code
        assert "_JsonFormatter" in app_code
        compile(app_code, "app_logging.py", "exec")

    def test_app_graceful_shutdown(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
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
        SCHEMA Sensor
            temp: float
            humidity: float
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in" SCHEMA: Sensor
            |> FILTER msg.temp > 30.0
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        from sigil.codegen import _build_const_map, _build_pipeline_ctx

        const_map = _build_const_map(model)
        ctx = _build_pipeline_ctx(model.pipelines[0], const_map)
        assert ctx.schema_name == "Sensor"

    def test_schema_no_binding_ctx_none(self):
        src = """
        SCHEMA Sensor
            temp: float
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        from sigil.codegen import _build_const_map, _build_pipeline_ctx

        const_map = _build_const_map(model)
        ctx = _build_pipeline_ctx(model.pipelines[0], const_map)
        assert ctx.schema_name is None

    def test_generated_code_compiles_with_schema(self):
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: SINK TO b :: "errors/p"
        ;
        """
        code = self._gen_code(src)
        assert "DlqPublisher" in code
        assert "DlqConnParams" in code

    def test_on_error_sink_generates_publish_to_dlq(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: SINK TO b :: "errors/p"
        ;
        """
        code = self._gen_code(src)
        assert "_publish_to_dlq" in code
        assert "_dlq_publisher" in code

    def test_on_error_drop_no_dlq_publisher(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: DROP
        ;
        """
        code = self._gen_code(src)
        assert "DlqPublisher" not in code
        assert "_dlq_publisher" not in code

    def test_on_error_drop_debug_logging(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: DROP
        ;
        """
        code = self._gen_code(src)
        assert "logger.debug" in code
        assert "on_error: drop" in code

    def test_no_on_error_keeps_existing_behavior(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_code(src)
        assert "DlqPublisher" not in code
        assert "_publish_to_dlq" not in code
        assert "on_error: drop" not in code

    def test_on_error_sink_code_compiles(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> FILTER msg.value > 0
            |> WINDOW tumbling(5 m)
            |> AGGREGATE { n = count() }
            |> SINK TO b :: "out"
            ON_ERROR: SINK TO b :: "errors/p"
        ;
        """
        code = self._gen_code(src)
        compile(code, "on_error_sink.py", "exec")

    def test_on_error_drop_code_compiles(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> FILTER msg.value > 0
            |> SINK TO b :: "out"
            ON_ERROR: DROP
        ;
        """
        code = self._gen_code(src)
        compile(code, "on_error_drop.py", "exec")

    def test_on_error_sink_has_dlq_topic(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: SINK TO b :: "dead-letters/p"
        ;
        """
        code = self._gen_code(src)
        assert "dead-letters/p" in code

    def test_on_error_sink_has_error_envelope(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: SINK TO b :: "errors/p"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> FLATTEN msg.items
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_get_nested(msg," in code
        assert "_process_flattened" in code
        assert "isinstance(" in code and "list" in code
        compile(code, "flatten_test.py", "exec")

    def test_distinct_codegen(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> DISTINCT msg.event_id WITHIN 30 s
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_distinct_seen" in code
        assert "_distinct_ttl" in code
        compile(code, "distinct_test.py", "exec")

    def test_throttle_drop_codegen(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> THROTTLE 100 PER 1 s OVERFLOW: DROP
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_throttle_tokens" in code
        assert "_throttle_limit" in code
        compile(code, "throttle_drop_test.py", "exec")

    def test_throttle_buffer_codegen(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> THROTTLE 10 PER 1 s OVERFLOW: BUFFER BUFFER: 500
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_throttle_buffer" in code
        assert "_schedule_throttle_drain" in code
        assert "_throttle_drain_loop" in code
        compile(code, "throttle_buffer_test.py", "exec")

    def test_sample_codegen(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SAMPLE 50 %
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "random.randint(1, 100)" in code
        compile(code, "sample_test.py", "exec")

    def test_all_new_stages_compile(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> FLATTEN msg.items
            |> DISTINCT msg.id WITHIN 10 s
            |> THROTTLE 50 PER 5 s OVERFLOW: BUFFER BUFFER: 200
            |> SAMPLE 75 %
            |> SINK TO b :: "out"
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            RETRY: 5 WITH BACKOFF 200 ms
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_MAX_PUBLISH_RETRIES = 5" in code
        assert "_PUBLISH_RETRY_BASE = 0.2" in code
        compile(code, "retry_custom.py", "exec")

    def test_no_retry_generates_defaults(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_MAX_PUBLISH_RETRIES = 3" in code
        assert "_PUBLISH_RETRY_BASE = 0.1" in code
        compile(code, "retry_default.py", "exec")

    def test_retry_with_seconds_BACKOFF(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            RETRY: 10 WITH BACKOFF 2 s
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_MAX_PUBLISH_RETRIES = 10" in code
        assert "_PUBLISH_RETRY_BASE = 2" in code
        compile(code, "retry_seconds.py", "exec")

    def test_retry_code_compiles_with_full_pipeline(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> FILTER msg.value > 0
            |> WINDOW tumbling(5 m)
            |> AGGREGATE { n = count() }
            |> SINK TO b :: "out"
            RETRY: 7 WITH BACKOFF 300 ms
        ;
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
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { n = count() }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_window_data" in code
        assert "_schedule_window_flush" in code
        assert "_window_data_short" not in code
        compile(code, "single_window.py", "exec")

    def test_multi_window_per_window_data(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW short tumbling(1 m)
            |> WINDOW long tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short,
                avg_5m = avg(msg.value) OVER long
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_window_data_short" in code
        assert "_window_data_long" in code
        assert "_schedule_window_flush_short" in code
        assert "_schedule_window_flush_long" in code
        compile(code, "multi_window.py", "exec")

    def test_multi_window_per_window_process(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW short tumbling(1 m)
            |> WINDOW long tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short,
                avg_5m = avg(msg.value) OVER long
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert "_process_window_short" in code
        assert "_process_window_long" in code
        compile(code, "multi_window_process.py", "exec")

    def test_multi_window_result_has_window_field(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW short tumbling(1 m)
            |> WINDOW long tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short,
                avg_5m = avg(msg.value) OVER long
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        assert '"_window"' in code
        compile(code, "multi_window_field.py", "exec")

    def test_multi_window_compiles(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW short tumbling(30 s)
            |> WINDOW long sliding(10 m, STEP = 2 m)
            |> AGGREGATE {
                avg_short = avg(msg.value) OVER short,
                n_long = count() OVER long
            }
            |> MAP {
                fast_avg = avg_short,
                slow_count = n_long
            }
            |> SINK TO b :: "out"
        ;
        """
        code = self._gen_pipeline_code(src)
        compile(code, "multi_window_full.py", "exec")


# ─── MatchExpr expression compilation ──────────────────────────────


class TestMatchExprCompilation:
    """Tests for match expression compilation to nested ternaries."""

    def _parse_map_expr(self, map_body, *, prefix=""):
        """Parse a map expression and return the value AST node."""
        src = f"""
        BROKER b HOST: "h" PORT: 1 ;
        {prefix}
        PIPELINE p
            SOURCE FROM b :: "t"
            |> MAP {{ {map_body} }}
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        stages = _stages(model.pipelines[0])
        m = next(s for s in stages if s.__class__.__name__ == "MapStage")
        return m.mappings[0].value

    def test_match_single_arm_compiles_to_ternary(self):
        """match msg.type { case "A" => 1 else => 0 } -> ternary."""
        expr = self._parse_map_expr('out = match msg.type { case "A" => 1 else => 0 }')
        code = compile_expr(expr, ExprContext.MAP)
        assert "if" in code
        assert "else" in code
        assert "1" in code
        assert "0" in code
        assert "'A'" in code

    def test_match_two_arms_compiles_to_nested_ternary(self):
        """match with two case arms produces nested ternary."""
        expr = self._parse_map_expr(
            'out = match msg.type { case "A" => 1 case "B" => 2 else => 0 }'
        )
        code = compile_expr(expr, ExprContext.MAP)
        # Should produce: (1 if <subject> == 'A' else (2 if <subject> == 'B' else 0))
        assert code.count("if") == 2
        assert code.count("else") == 2
        assert "'A'" in code
        assert "'B'" in code

    def test_match_three_arms_compiles(self):
        """match with three case arms produces deeply nested ternary."""
        expr = self._parse_map_expr(
            'out = match msg.level { case 1 => "low" case 2 => "med" case 3 => "high" else => "unknown" }'
        )
        code = compile_expr(expr, ExprContext.MAP)
        assert code.count("if") == 3
        assert "'low'" in code
        assert "'med'" in code
        assert "'high'" in code
        assert "'unknown'" in code

    def test_match_without_else_defaults_to_none(self):
        """match without else clause compiles with None default."""
        expr = self._parse_map_expr('out = match msg.type { case "A" => 1 }')
        code = compile_expr(expr, ExprContext.MAP)
        assert "None" in code

    def test_match_subject_is_field_ref(self):
        """match subject compiles to _get_nested for msg.field."""
        expr = self._parse_map_expr('out = match msg.status { case "ok" => 1 else => 0 }')
        code = compile_expr(expr, ExprContext.MAP)
        assert "_get_nested(msg, 'status')" in code

    def test_match_in_filter_context(self):
        """match expression works in filter context."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER match msg.type { case "valid" => true else => false }
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        f = _stages(model.pipelines[0])[0]
        code = compile_expr(f.condition, ExprContext.FILTER)
        assert "True" in code
        assert "False" in code
        assert "_get_nested(msg, 'type')" in code


# ─── UserFuncCall expression compilation ───────────────────────────


class TestUserFuncCallCompilation:
    """Tests for user-defined function inlining via compile_expr."""

    def _build_func_defs(self, src):
        """Parse source and return func_defs dict."""
        model = parse_string(src)
        return {f.name: f for f in (model.functions or [])}

    def test_simple_func_inlines_body(self):
        """func double(x) = x * 2 + double(msg.value) -> inlined."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC double(x) = x * 2
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER double(msg.value) > 100
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        func_defs = {f.name: f for f in model.functions}
        f = _stages(model.pipelines[0])[0]
        code = compile_expr(f.condition, ExprContext.FILTER, func_defs=func_defs)
        # double(msg.value) should inline to (_get_nested(msg, 'value') * 2)
        assert "_get_nested(msg, 'value')" in code
        assert "* 2" in code
        assert "100" in code

    def test_celsius_func_inlines(self):
        """func celsius(t) = (t - 32) * 5 / 9 inlines correctly."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC celsius(t) = (t - 32) * 5 / 9
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER celsius(msg.temp) > 0
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        func_defs = {f.name: f for f in model.functions}
        f = _stages(model.pipelines[0])[0]
        code = compile_expr(f.condition, ExprContext.FILTER, func_defs=func_defs)
        assert "_get_nested(msg, 'temp')" in code
        assert "32" in code
        assert "5" in code
        assert "9" in code

    def test_multi_arg_func_inlines(self):
        """func add(a, b) = a + b inlines both arguments."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC add(a, b) = a + b
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER add(msg.x, msg.y) > 0
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        func_defs = {f.name: f for f in model.functions}
        f = _stages(model.pipelines[0])[0]
        code = compile_expr(f.condition, ExprContext.FILTER, func_defs=func_defs)
        assert "_get_nested(msg, 'x')" in code
        assert "_get_nested(msg, 'y')" in code

    def test_func_in_map_context(self):
        """User func call in map context uses MAP compilation."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC double(x) = x * 2
        PIPELINE p
            SOURCE FROM b :: "t"
            |> MAP { result = double(msg.value) }
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        func_defs = {f.name: f for f in model.functions}
        m = next(s for s in _stages(model.pipelines[0]) if s.__class__.__name__ == "MapStage")
        code = compile_expr(m.mappings[0].value, ExprContext.MAP, func_defs=func_defs)
        assert "_get_nested(msg, 'value')" in code
        assert "* 2" in code

    def test_func_defs_none_raises(self):
        """Calling user func without func_defs raises ValueError."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC double(x) = x * 2
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER double(msg.value) > 0
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        f = _stages(model.pipelines[0])[0]
        with pytest.raises(ValueError, match="Unknown function"):
            compile_expr(f.condition, ExprContext.FILTER, func_defs=None)


# ─── Full codegen: user-defined functions ──────────────────────────


class TestUserFuncCodegen:
    """Full code generation tests for user-defined functions."""

    def test_func_in_filter_generates_valid_code(self):
        """Pipeline with func in filter generates compilable code."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC double(x) = x * 2
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER double(msg.value) > 100
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "func_filter.py", "exec")
            # Inlined expression should appear in generated code
            assert "* 2" in code

    def test_func_in_map_generates_valid_code(self):
        """Pipeline with func in map generates compilable code."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC triple(x) = x * 3
        PIPELINE p
            SOURCE FROM b :: "t"
            |> MAP { result = triple(msg.value) }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "func_map.py", "exec")
            assert "* 3" in code

    def test_multi_func_pipeline(self):
        """Pipeline using multiple user-defined functions."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC double(x) = x * 2
        FUNC offset(x) = x + 10
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER double(msg.value) > 50
            |> MAP { adjusted = offset(msg.value) }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "multi_func.py", "exec")
            assert "* 2" in code
            assert "+ 10" in code


# ─── Full codegen: pipeline-level let bindings ─────────────────────


class TestPipelineLetCodegen:
    """Full code generation tests for pipeline-level let bindings."""

    def test_single_let_generates_assignment(self):
        """Pipeline let binding generates assignment in _on_message."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            LET threshold = msg.value * 2
            |> FILTER msg.value > 0
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            assert "threshold = " in code
            compile(code, "let_single.py", "exec")

    def test_multiple_lets_generate_assignments(self):
        """Multiple pipeline let bindings generate ordered assignments."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            LET low = 10
            LET high = 100
            |> FILTER msg.value > 0
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            assert "low = " in code
            assert "high = " in code
            # Let bindings should appear before filter logic
            low_pos = code.index("low = ")
            high_pos = code.index("high = ")
            assert low_pos < high_pos
            compile(code, "let_multiple.py", "exec")

    def test_let_with_field_ref(self):
        """Pipeline let binding with msg field reference."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            LET scaled = msg.temperature * 1.8 + 32
            |> FILTER msg.value > 0
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            assert "scaled = " in code
            assert "1.8" in code
            assert "32" in code
            compile(code, "let_field_ref.py", "exec")


# ─── Full codegen: match expressions ───────────────────────────────


class TestMatchExprCodegen:
    """Full code generation tests for match expressions."""

    def test_match_in_map_generates_ternary(self):
        """Match expression in map generates nested ternary."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> MAP {
                category = match msg.level {
                    case 1 => "low"
                    case 2 => "med"
                    else => "high"
                }
            }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "match_map.py", "exec")
            # Should contain ternary pattern
            assert "if" in code
            assert "else" in code
            assert "'low'" in code
            assert "'med'" in code
            assert "'high'" in code

    def test_match_in_filter_generates_valid_code(self):
        """Match expression in filter generates compilable code."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER match msg.type { case "valid" => true else => false }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "match_filter.py", "exec")

    def test_match_many_arms_generates_valid_code(self):
        """Match with many arms generates deeply nested but valid code."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> MAP {
                label = match msg.code {
                    case 1 => "one"
                    case 2 => "two"
                    case 3 => "three"
                    case 4 => "four"
                    else => "other"
                }
            }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "match_many_arms.py", "exec")
            assert "'one'" in code
            assert "'four'" in code
            assert "'other'" in code


# ─── Full codegen: all three features combined ─────────────────────


class TestCombinedFeaturesCodegen:
    """Tests combining user funcs, pipeline lets, and match expressions."""

    def test_func_and_let_combined(self):
        """Pipeline using both user func and let binding."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC double(x) = x * 2
        PIPELINE p
            SOURCE FROM b :: "t"
            LET threshold = 50
            |> FILTER double(msg.value) > threshold
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "func_and_let.py", "exec")
            assert "threshold = " in code
            assert "* 2" in code

    def test_match_and_let_combined(self):
        """Pipeline using both match expression and let binding."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            LET base = msg.value * 10
            |> MAP {
                label = match msg.type {
                    case "A" => "alpha"
                    else => "other"
                }
            }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "match_and_let.py", "exec")
            assert "base = " in code
            assert "'alpha'" in code

    def test_all_three_features_combined(self):
        """Pipeline using user func, let binding, and match expression."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC scale(x) = x * 100
        PIPELINE p
            SOURCE FROM b :: "t"
            LET factor = msg.weight
            |> FILTER scale(msg.value) > 50
            |> MAP {
                category = match msg.level {
                    case 1 => "low"
                    case 2 => "mid"
                    else => "high"
                }
            }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "all_three.py", "exec")
            assert "factor = " in code
            assert "* 100" in code
            assert "'low'" in code
            assert "'mid'" in code
            assert "'high'" in code

    def test_func_with_match_in_map(self):
        """User func call combined with match in same map stage."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC negate(x) = x * -1
        PIPELINE p
            SOURCE FROM b :: "t"
            |> MAP {
                inverted = negate(msg.value),
                label = match msg.status {
                    case "ok" => "good"
                    else => "bad"
                }
            }
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate(model, tmpdir)
            with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
                code = f.read()
            compile(code, "func_with_match.py", "exec")
            assert "* -1" in code or "* (- 1)" in code or "-1" in code
            assert "'good'" in code
            assert "'bad'" in code


# ─── explain_model tests ──────────────────────────────────────────


class TestExplainConstants:
    """Test explain_model output for constants (lines 6-17)."""

    def test_explain_int_constant(self):
        src = """
        CONST THRESHOLD = 42
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Constants:" in output
        assert "THRESHOLD = 42" in output

    def test_explain_float_constant(self):
        src = """
        CONST FACTOR = 1.5
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Constants:" in output
        assert "FACTOR = 1.5" in output

    def test_explain_string_constant(self):
        src = """
        CONST LABEL = "hello"
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Constants:" in output
        assert "LABEL = 'hello'" in output

    def test_explain_bool_constant(self):
        src = """
        CONST ENABLED = true
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Constants:" in output
        assert "ENABLED = true" in output

    def test_explain_multiple_constants(self):
        src = """
        CONST A = 1
        CONST B = "two"
        CONST C = true
        CONST D = 3.14
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Constants:" in output
        assert "A = 1" in output
        assert "B = 'two'" in output
        assert "C = true" in output
        assert "D = 3.14" in output


class TestExplainSchemas:
    """Test explain_model output for schemas (lines 19-27, 203-215)."""

    def test_explain_strict_schema(self):
        src = """
        SCHEMA Sensor STRICT
            temperature: float
            humidity: float
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Schemas:" in output
        assert "Sensor (strict)" in output
        assert "temperature: float" in output
        assert "humidity: float" in output

    def test_explain_lenient_schema(self):
        src = """
        SCHEMA Event LENIENT
            event_type: str
            severity: int
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Event (lenient)" in output
        assert "event_type: str" in output
        assert "severity: int" in output

    def test_explain_schema_optional_field(self):
        src = """
        SCHEMA Reading
            value: float
            unit: str?
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "unit: str?" in output
        assert "value: float" in output
        # value is not optional, no trailing ?
        lines = output.split("\n")
        value_line = next(line for line in lines if "value:" in line)
        assert not value_line.strip().endswith("?")

    def test_explain_schema_nested_object_type(self):
        src = """
        SCHEMA Data
            meta: {
                timestamp: int,
                SOURCE: str
            }
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "meta: {timestamp: int, SOURCE: str}" in output

    def test_explain_schema_list_type(self):
        src = """
        SCHEMA Data
            values: [float]
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "values: [float]" in output

    def test_explain_schema_default_strictness(self):
        """Schema without explicit strictness defaults to strict."""
        src = """
        SCHEMA Simple
            x: int
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Simple (strict)" in output


class TestExplainFunctions:
    """Test explain_model output for user-defined functions (lines 29-34)."""

    def test_explain_single_param_func(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC double(x) = x * 2
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Functions:" in output
        assert "func double(x)" in output

    def test_explain_multi_param_func(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC add(a, b) = a + b
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "func add(a, b)" in output

    def test_explain_no_param_func(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        FUNC zero() = 0
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "func zero()" in output


class TestExplainPipelineLetBindings:
    """Test explain_model output for pipeline let bindings (lines 48-50)."""

    def test_explain_let_binding(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            LET threshold = msg.value * 2
            |> FILTER msg.value > 0
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "let threshold" in output
        assert "computed per message" in output


class TestExplainRoute:
    """Test explain_model output for route blocks (lines 58-67)."""

    def test_explain_route_with_arms(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> ROUTE
                when msg.v > 90 => SINK TO b :: "high"
                when msg.v > 50 => SINK TO b :: "med"
            ;
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Route:" in output
        assert "when" in output
        assert "high" in output
        assert "med" in output

    def test_explain_route_with_default(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> ROUTE
                when msg.v > 50 => SINK TO b :: "high"
                DEFAULT => SINK TO b :: "low"
            ;
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Route:" in output
        assert "default" in output
        assert "low" in output


class TestExplainRetry:
    """Test explain_model output for retry clause (lines 68-72)."""

    def test_explain_retry(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            RETRY: 5 WITH BACKOFF 200 ms
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Retry:" in output
        assert "5" in output
        assert "200 milliseconds" in output


class TestExplainOnError:
    """Test explain_model output for on_error clause (lines 73-79)."""

    def test_explain_on_error_dlq(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: SINK TO b :: "errors/p"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "On error:" in output
        assert "errors/p" in output

    def test_explain_on_error_drop(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SINK TO b :: "out"
            ON_ERROR: DROP
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "On error:" in output
        assert "drop" in output


class TestExplainHooks:
    """Test explain_model output for @initially/@finally hooks (lines 80-85)."""

    def test_explain_initially_hook(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            @INITIALLY { PUBLISH { status: "online" } }
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "@initially" in output
        assert "status" in output

    def test_explain_finally_hook(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
            @FINALLY { PUBLISH { status: "offline" } }
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "@finally" in output
        assert "status" in output

    def test_explain_both_hooks(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            @INITIALLY { PUBLISH { up: "yes" } }
            SOURCE FROM b :: "t"
            |> SINK TO b :: "o"
            @FINALLY { PUBLISH { down: "yes" } }
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "@initially" in output
        assert "@finally" in output
        assert "up" in output
        assert "down" in output


class TestExplainJoin:
    """Test explain_model output for join stage (lines 46-47, 91-96)."""

    def test_explain_join_inner(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "orders"
            |> JOIN b :: "customers"
                ON msg.customer_id == other.id
                WITHIN 10 s
                TYPE: inner
            ;
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Join" in output
        assert "inner" in output
        assert "customers" in output
        assert "10 seconds" in output
        assert "customer_id" in output

    def test_explain_join_default_type(self):
        """Join without explicit type defaults to inner."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "orders"
            |> JOIN b :: "profiles"
                ON msg.id == other.id
                WITHIN 5 s
            ;
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "Join (inner)" in output
        assert "profiles" in output


class TestExplainPipeSource:
    """Test explain_model output for pipe source (lines 101-102)."""

    def test_explain_pipe_source_chained(self):
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
        model = parse_string(src)
        output = explain_model(model)
        assert "receives from pipeline" in output
        assert "chained" in output
        assert "upstream" in output


class TestExplainSourceSchema:
    """Test explain_model output for source with schema (line 103)."""

    def test_explain_source_with_schema(self):
        src = """
        SCHEMA Sensor
            temp: float
        ;
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in" SCHEMA: Sensor
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "schema: Sensor" in output


class TestExplainAllStageTypes:
    """Test _explain_stage for all stage types (lines 107-188)."""

    def test_explain_deduplicate_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> DEDUPLICATE msg.id WITHIN 5 s
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[deduplicate]" in output
        assert "5 seconds" in output

    def test_explain_rate_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> RATE 100 PER 1 s
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[rate]" in output
        assert "100" in output

    def test_explain_flatten_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> FLATTEN msg.items
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[flatten]" in output
        assert "items" in output

    def test_explain_distinct_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> DISTINCT msg.val WITHIN 1 m
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[distinct]" in output
        assert "1 minutes" in output

    def test_explain_throttle_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> THROTTLE 10 PER 1 s
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[throttle]" in output
        assert "10" in output

    def test_explain_sample_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> SAMPLE 50 %
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[sample]" in output
        assert "50%" in output

    def test_explain_filter_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> FILTER msg.v > 0
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[filter]" in output
        assert "condition" in output.lower()

    def test_explain_group_by_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> GROUP_BY msg.region
            |> WINDOW tumbling(5 m)
            |> AGGREGATE { n = count() }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[group_by]" in output
        assert "region" in output

    def test_explain_window_tumbling(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(5 m)
            |> AGGREGATE { n = count() }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[window]" in output
        assert "tumbling" in output
        assert "5 minutes" in output

    def test_explain_window_sliding(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW sliding(10 m, STEP = 2 m)
            |> AGGREGATE { n = count() }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[window]" in output
        assert "sliding" in output
        assert "10 minutes" in output
        assert "2 minutes" in output

    def test_explain_window_named(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW short tumbling(1 m)
            |> WINDOW long tumbling(5 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short,
                avg_5m = avg(msg.value) OVER long
            }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "'short'" in output
        assert "'long'" in output

    def test_explain_aggregate_with_field(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(5 m)
            |> AGGREGATE {
                total = sum(msg.value)
            }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[aggregate]" in output
        assert "total = sum(msg.value)" in output

    def test_explain_aggregate_without_field(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(5 m)
            |> AGGREGATE {
                n = count()
            }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "n = count()" in output

    def test_explain_aggregate_with_window_ref(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW short tumbling(1 m)
            |> AGGREGATE {
                avg_1m = avg(msg.value) OVER short
            }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "over short" in output

    def test_explain_accumulate_with_param(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10 s)
            |> AGGREGATE { avg_val = avg(msg.v) }
            |> ACCUMULATE { smooth = ema(avg_val, 0.3) }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[accumulate]" in output
        assert "smooth = ema(avg_val, ...)" in output

    def test_explain_accumulate_without_param(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(10 s)
            |> AGGREGATE { total = sum(msg.v) }
            |> ACCUMULATE { running = cumsum(total) }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[accumulate]" in output
        assert "running = cumsum(total)" in output

    def test_explain_having_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(5 m)
            |> AGGREGATE { n = count() }
            |> HAVING n > 5
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[having]" in output
        assert "filter aggregated" in output

    def test_explain_map_stage(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(5 m)
            |> AGGREGATE { n = count() }
            |> MAP { result = n }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[map]" in output
        assert "emit result" in output

    def test_explain_map_with_let_bindings(self):
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "in"
            |> WINDOW tumbling(5 m)
            |> AGGREGATE { n = count() }
            |> MAP {
                LET doubled = n * 2,
                result = doubled
            }
            |> SINK TO b :: "out"
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        assert "[map]" in output
        assert "let doubled" in output
        assert "emit result" in output


class TestExplainComprehensivePipeline:
    """Comprehensive test covering many explain paths in a single pipeline."""

    def test_explain_full_pipeline_all_stages(self):
        """Exercise most stage types in a single pipeline."""
        src = """
        CONST LIMIT = 100
        SCHEMA Sensor STRICT
            id: str
            value: float
            region: str
            items: [int]
        ;
        BROKER b HOST: "localhost" PORT: 1883 ;
        FUNC double(x) = x * 2
        PIPELINE full
            @INITIALLY { PUBLISH { status: "started" } }
            SOURCE FROM b :: "sensors/in" SCHEMA: Sensor
            LET scaled = msg.value * 2
            |> DEDUPLICATE msg.id WITHIN 5 s
            |> FILTER msg.value > 0
            |> RATE 100 PER 1 s
            |> FLATTEN msg.items
            |> DISTINCT msg.value WITHIN 1 m
            |> THROTTLE 10 PER 1 s
            |> SAMPLE 50 %
            |> GROUP_BY msg.region
            |> WINDOW tumbling(5 m)
            |> AGGREGATE {
                total = sum(msg.value),
                n = count()
            }
            |> ACCUMULATE { smooth = ema(total, 0.3) }
            |> HAVING total > 100
            |> MAP {
                LET x = total * 2,
                result = x,
                count_val = n
            }
            |> SINK TO b :: "sensors/out"
            ON_ERROR: SINK TO b :: "errors/full"
            RETRY: 3 WITH BACKOFF 100 ms
            @FINALLY { PUBLISH { status: "stopped" } }
        ;
        """
        model = parse_string(src)
        output = explain_model(model)
        # Constants
        assert "Constants:" in output
        assert "LIMIT = 100" in output
        # Schema
        assert "Schemas:" in output
        assert "Sensor (strict)" in output
        # Functions
        assert "Functions:" in output
        assert "func double(x)" in output
        # Broker
        assert "Brokers:" in output
        assert "localhost" in output
        # Pipeline name
        assert "Pipeline: full" in output
        # Source with schema
        assert "schema: Sensor" in output
        # Let binding
        assert "let scaled" in output
        # All stages
        assert "[deduplicate]" in output
        assert "[filter]" in output
        assert "[rate]" in output
        assert "[flatten]" in output
        assert "[distinct]" in output
        assert "[throttle]" in output
        assert "[sample]" in output
        assert "[group_by]" in output
        assert "[window]" in output
        assert "[aggregate]" in output
        assert "[accumulate]" in output
        assert "[having]" in output
        assert "[map]" in output
        # Sink
        assert "sensors/out" in output
        # Retry
        assert "Retry:" in output
        assert "3" in output
        # On error
        assert "On error:" in output
        assert "errors/full" in output
        # Hooks
        assert "@initially" in output
        assert "@finally" in output


# ─── Coverage: codegen.py lines 419-421 (GroupByStage multiple fields) ──


def test_group_by_multiple_fields_codegen():
    """GroupByStage with multiple group-by fields (codegen.py lines 419-421)."""
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    PIPELINE p
        SOURCE FROM b :: "t"
        |> GROUP_BY msg.region, msg.zone
        |> WINDOW tumbling(1 m)
        |> AGGREGATE { total = sum(msg.value) }
        |> SINK TO b :: "o"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        compile(code, "group_by_multi.py", "exec")
        assert "'region'" in code
        assert "'zone'" in code


# ─── Coverage: codegen.py lines 583, 586-589 (string/bool constants) ───


def test_const_string_and_bool_codegen():
    """_build_const_map for string and bool constants (codegen.py lines 586-589)."""
    src = """
    BROKER b HOST: "h" PORT: 1 ;
    CONST S = "hello"
    CONST B = true
    PIPELINE p
        SOURCE FROM b :: "t"
        |> FILTER msg.label == S and B
        |> SINK TO b :: "o"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        compile(code, "const_str_bool.py", "exec")
        assert "'hello'" in code
        assert "True" in code


# ─── Coverage: expressions.py lines 136, 144-146 (OldRef/ChangedRef non-msg) ──


class TestExpressionCompilerCoverage:
    """Tests targeting specific uncovered lines in expressions.py."""

    def _parse_filter_expr(self, expr_src):
        src = f"""
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER {expr_src}
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        return _stages(model.pipelines[0])[0].condition

    def test_old_ref_bare_field(self):
        """OldRef with single-segment field (expressions.py line 136)."""
        expr = self._parse_filter_expr("old(value)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_get_nested(self._prev_msg, 'value')" in code

    def test_changed_ref_bare_field(self):
        """ChangedRef with single-segment field (expressions.py lines 144-146)."""
        expr = self._parse_filter_expr("changed(value)")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_get_nested(msg, 'value')" in code
        assert "_get_nested(self._prev_msg, 'value')" in code
        assert "!=" in code

    def test_int_literal_in_filter(self):
        """IntLiteral compilation (expressions.py line 173)."""
        expr = self._parse_filter_expr("msg.x > 42")
        code = compile_expr(expr, ExprContext.FILTER)
        assert "42" in code

    def test_case_without_else(self):
        """CaseExpr without else arm produces None (expressions.py line 199)."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> AGGREGATE { v = avg(msg.x) }
            |> MAP { out = case { when v > 0 => "pos" }}
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        m = _stages(model.pipelines[0])[1].mappings[0]
        code = compile_expr(m.value, ExprContext.MAP)
        assert "None" in code
        assert "'pos'" in code

    def test_field_ref_having_bare_name(self):
        """FieldRef in HAVING context with single segment (expressions.py lines 286-289)."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.v) }
            |> HAVING total > 10
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        having = _stages(model.pipelines[0])[2]
        code = compile_expr(having.condition, ExprContext.HAVING)
        assert "aggregated['total']" in code

    def test_field_ref_having_msg_nested(self):
        """FieldRef in HAVING context with msg.x (expressions.py lines 286-287)."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.v) }
            |> HAVING msg.count > 0
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        having = _stages(model.pipelines[0])[2]
        code = compile_expr(having.condition, ExprContext.HAVING)
        assert "_get_nested(msg, 'count')" in code

    def test_field_ref_filter_fallthrough(self):
        """FieldRef in FILTER context with non-msg bare name (expressions.py line 282)."""
        expr = self._parse_filter_expr("msg.x > 0")
        # Use a bare name that isn't msg and isn't a local
        code = compile_expr(expr, ExprContext.FILTER)
        assert "_get_nested(msg, 'x')" in code

    def test_field_ref_default_fallthrough(self):
        """FieldRef with no matching context (expressions.py line 300)."""
        self._parse_filter_expr("msg.x > 0")
        # Compile in a context that doesn't match any special case
        # The FieldRef for msg.x in FILTER context is handled, but
        # a multi-segment non-msg ref in a non-standard context falls through
        # We test the HAVING context with multi-segment non-msg ref
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> WINDOW tumbling(1 m)
            |> AGGREGATE { total = sum(msg.v) }
            |> HAVING msg.data.count > 0
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        having = _stages(model.pipelines[0])[2]
        code = compile_expr(having.condition, ExprContext.HAVING)
        assert "_get_nested(msg, 'data', 'count')" in code

    def test_exists_with_collection(self):
        """Quantifier with collection (expressions.py line 314-318, line 327)."""
        src = """
        BROKER b HOST: "h" PORT: 1 ;
        PIPELINE p
            SOURCE FROM b :: "t"
            |> FILTER exists(x in msg.items => x > 0)
            |> SINK TO b :: "t"
        ;
        """
        model = parse_string(src)
        filt = _stages(model.pipelines[0])[0]
        code = compile_expr(filt.condition, ExprContext.FILTER)
        assert "any(" in code
        assert "for x in" in code
        assert "or [])" in code


PREDICT_SRC = """
BROKER local HOST: "localhost" PORT: 1883 ;

MODEL anomaly_detector
    SOURCE: "models/anomaly.ONNX"
    RUNTIME: ONNX
    FEATURES: [msg.temperature, msg.humidity]
    RETURNS: [anomaly_score, is_anomaly]
;

PIPELINE sensor_check
    SOURCE FROM local :: "sensors/data"
    |> FILTER msg.temperature is not null
    |> PREDICT anomaly_detector
    |> SINK TO local :: "alerts/out"
;
"""


def test_predict_pipeline_ctx_fields():
    model = parse_string(PREDICT_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "sensor_check.py")) as f:
            code = f.read()
        assert "import numpy as np" in code
        assert "import onnxruntime as ort" in code


def test_predict_model_loading_onnx():
    model = parse_string(PREDICT_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "sensor_check.py")) as f:
            code = f.read()
        assert "ort.InferenceSession" in code
        assert "_model_anomaly_detector" in code


def test_predict_inference_block_onnx():
    model = parse_string(PREDICT_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "sensor_check.py")) as f:
            code = f.read()
        assert "_feat_anomaly_detector" in code
        assert 'msg["anomaly_score"]' in code
        assert 'msg["is_anomaly"]' in code


def test_predict_generated_compiles_onnx():
    model = parse_string(PREDICT_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "sensor_check.py")) as f:
            code = f.read()
        compile(code, "sensor_check.py", "exec")


def test_predict_generated_compiles_sklearn():
    src = """
    BROKER b HOST: "localhost" PORT: 1883 ;
    MODEL m SOURCE: "m.pkl" RUNTIME: SKLEARN FEATURES: [msg.x, msg.y] RETURNS: [pred] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> SINK TO b :: "out"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        compile(code, "p.py", "exec")
        assert "import joblib" in code
        assert "joblib.load" in code


def test_predict_generated_compiles_torch():
    src = """
    BROKER b HOST: "localhost" PORT: 1883 ;
    MODEL m SOURCE: "m.pt" RUNTIME: TORCH FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> SINK TO b :: "out"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        compile(code, "p.py", "exec")
        assert "import torch" in code
        assert "torch.jit.load" in code


def test_predict_generated_compiles_tflite():
    src = """
    BROKER b HOST: "localhost" PORT: 1883 ;
    MODEL m SOURCE: "m.TFLITE" RUNTIME: TFLITE FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> SINK TO b :: "out"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        compile(code, "p.py", "exec")
        assert "import tflite_runtime" in code


def test_predict_generated_compiles_custom():
    src = """
    BROKER b HOST: "localhost" PORT: 1883 ;
    MODEL m SOURCE: "my_module.py" RUNTIME: CUSTOM FEATURES: [msg.x] RETURNS: [y] ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> PREDICT m
        |> SINK TO b :: "out"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "p.py")) as f:
            code = f.read()
        compile(code, "p.py", "exec")
        assert "importlib.util" in code


def test_requirements_includes_ml_deps():
    model = parse_string(PREDICT_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "requirements.txt")) as f:
            reqs = f.read()
        assert "numpy" in reqs
        assert "onnxruntime" in reqs


def test_requirements_no_ml_deps_without_predict():
    src = """
    BROKER b HOST: "localhost" PORT: 1883 ;
    PIPELINE p
        SOURCE FROM b :: "in"
        |> FILTER msg.v > 0
        |> SINK TO b :: "out"
    ;
    """
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "requirements.txt")) as f:
            reqs = f.read()
        assert "numpy" not in reqs
        assert "onnxruntime" not in reqs


def test_explain_includes_predict():
    model = parse_string(PREDICT_SRC)
    text = explain_model(model)
    assert "Models:" in text
    assert "anomaly_detector" in text
    assert "onnx" in text
    assert "predict" in text
    assert "anomaly_detector" in text


NO_SINK_SRC = """
BROKER local HOST: "localhost" PORT: 1883 ;

PIPELINE hub
    SOURCE FROM local :: "input/data"
    |> FILTER msg.value > 0
;
"""


def test_no_sink_pipeline_generates():
    model = parse_string(NO_SINK_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "hub.py")) as f:
            code = f.read()
        publish_method = code.split("def _publish_message")[1].split("def ")[0]
        assert "_notify_downstream" in publish_method
        assert "_publish_with_retry" not in publish_method


def test_no_sink_pipeline_compiles():
    model = parse_string(NO_SINK_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "hub.py")) as f:
            code = f.read()
        compile(code, "hub.py", "exec")


def test_no_sink_pipeline_no_publisher_import():
    model = parse_string(NO_SINK_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "hub.py")) as f:
            code = f.read()
        assert "SinkConnParams" not in code
        assert "self._publisher" not in code


SELECT_SRC = """
BROKER b HOST: "localhost" PORT: 1883 ;
PIPELINE sel
    SOURCE FROM b :: "in"
    |> SELECT [id, value: raw_value, ts]
    |> SINK TO b :: "out"
;
"""


def test_select_stage_codegen_has_select():
    model = parse_string(SELECT_SRC)
    p = model.pipelines[0]
    from sigil.codegen import _build_pipeline_ctx

    ctx = _build_pipeline_ctx(p)
    assert ctx.has_select is True
    assert len(ctx.select_fields) == 3
    assert ctx.select_fields[0].output_name == "id"
    assert ctx.select_fields[0].source_name == "id"
    assert ctx.select_fields[1].output_name == "value"
    assert ctx.select_fields[1].source_name == "raw_value"
    assert ctx.select_fields[2].output_name == "ts"
    assert ctx.select_fields[2].source_name == "ts"


def test_select_stage_generated_code():
    model = parse_string(SELECT_SRC)
    with tempfile.TemporaryDirectory() as tmpdir:
        generate(model, tmpdir)
        with open(os.path.join(tmpdir, "pipelines", "sel.py")) as f:
            code = f.read()
        compile(code, "sel.py", "exec")
        assert '"id"' in code
        assert '"value"' in code
        assert '"raw_value"' in code
        assert '"ts"' in code


# ── Array indexing ──────────────────────────────────────────────────────────

BASE_SUB = """BROKER b HOST: "localhost" PORT: 1883;\n"""


def test_codegen_subscript_positive_index():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_get_nested(msg, 'items')[0]" in code
        compile(code, "<string>", "exec")


def test_codegen_subscript_negative_index():
    src = (
        BASE_SUB
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> FILTER msg.readings[-1] > 10
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_get_nested(msg, 'readings')[-1]" in code


def test_codegen_subscript_nested_path():
    src = (
        BASE_SUB
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> MAP { v = msg.data.items[2] }
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_get_nested(msg, 'data', 'items')[2]" in code


# ── DebugStage ───────────────────────────────────────────────────────────────

BASE_DBG = """BROKER b HOST: "localhost" PORT: 1883;\n"""


def test_codegen_debug_bare():
    src = (
        BASE_DBG
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> DEBUG
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "logger.debug" in code
        compile(code, "<string>", "exec")


def test_codegen_debug_with_label_and_fields():
    src = (
        BASE_DBG
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> DEBUG LABEL: "CHECK", FIELDS: [msg.id, msg.value]
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "CHECK" in code
        assert "logger.debug" in code


def test_codegen_multiple_debug_stages():
    src = (
        BASE_DBG
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> DEBUG LABEL: "first"
    |> DEBUG LABEL: "second"
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert code.count("logger.debug") >= 2


# ── percentile / distinct_count ──────────────────────────────────────────────

BASE_PCT = """BROKER b HOST: "localhost" PORT: 1883;\n"""


def test_codegen_percentile_template():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "sorted(" in code
        assert "p95" in code
        compile(code, "<string>", "exec")


def test_codegen_distinct_count_template():
    src = (
        BASE_PCT
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> WINDOW tumbling(1 m)
    |> AGGREGATE { n_dev = distinct_count(msg.device_id) }
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "set(" in code
        assert "n_dev" in code
        compile(code, "<string>", "exec")


def test_codegen_percentile_param_code():
    src = (
        BASE_PCT
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> WINDOW tumbling(1 m)
    |> AGGREGATE { med = percentile(msg.v, 50) }
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "50" in code


# ── ValidateStage ─────────────────────────────────────────────────────────────

BASE_VAL = """
BROKER b HOST: "localhost" PORT: 1883;
SCHEMA S STRICT id: str value: float active: bool?;
"""


def test_codegen_validate_generates_validate_msg():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_validate_msg" in code
        compile(code, "<string>", "exec")


def test_codegen_validate_schema_fields_in_code():
    src = (
        BASE_VAL
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> VALIDATE SCHEMA: S ON_ERROR: SKIP
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "'id'" in code or "id" in code
        assert "'value'" in code or "value" in code
        compile(code, "<string>", "exec")


def test_codegen_validate_on_error_drop():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "drop" in code or "return" in code
        compile(code, "<string>", "exec")


# ── EnrichStage ──────────────────────────────────────────────────────────────

BASE_ENR = """BROKER b HOST: "localhost" PORT: 1883;\n"""


def test_codegen_enrich_generates_method():
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
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_do_enrich_meta" in code
        assert "urllib.request" in code
        compile(code, "<string>", "exec")


def test_codegen_enrich_on_error_default():
    src = (
        BASE_ENR
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> ENRICH VIA HTTP
        URL: "https://api.example.com/"
        BIND: info
        ON_ERROR: DEFAULT { name: "unknown", region: "eu" }
    ;
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_do_enrich_info" in code
        assert "unknown" in code
        compile(code, "<string>", "exec")


def test_codegen_multiple_enrich_stages():
    src = (
        BASE_ENR
        + """
PIPELINE p
    SOURCE FROM b :: "in"
    |> ENRICH VIA HTTP
        URL: "https://api.example.com/a"
        BIND: first
    ;
    |> ENRICH VIA HTTP
        URL: "https://api.example.com/b"
        BIND: second
    ;
    |> SINK TO b :: "out"
;
"""
    )
    model = parse_string(src)
    with tempfile.TemporaryDirectory() as d:
        generate(model, d)
        with open(os.path.join(d, "pipelines", "p.py")) as f:
            code = f.read()
        assert "_do_enrich_first" in code
        assert "_do_enrich_second" in code
        compile(code, "<string>", "exec")
