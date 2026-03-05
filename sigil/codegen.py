import os
from dataclasses import dataclass, field as dc_field
from jinja2 import Environment, FileSystemLoader

from .expressions import ExprContext, compile_expr, extract_field_keys
from .semantics import UNIT_MULTIPLIERS, _unwrap_stages

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")


@dataclass
class BrokerCtx:
    name: str
    transport: str
    host: str
    port: int
    ssl: bool
    username: str | None
    password: str | None


@dataclass
class AggregationCtx:
    name: str
    func: str
    field_keys: list[str] | None
    field_keys_str: str = ""
    type_annotation: str | None = None
    window_ref: str | None = None


@dataclass
class AccumCtx:
    name: str
    func: str
    field_name: str
    param_code: str | None = None


@dataclass
class WindowCtx:
    name: str
    type: str
    size_seconds: float
    step_seconds: float | None = None


@dataclass
class JoinCtx:
    broker: "BrokerCtx"
    topic: str
    left_key_code: str
    right_key_code: str
    window_seconds: float
    join_type: str


@dataclass
class PipelineLetCtx:
    name: str
    value_code: str


@dataclass
class LetBindingCtx:
    name: str
    value_code: str


@dataclass
class MappingCtx:
    target: str
    value_code: str


@dataclass
class ObjectFieldCtx:
    name: str
    value_code: str


@dataclass
class RouteArmCtx:
    condition_code: str
    broker: "BrokerCtx"
    topic: str
    qos: int


@dataclass
class PipelineCtx:
    name: str
    class_name: str
    source_broker: BrokerCtx
    source_topic: str
    source_qos: int
    source_format: str
    sink_broker: BrokerCtx
    sink_topic: str
    sink_qos: int
    has_pipeline_lets: bool = False
    pipeline_lets: list[PipelineLetCtx] = dc_field(default_factory=list)
    has_filter: bool = False
    filter_code: str = ""
    has_group_by: bool = False
    group_by_keys: list[list[str]] = dc_field(default_factory=list)
    group_by_keys_strs: list[str] = dc_field(default_factory=list)
    has_window: bool = False
    window_type: str = "tumbling"
    window_size_seconds: float = 60
    window_step_seconds: float | None = None
    has_multi_window: bool = False
    windows: list[WindowCtx] = dc_field(default_factory=list)
    has_aggregate: bool = False
    aggregations: list[AggregationCtx] = dc_field(default_factory=list)
    has_having: bool = False
    having_code: str = ""
    has_map: bool = False
    let_bindings: list[LetBindingCtx] = dc_field(default_factory=list)
    mappings: list[MappingCtx] = dc_field(default_factory=list)
    has_dedup: bool = False
    dedup_key_code: str = ""
    dedup_ttl_seconds: float = 0
    has_old_ref: bool = False
    has_env_ref: bool = False
    has_quantifier_over_messages: bool = False
    has_initially: bool = False
    initially_fields: list[ObjectFieldCtx] = dc_field(default_factory=list)
    has_finally: bool = False
    finally_fields: list[ObjectFieldCtx] = dc_field(default_factory=list)
    sink_format: str = "json"
    has_batch: bool = False
    batch_size: int | None = None
    batch_timeout_seconds: float | None = None
    has_accumulate: bool = False
    accumulations: list[AccumCtx] = dc_field(default_factory=list)
    has_rate: bool = False
    rate_limit: int = 0
    rate_period_seconds: float = 0
    rate_on_excess: str = "drop"
    has_route: bool = False
    route_arms: list[RouteArmCtx] = dc_field(default_factory=list)
    route_default: RouteArmCtx | None = None
    has_pipe_source: bool = False
    pipe_source_name: str = ""
    has_join: bool = False
    join: JoinCtx | None = None
    has_flatten: bool = False
    flatten_key_code: str = ""
    has_distinct: bool = False
    distinct_key_code: str = ""
    distinct_ttl_seconds: float = 0
    has_throttle: bool = False
    throttle_limit: int = 0
    throttle_period_seconds: float = 0
    throttle_overflow: str = "drop"
    throttle_buffer_size: int = 1000
    has_sample: bool = False
    sample_percentage: int = 100
    schema_name: str | None = None
    has_on_error: bool = False
    on_error_drop: bool = False
    on_error_broker: BrokerCtx | None = None
    on_error_topic: str = ""
    has_retry: bool = False
    retry_max: int = 3
    retry_backoff_seconds: float = 0.1


def generate(model, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "pipelines"), exist_ok=True)

    const_map = _build_const_map(model)
    func_defs = _build_func_defs(model)
    env = _create_jinja_env()
    pipeline_contexts = [
        _build_pipeline_ctx(p, const_map, func_defs) for p in model.pipelines
    ]

    _render_file(
        env,
        "app.py.j2",
        os.path.join(output_dir, "app.py"),
        pipelines=pipeline_contexts,
    )

    for pctx in pipeline_contexts:
        path = os.path.join(output_dir, "pipelines", f"{pctx.name}.py")
        _render_file(env, "pipeline.py.j2", path, p=pctx)

    init_path = os.path.join(output_dir, "pipelines", "__init__.py")
    with open(init_path, "w") as f:
        f.write("")

    _render_file(
        env, "requirements.txt.j2", os.path.join(output_dir, "requirements.txt")
    )

    return output_dir


def _create_jinja_env():
    env = Environment(
        loader=FileSystemLoader(_TEMPLATE_DIR),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    return env


def _render_file(env, template_name, output_path, **kwargs):
    tmpl = env.get_template(template_name)
    content = tmpl.render(**kwargs)
    with open(output_path, "w") as f:
        f.write(content)


def _build_pipeline_ctx(
    pipeline, const_map: dict[str, str] | None = None, func_defs: dict | None = None
) -> PipelineCtx:
    src = pipeline.source
    snk = pipeline.sink

    if src.pipe:
        ctx = PipelineCtx(
            name=pipeline.name,
            class_name=_to_class_name(pipeline.name),
            source_broker=BrokerCtx(
                name="__pipe__",
                transport="mqtt",
                host="",
                port=0,
                ssl=False,
                username=None,
                password=None,
            ),
            source_topic="",
            source_qos=0,
            source_format="json",
            sink_broker=_build_broker_ctx(snk.broker)
            if snk
            else BrokerCtx(
                name="__route__",
                transport="mqtt",
                host="",
                port=0,
                ssl=False,
                username=None,
                password=None,
            ),
            sink_topic=snk.topic if snk else "",
            sink_qos=snk.qos if snk and snk.qos else 0,
            sink_format=snk.format if snk and snk.format else "json",
            has_pipe_source=True,
            pipe_source_name=src.pipe,
        )
    else:
        ctx = PipelineCtx(
            name=pipeline.name,
            class_name=_to_class_name(pipeline.name),
            source_broker=_build_broker_ctx(src.broker),
            source_topic=src.topic,
            source_qos=src.qos if src.qos else 0,
            source_format=src.format if src.format else "json",
            sink_broker=_build_broker_ctx(snk.broker)
            if snk
            else BrokerCtx(
                name="__route__",
                transport="mqtt",
                host="",
                port=0,
                ssl=False,
                username=None,
                password=None,
            ),
            sink_topic=snk.topic if snk else "",
            sink_qos=snk.qos if snk and snk.qos else 0,
            sink_format=snk.format if snk and snk.format else "json",
        )

    if not src.pipe and src.schema:
        ctx.schema_name = src.schema.name

    pipeline_let_names: set[str] = set()
    if pipeline.pipeline_lets:
        ctx.has_pipeline_lets = True
        for plb in pipeline.pipeline_lets:
            ctx.pipeline_lets.append(
                PipelineLetCtx(
                    name=plb.name,
                    value_code=compile_expr(
                        plb.value,
                        ExprContext.FILTER,
                        local_names=pipeline_let_names,
                        const_map=const_map,
                        func_defs=func_defs,
                    ),
                )
            )
            pipeline_let_names.add(plb.name)

    if pipeline.route:
        route = pipeline.route
        ctx.has_route = True
        for arm in route.arms:
            ctx.route_arms.append(
                RouteArmCtx(
                    condition_code=compile_expr(
                        arm.condition,
                        ExprContext.FILTER,
                        local_names=pipeline_let_names,
                        const_map=const_map,
                        func_defs=func_defs,
                    ),
                    broker=_build_broker_ctx(arm.sink.broker),
                    topic=arm.sink.topic,
                    qos=arm.sink.qos if arm.sink.qos else 0,
                )
            )
        if route.default:
            ctx.route_default = RouteArmCtx(
                condition_code="",
                broker=_build_broker_ctx(route.default.broker),
                topic=route.default.topic,
                qos=route.default.qos if route.default.qos else 0,
            )

    if snk and snk.batch:
        ctx.has_batch = True
        ctx.batch_size = snk.batch.size if snk.batch.size else None
        ctx.batch_timeout_seconds = (
            _dur_to_secs(snk.batch.timeout) if snk.batch.timeout else None
        )

    if pipeline.initially:
        ctx.has_initially = True
        for f in pipeline.initially.body.fields:
            ctx.initially_fields.append(
                ObjectFieldCtx(
                    name=f.name,
                    value_code=compile_expr(
                        f.value,
                        ExprContext.FILTER,
                        const_map=const_map,
                        func_defs=func_defs,
                    ),
                )
            )
            ctx.has_env_ref = ctx.has_env_ref or _has_env_ref(f.value)

    if pipeline.join:
        join = pipeline.join
        left_keys = extract_field_keys(join.left_key)
        right_keys = join.right_key.segments[1:]
        ctx.has_join = True
        ctx.join = JoinCtx(
            broker=_build_broker_ctx(join.broker),
            topic=join.topic,
            left_key_code=", ".join(repr(k) for k in left_keys),
            right_key_code=", ".join(repr(k) for k in right_keys),
            window_seconds=_dur_to_secs(join.window),
            join_type=join.type if join.type else "inner",
        )

    if pipeline.finally_:
        ctx.has_finally = True
        for f in pipeline.finally_.body.fields:
            ctx.finally_fields.append(
                ObjectFieldCtx(
                    name=f.name,
                    value_code=compile_expr(
                        f.value,
                        ExprContext.FILTER,
                        const_map=const_map,
                        func_defs=func_defs,
                    ),
                )
            )
            ctx.has_env_ref = ctx.has_env_ref or _has_env_ref(f.value)

    if pipeline.on_error:
        ctx.has_on_error = True
        oe = pipeline.on_error
        if oe.broker:
            ctx.on_error_broker = _build_broker_ctx(oe.broker)
            ctx.on_error_topic = oe.topic
        else:
            ctx.on_error_drop = True

    if pipeline.retry:
        ctx.has_retry = True
        ctx.retry_max = pipeline.retry.max_retries
        ctx.retry_backoff_seconds = _dur_to_secs(pipeline.retry.base)

    for stage in _unwrap_stages(pipeline):
        cls = stage.__class__.__name__

        if cls == "DeduplicateStage":
            ctx.has_dedup = True
            keys = extract_field_keys(stage.key)
            ctx.dedup_key_code = ", ".join(repr(k) for k in keys)
            ctx.dedup_ttl_seconds = _dur_to_secs(stage.ttl)

        elif cls == "FilterStage":
            ctx.has_filter = True
            ctx.filter_code = compile_expr(
                stage.condition,
                ExprContext.FILTER,
                local_names=pipeline_let_names,
                const_map=const_map,
                func_defs=func_defs,
            )
            ctx.has_old_ref = ctx.has_old_ref or _has_temporal_ref(stage.condition)
            ctx.has_env_ref = ctx.has_env_ref or _has_env_ref(stage.condition)

        elif cls == "GroupByStage":
            ctx.has_group_by = True
            ctx.group_by_keys = [extract_field_keys(ref) for ref in stage.fields]
            ctx.group_by_keys_strs = [
                ", ".join(repr(k) for k in keys) for keys in ctx.group_by_keys
            ]

        elif cls == "WindowStage":
            ctx.has_window = True
            wctx = WindowCtx(
                name=stage.wname if stage.wname else "",
                type=stage.type,
                size_seconds=_dur_to_secs(stage.size),
                step_seconds=_dur_to_secs(stage.step) if stage.step else None,
            )
            ctx.windows.append(wctx)
            if not ctx.has_multi_window:
                ctx.window_type = stage.type
                ctx.window_size_seconds = _dur_to_secs(stage.size)
                if stage.step:
                    ctx.window_step_seconds = _dur_to_secs(stage.step)
            if len(ctx.windows) > 1:
                ctx.has_multi_window = True

        elif cls == "AggregateStage":
            ctx.has_aggregate = True
            for agg in stage.assignments:
                keys = extract_field_keys(agg.field) if agg.field else None
                keys_str = ", ".join(repr(k) for k in keys) if keys else ""
                ctx.aggregations.append(
                    AggregationCtx(
                        name=agg.name,
                        func=agg.func,
                        field_keys=keys,
                        field_keys_str=keys_str,
                        type_annotation=agg.type if agg.type else None,
                        window_ref=agg.window_ref if agg.window_ref else None,
                    )
                )

        elif cls == "RateStage":
            ctx.has_rate = True
            ctx.rate_limit = stage.limit
            ctx.rate_period_seconds = _dur_to_secs(stage.period)
            ctx.rate_on_excess = stage.on_excess if stage.on_excess else "drop"

        elif cls == "FlattenStage":
            ctx.has_flatten = True
            keys = extract_field_keys(stage.field)
            ctx.flatten_key_code = ", ".join(repr(k) for k in keys)

        elif cls == "DistinctStage":
            ctx.has_distinct = True
            keys = extract_field_keys(stage.field)
            ctx.distinct_key_code = ", ".join(repr(k) for k in keys)
            ctx.distinct_ttl_seconds = _dur_to_secs(stage.ttl)

        elif cls == "ThrottleStage":
            ctx.has_throttle = True
            ctx.throttle_limit = stage.limit
            ctx.throttle_period_seconds = _dur_to_secs(stage.period)
            ctx.throttle_overflow = stage.overflow if stage.overflow else "drop"
            ctx.throttle_buffer_size = stage.buffer_size if stage.buffer_size else 1000

        elif cls == "SampleStage":
            ctx.has_sample = True
            ctx.sample_percentage = stage.percentage

        elif cls == "AccumulateStage":
            ctx.has_accumulate = True
            for acc in stage.assignments:
                field_name = acc.field.segments[0] if acc.field else ""
                param_code = None
                if acc.param is not None:
                    param_code = compile_expr(
                        acc.param,
                        ExprContext.FILTER,
                        const_map=const_map,
                        func_defs=func_defs,
                    )
                ctx.accumulations.append(
                    AccumCtx(
                        name=acc.name,
                        func=acc.func,
                        field_name=field_name,
                        param_code=param_code,
                    )
                )

        elif cls == "HavingStage":
            ctx.has_having = True
            ctx.having_code = compile_expr(
                stage.condition,
                ExprContext.HAVING,
                local_names=pipeline_let_names,
                const_map=const_map,
                func_defs=func_defs,
            )
            ctx.has_env_ref = ctx.has_env_ref or _has_env_ref(stage.condition)
            ctx.has_quantifier_over_messages = (
                ctx.has_quantifier_over_messages
                or _has_quantifier_over_messages(stage.condition)
            )

        elif cls == "MapStage":
            ctx.has_map = True
            local_names: set[str] = set(pipeline_let_names)
            for lb in stage.bindings:
                ctx.let_bindings.append(
                    LetBindingCtx(
                        name=lb.name,
                        value_code=compile_expr(
                            lb.value,
                            ExprContext.MAP,
                            local_names=local_names,
                            const_map=const_map,
                            func_defs=func_defs,
                        ),
                    )
                )
                local_names.add(lb.name)
            for m in stage.mappings:
                ctx.mappings.append(
                    MappingCtx(
                        target=m.target,
                        value_code=compile_expr(
                            m.value,
                            ExprContext.MAP,
                            local_names=local_names,
                            const_map=const_map,
                            func_defs=func_defs,
                        ),
                    )
                )
                ctx.has_env_ref = ctx.has_env_ref or _has_env_ref(m.value)

    return ctx


def _build_broker_ctx(broker) -> BrokerCtx:
    return BrokerCtx(
        name=broker.name,
        transport=broker.transport if broker.transport else "mqtt",
        host=broker.host,
        port=broker.port,
        ssl=broker.ssl if broker.ssl else False,
        username=broker.username if broker.username else None,
        password=broker.password if broker.password else None,
    )


def _to_class_name(snake: str) -> str:
    return "".join(part.capitalize() for part in snake.split("_"))


def _dur_to_secs(dur) -> float:
    return dur.value * UNIT_MULTIPLIERS.get(dur.unit, 1)


def _build_const_map(model) -> dict[str, str]:
    result = {}
    for c in model.constants:
        val = c.value
        cls = val.__class__.__name__
        if cls == "IntLiteral":
            result[c.name] = repr(val.val)
        elif cls == "FloatLiteral":
            result[c.name] = repr(val.val)
        elif cls == "StringLiteral":
            result[c.name] = repr(val.val)
        elif cls == "BoolLiteral":
            result[c.name] = "True" if val.val else "False"
    return result


def _build_func_defs(model) -> dict:
    result = {}
    for f in model.functions or []:
        result[f.name] = f
    return result


def _has_temporal_ref(node) -> bool:
    cls = node.__class__.__name__
    if cls in ("OldRef", "ChangedRef"):
        return True
    for attr_name in _AST_WALK_ATTRS:
        child = getattr(node, attr_name, None)
        if child is None:
            continue
        if isinstance(child, list):
            for item in child:
                if (
                    hasattr(item, "__class__")
                    and item.__class__.__module__ != "builtins"
                    and _has_temporal_ref(item)
                ):
                    return True
        elif (
            hasattr(child, "__class__")
            and child.__class__.__module__ != "builtins"
            and _has_temporal_ref(child)
        ):
            return True
    return False


_AST_WALK_ATTRS = (
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
    "predicate",
    "collection",
)


def _has_env_ref(node) -> bool:
    cls = node.__class__.__name__
    if cls == "EnvRef":
        return True
    for attr_name in _AST_WALK_ATTRS:
        child = getattr(node, attr_name, None)
        if child is None:
            continue
        if isinstance(child, list):
            for item in child:
                if (
                    hasattr(item, "__class__")
                    and item.__class__.__module__ != "builtins"
                    and _has_env_ref(item)
                ):
                    return True
        elif (
            hasattr(child, "__class__")
            and child.__class__.__module__ != "builtins"
            and _has_env_ref(child)
        ):
            return True
    return False


def _has_quantifier_over_messages(node) -> bool:
    cls = node.__class__.__name__
    if cls in ("ForallExpr", "ExistsExpr") and not node.collection:
        return True
    for attr_name in _AST_WALK_ATTRS:
        child = getattr(node, attr_name, None)
        if child is None:
            continue
        if isinstance(child, list):
            for item in child:
                if (
                    hasattr(item, "__class__")
                    and item.__class__.__module__ != "builtins"
                    and _has_quantifier_over_messages(item)
                ):
                    return True
        elif (
            hasattr(child, "__class__")
            and child.__class__.__module__ != "builtins"
            and _has_quantifier_over_messages(child)
        ):
            return True
    return False
