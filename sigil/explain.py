from .semantics import UNIT_MULTIPLIERS, _unwrap_stages


def explain_model(model) -> str:
    lines = []
    if model.constants:
        lines.append("Constants:")
        for c in model.constants:
            cls = c.value.__class__.__name__
            if cls == "StringLiteral":
                val = repr(c.value.val)
            elif cls == "BoolLiteral":
                val = str(c.value.val).lower()
            else:
                val = str(c.value.val)
            lines.append(f"  {c.name} = {val}")
        lines.append("")

    if model.schemas:
        lines.append("Schemas:")
        for s in model.schemas:
            strictness = s.strictness if s.strictness else "strict"
            lines.append(f"  {s.name} ({strictness}):")
            for f in s.fields:
                opt = "?" if f.optional else ""
                lines.append(f"    {f.name}: {_format_schema_type(f.type)}{opt}")
        lines.append("")

    if model.functions:
        lines.append("Functions:")
        for f in model.functions:
            params = ", ".join(f.params) if f.params else ""
            lines.append(f"  func {f.name}({params}) = <expression>")
        lines.append("")

    if model.models:
        lines.append("Models:")
        for m in model.models:
            runtime = m.runtime
            lines.append(f"  {m.name}: {runtime}")
            if m.features:
                input_strs = [".".join(ref.segments) for ref in m.features]
                lines.append(f"    features: [{', '.join(input_strs)}]")
            if m.returns:
                lines.append(f"    returns: [{', '.join(m.returns)}]")
        lines.append("")

    if model.brokers:
        lines.append("Brokers:")
        for b in model.brokers:
            transport = b.transport if b.transport else "mqtt"
            lines.append(f"  {b.name}: {transport}://{b.host}:{b.port}")
        lines.append("")

    for p in model.pipelines:
        lines.append(f"Pipeline: {p.name}")
        lines.append(f"  {_explain_source(p)}")
        if p.join:
            lines.append(f"  {_explain_join(p.join)}")
        if p.pipeline_lets:
            for lb in p.pipeline_lets:
                lines.append(f"  let {lb.name} = <expression> (computed per message)")
        stages = _unwrap_stages(p)
        for i, stage in enumerate(stages):
            lines.append(f"  {_explain_stage(stage, i + 1)}")
        if p.sink:
            lines.append(
                f"  → Sink: publish to '{p.sink.topic}' via {p.sink.broker.name}"
            )
        if p.route:
            via_part = f" via {p.route.via_broker.name}" if p.route.via_broker else ""
            lines.append(f"  → Route{via_part}:")
            for arm in p.route.arms:
                if arm.sink:
                    lines.append(
                        f"      when <condition> → publish to '{arm.sink.topic}' via {arm.sink.broker.name}"
                    )
                else:
                    broker_name = p.route.via_broker.name if p.route.via_broker else "?"
                    lines.append(
                        f"      when <condition> → publish to '{arm.topic}' via {broker_name}"
                    )
            if p.route.default_sink:
                lines.append(
                    f"      default → publish to '{p.route.default_sink.topic}' via {p.route.default_sink.broker.name}"
                )
            elif p.route.default_topic:
                broker_name = p.route.via_broker.name if p.route.via_broker else "?"
                lines.append(
                    f"      default → publish to '{p.route.default_topic}' via {broker_name}"
                )
        if p.retry:
            base = _format_duration(p.retry.base)
            lines.append(
                f"  ↻ Retry: up to {p.retry.max_retries} attempts with {base} backoff"
            )
        if p.on_error:
            if p.on_error.broker:
                lines.append(
                    f"  ⚠ On error: route to '{p.on_error.topic}' via {p.on_error.broker.name}"
                )
            else:
                lines.append(f"  ⚠ On error: drop (silently discard)")
        if p.initially:
            fields = ", ".join(f.name for f in p.initially.body.fields)
            lines.append(f"  @initially: publish {{{fields}}}")
        if p.finally_:
            fields = ", ".join(f.name for f in p.finally_.body.fields)
            lines.append(f"  @finally: publish {{{fields}}}")
        lines.append("")

    return "\n".join(lines)


def _explain_join(join):
    left = ".".join(join.left_key.segments)
    right = ".".join(join.right_key.segments)
    window = _format_duration(join.window)
    jtype = join.type if join.type else "inner"
    return f"⋈ Join ({jtype}): {join.broker.name}::'{join.topic}' on {left} == {right} within {window}"


def _explain_source(pipeline):
    src = pipeline.source
    if src.pipe:
        return f"← Source: receives from pipeline '{src.pipe}' (chained)"
    schema_info = f" (schema: {src.schema.name})" if src.schema else ""
    return f"← Source: subscribe to '{src.topic}' via {src.broker.name}{schema_info}"


def _explain_stage(stage, index):
    cls = stage.__class__.__name__
    prefix = f"Stage {index}"

    if cls == "DeduplicateStage":
        field = ".".join(stage.key.segments)
        ttl = _format_duration(stage.ttl)
        return f"{prefix} [deduplicate]: drop duplicates on {field} within {ttl}"

    if cls == "RateStage":
        period = _format_duration(stage.period)
        excess = stage.on_excess if stage.on_excess else "drop"
        return f"{prefix} [rate]: limit to {stage.limit} messages per {period} (on_excess: {excess})"

    if cls == "FlattenStage":
        field = ".".join(stage.field.segments)
        return (
            f"{prefix} [flatten]: expand array field {field} into individual messages"
        )

    if cls == "DistinctStage":
        field = ".".join(stage.field.segments)
        ttl = _format_duration(stage.ttl)
        return f"{prefix} [distinct]: deduplicate by {field} within {ttl}"

    if cls == "ThrottleStage":
        period = _format_duration(stage.period)
        overflow = stage.overflow if stage.overflow else "drop"
        return f"{prefix} [throttle]: limit to {stage.limit} messages per {period} (overflow: {overflow})"

    if cls == "SampleStage":
        return f"{prefix} [sample]: randomly sample {stage.percentage}% of messages"

    if cls == "FilterStage":
        return f"{prefix} [filter]: keep messages where <condition>"

    if cls == "GroupByStage":
        fields = ", ".join(".".join(f.segments) for f in stage.fields)
        return f"{prefix} [group_by]: partition messages by {fields}"

    if cls == "WindowStage":
        size = _format_duration(stage.size)
        name_part = f" '{stage.wname}'" if stage.wname else ""
        if stage.type == "sliding" and stage.step:
            step = _format_duration(stage.step)
            return f"{prefix} [window{name_part}]: sliding window of {size}, advancing every {step}"
        return f"{prefix} [window{name_part}]: tumbling window of {size}"

    if cls == "AggregateStage":
        aggs = []
        for a in stage.assignments:
            over_part = f" over {a.window_ref}" if a.window_ref else ""
            if a.field:
                field = ".".join(a.field.segments)
                aggs.append(f"{a.name} = {a.func}({field}){over_part}")
            else:
                aggs.append(f"{a.name} = {a.func}(){over_part}")
        return f"{prefix} [aggregate]: compute {', '.join(aggs)}"

    if cls == "AccumulateStage":
        accs = []
        for a in stage.assignments:
            field = ".".join(a.field.segments)
            if a.param is not None:
                accs.append(f"{a.name} = {a.func}({field}, ...)")
            else:
                accs.append(f"{a.name} = {a.func}({field})")
        return f"{prefix} [accumulate]: compute {', '.join(accs)}"

    if cls == "HavingStage":
        return f"{prefix} [having]: filter aggregated results where <condition>"

    if cls == "MapStage":
        targets = [m.target for m in stage.mappings]
        bindings = [b.name for b in stage.bindings] if stage.bindings else []
        parts = []
        if bindings:
            parts.append(f"let {', '.join(bindings)}")
        parts.append(f"emit {', '.join(targets)}")
        return f"{prefix} [map]: {'; '.join(parts)}"

    if cls == "SelectStage":
        parts = []
        for f in stage.fields:
            if f.alias:
                parts.append(f"{f.alias} (from {f.source})")
            else:
                parts.append(f.name)
        return f"{prefix} [select]: emit {', '.join(parts)}"

    if cls == "PredictStage":
        return f"{prefix} [predict]: apply model '{stage.model.name}'"

    if cls == "ValidateStage":
        on_err = ""
        if stage.validate_err is not None:
            ve = stage.validate_err
            if ve.broker is not None:
                on_err = f" (on_error: sink to '{ve.topic}' via {ve.broker.name})"
            elif ve.action:
                on_err = f" (on_error: {ve.action})"
        return f"{prefix} [validate]: validate messages against schema '{stage.schema.name}'{on_err}"

    if cls == "EnrichStage":
        method = stage.method if stage.method else "GET"
        timeout_part = (
            f", timeout: {_format_duration(stage.timeout)}" if stage.timeout else ""
        )
        return f"{prefix} [enrich]: HTTP {method} call bound as '{stage.bind}'{timeout_part}"

    if cls == "DebugStage":
        label = stage.label if stage.label else "debug"
        if stage.fields:
            field_strs = [".".join(f.segments) for f in stage.fields]
            return f"{prefix} [debug]: checkpoint '{label}', log fields: {', '.join(field_strs)}"
        return f"{prefix} [debug]: checkpoint '{label}', log full message"

    return f"{prefix} [{cls}]"


def _format_duration(dur):
    unit_names = {
        "ms": "milliseconds",
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days",
    }
    name = unit_names.get(dur.unit, dur.unit)
    return f"{dur.value} {name}"


def _format_schema_type(stype):
    cls = stype.__class__.__name__
    if cls == "SchemaScalarType":
        return stype.name
    elif cls == "SchemaObjectType":
        field_strs = [
            f"{f.name}: {_format_schema_type(f.type)}{'?' if f.optional else ''}"
            for f in stype.fields
        ]
        return "{" + ", ".join(field_strs) + "}"
    elif cls == "SchemaListType":
        return f"[{_format_schema_type(stype.element)}]"
    return "unknown"
