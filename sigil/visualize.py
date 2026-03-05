from .semantics import _unwrap_stages


def visualize_dot(model) -> str:
    """Generate a Graphviz DOT diagram from a parsed Sigil model."""
    lines = []
    lines.append("digraph sigil {")
    lines.append("    rankdir=LR;")
    lines.append('    fontname="Helvetica";')
    lines.append('    node [fontname="Helvetica" fontsize=10];')
    lines.append('    edge [fontname="Helvetica" fontsize=9];')
    lines.append("")

    # Constants node
    if model.constants:
        const_rows = []
        for c in model.constants:
            cls = c.value.__class__.__name__
            if cls == "StringLiteral":
                val = repr(c.value.val)
            elif cls == "BoolLiteral":
                val = str(c.value.val).lower()
            else:
                val = str(c.value.val)
            const_rows.append(f"{c.name} = {val}")
        label = "constants\\n" + "\\n".join(const_rows)
        lines.append(f'    constants [shape=note label="{label}"];')
        lines.append("")

    if model.functions:
        func_rows = []
        for f in model.functions:
            params = ", ".join(f.params) if f.params else ""
            func_rows.append(f"func {f.name}({params})")
        label = "functions\\n" + "\\n".join(func_rows)
        lines.append(f'    functions [shape=note label="{label}"];')
        lines.append("")

    # Broker nodes
    broker_ids = {}
    for b in model.brokers:
        bid = f"broker_{b.name}"
        broker_ids[b.name] = bid
        transport = b.transport if b.transport else "mqtt"
        label = f"{b.name}\\n{transport}://{b.host}:{b.port}"
        lines.append(f'    {bid} [shape=cylinder label="{label}"];')
    if model.brokers:
        lines.append("")

    # Pipelines
    for p in model.pipelines:
        _dot_pipeline(p, lines, broker_ids, model)

    lines.append("}")
    return "\n".join(lines)


def _dot_pipeline(pipeline, lines, broker_ids, model):
    """Emit DOT subgraph for a single pipeline."""
    pname = pipeline.name
    prefix = f"p_{pname}"

    lines.append(f"    subgraph cluster_{pname} {{")
    retry_label = ""
    if pipeline.retry:
        base = _format_duration(pipeline.retry.base)
        retry_label = f"\\nretry: {pipeline.retry.max_retries}x, backoff {base}"
    lines.append(f'        label="{pname}{retry_label}";')
    lines.append("        style=rounded;")
    lines.append("")

    node_chain = []

    # @initially hook
    if pipeline.initially:
        nid = f"{prefix}_initially"
        fields = ", ".join(f.name for f in pipeline.initially.body.fields)
        label = f"@initially\\npublish {{{fields}}}"
        lines.append(f'        {nid} [shape=note label="{label}"];')
        node_chain.append(nid)

    # Source
    src = pipeline.source
    src_id = f"{prefix}_source"
    if src.pipe:
        label = f"source\\npipe: {src.pipe}"
        lines.append(f'        {src_id} [shape=cds label="{label}"];')
    else:
        fmt_part = f"\\nformat: {src.format}" if src.format else ""
        schema_part = f"\\nschema: {src.schema.name}" if src.schema else ""
        label = f"source\\n{src.topic}{fmt_part}{schema_part}"
        lines.append(f'        {src_id} [shape=cds label="{label}"];')
    node_chain.append(src_id)

    if pipeline.join:
        join_id = f"{prefix}_join"
        jtype = pipeline.join.type if pipeline.join.type else "inner"
        left = ".".join(pipeline.join.left_key.segments)
        right = ".".join(pipeline.join.right_key.segments)
        window = _format_duration(pipeline.join.window)
        label = f"join ({jtype})\\n{left} == {right}\\nwithin {window}"
        lines.append(f'        {join_id} [shape=box style=bold label="{label}"];')
        node_chain.append(join_id)

    if pipeline.pipeline_lets:
        let_id = f"{prefix}_lets"
        let_names = ", ".join(lb.name for lb in pipeline.pipeline_lets)
        label = f"let\\n{let_names}"
        lines.append(f'        {let_id} [shape=note label="{label}"];')
        node_chain.append(let_id)

    raw_stages = _unwrap_stages(pipeline)
    annotations_map = _build_annotations_map(pipeline)

    for i, stage in enumerate(raw_stages):
        nid = f"{prefix}_stage{i}"
        cls = stage.__class__.__name__
        label = _dot_stage_label(stage, cls)

        ann_text = annotations_map.get(id(stage))
        if ann_text:
            label = f"{label}\\n{ann_text}"

        lines.append(f'        {nid} [shape=box label="{label}"];')
        node_chain.append(nid)

    # Sink or Route
    if pipeline.sink:
        sink_id = f"{prefix}_sink"
        snk = pipeline.sink
        label = f"sink\\n{snk.topic}"
        if snk.format:
            label += f"\\nformat: {snk.format}"
        if snk.batch:
            batch_parts = []
            if snk.batch.size:
                batch_parts.append(f"size={snk.batch.size}")
            if snk.batch.timeout:
                batch_parts.append(f"timeout={_format_duration(snk.batch.timeout)}")
            label += f"\\nbatch: {', '.join(batch_parts)}"
        lines.append(f'        {sink_id} [shape=cds label="{label}"];')
        node_chain.append(sink_id)

    if pipeline.route:
        route_id = f"{prefix}_route"
        lines.append(f'        {route_id} [shape=diamond label="route"];')
        node_chain.append(route_id)

        for j, arm in enumerate(pipeline.route.arms):
            arm_id = f"{prefix}_route_arm{j}"
            arm_label = f"sink\\n{arm.sink.topic}"
            lines.append(f'        {arm_id} [shape=cds label="{arm_label}"];')
            lines.append(f'        {route_id} -> {arm_id} [label="when"];')

        if pipeline.route.default:
            def_id = f"{prefix}_route_default"
            def_label = f"sink\\n{pipeline.route.default.topic}"
            lines.append(f'        {def_id} [shape=cds label="{def_label}"];')
            lines.append(f'        {route_id} -> {def_id} [label="default"];')

    # @finally hook
    finally_id = None
    if pipeline.finally_:
        finally_id = f"{prefix}_finally"
        fields = ", ".join(f.name for f in pipeline.finally_.body.fields)
        label = f"@finally\\npublish {{{fields}}}"
        lines.append(f'        {finally_id} [shape=note label="{label}"];')

    # DLQ node (on_error with sink)
    dlq_id = None
    if pipeline.on_error and pipeline.on_error.broker:
        dlq_id = f"{prefix}_dlq"
        dlq_label = f"DLQ\\n{pipeline.on_error.topic}"
        lines.append(
            f'        {dlq_id} [shape=hexagon style=filled fillcolor="#ffcccc" label="{dlq_label}"];'
        )

    # Edges: chain nodes
    for a, b in zip(node_chain, node_chain[1:]):
        lines.append(f"        {a} -> {b};")

    # Connect @finally to last node in chain
    if finally_id and node_chain:
        lines.append(f"        {node_chain[-1]} -> {finally_id};")

    # Connect DLQ to pipeline node
    if dlq_id:
        pipeline_node = node_chain[-1] if node_chain else src_id
        lines.append(
            f'        {pipeline_node} -> {dlq_id} [label="on_error" style=dashed color=red];'
        )

    lines.append("    }")
    lines.append("")

    # External edges (outside subgraph)

    # Source broker edge
    if not src.pipe and src.broker.name in broker_ids:
        lines.append(f"    {broker_ids[src.broker.name]} -> {src_id};")

    # Join broker edge
    if pipeline.join and pipeline.join.broker.name in broker_ids:
        join_id = f"{prefix}_join"
        lines.append(
            f"    {broker_ids[pipeline.join.broker.name]} -> {join_id} [style=dashed];"
        )

    # Sink broker edge
    if pipeline.sink and pipeline.sink.broker.name in broker_ids:
        sink_id = f"{prefix}_sink"
        lines.append(f"    {sink_id} -> {broker_ids[pipeline.sink.broker.name]};")

    # Route broker edges
    if pipeline.route:
        for j, arm in enumerate(pipeline.route.arms):
            arm_id = f"{prefix}_route_arm{j}"
            if arm.sink.broker.name in broker_ids:
                lines.append(f"    {arm_id} -> {broker_ids[arm.sink.broker.name]};")
        if pipeline.route.default:
            def_id = f"{prefix}_route_default"
            if pipeline.route.default.broker.name in broker_ids:
                lines.append(
                    f"    {def_id} -> {broker_ids[pipeline.route.default.broker.name]};"
                )

    # DLQ broker edge
    if pipeline.on_error and pipeline.on_error.broker:
        dlq_id = f"{prefix}_dlq"
        if pipeline.on_error.broker.name in broker_ids:
            lines.append(
                f"    {dlq_id} -> {broker_ids[pipeline.on_error.broker.name]};"
            )

    # Pipe source: dashed edge from upstream pipeline
    if src.pipe:
        upstream_pipelines = [up for up in model.pipelines if up.name == src.pipe]
        if upstream_pipelines:
            upstream = upstream_pipelines[0]
            if upstream.sink:
                upstream_sink = f"p_{upstream.name}_sink"
                lines.append(
                    f'    {upstream_sink} -> {src_id} [style=dashed label="pipe"];'
                )
            elif upstream.route:
                upstream_route = f"p_{upstream.name}_route"
                lines.append(
                    f'    {upstream_route} -> {src_id} [style=dashed label="pipe"];'
                )

    lines.append("")


def _dot_stage_label(stage, cls):
    """Build a DOT label string for a stage node."""
    if cls == "DeduplicateStage":
        field = ".".join(stage.key.segments)
        ttl = _format_duration(stage.ttl)
        return f"deduplicate\\n{field} within {ttl}"

    if cls == "RateStage":
        period = _format_duration(stage.period)
        excess = stage.on_excess if stage.on_excess else "drop"
        return f"rate\\n{stage.limit} per {period}\\non_excess: {excess}"

    if cls == "FlattenStage":
        field = ".".join(stage.field.segments)
        return f"flatten\\n{field}"

    if cls == "DistinctStage":
        field = ".".join(stage.field.segments)
        ttl = _format_duration(stage.ttl)
        return f"distinct\\n{field} within {ttl}"

    if cls == "ThrottleStage":
        period = _format_duration(stage.period)
        overflow = stage.overflow if stage.overflow else "drop"
        return f"throttle\\n{stage.limit} per {period}\\noverflow: {overflow}"

    if cls == "SampleStage":
        return f"sample\\n{stage.percentage}%"

    if cls == "FilterStage":
        return "filter\\n<condition>"

    if cls == "GroupByStage":
        fields = ", ".join(".".join(f.segments) for f in stage.fields)
        return f"group_by\\n{fields}"

    if cls == "WindowStage":
        size = _format_duration(stage.size)
        name_part = f" '{stage.wname}'" if stage.wname else ""
        if stage.type == "sliding" and stage.step:
            step = _format_duration(stage.step)
            return f"window{name_part}\\nsliding({size}, step={step})"
        return f"window{name_part}\\ntumbling({size})"

    if cls == "AggregateStage":
        aggs = []
        for a in stage.assignments:
            over_part = f" over {a.window_ref}" if a.window_ref else ""
            if a.field:
                field = ".".join(a.field.segments)
                aggs.append(f"{a.func}({field}){over_part}")
            else:
                aggs.append(f"{a.func}(){over_part}")
        return f"aggregate\\n{', '.join(aggs)}"

    if cls == "AccumulateStage":
        accs = []
        for a in stage.assignments:
            field = ".".join(a.field.segments)
            accs.append(f"{a.func}({field})")
        return f"accumulate\\n{', '.join(accs)}"

    if cls == "HavingStage":
        return "having\\n<condition>"

    if cls == "MapStage":
        targets = [m.target for m in stage.mappings]
        return f"map\\n{', '.join(targets)}"

    return cls


def _build_annotations_map(pipeline):
    """Build a map from stage object id to annotation text."""
    ann_map = {}
    for item in pipeline.stages:
        if item.__class__.__name__ == "AnnotatedStage" and item.annotations:
            parts = []
            for ann in item.annotations:
                parts.append(f"@{ann.severity}: {ann.message}")
            ann_map[id(item.stage)] = "\\n".join(parts)
    return ann_map


def _format_duration(dur):
    """Format a Duration AST node as a compact string."""
    return f"{dur.value}{dur.unit}"


# ─── Mermaid output ─────────────────────────────────────────────────


def visualize_mermaid(model) -> str:
    """Generate a Mermaid diagram from a parsed Sigil model."""
    lines = []
    lines.append("graph LR")
    lines.append("")

    # Constants node
    if model.constants:
        const_rows = []
        for c in model.constants:
            cls = c.value.__class__.__name__
            if cls == "StringLiteral":
                val = repr(c.value.val)
            elif cls == "BoolLiteral":
                val = str(c.value.val).lower()
            else:
                val = str(c.value.val)
            const_rows.append(f"{c.name} = {val}")
        label = "constants\n" + "\n".join(const_rows)
        lines.append(f'    constants["{_mermaid_escape(label)}"]')
        lines.append("")

    if model.functions:
        func_rows = []
        for f in model.functions:
            params = ", ".join(f.params) if f.params else ""
            func_rows.append(f"func {f.name}({params})")
        label = "functions\n" + "\n".join(func_rows)
        lines.append(f'    functions["{_mermaid_escape(label)}"]')
        lines.append("")

    broker_ids = {}
    for b in model.brokers:
        bid = f"broker_{b.name}"
        broker_ids[b.name] = bid
        transport = b.transport if b.transport else "mqtt"
        label = f"{b.name}\n{transport}://{b.host}:{b.port}"
        lines.append(f'    {bid}[("{_mermaid_escape(label)}")]')
    if model.brokers:
        lines.append("")

    for p in model.pipelines:
        _mermaid_pipeline(p, lines, broker_ids, model)

    return "\n".join(lines)


def _mermaid_pipeline(pipeline, lines, broker_ids, model):
    """Emit Mermaid subgraph for a single pipeline."""
    pname = pipeline.name
    prefix = f"p_{pname}"

    retry_label = ""
    if pipeline.retry:
        base = _format_duration(pipeline.retry.base)
        retry_label = f"\nretry: {pipeline.retry.max_retries}x, backoff {base}"
    lines.append(f'    subgraph {prefix}["{_mermaid_escape(pname + retry_label)}"]')

    node_chain = []

    # @initially hook
    if pipeline.initially:
        nid = f"{prefix}_initially"
        fields = ", ".join(f.name for f in pipeline.initially.body.fields)
        label = f"@initially\npublish {{{fields}}}"
        lines.append(f'        {nid}["{_mermaid_escape(label)}"]')
        node_chain.append(nid)

    # Source
    src = pipeline.source
    src_id = f"{prefix}_source"
    if src.pipe:
        label = f"source\npipe: {src.pipe}"
        lines.append(f'        {src_id}[/"{_mermaid_escape(label)}"/]')
    else:
        fmt_part = f"\nformat: {src.format}" if src.format else ""
        schema_part = f"\nschema: {src.schema.name}" if src.schema else ""
        label = f"source\n{src.topic}{fmt_part}{schema_part}"
        lines.append(f'        {src_id}[/"{_mermaid_escape(label)}"/]')
    node_chain.append(src_id)

    if pipeline.join:
        join_id = f"{prefix}_join"
        jtype = pipeline.join.type if pipeline.join.type else "inner"
        left = ".".join(pipeline.join.left_key.segments)
        right = ".".join(pipeline.join.right_key.segments)
        window = _format_duration(pipeline.join.window)
        label = f"join ({jtype})\n{left} == {right}\nwithin {window}"
        lines.append(f'        {join_id}["{_mermaid_escape(label)}"]')
        node_chain.append(join_id)

    if pipeline.pipeline_lets:
        let_id = f"{prefix}_lets"
        let_names = ", ".join(lb.name for lb in pipeline.pipeline_lets)
        label = f"let\n{let_names}"
        lines.append(f'        {let_id}["{_mermaid_escape(label)}"]')
        node_chain.append(let_id)

    raw_stages = _unwrap_stages(pipeline)
    annotations_map = _build_annotations_map(pipeline)

    for i, stage in enumerate(raw_stages):
        nid = f"{prefix}_stage{i}"
        cls = stage.__class__.__name__
        label = _mermaid_stage_label(stage, cls)

        ann_text = annotations_map.get(id(stage))
        if ann_text:
            ann_clean = ann_text.replace("\\n", "\n")
            label = f"{label}\n{ann_clean}"

        lines.append(f'        {nid}["{_mermaid_escape(label)}"]')
        node_chain.append(nid)

    # Sink or Route
    if pipeline.sink:
        sink_id = f"{prefix}_sink"
        snk = pipeline.sink
        label = f"sink\n{snk.topic}"
        if snk.format:
            label += f"\nformat: {snk.format}"
        lines.append(f'        {sink_id}[/"{_mermaid_escape(label)}"/]')
        node_chain.append(sink_id)

    if pipeline.route:
        route_id = f"{prefix}_route"
        lines.append(f'        {route_id}{{"route"}}')
        node_chain.append(route_id)

        for j, arm in enumerate(pipeline.route.arms):
            arm_id = f"{prefix}_route_arm{j}"
            arm_label = f"sink\n{arm.sink.topic}"
            lines.append(f'        {arm_id}[/"{_mermaid_escape(arm_label)}"/]')
            lines.append(f'        {route_id} -->|"when"| {arm_id}')

        if pipeline.route.default:
            def_id = f"{prefix}_route_default"
            def_label = f"sink\n{pipeline.route.default.topic}"
            lines.append(f'        {def_id}[/"{_mermaid_escape(def_label)}"/]')
            lines.append(f'        {route_id} -->|"default"| {def_id}')

    # @finally hook
    finally_id = None
    if pipeline.finally_:
        finally_id = f"{prefix}_finally"
        fields = ", ".join(f.name for f in pipeline.finally_.body.fields)
        label = f"@finally\npublish {{{fields}}}"
        lines.append(f'        {finally_id}["{_mermaid_escape(label)}"]')

    # DLQ node (on_error with sink)
    dlq_id = None
    if pipeline.on_error and pipeline.on_error.broker:
        dlq_id = f"{prefix}_dlq"
        dlq_label = f"DLQ: {pipeline.on_error.topic}"
        lines.append(f'        {dlq_id}{{{{"{_mermaid_escape(dlq_label)}"}}}}')

    # Edges: chain nodes
    for a, b in zip(node_chain, node_chain[1:]):
        lines.append(f"        {a} --> {b}")

    # Connect @finally to last node in chain
    if finally_id and node_chain:
        lines.append(f"        {node_chain[-1]} --> {finally_id}")

    # Connect DLQ to pipeline node
    if dlq_id and node_chain:
        pipeline_node = node_chain[-1]
        lines.append(f'        {pipeline_node} -.->|"on_error"| {dlq_id}')

    lines.append("    end")
    lines.append("")

    # External edges (outside subgraph)

    # Source broker edge
    if not src.pipe and src.broker.name in broker_ids:
        lines.append(f"    {broker_ids[src.broker.name]} --> {src_id}")

    # Join broker edge
    if pipeline.join and pipeline.join.broker.name in broker_ids:
        join_id = f"{prefix}_join"
        lines.append(f"    {broker_ids[pipeline.join.broker.name]} -.-> {join_id}")

    # Sink broker edge
    if pipeline.sink and pipeline.sink.broker.name in broker_ids:
        sink_id = f"{prefix}_sink"
        lines.append(f"    {sink_id} --> {broker_ids[pipeline.sink.broker.name]}")

    # Route broker edges
    if pipeline.route:
        for j, arm in enumerate(pipeline.route.arms):
            arm_id = f"{prefix}_route_arm{j}"
            if arm.sink.broker.name in broker_ids:
                lines.append(f"    {arm_id} --> {broker_ids[arm.sink.broker.name]}")
        if pipeline.route.default:
            def_id = f"{prefix}_route_default"
            if pipeline.route.default.broker.name in broker_ids:
                lines.append(
                    f"    {def_id} --> {broker_ids[pipeline.route.default.broker.name]}"
                )

    # DLQ broker edge
    if pipeline.on_error and pipeline.on_error.broker:
        dlq_id = f"{prefix}_dlq"
        if pipeline.on_error.broker.name in broker_ids:
            lines.append(
                f"    {dlq_id} --> {broker_ids[pipeline.on_error.broker.name]}"
            )

    # Pipe source: dotted edge from upstream pipeline
    if src.pipe:
        upstream_pipelines = [up for up in model.pipelines if up.name == src.pipe]
        if upstream_pipelines:
            upstream = upstream_pipelines[0]
            if upstream.sink:
                upstream_sink = f"p_{upstream.name}_sink"
                lines.append(f'    {upstream_sink} -.->|"pipe"| {src_id}')
            elif upstream.route:
                upstream_route = f"p_{upstream.name}_route"
                lines.append(f'    {upstream_route} -.->|"pipe"| {src_id}')

    lines.append("")


def _mermaid_stage_label(stage, cls):
    """Build a Mermaid label string for a stage node."""
    if cls == "DeduplicateStage":
        field = ".".join(stage.key.segments)
        ttl = _format_duration(stage.ttl)
        return f"deduplicate\n{field} within {ttl}"

    if cls == "RateStage":
        period = _format_duration(stage.period)
        excess = stage.on_excess if stage.on_excess else "drop"
        return f"rate\n{stage.limit} per {period}\non_excess: {excess}"

    if cls == "FlattenStage":
        field = ".".join(stage.field.segments)
        return f"flatten\n{field}"

    if cls == "DistinctStage":
        field = ".".join(stage.field.segments)
        ttl = _format_duration(stage.ttl)
        return f"distinct\n{field} within {ttl}"

    if cls == "ThrottleStage":
        period = _format_duration(stage.period)
        overflow = stage.overflow if stage.overflow else "drop"
        return f"throttle\n{stage.limit} per {period}\noverflow: {overflow}"

    if cls == "SampleStage":
        return f"sample\n{stage.percentage}%"

    if cls == "FilterStage":
        return "filter\n<condition>"

    if cls == "GroupByStage":
        fields = ", ".join(".".join(f.segments) for f in stage.fields)
        return f"group_by\n{fields}"

    if cls == "WindowStage":
        size = _format_duration(stage.size)
        name_part = f" '{stage.wname}'" if stage.wname else ""
        if stage.type == "sliding" and stage.step:
            step = _format_duration(stage.step)
            return f"window{name_part}\nsliding({size}, step={step})"
        return f"window{name_part}\ntumbling({size})"

    if cls == "AggregateStage":
        aggs = []
        for a in stage.assignments:
            over_part = f" over {a.window_ref}" if a.window_ref else ""
            if a.field:
                field = ".".join(a.field.segments)
                aggs.append(f"{a.func}({field}){over_part}")
            else:
                aggs.append(f"{a.func}(){over_part}")
        return f"aggregate\n{', '.join(aggs)}"

    if cls == "AccumulateStage":
        accs = []
        for a in stage.assignments:
            field = ".".join(a.field.segments)
            accs.append(f"{a.func}({field})")
        return f"accumulate\n{', '.join(accs)}"

    if cls == "HavingStage":
        return "having\n<condition>"

    if cls == "MapStage":
        targets = [m.target for m in stage.mappings]
        return f"map\n{', '.join(targets)}"

    return cls


def _mermaid_escape(text):
    """Escape special characters for Mermaid labels."""
    return text.replace('"', "#quot;")
