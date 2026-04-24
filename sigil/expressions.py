from enum import Enum


class ExprContext(Enum):
    FILTER = "filter"
    MAP = "map"
    AGGREGATE = "aggregate"
    HAVING = "having"
    QUANTIFIER = "quantifier"


SCALAR_FUNC_MAP = {
    "abs": lambda args: f"abs({args[0]})",
    "round": lambda args: f"round({', '.join(args)})",
    "len": lambda args: f"len({args[0]})",
    "lower": lambda args: f"str({args[0]}).lower()",
    "upper": lambda args: f"str({args[0]}).upper()",
    "floor": lambda args: f"math.floor({args[0]})",
    "ceil": lambda args: f"math.ceil({args[0]})",
    "fmt": lambda args: f"{args[0]}.format({', '.join(args[1:])})",
    "trim": lambda args: f"str({args[0]}).strip()",
    "contains": lambda args: f"({args[1]} in str({args[0]}))",
    "starts_with": lambda args: f"str({args[0]}).startswith({args[1]})",
    "ends_with": lambda args: f"str({args[0]}).endswith({args[1]})",
    "split": lambda args: f"str({args[0]}).split({args[1]})",
    "replace": lambda args: f"str({args[0]}).replace({args[1]}, {args[2]})",
    "substring": lambda args: (
        f"str({args[0]})[{args[1]}:{args[2]}]"
        if len(args) == 3
        else f"str({args[0]})[{args[1]}:]"
    ),
    "sqrt": lambda args: f"math.sqrt({args[0]})",
    "pow": lambda args: f"math.pow({args[0]}, {args[1]})",
    "log": lambda args: f"math.log({args[0]})",
    "log10": lambda args: f"math.log10({args[0]})",
    "exp": lambda args: f"math.exp({args[0]})",
    "to_int": lambda args: f"int({args[0]})",
    "to_float": lambda args: f"float({args[0]})",
    "to_str": lambda args: f"str({args[0]})",
    "to_bool": lambda args: f"bool({args[0]})",
    "now": lambda args: "time.time()",
    "timestamp_ms": lambda args: "int(time.time() * 1000)",
    "date_format": lambda args: f"datetime.fromtimestamp({args[0]}).strftime({args[1]})",
    "date_diff": lambda args: f"({args[0]} - {args[1]})",
    "matches": lambda args: f"(bool(re.search({args[1]}, str({args[0]}))))",
    "extract": lambda args: (
        f"((lambda _m: _m.group({args[2]}) if _m else None)"
        f"(re.search({args[1]}, str({args[0]}))))"
    ),
}


def _flatten_binary(node, cls_name: str):
    """Flatten a right-recursive binary tree into a left-to-right sequence.

    The grammar uses right recursion (``left=X op=Op right=Rule``) to work
    around a PEG parser limitation.  We flatten the tree so the expression
    compiler can emit left-associative code (important for ``-`` and ``/``).
    """
    operands = [node.left]
    ops: list[str] = []
    cur = node
    while cur.op:
        ops.append(cur.op)
        operands.append(cur.right.left)
        cur = cur.right
    return operands, ops


def compile_expr(
    node,
    ctx: ExprContext = ExprContext.FILTER,
    local_names: set[str] | None = None,
    const_map: dict[str, str] | None = None,
    func_defs: dict | None = None,
) -> str:
    cls = node.__class__.__name__

    # Helper to recurse with same context
    def _r(n):
        return compile_expr(n, ctx, local_names, const_map, func_defs)

    if cls == "Expression":
        parts = [_r(op) for op in node.operands]
        return " or ".join(parts) if len(parts) > 1 else parts[0]

    if cls == "AndExpr":
        parts = [_r(op) for op in node.operands]
        return " and ".join(parts) if len(parts) > 1 else parts[0]

    if cls == "Comparison":
        left = _r(node.left)
        if node.op:
            right = _r(node.right)
            return f"{left} {node.op} {right}"
        if node.nullCheck:
            if node.nullCheck.negated:
                return f"({left} is not None)"
            return f"({left} is None)"
        if node.inCheck:
            items = ", ".join(_r(item) for item in node.inCheck.list.items)
            if node.inCheck.negated:
                return f"({left} not in [{items}])"
            return f"({left} in [{items}])"
        return left

    if cls == "Addition":
        operands, ops = _flatten_binary(node, "Addition")
        result = _r(operands[0])
        for op, operand in zip(ops, operands[1:]):
            result = f"{result} {op} {_r(operand)}"
        return result

    if cls == "Multiplication":
        operands, ops = _flatten_binary(node, "Multiplication")
        result = _r(operands[0])
        for op, operand in zip(ops, operands[1:]):
            result = f"{result} {op} {_r(operand)}"
        return result

    if cls == "Coalesce":
        left = _r(node.left)
        if node.right:
            right = _r(node.right)
            return f"_coalesce({left}, {right})"
        return left

    if cls == "Unary":
        if node.op:
            inner = _r(node.operand)
            if node.op == "not":
                return f"not {inner}"
            return f"(-{inner})"
        return _r(node.atom)

    if cls == "ParenExpr":
        return f"({_r(node.expr)})"

    if cls == "IfExpr":
        cond = _r(node.condition)
        then = _r(node.thenExpr)
        els = _r(node.elseExpr)
        return f"({then} if {cond} else {els})"

    if cls == "CaseExpr":
        return _compile_case_expr(node, ctx, local_names, const_map, func_defs)

    if cls == "MatchExpr":
        return _compile_match_expr(node, ctx, local_names, const_map, func_defs)

    if cls == "UserFuncCall":
        return _compile_user_func_call(node, ctx, local_names, const_map, func_defs)

    if cls == "EnvRef":
        key = repr(node.key)
        if node.default:
            default = _r(node.default)
            return f"os.environ.get({key}, {default})"
        return f"os.environ[{key}]"

    if cls == "OldRef":
        keys = ", ".join(repr(s) for s in node.field.segments[1:])
        if node.field.segments[0] == "msg" and len(node.field.segments) > 1:
            return f"_get_nested(self._prev_msg, {keys})"
        return f"_get_nested(self._prev_msg, {', '.join(repr(s) for s in node.field.segments)})"

    if cls == "ChangedRef":
        if node.field.segments[0] == "msg" and len(node.field.segments) > 1:
            keys = ", ".join(repr(s) for s in node.field.segments[1:])
            cur = f"_get_nested(msg, {keys})"
            prev = f"_get_nested(self._prev_msg, {keys})"
        else:
            keys = ", ".join(repr(s) for s in node.field.segments)
            cur = f"_get_nested(msg, {keys})"
            prev = f"_get_nested(self._prev_msg, {keys})"
        return f"({cur} != {prev})"

    if cls == "ForallExpr":
        return _compile_quantifier_expr(
            node, "all", ctx, local_names, const_map, func_defs
        )

    if cls == "ExistsExpr":
        return _compile_quantifier_expr(
            node, "any", ctx, local_names, const_map, func_defs
        )

    if cls == "FuncCall":
        args = [_r(a) for a in node.args]
        gen = SCALAR_FUNC_MAP.get(node.name)
        if gen:
            return gen(args)
        raise ValueError(f"Unknown scalar function: {node.name}")

    if cls == "BoolLiteral":
        return "True" if node.val else "False"

    if cls == "FloatLiteral":
        return repr(node.val)

    if cls == "IntLiteral":
        return repr(node.val)

    if cls == "StringLiteral":
        return repr(node.val)

    if cls == "FieldRef":
        return _compile_field_ref(
            node.segments,
            ctx,
            local_names,
            const_map,
            node.subscript if node.subscript else None,
        )

    raise ValueError(f"Unknown expression node: {cls}")


def _compile_case_expr(node, ctx, local_names, const_map, func_defs=None):
    """Compile case { when c1 => r1  when c2 => r2  else => r_else }
    into nested Python ternary: (r1 if c1 else (r2 if c2 else r_else))
    """

    def _r(n):
        return compile_expr(n, ctx, local_names, const_map, func_defs)

    if not node.arms:
        raise ValueError("CaseExpr must have at least one 'when' arm")

    # Build from inside out (last arm first)
    if node.elseExpr:
        result = _r(node.elseExpr)
    else:
        result = "None"

    for arm in reversed(node.arms):
        cond = _r(arm.condition)
        val = _r(arm.result)
        result = f"({val} if {cond} else {result})"

    return result


def _compile_match_expr(node, ctx, local_names, const_map, func_defs=None):
    """Compile match subject { case p1 => r1  case p2 => r2  else => d }
    into nested Python ternary: (r1 if subject == p1 else (r2 if subject == p2 else d))
    """

    def _r(n):
        return compile_expr(n, ctx, local_names, const_map, func_defs)

    if not node.arms:
        raise ValueError("MatchExpr must have at least one 'case' arm")

    subject = _r(node.subject)

    if node.elseExpr:
        result = _r(node.elseExpr)
    else:
        result = "None"

    for arm in reversed(node.arms):
        pattern = _r(arm.pattern)
        val = _r(arm.result)
        result = f"({val} if {subject} == {pattern} else {result})"

    return result


def _compile_user_func_call(node, ctx, local_names, const_map, func_defs=None):
    """Inline user-defined function: celsius(msg.t) → ((msg.t - 32) * 5 / 9)"""
    if not func_defs or node.name not in func_defs:
        raise ValueError(f"Unknown function: {node.name}")

    func_def = func_defs[node.name]
    params = func_def.params or []
    args = node.args or []

    if len(args) != len(params):
        raise ValueError(
            f"Function '{node.name}' expects {len(params)} argument(s), got {len(args)}"
        )

    param_map = {}
    for param, arg in zip(params, args):
        param_map[param] = compile_expr(arg, ctx, local_names, const_map, func_defs)

    merged_const = dict(const_map) if const_map else {}
    merged_const.update(param_map)

    inner_locals = set(local_names) if local_names else set()
    return (
        f"({compile_expr(func_def.body, ctx, inner_locals, merged_const, func_defs)})"
    )


def _compile_field_ref(
    segments: list[str],
    ctx: ExprContext,
    local_names: set[str] | None = None,
    const_map: dict[str, str] | None = None,
    subscript=None,
) -> str:
    # Check if this is a const reference (bare ID matching a const name)
    if const_map and len(segments) == 1 and segments[0] in const_map:
        return const_map[segments[0]]

    if segments[0] == "other" and len(segments) > 1:
        keys = ", ".join(repr(s) for s in segments[1:])
        base = f"_get_nested(other, {keys})"
        if subscript:
            idx = -subscript.index if subscript.neg else subscript.index
            return f"{base}[{idx}]"
        return base

    if ctx in (ExprContext.FILTER, ExprContext.AGGREGATE, ExprContext.QUANTIFIER):
        if local_names and len(segments) == 1 and segments[0] in local_names:
            base = segments[0]
            if subscript:
                idx = -subscript.index if subscript.neg else subscript.index
                return f"{base}[{idx}]"
            return base
        if segments[0] == "msg" and len(segments) > 1:
            keys = ", ".join(repr(s) for s in segments[1:])
            base = f"_get_nested(msg, {keys})"
            if subscript:
                idx = -subscript.index if subscript.neg else subscript.index
                return f"{base}[{idx}]"
            return base
        base = ".".join(segments)
        if subscript:
            idx = -subscript.index if subscript.neg else subscript.index
            return f"{base}[{idx}]"
        return base

    if ctx == ExprContext.HAVING:
        if segments[0] == "msg" and len(segments) > 1:
            keys = ", ".join(repr(s) for s in segments[1:])
            base = f"_get_nested(msg, {keys})"
            if subscript:
                idx = -subscript.index if subscript.neg else subscript.index
                return f"{base}[{idx}]"
            return base
        if len(segments) == 1:
            base = f"aggregated['{segments[0]}']"
            if subscript:
                idx = -subscript.index if subscript.neg else subscript.index
                return f"{base}[{idx}]"
            return base
        base = ".".join(segments)
        if subscript:
            idx = -subscript.index if subscript.neg else subscript.index
            return f"{base}[{idx}]"
        return base

    if ctx == ExprContext.MAP:
        if segments[0] == "msg" and len(segments) > 1:
            keys = ", ".join(repr(s) for s in segments[1:])
            base = f"_get_nested(msg, {keys})"
            if subscript:
                idx = -subscript.index if subscript.neg else subscript.index
                return f"{base}[{idx}]"
            return base
        if local_names and len(segments) == 1 and segments[0] in local_names:
            base = segments[0]
            if subscript:
                idx = -subscript.index if subscript.neg else subscript.index
                return f"{base}[{idx}]"
            return base
        base = f"aggregated['{'.'.join(segments)}']"
        if subscript:
            idx = -subscript.index if subscript.neg else subscript.index
            return f"{base}[{idx}]"
        return base

    base = ".".join(segments)
    if subscript:
        idx = -subscript.index if subscript.neg else subscript.index
        return f"{base}[{idx}]"
    return base


def _compile_quantifier_expr(
    node, func_name, ctx, local_names, const_map, func_defs=None
):
    var = node.var
    inner_locals = set(local_names) if local_names else set()
    inner_locals.add(var)

    predicate_code = compile_expr(
        node.predicate, ExprContext.QUANTIFIER, inner_locals, const_map, func_defs
    )

    if node.collection:
        collection_code = _compile_field_ref(
            node.collection.segments,
            ctx,
            local_names,
            const_map,
            node.collection.subscript if node.collection.subscript else None,
        )
        return f"{func_name}({predicate_code} for {var} in ({collection_code} or []))"
    else:
        return f"{func_name}({predicate_code} for {var} in messages)"


def extract_field_keys(field_ref) -> list[str]:
    segs = field_ref.segments
    if segs[0] == "msg" and len(segs) > 1:
        return segs[1:]
    return segs
