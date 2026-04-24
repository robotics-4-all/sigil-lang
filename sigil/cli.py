"""Sigil command-line interface.

Subcommands: check, generate, explain, visualize.

Exit codes:
  0  success
  1  CLI usage error / unknown subcommand
  2  parse error (invalid syntax in .sigil source)
  3  semantic validation error
  4  I/O error (file not found, permission denied, write failure)
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from . import __version__
from .codegen import generate
from .diagnostics import (
    Diagnostic,
    from_semantic_error,
    from_textx_error,
    render_all,
)
from .explain import explain_model
from .language import parse
from .semantics import SemanticValidationError, validate, validate_or_raise
from .visualize import visualize_dot, visualize_mermaid

EXIT_OK = 0
EXIT_USAGE = 1
EXIT_PARSE = 2
EXIT_SEMANTIC = 3
EXIT_IO = 4


def main(argv: list[str] | None = None) -> int:
    parser = _build_argparser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return EXIT_USAGE

    color = _decide_color(args)

    if args.command == "check":
        return _cmd_check(args.source, color=color, as_json=args.json)
    if args.command == "generate":
        return _cmd_generate(args.source, args.output, color=color)
    if args.command == "explain":
        return _cmd_explain(args.source, color=color)
    if args.command == "visualize":
        return _cmd_visualize(args.source, args.format, args.output, color=color)

    parser.print_help()
    return EXIT_USAGE


def _build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="sigil",
        description="Sigil -- stream-processing pipeline DSL compiler",
    )
    ap.add_argument("--version", action="version", version=f"sigil {__version__}")
    color_group = ap.add_mutually_exclusive_group()
    color_group.add_argument(
        "--color",
        dest="color",
        action="store_true",
        default=None,
        help="Force colored output",
    )
    color_group.add_argument(
        "--no-color",
        dest="color",
        action="store_false",
        help="Disable colored output",
    )

    sub = ap.add_subparsers(dest="command")

    p_check = sub.add_parser("check", help="Parse and validate a .sigil file")
    p_check.add_argument("source", help="Path to .sigil source file")
    p_check.add_argument(
        "--json",
        action="store_true",
        help="Emit diagnostics as JSON for tooling.",
    )

    p_gen = sub.add_parser("generate", help="Generate Python application")
    p_gen.add_argument("source", help="Path to .sigil source file")
    p_gen.add_argument(
        "-o",
        "--output",
        default="generated",
        help="Output directory (default: generated/)",
    )

    p_explain = sub.add_parser("explain", help="Explain pipeline structure")
    p_explain.add_argument("source", help="Path to .sigil source file")

    p_viz = sub.add_parser("visualize", help="Generate pipeline diagram")
    p_viz.add_argument("source", help="Path to .sigil source file")
    p_viz.add_argument(
        "--format",
        "-f",
        choices=["dot", "mermaid"],
        default="dot",
        help="Output format (default: dot)",
    )
    p_viz.add_argument("--output", "-o", help="Output file (default: stdout)")
    return ap


def _decide_color(args) -> bool:
    if getattr(args, "color", None) is not None:
        return bool(args.color)
    if os.environ.get("NO_COLOR"):
        return False
    return sys.stderr.isatty()


def _parse_with_diagnostics(source_path: str, *, color: bool, as_json: bool = False):
    try:
        return parse(source_path), None
    except FileNotFoundError as exc:
        if as_json:
            print(json.dumps([{"severity": "error", "message": str(exc), "code": "E0000"}]))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return None, EXIT_IO
    except Exception as exc:
        diag = from_textx_error(exc, source_path=source_path)
        if as_json:
            _emit_json([diag])
        else:
            print(render_all([diag], color=color), file=sys.stderr)
        return None, EXIT_PARSE


def _cmd_check(source_path: str, *, color: bool, as_json: bool) -> int:
    model, fail = _parse_with_diagnostics(source_path, color=color, as_json=as_json)
    if fail is not None or model is None:
        return fail if fail is not None else EXIT_PARSE

    errors = validate(model)
    diags = [from_semantic_error(e) for e in errors]

    if as_json:
        _emit_json(diags)
    elif diags:
        print(render_all(diags, color=color), file=sys.stderr)

    hard = [d for d in diags if d.severity == "error"]
    if hard:
        if not as_json:
            print(f"\n{len(hard)} error(s) found.", file=sys.stderr)
        return EXIT_SEMANTIC

    if not as_json:
        pipelines = len(model.pipelines)
        brokers = len(model.brokers)
        warnings = sum(1 for d in diags if d.severity == "warning")
        warn_part = f", {warnings} warning(s)" if warnings else ""
        print(f"OK -- {brokers} broker(s), {pipelines} pipeline(s){warn_part}")
    return EXIT_OK


def _cmd_generate(source_path: str, output_dir: str, *, color: bool) -> int:
    model, fail = _parse_with_diagnostics(source_path, color=color)
    if fail is not None or model is None:
        return fail if fail is not None else EXIT_PARSE

    try:
        validate_or_raise(model)
    except SemanticValidationError as exc:
        diags = [from_semantic_error(e) for e in exc.errors]
        print(render_all(diags, color=color), file=sys.stderr)
        return EXIT_SEMANTIC

    try:
        out = generate(model, output_dir)
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_IO

    print(f"Generated application in: {out}/")
    return EXIT_OK


def _cmd_explain(source_path: str, *, color: bool) -> int:
    model, fail = _parse_with_diagnostics(source_path, color=color)
    if fail is not None or model is None:
        return fail if fail is not None else EXIT_PARSE
    print(explain_model(model))
    return EXIT_OK


def _cmd_visualize(source_path: str, fmt: str, output: str | None, *, color: bool) -> int:
    model, fail = _parse_with_diagnostics(source_path, color=color)
    if fail is not None or model is None:
        return fail if fail is not None else EXIT_PARSE

    result = visualize_mermaid(model) if fmt == "mermaid" else visualize_dot(model)

    if output:
        try:
            with open(output, "w", encoding="utf-8") as fh:
                fh.write(result)
        except OSError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_IO
        print(f"Diagram written to: {output}")
    else:
        print(result)
    return EXIT_OK


def _emit_json(diags: list[Diagnostic]) -> None:
    payload = [
        {
            "severity": d.severity,
            "message": d.message,
            "code": d.code,
            "file": d.file,
            "line": d.line,
            "column": d.column,
            "end_line": d.end_line,
            "end_column": d.end_column,
            "suggestion": d.suggestion,
        }
        for d in diags
    ]
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    sys.exit(main())
