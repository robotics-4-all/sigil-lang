import argparse
import sys

from .language import parse
from .semantics import validate, validate_or_raise, SemanticValidationError
from .codegen import generate
from .explain import explain_model
from .visualize import visualize_dot, visualize_mermaid


def main():
    ap = argparse.ArgumentParser(
        prog="sigil",
        description="Sigil — stream-processing pipeline DSL compiler",
    )
    sub = ap.add_subparsers(dest="command")

    p_check = sub.add_parser("check", help="Parse and validate a .sigil file")
    p_check.add_argument("source", help="Path to .sigil source file")

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

    args = ap.parse_args()
    if not args.command:
        ap.print_help()
        sys.exit(1)

    if args.command == "check":
        _cmd_check(args.source)
    elif args.command == "generate":
        _cmd_generate(args.source, args.output)
    elif args.command == "explain":
        _cmd_explain(args.source)
    elif args.command == "visualize":
        _cmd_visualize(args.source, args.format, args.output)


def _cmd_check(source_path: str):
    try:
        model = parse(source_path)
    except Exception as exc:
        print(f"Parse error: {exc}", file=sys.stderr)
        sys.exit(1)

    errors = validate(model)
    warnings = [e for e in errors if e.severity == "warning"]
    hard = [e for e in errors if e.severity == "error"]

    for w in warnings:
        print(f"  {w}")
    for e in hard:
        print(f"  {e}")

    if hard:
        print(f"\n{len(hard)} error(s) found.", file=sys.stderr)
        sys.exit(1)

    pipelines = len(model.pipelines)
    brokers = len(model.brokers)
    print(f"OK — {brokers} broker(s), {pipelines} pipeline(s)")


def _cmd_generate(source_path: str, output_dir: str):
    try:
        model = parse(source_path)
    except Exception as exc:
        print(f"Parse error: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        validate_or_raise(model)
    except SemanticValidationError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    out = generate(model, output_dir)
    print(f"Generated application in: {out}/")


def _cmd_explain(source_path: str):
    try:
        model = parse(source_path)
    except Exception as exc:
        print(f"Parse error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(explain_model(model))


def _cmd_visualize(source_path: str, fmt: str, output: str | None):
    try:
        model = parse(source_path)
    except Exception as exc:
        print(f"Parse error: {exc}", file=sys.stderr)
        sys.exit(1)

    if fmt == "mermaid":
        result = visualize_mermaid(model)
    else:
        result = visualize_dot(model)

    if output:
        with open(output, "w") as f:
            f.write(result)
        print(f"Diagram written to: {output}")
    else:
        print(result)


if __name__ == "__main__":
    main()
