"""Source-aware diagnostic rendering for the Sigil CLI.

Produces Rust/Elm-style multi-line error messages with file:line:col
header, source context window and a caret span. Used for both
``TextXSyntaxError`` (parse) and :class:`SemanticError` (semantic) reports.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from .semantics import SemanticError


@dataclass
class Diagnostic:
    severity: str
    message: str
    file: str | None = None
    line: int | None = None
    column: int | None = None
    end_line: int | None = None
    end_column: int | None = None
    code: str | None = None
    suggestion: str | None = None
    notes: list[str] = field(default_factory=list)


def from_semantic_error(err: SemanticError) -> Diagnostic:
    return Diagnostic(
        severity=err.severity,
        message=err.message,
        file=err.file,
        line=err.line,
        column=err.column,
        end_line=err.end_line,
        end_column=err.end_column,
        code=err.code,
        suggestion=err.suggestion,
    )


def from_textx_error(exc, source_path: str | None = None) -> Diagnostic:
    return Diagnostic(
        severity="error",
        message=_clean_textx_message(getattr(exc, "message", str(exc)) or str(exc)),
        file=getattr(exc, "filename", None) or source_path,
        line=getattr(exc, "line", None),
        column=getattr(exc, "col", None),
        code="E0000",
    )


def _clean_textx_message(message: str) -> str:
    if " => '" in message and message.endswith("'"):
        message = message.split(" => '")[0]
    if message.startswith("Expected "):
        message = "syntax error: " + message[0].lower() + message[1:]
    return message


def render(diag: Diagnostic, *, color: bool = False, source_text: str | None = None) -> str:
    """Render a single diagnostic as multi-line text with source context."""
    sev = diag.severity.lower()
    paint = _palette(color)
    sev_color = paint["red"] if sev == "error" else paint["yellow"]
    head_label = f"{sev}"
    if diag.code:
        head_label = f"{sev}[{diag.code}]"
    header = (
        f"{sev_color}{head_label}{paint['reset']}: {paint['bold']}{diag.message}{paint['reset']}"
    )

    location = ""
    if diag.file or diag.line:
        file_disp = _short_path(diag.file) if diag.file else "<unknown>"
        line_disp = diag.line if diag.line is not None else "?"
        col_disp = diag.column if diag.column is not None else "?"
        location = f"\n  {paint['blue']}-->{paint['reset']} {file_disp}:{line_disp}:{col_disp}"

    snippet = _render_snippet(diag, paint=paint, source_text=source_text)

    hint = ""
    if diag.suggestion:
        hint = f"\n  {paint['cyan']}= hint:{paint['reset']} {diag.suggestion}"

    notes = ""
    for note in diag.notes:
        notes += f"\n  {paint['cyan']}= note:{paint['reset']} {note}"

    return header + location + snippet + hint + notes


def _render_snippet(diag: Diagnostic, *, paint: dict, source_text: str | None) -> str:
    if diag.line is None:
        return ""
    text = source_text
    if text is None and diag.file:
        try:
            with open(diag.file, encoding="utf-8") as fh:
                text = fh.read()
        except OSError:
            return ""
    if text is None:
        return ""

    lines = text.splitlines()
    line_idx = diag.line - 1
    if not (0 <= line_idx < len(lines)):
        return ""

    gutter_width = len(str(diag.line))
    bar = f"{paint['blue']}|{paint['reset']}"
    pad = " " * gutter_width

    src_line = lines[line_idx]
    out = f"\n  {pad} {bar}"
    out += f"\n  {paint['blue']}{diag.line:>{gutter_width}}{paint['reset']} {bar} {src_line}"

    col = max(diag.column or 1, 1)
    span = max((diag.end_column or col) - col, 1) if diag.end_line == diag.line else 1
    span = max(span, 1)
    caret_pad = " " * (col - 1)
    caret = paint["red"] + ("^" * span) + paint["reset"]
    out += f"\n  {pad} {bar} {caret_pad}{caret}"
    return out


def render_all(diags: list[Diagnostic], *, color: bool = False) -> str:
    return "\n\n".join(render(d, color=color) for d in diags)


def _short_path(path: str) -> str:
    try:
        rel = os.path.relpath(path)
        return rel if not rel.startswith("..") else path
    except ValueError:
        return path


def _palette(color: bool) -> dict:
    if not color:
        return {k: "" for k in ("red", "yellow", "blue", "cyan", "bold", "reset")}
    return {
        "red": "\033[31m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "cyan": "\033[36m",
        "bold": "\033[1m",
        "reset": "\033[0m",
    }
