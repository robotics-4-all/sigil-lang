"""Sigil compiler HTTP service.

Exposes model validation and M2T generation over HTTP so that
external platforms (e.g. LCP) can use sigil-lang without importing
it as a library.

Endpoints:
    GET  /health     - liveness probe
    POST /validate   - parse + semantic validation -> diagnostics
    POST /generate   - parse + validate + M2T generation -> file artifacts

Notes:
    - Single-source strings only. ``parse_string`` does not resolve
      ``import "..."`` (file-based ``parse`` does that via the filesystem).
      Sources containing imports are rejected with IMPORTS_UNSUPPORTED.
    - ``SemanticError`` carries only message+severity, so diagnostics
      have no line/column locations (null).
    - ``generate`` writes files to a temp dir, then returns every file
      base64-encoded so callers without shared storage can persist them.
"""

import base64
import os
import re
import tempfile

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .codegen import generate
from .language import parse_string
from .semantics import SemanticValidationError, validate, validate_or_raise

SERVICE_VERSION = "1.0.0"
MAX_GENERATED_BYTES = 2 * 1024 * 1024  # total cap for all artifacts

_IMPORT_RE = re.compile(r'^\s*import\s+"', re.MULTILINE)

_CONTENT_TYPES = {
    ".py": "text/x-python",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".json": "application/json",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
}

app = FastAPI(title="Sigil Compiler Service", version=SERVICE_VERSION)


class ValidateRequest(BaseModel):
    source: str = Field(..., min_length=1)

    class Config:
        extra = "ignore"


class Diagnostic(BaseModel):
    severity: str
    message: str
    code: str | None = None
    line: int | None = None
    column: int | None = None


class ValidateResponse(BaseModel):
    valid: bool
    diagnostics: list[Diagnostic] = Field(default_factory=list)
    summary: str


class GenerateRequest(BaseModel):
    source: str = Field(..., min_length=1)
    name: str | None = None

    class Config:
        extra = "ignore"


class Artifact(BaseModel):
    name: str
    kind: str
    size_bytes: int
    content_type: str
    content_base64: str


class GenerateResponse(BaseModel):
    artifacts: list[Artifact]
    metadata: dict = Field(default_factory=dict)
    summary: str


class ErrorResponse(BaseModel):
    error: str
    code: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "sigil-lang", "version": SERVICE_VERSION}


def _parse_error_diagnostic(exc: Exception) -> Diagnostic:
    return Diagnostic(
        severity="error",
        message=f"Parse error: {exc}",
        code="PARSE_ERROR",
    )


@app.post("/validate", response_model=ValidateResponse)
def validate_source(req: ValidateRequest) -> ValidateResponse:
    if _IMPORT_RE.search(req.source):
        diag = Diagnostic(
            severity="error",
            message=(
                "import statements require file access and are not supported "
                "via API v1; inline all definitions."
            ),
            code="IMPORTS_UNSUPPORTED",
        )
        return ValidateResponse(
            valid=False, diagnostics=[diag], summary="Validation failed: imports unsupported via API."
        )
    try:
        model = parse_string(req.source)
    except Exception as exc:  # textX syntax errors, circular imports, etc.
        diag = _parse_error_diagnostic(exc)
        return ValidateResponse(valid=False, diagnostics=[diag], summary="Parse failed.")

    errors = validate(model)
    hard = [e for e in errors if e.severity == "error"]
    warnings = [e for e in errors if e.severity != "error"]
    diagnostics = [
        Diagnostic(severity=e.severity, message=e.message) for e in errors
    ]
    valid = len(hard) == 0
    n_brokers = len(getattr(model, "brokers", []) or [])
    n_pipelines = len(getattr(model, "pipelines", []) or [])
    if valid:
        summary = (
            f"Validation passed: {n_brokers} broker(s), {n_pipelines} pipeline(s), "
            f"{len(warnings)} warning(s)."
        )
    else:
        summary = (
            f"Validation failed with {len(hard)} error(s), {len(warnings)} warning(s)."
        )
    return ValidateResponse(valid=valid, diagnostics=diagnostics, summary=summary)


def _artifact_kind(name: str) -> str:
    base = os.path.basename(name)
    if base == "requirements.txt":
        return "manifest"
    return "source"


def _content_type(name: str) -> str:
    _, ext = os.path.splitext(name)
    return _CONTENT_TYPES.get(ext.lower(), "application/octet-stream")


@app.post("/generate", response_model=GenerateResponse, responses={422: {"model": ErrorResponse}})
def generate_source(req: GenerateRequest):
    from fastapi.responses import JSONResponse

    if _IMPORT_RE.search(req.source):
        return JSONResponse(
            status_code=422,
            content={
                "error": "import statements require file access and are not supported via API v1.",
                "code": "IMPORTS_UNSUPPORTED",
            },
        )
    try:
        model = parse_string(req.source)
    except Exception as exc:
        return JSONResponse(
            status_code=422,
            content={"error": f"Parse error: {exc}", "code": "PARSE_ERROR"},
        )
    try:
        validate_or_raise(model)
    except SemanticValidationError as exc:
        return JSONResponse(
            status_code=422,
            content={"error": str(exc), "code": "SEMANTIC_ERROR"},
        )

    with tempfile.TemporaryDirectory() as tmp:
        generate(model, tmp)
        artifacts: list[Artifact] = []
        total = 0
        for root, _dirs, files in os.walk(tmp):
            # Skip caches if any backend ever emits them.
            if "__pycache__" in root:
                continue
            for fname in sorted(files):
                if fname.endswith((".pyc", ".pyo")):
                    continue
                full = os.path.join(root, fname)
                rel = os.path.relpath(full, tmp).replace(os.sep, "/")
                with open(full, "rb") as f:
                    data = f.read()
                total += len(data)
                if total > MAX_GENERATED_BYTES:
                    return JSONResponse(
                        status_code=413,
                        content={
                            "error": f"Generated output exceeds {MAX_GENERATED_BYTES} bytes.",
                            "code": "OUTPUT_TOO_LARGE",
                        },
                    )
                artifacts.append(
                    Artifact(
                        name=rel,
                        kind=_artifact_kind(rel),
                        size_bytes=len(data),
                        content_type=_content_type(rel),
                        content_base64=base64.b64encode(data).decode("ascii"),
                    )
                )

    n_pipelines = len(getattr(model, "pipelines", []) or [])
    n_brokers = len(getattr(model, "brokers", []) or [])
    return GenerateResponse(
        artifacts=artifacts,
        metadata={
            "pipelines": n_pipelines,
            "brokers": n_brokers,
            "generator": "sigil-lang",
            "version": SERVICE_VERSION,
        },
        summary=f"Generated {len(artifacts)} file(s) for {n_pipelines} pipeline(s).",
    )
