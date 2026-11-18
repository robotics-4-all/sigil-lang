"""API tests: validate + generate contract for external consumers (LCP)."""

import base64
import json

from fastapi.testclient import TestClient

from sigil.api import app

client = TestClient(app)


def _simple() -> str:
    with open("examples/simple.sigil") as f:
        return f.read()


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["service"] == "sigil-lang"


def test_validate_ok():
    r = client.post("/validate", json={"source": _simple()})
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is True
    assert body["summary"]


def test_validate_semantic_error():
    bad = _simple().replace("|> WINDOW tumbling(5 m)", "")
    r = client.post("/validate", json={"source": bad})
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is False
    assert any(d["severity"] == "error" for d in body["diagnostics"])


def test_validate_parse_error():
    r = client.post("/validate", json={"source": "pipeline broken |> filter"})
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is False
    assert body["diagnostics"][0]["code"] == "PARSE_ERROR"


def test_validate_imports_rejected():
    r = client.post("/validate", json={"source": 'import "shared/common.sigil"\n' + _simple()})
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is False
    assert body["diagnostics"][0]["code"] == "IMPORTS_UNSUPPORTED"


def test_generate_ok():
    r = client.post("/generate", json={"source": _simple()})
    assert r.status_code == 200
    body = r.json()
    names = [a["name"] for a in body["artifacts"]]
    assert "app.py" in names
    assert "requirements.txt" in names
    assert body["metadata"]["pipelines"] == 1
    app_py = next(a for a in body["artifacts"] if a["name"] == "app.py")
    assert app_py["content_base64"]
    compile(base64.b64decode(app_py["content_base64"]).decode(), "app.py", "exec")


def test_generate_semantic_error_is_422():
    bad = _simple().replace("|> WINDOW tumbling(5 m)", "")
    r = client.post("/generate", json={"source": bad})
    assert r.status_code == 422
    assert r.json()["code"] == "SEMANTIC_ERROR"


def test_generate_parse_error_is_422():
    r = client.post("/generate", json={"source": "pipeline broken |> filter"})
    assert r.status_code == 422
    assert r.json()["code"] == "PARSE_ERROR"


def test_generate_notebook_target():
    r = client.post("/generate", json={"source": _simple(), "target": "notebook"})
    assert r.status_code == 200
    body = r.json()
    assert body["metadata"]["target"] == "notebook"
    assert len(body["artifacts"]) == 1
    artifact = body["artifacts"][0]
    assert artifact["name"] == "notebook.ipynb"
    assert artifact["kind"] == "notebook"
    assert artifact["content_type"] == "application/x-ipynb+json"
    assert artifact["size_bytes"] == len(base64.b64decode(artifact["content_base64"]))
    notebook = json.loads(base64.b64decode(artifact["content_base64"]))
    assert notebook["nbformat"] == 4
    assert notebook["cells"]
    assert "notebook.ipynb" in body["summary"]


def test_generate_notebook_rejects_imports():
    source = 'import "shared/common.sigil"\n' + _simple()
    r = client.post("/generate", json={"source": source, "target": "notebook"})
    assert r.status_code == 422
    assert r.json()["code"] == "IMPORTS_UNSUPPORTED"


def test_generate_unknown_target():
    r = client.post("/generate", json={"source": _simple(), "target": "wheel"})
    assert r.status_code == 422
    assert "artifacts" not in r.json()


def test_generate_app_target_unchanged():
    default = client.post("/generate", json={"source": _simple()})
    explicit = client.post("/generate", json={"source": _simple(), "target": "app"})
    assert default.status_code == 200
    assert explicit.status_code == 200
    default_body = default.json()
    explicit_body = explicit.json()
    assert default_body["artifacts"] == explicit_body["artifacts"]
    assert default_body["metadata"]["target"] == "app"
    assert explicit_body["metadata"]["target"] == "app"
    names = [a["name"] for a in default_body["artifacts"]]
    assert "app.py" in names
    assert "requirements.txt" in names
    assert "notebook.ipynb" not in names
