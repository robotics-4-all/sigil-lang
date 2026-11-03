"""API tests: validate + generate contract for external consumers (LCP)."""

import base64

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
    bad = _simple().replace("|> window tumbling(5 m)", "")
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
    bad = _simple().replace("|> window tumbling(5 m)", "")
    r = client.post("/generate", json={"source": bad})
    assert r.status_code == 422
    assert r.json()["code"] == "SEMANTIC_ERROR"


def test_generate_parse_error_is_422():
    r = client.post("/generate", json={"source": "pipeline broken |> filter"})
    assert r.status_code == 422
    assert r.json()["code"] == "PARSE_ERROR"
