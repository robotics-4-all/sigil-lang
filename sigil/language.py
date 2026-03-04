import os
from functools import lru_cache
from textx import metamodel_from_file

_GRAMMAR_PATH = os.path.join(os.path.dirname(__file__), "grammar.tx")


@lru_cache(maxsize=1)
def build_metamodel():
    return metamodel_from_file(
        _GRAMMAR_PATH,
        auto_init_attributes=True,
        use_regexp_group=True,
    )


def parse(source_path: str):
    mm = build_metamodel()
    model = mm.model_from_file(source_path)
    _resolve_imports(model, source_path, mm)
    return model


def parse_string(source: str):
    mm = build_metamodel()
    return mm.model_from_str(source)


class CircularImportError(Exception):
    pass


def _resolve_imports(model, source_path: str, mm, _seen: set | None = None):
    """Recursively resolve import declarations, merging brokers/constants/pipelines."""
    if _seen is None:
        _seen = set()

    source_abs = os.path.abspath(source_path)
    if source_abs in _seen:
        raise CircularImportError(
            f"Circular import detected: '{source_path}' is already being imported."
        )
    _seen.add(source_abs)

    base_dir = os.path.dirname(source_abs)
    for imp in model.imports:
        imp_path = os.path.normpath(os.path.join(base_dir, imp.path))
        if not os.path.isfile(imp_path):
            raise FileNotFoundError(
                f"Import error: file '{imp.path}' not found (resolved to '{imp_path}')."
            )
        if os.path.abspath(imp_path) in _seen:
            raise CircularImportError(
                f"Circular import detected: '{imp.path}' (resolved to '{imp_path}')."
            )

        imported = mm.model_from_file(imp_path)
        _resolve_imports(imported, imp_path, mm, _seen)

        for b in imported.brokers:
            model.brokers.append(b)
        for c in imported.constants:
            model.constants.append(c)
        for p in imported.pipelines:
            model.pipelines.append(p)
        for s in imported.schemas:
            model.schemas.append(s)
