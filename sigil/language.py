import os
import re
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
    source_abs = os.path.abspath(source_path)
    if not os.path.isfile(source_abs):
        raise FileNotFoundError(f"Import error: file not found: '{source_path}'.")

    ordered_paths = _topo_sort_imports(source_abs)

    brokers: dict = {}
    schemas: dict = {}
    models_map: dict = {}

    from textx.scoping.providers import PlainName as _PlainName

    _plain = _PlainName()

    def _broker_scope(obj, attr, attr_ref):
        found = brokers.get(attr_ref.obj_name)
        return found if found is not None else _plain(obj, attr, attr_ref)

    def _schema_scope(obj, attr, attr_ref):
        found = schemas.get(attr_ref.obj_name)
        return found if found is not None else _plain(obj, attr, attr_ref)

    def _model_scope(obj, attr, attr_ref):
        found = models_map.get(attr_ref.obj_name)
        return found if found is not None else _plain(obj, attr, attr_ref)

    mm = metamodel_from_file(
        _GRAMMAR_PATH,
        auto_init_attributes=True,
        use_regexp_group=True,
    )
    mm.register_scope_providers(
        {
            "Source.broker": _broker_scope,
            "Sink.broker": _broker_scope,
            "JoinStage.broker": _broker_scope,
            "RouteSink.broker": _broker_scope,
            "OnErrorClause.broker": _broker_scope,
            "Source.schema": _schema_scope,
            "PredictStage.model": _model_scope,
        }
    )

    parsed_models: dict = {}
    for path in ordered_paths:
        model = mm.model_from_file(path)
        parsed_models[path] = model
        for b in model.brokers:
            brokers[b.name] = b
        for s in model.schemas:
            schemas[s.name] = s
        for md in model.models:
            models_map[md.name] = md

    main_model = parsed_models[source_abs]
    _merge_imports(main_model, source_abs, parsed_models)
    return main_model


def parse_string(source: str):
    mm = build_metamodel()
    return mm.model_from_str(source)


class CircularImportError(Exception):
    pass


def _topo_sort_imports(
    abs_path: str,
    visited: set | None = None,
    stack: set | None = None,
) -> list[str]:
    if visited is None:
        visited = set()
    if stack is None:
        stack = set()

    if abs_path in stack:
        raise CircularImportError(
            f"Circular import detected: '{abs_path}' is already being imported."
        )
    if abs_path in visited:
        return []

    stack.add(abs_path)
    result: list[str] = []

    import_paths = _scan_import_paths(abs_path)
    for imp_abs in import_paths:
        result.extend(_topo_sort_imports(imp_abs, visited, stack))

    stack.remove(abs_path)
    visited.add(abs_path)
    result.append(abs_path)
    return result


def _scan_import_paths(abs_path: str) -> list[str]:
    try:
        with open(abs_path) as f:
            content = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Import error: file not found: '{abs_path}'.")

    content_stripped = re.sub(r"//[^\n]*", "", content)
    content_stripped = re.sub(r"/\*.*?\*/", "", content_stripped, flags=re.DOTALL)

    base_dir = os.path.dirname(abs_path)
    result: list[str] = []
    for m in re.finditer(r'import\s+"([^"]+)"', content_stripped):
        imp_rel = m.group(1)
        imp_abs = os.path.normpath(os.path.join(base_dir, imp_rel))
        if not os.path.isfile(imp_abs):
            raise FileNotFoundError(
                f"Import error: file '{imp_rel}' not found (resolved to '{imp_abs}')."
            )
        result.append(imp_abs)
    return result


def _merge_imports(
    main_model,
    main_abs: str,
    parsed_models: dict,
    _merged: set | None = None,
) -> None:
    if _merged is None:
        _merged = set()
    main_import_paths = _scan_import_paths(main_abs)
    for imp_abs in main_import_paths:
        if imp_abs not in parsed_models or imp_abs in _merged:
            continue
        _merged.add(imp_abs)
        imp_model = parsed_models[imp_abs]
        _merge_imports(imp_model, imp_abs, parsed_models, _merged)
        for b in imp_model.brokers:
            main_model.brokers.append(b)
        for c in imp_model.constants:
            main_model.constants.append(c)
        for p in imp_model.pipelines:
            main_model.pipelines.append(p)
        for s in imp_model.schemas:
            main_model.schemas.append(s)
        for md in imp_model.models:
            main_model.models.append(md)
