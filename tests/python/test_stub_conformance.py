"""The type stub must describe what the module really provides."""

import ast
import inspect
from pathlib import Path

import pytest

import object_storage_client as module

STUB = Path(module.__file__).with_name("__init__.pyi")
TREE = ast.parse(STUB.read_text())


def _stub_classes() -> list[ast.ClassDef]:
    return [n for n in TREE.body if isinstance(n, ast.ClassDef)]


def _typeddict_names() -> set[str]:
    """Stub classes that derive (directly or not) from TypedDict."""
    names = {"TypedDict"}
    for node in _stub_classes():  # bases are declared before their subclasses
        if any(isinstance(b, ast.Name) and b.id in names for b in node.bases):
            names.add(node.name)
    return names - {"TypedDict"}


def _is_typeddict(node: ast.ClassDef) -> bool:
    return node.name in _typeddict_names()


def _members(node: ast.ClassDef) -> list[str]:
    """Methods and properties of a stub class (annotated fields excluded)."""
    return [
        n.name
        for n in node.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _fields(node: ast.ClassDef) -> set[str]:
    """Annotated fields of a stub TypedDict, including inherited ones."""
    out = {
        n.target.id
        for n in node.body
        if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name)
    }
    by_name = {c.name: c for c in _stub_classes()}
    for b in node.bases:
        if isinstance(b, ast.Name) and b.id in by_name:
            out |= _fields(by_name[b.id])
    return out


def _params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[tuple[str, str]]:
    """(name, kind) of the stub function's parameters, without self."""
    a = fn.args
    out = [(p.arg, "POSITIONAL_OR_KEYWORD") for p in a.posonlyargs + a.args]
    if a.vararg:
        out.append((a.vararg.arg, "VAR_POSITIONAL"))
    out += [(p.arg, "KEYWORD_ONLY") for p in a.kwonlyargs]
    if a.kwarg:
        out.append((a.kwarg.arg, "VAR_KEYWORD"))
    if out and out[0][0] in ("self", "cls"):
        out = out[1:]
    return out


def _runtime_params(sig: inspect.Signature) -> list[tuple[str, str]]:
    out = [(n, p.kind.name) for n, p in sig.parameters.items()]
    if out and out[0][0] in ("self", "cls"):
        out = out[1:]
    return out


def test_every_stub_name_exists_at_runtime():
    missing = []
    for node in TREE.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not hasattr(module, node.name):
                missing.append(node.name)
        elif isinstance(node, ast.ClassDef):
            cls = getattr(module, node.name, None)
            if cls is None:
                missing.append(node.name)
                continue
            if not _is_typeddict(node):
                missing += [
                    f"{node.name}.{m}" for m in _members(node) if not hasattr(cls, m)
                ]
    assert missing == []


def test_stub_has_classes_to_check():
    """Guards the conformance tests against an empty or misparsed stub."""
    names = {n.name for n in _stub_classes()}
    assert {"KeyringStatus", "KeyInfo", "ObjectStorageClient", "Keyring"} <= names


@pytest.mark.parametrize(
    "node", [n for n in _stub_classes() if _is_typeddict(n)], ids=lambda n: n.name
)
def test_typeddict_fields_match(node):
    cls = getattr(module, node.name)
    assert set(cls.__required_keys__) | set(cls.__optional_keys__) == _fields(node)


def _runtime_signature(node: ast.ClassDef, fn) -> inspect.Signature | None:
    """The runtime signature of a stub function, or None where Python has none
    (properties, slot wrappers)."""
    cls = getattr(module, node.name)
    target = cls if fn.name == "__init__" else inspect.getattr_static(cls, fn.name)
    if isinstance(target, (staticmethod, classmethod)):
        target = getattr(cls, fn.name)
    try:
        return inspect.signature(target)
    except (TypeError, ValueError):
        return None


def _signature_cases():
    for node in _stub_classes():
        if _is_typeddict(node):
            continue
        for fn in node.body:
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                sig = _runtime_signature(node, fn)
                if sig is not None:
                    yield pytest.param(sig, fn, id=f"{node.name}.{fn.name}")


@pytest.mark.parametrize(("sig", "fn"), list(_signature_cases()))
def test_signatures_match(sig, fn):
    assert _runtime_params(sig) == _params(fn)


def test_signature_cases_cover_the_client_methods():
    ids = {c.id for c in _signature_cases()}
    assert {"ObjectStorageClient.__init__", "ObjectStorageClient.get_object"} <= ids


def test_all_is_in_stub():
    defined = {n.name for n in TREE.body if isinstance(n, (ast.ClassDef, ast.FunctionDef))}
    imported = {
        a.asname or a.name
        for n in TREE.body
        if isinstance(n, (ast.Import, ast.ImportFrom))
        for a in n.names
    }
    assert set(module.__all__) - defined - imported == set()
