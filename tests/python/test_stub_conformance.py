"""The type stub must describe what the module really provides."""

import ast
import inspect
from pathlib import Path

import object_storage_client as module
import pytest

STUB = Path(module.__file__).with_name("__init__.pyi")
TREE = ast.parse(STUB.read_text())


def _stub_classes() -> list[ast.ClassDef]:
    return [n for n in TREE.body if isinstance(n, ast.ClassDef)]


def _members(node: ast.ClassDef) -> list[str]:
    """Methods and properties of a stub class (annotated fields excluded)."""
    return [
        n.name
        for n in node.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


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
            missing += [
                f"{node.name}.{m}" for m in _members(node) if not hasattr(cls, m)
            ]
    assert missing == []


def test_stub_has_classes_to_check():
    """Guards the conformance tests against an empty or misparsed stub."""
    names = {n.name for n in _stub_classes()}
    assert {"ObjectStorageClient", "EncryptedObjectStorageClient", "Keyring"} <= names


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


@pytest.mark.parametrize(
    "node",
    [n for n in _stub_classes() if n.name in {"ObjectStorageClient", "EncryptedObjectStorageClient"}],
    ids=lambda n: n.name,
)
def test_client_methods_are_not_coroutines_in_the_stub(node):
    # Calls return an asyncio.Future, which create_task rejects.
    coroutines = [n.name for n in node.body if isinstance(n, ast.AsyncFunctionDef)]
    assert coroutines == []


def test_all_is_in_stub():
    defined = {n.name for n in TREE.body if isinstance(n, (ast.ClassDef, ast.FunctionDef))}
    imported = {
        a.asname or a.name
        for n in TREE.body
        if isinstance(n, (ast.Import, ast.ImportFrom))
        for a in n.names
    }
    assert set(module.__dict__["__all__"]) - defined - imported == set()
