"""The extension refuses to load outside the main interpreter."""

import textwrap

import pytest

interpreters = pytest.importorskip("_interpreters")


def _legacy_config():
    try:
        return interpreters.new_config("legacy")
    except (AttributeError, ValueError):
        pytest.skip("legacy subinterpreters are unavailable")


def test_import_in_legacy_subinterpreter_raises_import_error():
    interp = interpreters.create(_legacy_config())
    try:
        script = textwrap.dedent(
            """
            try:
                import object_storage_client
            except ImportError as e:
                assert "main interpreter" in str(e), e
            else:
                raise AssertionError("import succeeded in a subinterpreter")
            """
        )
        assert interpreters.exec(interp, script) is None
    finally:
        interpreters.destroy(interp)


async def test_main_interpreter_keeps_working_around_the_attempt(tmp_url, plain):
    url = f"{tmp_url}/a.txt"
    await plain.put_object(url, b"hi")
    interp = interpreters.create(_legacy_config())
    try:
        interpreters.exec(
            interp, "try:\n import object_storage_client\nexcept ImportError: pass"
        )
    finally:
        interpreters.destroy(interp)
    assert await plain.get_object(url) == b"hi"
