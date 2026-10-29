"""Runs only on a free-threaded interpreter (the python-free-threaded CI job)."""

import sys
import sysconfig

import pytest


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="needs a free-threaded interpreter",
)
def test_import_reenables_the_gil():
    import object_storage_client  # noqa: F401

    assert sys._is_gil_enabled()
