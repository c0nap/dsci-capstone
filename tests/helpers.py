import importlib.util
import pytest
from typing import Any

def optional_param(name: str, package: str) -> Any:  # ParameterSet is internal to PyTest
    """Return a pytest.param that is skipped if the given package is missing.
    @param name  The fixture name to include in the parameter list.
    @param package  The name of a Python package to check for.
    @return  PyTest parameter with the skip flag set if package is not installed."""
    exists = importlib.util.find_spec(package) is not None
    return pytest.param(name, marks=pytest.mark.skipif(not exists, reason=f"{package} not installed"))
