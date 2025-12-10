import pytest
from typing import Any

def pytest_addoption(parser: Any) -> None:
    """Add command-line flags for pytest."""
    parser.addoption("--log-success", action="store_true", default=False)
    parser.addoption("--no-log-colors", action="store_false", default=True)
