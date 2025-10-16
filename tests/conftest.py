import pytest
from src.setup import Session
from src.util import Log


# Command-line flags for pytest
# Usage: pytest --log-success --no-log-colors .
def pytest_addoption(parser):
    parser.addoption("--log-success", action="store_true", default=False)
    parser.addoption("--no-log-colors", action="store_false", default=True)


@pytest.fixture(scope="session")
def session(request):
    """Fixture to create session."""
    # Parse control args
    verbose = request.config.getoption("--log-success")
    Log.USE_COLORS = request.config.getoption("--no-log-colors")
    _session = Session(verbose)
    yield _session
    _session.reset()