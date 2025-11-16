import pytest
from src.core.context import get_session, Session
from src.util import Log
from src.connectors.relational import RelationalConnector
from src.connectors.document import DocumentConnector
from src.connectors.graph import GraphConnector


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
    _session = get_session()
    _session.verbose = verbose
    yield _session



# ------------------------------------------------------------------------------
# DATABASE FIXTURES: Checkpoint the database connector instances from Session.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def relational_db(session: Session) -> RelationalConnector:
    """Fixture to get relational database connection."""
    _relational_db = session.relational_db
    if _relational_db.database_exists("pytest"):
    	_relational_db.drop_database("pytest")
    with _relational_db.temp_database("pytest"):
        yield _relational_db


@pytest.fixture(scope="module")
def docs_db(session: Session) -> DocumentConnector:
    """Fixture to get document database connection."""
    _docs_db = session.docs_db
    if _docs_db.database_exists("pytest"):
    	_docs_db.drop_database("pytest")
    with _docs_db.temp_database("pytest"):
        yield _docs_db


@pytest.fixture(scope="module")
def graph_db(session: Session) -> GraphConnector:
    """Fixture to get document database connection."""
    _graph_db = session.graph_db
    if _graph_db.database_exists("pytest"):
    	_graph_db.drop_database("pytest")
    with _graph_db.temp_database("pytest"):
        yield _graph_db

