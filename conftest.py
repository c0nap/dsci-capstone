import importlib.util
import pytest
from src.components.fact_storage import KnowledgeGraph
from src.connectors.document import DocumentConnector
from src.connectors.graph import GraphConnector
from src.connectors.relational import RelationalConnector
from src.core.context import get_session, Session
from src.util import Log
from typing import Any, Generator


def pytest_addoption(parser: Any) -> None:
    """Command-line flags for pytest
    @details  Usage: pytest --log-success --no-log-colors"""
    parser.addoption("--log-success", action="store_true", default=False)
    parser.addoption("--no-log-colors", action="store_false", default=True)


def optional_param(name: str, package: str) -> Any:  # ParameterSet is internal to PyTest
    """Return a pytest.param that is skipped if the given package is missing.
    @param name  The fixture name to include in the parameter list.
    @param package  The name of a Python package to check for.
    @return  PyTest parameter with the skip flag set if package is not installed."""
    exists = importlib.util.find_spec(package) is not None
    return pytest.param(name, marks=pytest.mark.skipif(not exists, reason=f"{package} not installed"))


@pytest.fixture(scope="session", autouse=True)
def session(request: pytest.FixtureRequest) -> Generator[Session, None, None]:
    """Fixture to create session.
    @details
    - autouse=True ensures Session.setup() runs once for the whole suite.
    - See smoke test for pipeline_E: helper function imports session, but the test doesn't ask for it."""
    # Parse control args
    verbose = request.config.getoption("--log-success")
    Log.USE_COLORS = request.config.getoption("--no-log-colors")
    _session = get_session(verbose=verbose)
    _session.setup()
    yield _session


# ------------------------------------------------------------------------------
# DATABASE FIXTURES: Checkpoint the database connector instances from Session.
# ------------------------------------------------------------------------------
@pytest.fixture
def relational_db(session: Session) -> Generator[RelationalConnector, None, None]:
    """Fixture to get relational database connection."""
    database_name = "pytest"
    _relational_db = session.relational_db
    if _relational_db.database_exists(database_name):
        _relational_db.drop_database(database_name)
    with _relational_db.temp_database(database_name):
        yield _relational_db


@pytest.fixture
def docs_db(session: Session) -> Generator[DocumentConnector, None, None]:
    """Fixture to get document database connection."""
    database_name = "pytest"
    _docs_db = session.docs_db
    if _docs_db.database_exists(database_name):
        _docs_db.drop_database(database_name)
    with _docs_db.temp_database(database_name):
        yield _docs_db


@pytest.fixture
def graph_db(session: Session) -> Generator[GraphConnector, None, None]:
    """Fixture to get document database connection."""
    database_name = "pytest"
    _graph_db = session.graph_db
    if _graph_db.database_exists(database_name):
        _graph_db.drop_database(database_name)
    with _graph_db.temp_database(database_name):
        yield _graph_db


@pytest.fixture(params=["empty_graph"])
def main_graph(request: pytest.FixtureRequest, graph_db: GraphConnector, session: Session) -> Generator[KnowledgeGraph, None, None]:
    """Fixture to get document database connection."""
    graph_name = request.param
    _main_graph = session.main_graph
    _main_graph.graph_name = graph_name
    if graph_db.graph_exists(graph_name):
        graph_db.drop_graph(graph_name)
    with graph_db.temp_graph(graph_name):
        yield _main_graph
