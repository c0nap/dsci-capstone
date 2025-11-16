from src.connectors.relational import RelationalConnector
from src.connectors.document import DocumentConnector
from src.connectors.graph import GraphConnector
import pytest
from src.util import Log


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run check_connection() for minimal connection test.
# ------------------------------------------------------------------------------
@pytest.mark.relational
@pytest.mark.order(1)
@pytest.mark.dependency(name="rel_minimal")
def test_db_relational_minimal(relational_db: RelationalConnector) -> None:
    """Tests if the RelationalConnector has a valid connection string."""
    connected = relational_db.check_connection(log_source=Log.pytest_db, raise_error=True)
    assert connected


@pytest.mark.document
@pytest.mark.order(2)
@pytest.mark.dependency(name="docs_minimal")
def test_db_docs_minimal(docs_db: DocumentConnector) -> None:
    """Tests if the DocumentConnector has a valid connection string."""
    connected = docs_db.check_connection(log_source=Log.pytest_db, raise_error=True)
    assert connected


@pytest.mark.graph
@pytest.mark.kg
@pytest.mark.order(3)
@pytest.mark.dependency(name="graph_minimal")
def test_db_graph_minimal(graph_db: GraphConnector) -> None:
    """Tests if the GraphConnector has a valid connection string."""
    connected = graph_db.check_connection(log_source=Log.pytest_db, raise_error=True)
    assert connected


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run test_operations() for comprehensive usage tests.
# ------------------------------------------------------------------------------
@pytest.mark.relational
@pytest.mark.order(4)
@pytest.mark.dependency(name="rel_comprehensive", depends=["rel_minimal"])
def test_db_relational_comprehensive(relational_db: RelationalConnector) -> None:
    """Tests if the GraphConnector is working as intended."""
    operational = relational_db.test_operations(raise_error=True)
    assert operational


@pytest.mark.document
@pytest.mark.order(5)
@pytest.mark.dependency(name="docs_comprehensive", depends=["docs_minimal"])
def test_db_docs_comprehensive(docs_db: DocumentConnector) -> None:
    """Tests if the GraphConnector is working as intended."""
    operational = docs_db.test_operations(raise_error=True)
    assert operational


@pytest.mark.graph
@pytest.mark.kg
@pytest.mark.order(6)
@pytest.mark.dependency(name="graph_comprehensive", depends=["graph_minimal"])
def test_db_graph_comprehensive(graph_db: GraphConnector) -> None:
    """Tests if the GraphConnector is working as intended."""
    operational = graph_db.test_operations(raise_error=True)
    assert operational

