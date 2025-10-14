import pytest
import sys
import os
import time
from src.setup import Session
from src.util import Log

# ------------------------------------------------------------------------------
# DATABASE FIXTURES: Checkpoint the database connector instances from Session.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def relational_db(session):
    """Fixture to get relational database connection."""
    _relational_db = session.relational_db
    yield _relational_db

@pytest.fixture(scope="session")
def docs_db(session):
    """Fixture to get document database connection."""
    _docs_db = session.docs_db
    yield _docs_db

@pytest.fixture(scope="session")
def graph_db(session):
    """Fixture to get document database connection."""
    _graph_db = session.graph_db
    yield _graph_db



# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run check_connection() for minimal connection test.
# ------------------------------------------------------------------------------
@pytest.mark.order(1)
def test_db_relational_minimal(relational_db):
    """Tests if the RelationalConnector has a valid connection string."""
    assert relational_db.check_connection(log_source=Log.pytest_db, raise_error=True), "Minimal connection test on relational database failed."

@pytest.mark.order(2)
def test_db_docs_minimal(docs_db):
    """Tests if the DocumentConnector has a valid connection string."""
    assert docs_db.check_connection(log_source=Log.pytest_db, raise_error=True), "Minimal connection test on document database failed."

@pytest.mark.order(3)
def test_db_graph_minimal(graph_db):
    """Tests if the GraphConnector has a valid connection string."""
    assert graph_db.check_connection(log_source=Log.pytest_db, raise_error=True), "Minimal connection test on graph database failed."


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run test_connection() for comprehensive usage tests.
# ------------------------------------------------------------------------------
@pytest.mark.order(4)
def test_db_relational_comprehensive(relational_db):
    """Tests if the GraphConnector is working as intended."""
    assert relational_db.test_connection(), "Comprehensive connection test on relational database failed."

@pytest.mark.order(5)
def test_db_docs_comprehensive(docs_db):
    """Tests if the GraphConnector is working as intended."""
    assert docs_db.test_connection(), "Comprehensive connection test on document database failed."

@pytest.mark.order(6)
def test_db_graph_comprehensive(graph_db):
    """Tests if the GraphConnector is working as intended."""
    assert graph_db.test_connection(), "Comprehensive connection test on graph database failed."


# ------------------------------------------------------------------------------
# DATABASE FILE TESTS: Run execute_file with example scripts.
# ------------------------------------------------------------------------------
@pytest.mark.order(7)
def test_sql_examples(relational_db):
    """Run queries from test files."""
    if relational_db.db_type == "MYSQL":
        _test_sql_file(relational_db, "./tests/examples-db/rel_postgres_schema.sql")
    elif relational_db.db_type == "POSTGRES":
        _test_sql_file(relational_db, "./tests/examples-db/rel_postgres_schema.sql")
    else:
        raise Exception(f"Unknown database engine '{relational_db.db_type}'")

    _test_sql_file(
        relational_db,
        "./tests/examples-db/relational_df1.sql",
        expect_df=True,
    )
    _test_sql_file(
        relational_db,
        "./tests/examples-db/relational_df2.sql",
        expect_df=True,
    )

    df = relational_db.get_dataframe(
        "EntityName"
    )  # Internal errors are handled by the class itself.
    assert (
        df is not None
    )  # We can just check results since implementation is checked by RelationalConnector.



# ------------------------------------------------------------------------------
# FILE TEST WRAPPERS: Reuse the logic to test multiple files within a single test.
# ------------------------------------------------------------------------------
def _test_sql_file(relational_db, filename: str, expect_df: bool = False):
    """Run queries from a local file through the database.
    @param relational_db  Fixture corresponding to the current session's relational database.
    @param filename  The name of a .sql file.
    @param expect_df  Whether to throw an error if the queries fail to return a DataFrame."""
    try:
        df = relational_db.execute_file(filename)
        if expect_df:
            assert (
                df is not None
            ), f"Execution of '{filename}' failed to produce results."
    except Exception as e:
        Log.fail(Log.pytest_db + Log.run_f, Log.msg_bad_exec_f(filename), raise_error=True, other_error=e)
