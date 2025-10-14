import pytest
import sys
import os
import time
from src.setup import Session
from src.util import Log


@pytest.fixture(scope="session")
def session():
    """Fixture to create session."""
    session = Session()
    relational_db = session.relational_db
    yield session
    session.reset()


# ------------------------------------------------------------------------------
# DATABASE FIXTURES: Checkpoint the database connector instances from Session.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def relational_db(session):
    """Fixture to get relational database connection."""
    relational_db = session.relational_db
    saved_verbose = relational_db.verbose
    relational_db.verbose = True
    yield relational_db
    relational_db.verbose = saved_verbose  # Restore verbose afterwards

@pytest.fixture(scope="session")
def docs_db(session):
    """Fixture to get document database connection."""
    docs_db = session.docs_db
    saved_verbose = docs_db.verbose
    docs_db.verbose = True
    yield docs_db
    docs_db.verbose = saved_verbose  # Restore verbose afterwards

# @pytest.fixture(scope="session")
# def graph_db(session):
#     """Fixture to get document database connection."""
#     graph_db = session.graph_db
#     saved_verbose = graph_db.verbose
#     graph_db.verbose = True
#     yield graph_db
#     graph_db.verbose = saved_verbose  # Restore verbose afterwards



# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run check_connection() for minimal connection test.
# ------------------------------------------------------------------------------
@pytest.mark.order(1)
def test_db_relational_minimal(relational_db):
    """Tests if the RelationalConnector has a valid connection string."""
    assert relational_db.check_connection(log_source=Log.pytest_db, raise_error=True), "Minimal connection test on relational database failed."

@pytest.mark.order(1)
def test_db_docs_minimal(docs_db):
    """Tests if the DocumentConnector has a valid connection string."""
    assert docs_db.check_connection(log_source=Log.pytest_db, raise_error=True), "Minimal connection test on document database failed."

# @pytest.mark.order(1)
# def test_db_graph_minimal(graph_db):
#     """Tests if the GraphConnector has a valid connection string."""
#     assert graph_db.check_connection(log_source=Log.pytest_db, raise_error=True), "Minimal connection test on graph database failed."


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run test_connection() for comprehensive usage tests.
# ------------------------------------------------------------------------------
@pytest.mark.order(2)
def test_db_relational_comprehensive(relational_db):
    """Tests if the GraphConnector is working as intended."""
    assert relational_db.test_connection(), "Comprehensive connection test on relational database failed."

@pytest.mark.order(2)
def test_db_docs_comprehensive(docs_db):
    """Tests if the GraphConnector is working as intended."""
    assert docs_db.test_connection(), "Comprehensive connection test on document database failed."

# @pytest.mark.order(2)
# def test_db_graph_comprehensive(graph_db):
#     """Tests if the GraphConnector is working as intended."""
#     assert graph_db.test_connection(), "Comprehensive connection test on graph database failed."


# ------------------------------------------------------------------------------
# DATABASE FILE TESTS: Run execute_file with example scripts.
# ------------------------------------------------------------------------------
@pytest.mark.order(3)
def test_sql_examples(relational_db):
    """Run queries from test files."""
    if relational_db.db_type == "MYSQL":
        _test_sql_file(relational_db, "./db/tables_mysql.sql", expect_df=False)
    elif relational_db.db_type == "POSTGRES":
        _test_sql_file(relational_db, "./db/tables_postgres.sql", expect_df=False)
    else:
        raise Exception(f"Unknown database engine '{relational_db.db_type}'")

    _test_sql_file(
        relational_db,
        "./db/example1.sql",
        expect_df=True,
        df_header="EntityName table:",
    )
    _test_sql_file(
        relational_db,
        "./db/example2.sql",
        expect_df=True,
        df_header="ExampleEAV table:",
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
def _test_sql_file(relational_db, filename: str, expect_df: bool, df_header: str = ""):
    """Run queries from a local file through the database.
    @param relational_db  Fixture corresponding to the current session's relational database.
    @param filename  The name of a .sql file.
    @param expect_df  Whether to throw an error if the queries fail to return a DataFrame.
    @param df_header  (Optional) A string to print before displaying the DataFrame."""
    try:
        df = relational_db.execute_file(filename)
        if expect_df:
            assert (
                df is not None
            ), f"Execution of '{filename}' failed to produce results."
            if df_header:
                print(df_header)
                print(df)
    except Exception as e:
        Log.fail(Log.pytest_db + Log.run_f, Log.msg_bad_exec_f(filename), raise_error=True, other_error=e)
