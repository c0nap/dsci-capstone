import pytest
import sys
import os
import time
from typing import List
from src.setup import Session
from src.util import Log

# ------------------------------------------------------------------------------
# DATABASE FIXTURES: Checkpoint the database connector instances from Session.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def relational_db(session):
    """Fixture to get relational database connection."""
    _relational_db = session.relational_db
    yield _relational_db

@pytest.fixture(scope="module")
def docs_db(session):
    """Fixture to get document database connection."""
    _docs_db = session.docs_db
    yield _docs_db

@pytest.fixture(scope="module")
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
@pytest.fixture(scope="module")
def load_examples_relational(relational_db):
    """Fixture to create relational tables using engine-specific syntax."""
    if relational_db.db_type == "MYSQL":
        _test_query_file(relational_db, "./tests/examples-db/rel_postgres_schema.sql", ["sql"])
        yield
        relational_db.execute_query("DROP TABLE EntityName; DROP TABLE ExampleEAV;")
    elif relational_db.db_type == "POSTGRES":
        _test_query_file(relational_db, "./tests/examples-db/rel_postgres_schema.sql", ["sql"])
        yield
        relational_db.execute_query("DROP TABLE entityname; DROP TABLE exampleeav;")
    else:
        raise Exception(f"Unknown database engine '{relational_db.db_type}'")

@pytest.mark.order(7)
def test_sql_example_1(relational_db, load_examples_relational):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected.
    @note  Uses a table-creation fixture to load / unload schema."""
    _test_query_file(
        relational_db,
        "./tests/examples-db/relational_df1.sql",
        valid_files=["sql"],
        expect_df=True,
    )
    df = relational_db.get_dataframe("EntityName")
    assert (df is not None)
    assert (df.loc[1, 'name'] == 'Fluffy')

@pytest.mark.order(8)
def test_sql_example_2(relational_db, load_examples_relational):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected.
    @note  Uses a table-creation fixture to load / unload schema."""
    _test_query_file(
        relational_db,
        "./tests/examples-db/relational_df2.sql",
        valid_files=["sql"],
        expect_df=True,
    )
    df = relational_db.get_dataframe("ExampleEAV")
    assert (df is not None)
    assert (df.loc[0, 'value'] == 'Timber')


@pytest.mark.order(9)
def test_mongo_example_1(docs_db):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    _test_query_file(
        docs_db,
        "./tests/examples-db/document_df1.mongo",
        valid_files=["json", "mongo"],
        expect_df=True,
    )
    df = docs_db.get_dataframe("books")
    assert (df is not None)
    assert (df.loc[0, 'title'] == 'Wuthering Heights')
    assert (df.iloc[-1]['chapters.pages'] == 25)
    docs_db.execute_query('{"drop": "books"}')
# ------------------------------------------------------------------------------
# FILE TEST WRAPPERS: Reuse the logic to test multiple files within a single test.
# ------------------------------------------------------------------------------
def _test_query_file(db_fixture, filename: str, valid_files: List, expect_df: bool = False):
    """Run queries from a local file through the database.
    @param db_fixture  Fixture corresponding to the current session's database.
    @param filename  The name of a query file (for example ./tests/example1.sql).
    @param valid_files  A list of file extensions valid for this database type.
    @param expect_df  Whether to throw an error if the queries fail to return a DataFrame."""
    file_ext = filename.split('.')[-1].lower()
    assert file_ext in valid_files, f"Received '{filename}', but cannot execute .{file_ext} files."
    df = db_fixture.execute_file(filename)
    if expect_df:
        assert (
            df is not None
        ), f"Execution of '{filename}' failed to produce results."
