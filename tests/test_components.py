import pytest
import sys
import os
from src.setup import Session
from src.util import Log


@pytest.fixture(scope="session")
def session():
    """Fixture to create session."""
    session = Session()
    relational_db = session.relational_db
    yield session
    session.reset()


@pytest.fixture(scope="session")
def relational_db(session):
    """Fixture to get relational database connection."""
    relational_db = session.relational_db
    saved_config = {
        "verbose": relational_db.verbose,
        "working_database": relational_db.verbose,
    }
    relational_db.verbose = True
    relational_db.change_database(relational_db._default_database)
    yield relational_db
    relational_db.verbose = saved_config["verbose"]
    relational_db.change_database(saved_config["working_database"])


@pytest.mark.order(1)
def test_relational(relational_db):
    """Tests if the relational database is working correctly.
    @note  Database connectors have internal tests, so use those instead."""
    assert relational_db.test_connection(
        print_results=True
    ), "Basic tests on relational database connection failed."


@pytest.mark.order(2)
def test_sql_examples(relational_db):
    """Run queries from test files."""
    _test_sql_file(relational_db, "./tests/reset.sql", expect_df=False)
    _test_sql_file(
        relational_db,
        "./tests/example1.sql",
        expect_df=True,
        df_header="EntityName table:",
    )
    _test_sql_file(
        relational_db,
        "./tests/example2.sql",
        expect_df=True,
        df_header="ExampleEAV table:",
    )
    df = relational_db.get_dataframe(
        "EntityName"
    )  # Internal errors are handled by the class itself.
    assert (
        df is not None
    )  # We can just check results since implementation is checked by RelationalConnector.


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
    except:
        Log.fail(f"Unexpected error while executing queries from '{filename}'.")
