import os
import pytest
from src.setup import Session
from src.util import Log
import sys
import time
from typing import List


# ------------------------------------------------------------------------------
# DATABASE FIXTURES: Checkpoint the database connector instances from Session.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def relational_db(session):
    """Fixture to get relational database connection."""
    _relational_db = session.relational_db
    with _relational_db.temp_database("pytest"):
        yield _relational_db


@pytest.fixture(scope="module")
def docs_db(session):
    """Fixture to get document database connection."""
    _docs_db = session.docs_db
    with _docs_db.temp_database("pytest"):
        yield _docs_db


@pytest.fixture(scope="module")
def graph_db(session):
    """Fixture to get document database connection."""
    _graph_db = session.graph_db
    with _graph_db.temp_database("pytest"):
        yield _graph_db


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run check_connection() for minimal connection test.
# ------------------------------------------------------------------------------
@pytest.mark.order(1)
def test_db_relational_minimal(relational_db):
    """Tests if the RelationalConnector has a valid connection string."""
    relational_db.check_connection(log_source=Log.pytest_db, raise_error=True)


@pytest.mark.order(2)
def test_db_docs_minimal(docs_db):
    """Tests if the DocumentConnector has a valid connection string."""
    docs_db.check_connection(log_source=Log.pytest_db, raise_error=True)


@pytest.mark.order(3)
def test_db_graph_minimal(graph_db):
    """Tests if the GraphConnector has a valid connection string."""
    graph_db.check_connection(log_source=Log.pytest_db, raise_error=True)


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run test_connection() for comprehensive usage tests.
# ------------------------------------------------------------------------------
@pytest.mark.order(4)
def test_db_relational_comprehensive(relational_db):
    """Tests if the GraphConnector is working as intended."""
    relational_db.test_connection(raise_error=True)


@pytest.mark.order(5)
def test_db_docs_comprehensive(docs_db):
    """Tests if the GraphConnector is working as intended."""
    docs_db.test_connection(raise_error=True)


@pytest.mark.order(6)
def test_db_graph_comprehensive(graph_db):
    """Tests if the GraphConnector is working as intended."""
    graph_db.test_connection(raise_error=True)


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
    _test_query_file(relational_db, "./tests/examples-db/relational_df1.sql", valid_files=["sql"])
    df = relational_db.get_dataframe("EntityName")
    assert df is not None
    assert not df.empty
    assert df.loc[1, 'name'] == 'Fluffy'


@pytest.mark.order(8)
def test_sql_example_2(relational_db, load_examples_relational):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected.
    @note  Uses a table-creation fixture to load / unload schema."""
    _test_query_file(relational_db, "./tests/examples-db/relational_df2.sql", valid_files=["sql"])
    df = relational_db.get_dataframe("ExampleEAV", ["entity", "attribute", "value"])
    assert df is not None
    assert not df.empty
    assert df.iloc[-1]['value'] == 'Timber'


@pytest.mark.order(9)
def test_mongo_example_1(docs_db):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    _test_query_file(docs_db, "./tests/examples-db/document_df1.mongo", valid_files=["json", "mongo"])
    df = docs_db.get_dataframe("books")
    assert df is not None
    assert not df.empty
    assert df.loc[0, 'title'] == 'Wuthering Heights'
    assert df.iloc[-1]['chapters.pages'] == 22
    docs_db.execute_query('{"drop": "books"}')


@pytest.mark.order(10)
def test_mongo_example_2(docs_db):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    _test_query_file(docs_db, "./tests/examples-db/document_df2.json", valid_files=["json", "mongo"])
    df = docs_db.get_dataframe("qa_exam")
    assert df is not None
    assert not df.empty
    assert df.iloc[0]['answer'] == 'Paul Atreides'
    assert any((df['gold_answer'] == 'The Fremen') & (df['is_correct'] == False))
    docs_db.execute_query('{"drop": "qa_exam"}')


@pytest.mark.order(11)
def test_mongo_example_3(docs_db):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    _test_query_file(docs_db, "./tests/examples-db/document_df3.mongo", valid_files=["json", "mongo"])
    df = docs_db.get_dataframe("potions")
    assert df is not None
    assert df.loc[4, 'potion_name'] == 'Elixir of Wisdom'
    assert "effects.description" in df.columns
    assert any((df['potion_name'] == 'Invisibility Draught') & (df['effects.description'] == 'Silent movement'))
    assert df.loc[12, 'ingredients.name'] == 'Mirage Powder'
    assert "effects.seconds" in df.columns
    assert any((df['potion_name'] == 'Catkin Tincture') & (df['effects.seconds'] == 0))
    docs_db.execute_query('{"drop": "potions"}')


@pytest.mark.order(12)
def test_cypher_example_1(graph_db):
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    graph_db.drop_graph("pets")
    _test_query_file(
        graph_db,
        "./tests/examples-db/graph_df1.cql",
        valid_files=["cql", "cypher"]
    )
    df = graph_db.get_dataframe("pets")
    assert (df is not None)
    assert (len(df) == 5)
    assert ("node_id" in df.columns and "labels" in df.columns)
    assert ("db" in df.columns and "kg" in df.columns)
    assert (len(df.columns) == 8)
    assert (df.iloc[-1]['name'] == 'Buddy')
    assert any((df['species'] == 'Rabbit') & (df['age'] == 4))
    graph_db.drop_graph("pets")


@pytest.mark.order(13)
def test_cypher_example_2(graph_db):
    """Test social network graph with relationships and mixed query patterns.
    @details  Validates comment parsing, semicolon splitting, CREATE/MERGE/MATCH,
    relationships with properties, and TAG_NODES_ with/without RETURN."""
    graph_db.drop_graph("social")
    _test_query_file(
        graph_db,
        "./tests/examples-db/graph_df2.cypher",
        valid_files=["cql", "cypher"]
    )
    
    # Verify nodes were created correctly
    df = graph_db.get_dataframe("social")
    assert df is not None
    assert len(df) == 5  # Alice, Bob, Charlie, Dave, Frank
    assert "node_id" in df.columns and "labels" in df.columns
    assert "db" in df.columns and "kg" in df.columns
    
    # Check specific nodes
    assert any(df['name'] == 'Alice')
    assert any(df['name'] == 'Frank')
    
    # Verify list properties were stored
    frank_row = df[df['name'] == 'Frank'].iloc[0]
    assert 'hobbies' in frank_row or 'scores' in frank_row  # At least one list property
    
    # Debug: Check what's actually in the database
    debug_query = """
    MATCH (s)-[r]->(o)
    WHERE s.kg = 'social' AND o.kg = 'social'
    RETURN s.name AS subject, s.db AS s_db, type(r) AS relation, 
           o.name AS object, o.db AS o_db, r.db AS r_db
    """
    debug_df = graph_db.execute_query(debug_query, _filter_results=False)
    print("\n=== DEBUG: Raw relationships ===")
    print(debug_df)

    # Debug: Check if the query is finding anything
    debug_query2 = """
    MATCH (s)-[r]->(o)
    WHERE s.kg = 'social' AND o.kg = 'social' AND r.kg = 'social'
      AND s.db = 'pytest' AND o.db = 'pytest' AND r.db = 'pytest'
    RETURN s.name AS subject, type(r) AS relation, o.name AS object
    """
    debug_df2 = graph_db.execute_query(debug_query2, _filter_results=False)
    print("\n=== DEBUG: Manual triples query ===")
    print(debug_df2)

    # Verify relationships exist
    triples_df = graph_db.get_all_triples()
    assert triples_df is not None
    assert len(triples_df) > 0
    assert any((triples_df['subject'] == 'Bob') & (triples_df['relation'] == 'KNOWS'))
    assert any((triples_df['subject'] == 'Bob') & (triples_df['relation'] == 'FOLLOWS'))
    assert any(triples_df['relation'] == 'COLLABORATES')
    
    graph_db.drop_graph("social")


# ------------------------------------------------------------------------------
# FILE TEST WRAPPERS: Reuse the logic to test multiple files within a single test.
# ------------------------------------------------------------------------------
def _test_query_file(db_fixture, filename: str, valid_files: List):
    """Run queries from a local file through the database.
    @param db_fixture  Fixture corresponding to the current session's database.
    @param filename  The name of a query file (for example ./tests/example1.sql).
    @param valid_files  A list of file extensions valid for this database type."""
    file_ext = filename.split('.')[-1].lower()
    assert file_ext in valid_files, f"Received '{filename}', but cannot execute .{file_ext} files."
    df = db_fixture.execute_file(filename)
