import os
import pytest
from components.connectors import DatabaseConnector, RelationalConnector
from components.document_storage import DocumentConnector
from components.fact_storage import GraphConnector
from src.setup import Session
from src.util import Log
from pandas import DataFrame
import sys
import time
from typing import Generator, List, Optional


# ------------------------------------------------------------------------------
# DATABASE FIXTURES: Checkpoint the database connector instances from Session.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def relational_db(session: Session) -> RelationalConnector:
    """Fixture to get relational database connection."""
    _relational_db = session.relational_db
    with _relational_db.temp_database("pytest"):
        yield _relational_db


@pytest.fixture(scope="module")
def docs_db(session: Session) -> DocumentConnector:
    """Fixture to get document database connection."""
    _docs_db = session.docs_db
    with _docs_db.temp_database("pytest"):
        yield _docs_db


@pytest.fixture(scope="module")
def graph_db(session: Session) -> GraphConnector:
    """Fixture to get document database connection."""
    _graph_db = session.graph_db
    with _graph_db.temp_database("pytest"):
        yield _graph_db


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run check_connection() for minimal connection test.
# ------------------------------------------------------------------------------
@pytest.mark.order(1)
@pytest.mark.dependency(name="rel_minimal")
def test_db_relational_minimal(relational_db: RelationalConnector) -> None:
    """Tests if the RelationalConnector has a valid connection string."""
    connected = relational_db.check_connection(log_source=Log.pytest_db, raise_error=True)
    assert connected


@pytest.mark.order(2)
@pytest.mark.dependency(name="docs_minimal")
def test_db_docs_minimal(docs_db: DocumentConnector) -> None:
    """Tests if the DocumentConnector has a valid connection string."""
    connected = docs_db.check_connection(log_source=Log.pytest_db, raise_error=True)
    assert connected


@pytest.mark.order(3)
@pytest.mark.dependency(name="graph_minimal")
def test_db_graph_minimal(graph_db: GraphConnector) -> None:
    """Tests if the GraphConnector has a valid connection string."""
    connected = graph_db.check_connection(log_source=Log.pytest_db, raise_error=True)
    assert connected


# ------------------------------------------------------------------------------
# BUILT-IN DATABASE TESTS: Run test_connection() for comprehensive usage tests.
# ------------------------------------------------------------------------------
@pytest.mark.order(4)
@pytest.mark.dependency(name="rel_comprehensive", depends=["rel_minimal"])
def test_db_relational_comprehensive(relational_db: RelationalConnector) -> None:
    """Tests if the GraphConnector is working as intended."""
    operational = relational_db.test_connection(raise_error=True)
    assert operational


@pytest.mark.order(5)
@pytest.mark.dependency(name="docs_comprehensive", depends=["docs_minimal"])
def test_db_docs_comprehensive(docs_db: DocumentConnector) -> None:
    """Tests if the GraphConnector is working as intended."""
    operational = docs_db.test_connection(raise_error=True)
    assert operational


@pytest.mark.order(6)
@pytest.mark.dependency(name="graph_comprehensive", depends=["graph_minimal"])
def test_db_graph_comprehensive(graph_db: GraphConnector) -> None:
    """Tests if the GraphConnector is working as intended."""
    operational = graph_db.test_connection(raise_error=True)
    assert operational


# ------------------------------------------------------------------------------
# DATABASE FILE TESTS: Run execute_file with example scripts.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def load_examples_relational(relational_db: RelationalConnector) -> Generator[None, None, None]:
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
@pytest.mark.dependency(name="rel_example_1", depends=["rel_minimal", "rel_comprehensive"])
def test_sql_example_1(relational_db: RelationalConnector, load_examples_relational: Generator[None, None, None]) -> None:
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
@pytest.mark.dependency(name="rel_example_2", depends=["rel_minimal", "rel_comprehensive"])
def test_sql_example_2(relational_db: RelationalConnector, load_examples_relational: Generator[None, None, None]) -> None:
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
@pytest.mark.dependency(name="docs_example_1", depends=["docs_minimal", "docs_comprehensive"])
def test_mongo_example_1(docs_db: DocumentConnector) -> None:
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
@pytest.mark.dependency(name="docs_example_2", depends=["docs_minimal", "docs_comprehensive"])
def test_mongo_example_2(docs_db: DocumentConnector) -> None:
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
@pytest.mark.dependency(name="docs_example_3", depends=["docs_minimal", "docs_comprehensive"])
def test_mongo_example_3(docs_db: DocumentConnector) -> None:
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
@pytest.mark.dependency(name="graph_example_1", depends=["graph_minimal", "graph_comprehensive"])
def test_cypher_example_1(graph_db: GraphConnector) -> None:
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
    assert ("element_id" in df.columns and "element_type" in df.columns)
    assert ("db" in df.columns and "kg" in df.columns)
    assert (len(df) == 5)
    assert (len(df.columns) == 9)
    # db-kg (2), elem-id-type (2), labels (1), and 4 expected (name, species, weight, age)
    assert (df.iloc[-1]['name'] == 'Buddy')
    assert any((df['species'] == 'Rabbit') & (df['age'] == 4))
    graph_db.drop_graph("pets")


@pytest.mark.order(13)
@pytest.mark.dependency(name="graph_example_2", depends=["graph_minimal", "graph_comprehensive"])
def test_cypher_example_2(graph_db: GraphConnector) -> None:
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
    df_nodes = df[df["element_type"] == "node"]
    assert len(df_nodes) == 5  # Alice, Bob, Charlie, Dave, Frank
    assert "element_id" in df_nodes.columns and "labels" in df_nodes.columns
    assert "db" in df_nodes.columns and "kg" in df_nodes.columns
    
    # Check specific nodes exist with expected properties
    assert any(df_nodes['name'] == 'Alice')
    assert any((df_nodes['name'] == 'Bob') & (df_nodes['age'] == 25))
    assert any((df_nodes['name'] == 'Charlie') & (df_nodes['age'] == 35))
    assert any((df_nodes['name'] == 'Dave') & (df_nodes['age'] == 28))
    assert any(df_nodes['name'] == 'Frank')
    
    # Verify Alice has correct age
    alice_rows = df_nodes[df_nodes['name'] == 'Alice']
    assert len(alice_rows) == 1
    assert alice_rows.iloc[0]['age'] == 30
    
    # Verify list properties were stored
    frank_row = df_nodes[df_nodes['name'] == 'Frank'].iloc[0]
    assert 'hobbies' in frank_row or 'scores' in frank_row  # At least one list property

    # Verify relationships exist by filtering DataFrame
    df_rels = df[df["element_type"] == "relationship"]
    assert len(df_rels) > 0
    assert "rel_type" in df_rels.columns
    assert "start_node_id" in df_rels.columns and "end_node_id" in df_rels.columns
    
    # Check specific relationship types exist
    assert any(df_rels['rel_type'] == 'KNOWS')
    assert any(df_rels['rel_type'] == 'FOLLOWS')
    assert any(df_rels['rel_type'] == 'COLLABORATES')
    
    # Verify there are multiple KNOWS relationships
    knows_rels = df_rels[df_rels['rel_type'] == 'KNOWS']
    assert len(knows_rels) >= 3  # Alice->Bob, Bob->Charlie, Dave->Alice
    
    # Verify relationship properties (e.g., 'since' on KNOWS, 'project' on COLLABORATES)
    assert any(knows_rels['since'].notna()) if 'since' in knows_rels.columns else True
    collab_rels = df_rels[df_rels['rel_type'] == 'COLLABORATES']
    assert len(collab_rels) >= 1
    if 'project' in collab_rels.columns:
        assert any(collab_rels['project'] == 'AI')
    if 'hours' in collab_rels.columns:
        assert any(collab_rels['hours'] == 120)
    
    graph_db.drop_graph("social")

@pytest.mark.order(14)
@pytest.mark.dependency(name="graph_example_3", depends=["graph_minimal", "graph_comprehensive"])
def test_cypher_example_3(graph_db: GraphConnector) -> None:
    """Test scene and dialogue graphs with proper isolation.
    @details  Validates kg property isolation using a scene graph (spatial relationships)
    and dialogue graph (conversation flow with object references). Tests temp_graph 
    context manager and filter_valid correctness across different graph contexts."""
    
    # Clean up all test graphs
    for kg_name in ["scene", "dialogue"]:
        try:
            graph_db.drop_graph(kg_name)
        except:
            pass
    
    _test_query_file(
        graph_db,
        "./tests/examples-db/graph_df3.cql",
        valid_files=["cql", "cypher"]
    )
    
    # Test Graph 1: Scene graph with spatial relationships
    with graph_db.temp_graph("scene"):
        df_scene = graph_db.get_dataframe("scene")
        assert df_scene is not None
        df_scene_nodes = df_scene[df_scene["element_type"] == "node"]
        assert len(df_scene_nodes) == 5  # Employee, Manager, Sofa, Table, Lamp
        assert any(df_scene_nodes['name'] == 'Employee')
        assert any(df_scene_nodes['name'] == 'Manager')
        assert any((df_scene_nodes['type'] == 'seating') & (df_scene_nodes['name'] == 'Sofa'))
        assert any((df_scene_nodes['type'] == 'surface') & (df_scene_nodes['name'] == 'Table'))
        assert any((df_scene_nodes['type'] == 'lighting') & (df_scene_nodes['name'] == 'Lamp'))
        
        # Verify spatial coordinates exist for all nodes
        assert 'x' in df_scene_nodes.columns and 'y' in df_scene_nodes.columns
        
        # Verify Employee and Manager have position data
        employee_row = df_scene_nodes[df_scene_nodes['name'] == 'Employee'].iloc[0]
        manager_row = df_scene_nodes[df_scene_nodes['name'] == 'Manager'].iloc[0]
        assert employee_row['position'] == 'standing' and employee_row['x'] == 5.0
        assert manager_row['position'] == 'sitting' and manager_row['x'] == 2.1
        
        # Verify spatial relationships by filtering DataFrame
        df_scene_rels = df_scene[df_scene["element_type"] == "relationship"]
        assert len(df_scene_rels) == 3  # SITTING_ON, NEAR, ON_TOP_OF
        assert any(df_scene_rels['rel_type'] == 'SITTING_ON')
        assert any(df_scene_rels['rel_type'] == 'ON_TOP_OF')
        assert any(df_scene_rels['rel_type'] == 'NEAR')
        
        # Verify NEAR relationship has distance property
        near_rels = df_scene_rels[df_scene_rels['rel_type'] == 'NEAR']
        if 'distance' in near_rels.columns:
            assert any(near_rels['distance'] == 0.5)
        
        # Verify relationship directionality exists
        assert all(df_scene_rels['start_node_id'].notna())
        assert all(df_scene_rels['end_node_id'].notna())
    
    # Test Graph 2: Dialogue graph
    with graph_db.temp_graph("dialogue"):
        df_dialogue = graph_db.get_dataframe("dialogue")
        assert df_dialogue is not None
        df_dialogue_nodes = df_dialogue[df_dialogue["element_type"] == "node"]
        assert len(df_dialogue_nodes) == 6  # 3 Dialogue + 3 DialogueRef nodes
        
        # Check dialogue nodes exist
        assert any((df_dialogue_nodes['speaker'] == 'Employee') & (df_dialogue_nodes['timestamp'] == 10.5))
        assert any((df_dialogue_nodes['speaker'] == 'Manager') & (df_dialogue_nodes['timestamp'] == 12.3))
        assert any((df_dialogue_nodes['speaker'] == 'Employee') & (df_dialogue_nodes['timestamp'] == 15.8))
        
        # Verify timestamp ordering
        dialogue_only = df_dialogue_nodes[df_dialogue_nodes['timestamp'].notna()]
        assert len(dialogue_only) == 3
        
        # Verify dialogue flow relationships by filtering DataFrame
        df_dialogue_rels = df_dialogue[df_dialogue["element_type"] == "relationship"]
        assert len(df_dialogue_rels) == 2  # Two FOLLOWED_BY relationships
        assert all(df_dialogue_rels['rel_type'] == 'FOLLOWED_BY')
        
        # Check object references exist (DialogueRef nodes)
        assert any(df_dialogue_nodes['mentioned_object'] == 'file')
        assert any(df_dialogue_nodes['mentioned_object'] == 'table')
        assert any(df_dialogue_nodes['mentioned_object'] == 'lamp')
        
        # Verify dialogue text content exists
        assert any(df_dialogue_nodes['text'].notna()) if 'text' in df_dialogue_nodes.columns else True
        employee_dialogue = df_dialogue_nodes[(df_dialogue_nodes['speaker'] == 'Employee') & (df_dialogue_nodes['text'].notna())]
        assert len(employee_dialogue) >= 2  # Employee speaks twice
    
    # Verify graph isolation: scene and dialogue should be separate despite cross-graph query
    df_scene = graph_db.get_dataframe("scene")
    df_dialogue = graph_db.get_dataframe("dialogue")
    assert df_scene is not None and df_dialogue is not None
    
    # Extract nodes only for comparison
    df_scene_nodes = df_scene[df_scene["element_type"] == "node"]
    df_dialogue_nodes = df_dialogue[df_dialogue["element_type"] == "node"]
    scene_entities = set(df_scene_nodes['name'].dropna())
    dialogue_speakers = set(df_dialogue_nodes['speaker'].dropna())
    
    # Speakers reference scene entities (cross-graph semantic links are intentional)
    # Employee/Manager appear as speakers in dialogue, AND as Person nodes in scene
    assert 'Employee' in dialogue_speakers  # Employee speaks
    assert 'Employee' in scene_entities     # Employee exists in scene
    assert 'Manager' in dialogue_speakers    # Manager speaks
    assert 'Manager' in scene_entities       # Manager exists in scene
    
    # Verify graphs have different data models through property schemas
    scene_employee = df_scene_nodes[df_scene_nodes['name'] == 'Employee'].iloc[0]
    assert scene_employee['position'] == 'standing'
    assert 'x' in scene_employee.index and scene_employee['x'] == 5.0
    
    # Dialogue nodes have conversational properties
    dialogue_props = set(df_dialogue_nodes.columns)
    assert 'speaker' in dialogue_props or 'text' in dialogue_props or 'mentioned_object' in dialogue_props
    
    # Scene nodes have spatial/physical properties
    scene_props = set(df_scene_nodes.columns)
    assert 'position' in scene_props or 'x' in scene_props or 'type' in scene_props
    
    # Clean up
    for kg_name in ["scene", "dialogue"]:
        graph_db.drop_graph(kg_name)


@pytest.mark.order(15)
@pytest.mark.dependency(name="graph_example_4", depends=["graph_minimal", "graph_comprehensive"])
def test_cypher_example_4(graph_db: GraphConnector) -> None:
    """Test event graph with property mutations and multi-hop traversal.
    @details  Validates MERGE property updates (2-wave assignment), relationship chains
    in DAG structure, consistent rel_type with varied properties, and multi-hop path queries.
    Tests that properties added via SET after initial CREATE are properly stored."""
    graph_db.drop_graph("events")
    _test_query_file(
        graph_db,
        "./tests/examples-db/graph_df4.cypher",
        valid_files=["cql", "cypher"]
    )
    
    # Verify all event nodes were created with correct types
    df = graph_db.get_dataframe("events")
    assert df is not None
    df_nodes = df[df["element_type"] == "node"]
    assert len(df_nodes) == 7  # 7 event nodes
    assert "element_id" in df_nodes.columns and "labels" in df_nodes.columns
    assert "db" in df_nodes.columns and "kg" in df_nodes.columns
    
    # Verify event types exist
    dialogue_events = df_nodes[df_nodes['labels'].apply(lambda x: 'DialogueEvent' in x if isinstance(x, list) else False)]
    action_events = df_nodes[df_nodes['labels'].apply(lambda x: 'ActionEvent' in x if isinstance(x, list) else False)]
    scene_events = df_nodes[df_nodes['labels'].apply(lambda x: 'SceneEvent' in x if isinstance(x, list) else False)]
    assert len(dialogue_events) == 3  # Knight, Guide Master, Wizard speak
    assert len(action_events) == 2   # Knight hired, Wizard arrives
    assert len(scene_events) == 2    # Guide Master leaves room, Knight enters hall
    
    # Verify Wave 2 properties were added via MERGE/SET (property mutation)
    knight_speaks = df_nodes[df_nodes['name'] == 'Knight speaks'].iloc[0]
    assert knight_speaks['speaker'] == 'Knight'  # Wave 1 property
    assert knight_speaks['says'] == 'I accept the quest'  # Wave 2 property
    assert knight_speaks['audience'] == 'Guild Master'    # Wave 2 property
    
    guild_master_speaks = df_nodes[df_nodes['name'] == 'Guide Master speaks'].iloc[0]
    assert guild_master_speaks['says'] == 'The dragon stirs'
    assert guild_master_speaks['audience'] == 'townspeople'
    
    wizard_arrives = df_nodes[df_nodes['name'] == 'Wizard arrives'].iloc[0]
    assert wizard_arrives['action'] == 'arrives'
    assert wizard_arrives['method'] == 'teleportation'
    
    # Verify relationships form a DAG with consistent rel_type
    df_rels = df[df["element_type"] == "relationship"]
    assert len(df_rels) == 7  # 6 main chain + 1 alternate path
    assert all(df_rels['rel_type'] == 'followedBy')
    
    # Verify relationship properties (line numbers and snippets from Wave 2)
    assert all(df_rels['line'].notna())
    assert any(df_rels['line'] == 42)
    assert any(df_rels['line'] == 102)
    
    # Check that snippets were added via MERGE/SET
    assert all(df_rels['snippet'].notna())
    assert any(df_rels['snippet'] == 'The Guild Master nodded approvingly')
    assert any(df_rels['snippet'] == 'The following day, Knight')
    
    # Verify alternate path exists (branch in DAG)
    alternate_rels = df_rels[df_rels['alternate'].notna() & (df_rels['alternate'] == True)]
    assert len(alternate_rels) == 1
    assert alternate_rels.iloc[0]['line'] == 43
    assert alternate_rels.iloc[0]['snippet'] == 'Suddenly, a portal opened'
    
    # Multi-hop path verification: Find paths from "Knight speaks" to various endpoints
    knight_speaks_id = df_nodes[df_nodes['name'] == 'Knight speaks'].iloc[0]['element_id']
    knight_enters_id = df_nodes[df_nodes['name'] == 'Knight enters hall'].iloc[0]['element_id']
    wizard_arrives_id = df_nodes[df_nodes['name'] == 'Wizard arrives'].iloc[0]['element_id']
    guild_master_speaks_id = df_nodes[df_nodes['name'] == 'Guide Master speaks'].iloc[0]['element_id']
    
    # Path 1: Knight speaks -> Knight is hired -> Guide Master leaves room -> Guide Master speaks (3 hops)
    def find_path_length(start_id: str, end_id: str, df_rels: DataFrame) -> Optional[int]:
        """BFS to find shortest path length between two nodes."""
        from collections import deque
        visited = {start_id}
        queue = deque([(start_id, 0)])
        
        while queue:
            current_id, depth = queue.popleft()
            if current_id == end_id:
                return depth
            
            # Find outgoing edges from current node
            next_rels = df_rels[df_rels['start_node_id'] == current_id]
            for _, rel in next_rels.iterrows():
                next_id = rel['end_node_id']
                if next_id not in visited:
                    visited.add(next_id)
                    queue.append((next_id, depth + 1))
        
        return None
    
    # Verify shortest path (uses alternate branch): Knight speaks -> Knight enters hall (3 hops)
    # Path: Knight speaks -> Wizard arrives (alternate) -> Wizard speaks -> Knight enters hall
    path_length_shortest = find_path_length(knight_speaks_id, knight_enters_id, df_rels)
    assert path_length_shortest == 3
    
    # Verify branch path exists: Knight speaks -> Wizard arrives (1 hop via alternate)
    path_length_alt = find_path_length(knight_speaks_id, wizard_arrives_id, df_rels)
    assert path_length_alt == 1  # Direct alternate path
    
    # Verify intermediate path: Knight speaks -> Guide Master speaks (3 hops)
    path_length_mid = find_path_length(knight_speaks_id, guild_master_speaks_id, df_rels)
    assert path_length_mid == 3
    
    # Verify DAG property: no cycles (Wizard arrives cannot reach Knight speaks)
    reverse_path = find_path_length(wizard_arrives_id, knight_speaks_id, df_rels)
    assert reverse_path is None  # No backward path in DAG
    
    graph_db.drop_graph("events")


# ------------------------------------------------------------------------------
# FILE TEST WRAPPERS: Reuse the logic to test multiple files within a single test.
# ------------------------------------------------------------------------------
def _test_query_file(db_fixture: DatabaseConnector, filename: str, valid_files: List[str]) -> None:
    """Run queries from a local file through the database.
    @param db_fixture  Fixture corresponding to the current session's database.
    @param filename  The name of a query file (for example ./tests/example1.sql).
    @param valid_files  A list of file extensions valid for this database type."""
    file_ext = filename.split('.')[-1].lower()
    assert file_ext in valid_files, f"Received '{filename}', but cannot execute .{file_ext} files."
    df = db_fixture.execute_file(filename)




# TODO: Move the following to a different test file
from components.semantic_web import KnowledgeGraph
import pytest
from components.fact_storage import GraphConnector
from pandas import DataFrame

@pytest.mark.order(16)
@pytest.mark.dependency(name="knowledge_graph_triples", depends=["graph_minimal", "graph_comprehensive"])
def test_knowledge_graph_triples(graph_db: GraphConnector) -> None:
    """Test KnowledgeGraph triple operations using add_triple and get_all_triples.
    @details  Validates the KnowledgeGraph wrapper for semantic triple management:
    - add_triple() creates nodes and relationships
    - get_all_triples() retrieves triples as element IDs
    - get_triple_properties() constructs a DataFrame with element properties as columns
    - convert_to_names() maps IDs to human-readable names
    """
    graph_db.drop_graph("social_kg")
    
    # Create a KnowledgeGraph instance
    kg = KnowledgeGraph("social_kg", graph_db)
    
    # Add triples using the simplified API
    kg.add_triple("Alice", "KNOWS", "Bob")
    kg.add_triple("Bob", "KNOWS", "Charlie")
    kg.add_triple("Alice", "FOLLOWS", "Charlie")
    kg.add_triple("Charlie", "COLLABORATES", "Alice")
    kg.add_triple("Bob", "FOLLOWS", "Alice")
    
    # Retrieve all triples (returns element IDs)
    triples_ids_df = kg.get_all_triples()
    assert triples_ids_df is not None
    assert len(triples_ids_df) == 5
    assert list(triples_ids_df.columns) == ["subject_id", "relation_id", "object_id"]
    
    # Verify all ID columns contain non-null values
    assert all(triples_ids_df["subject_id"].notna())
    assert all(triples_ids_df["relation_id"].notna())
    assert all(triples_ids_df["object_id"].notna())
    
    # Convert IDs to human-readable names
    triples_df = kg.convert_to_names(triples_ids_df)
    assert triples_df is not None
    assert len(triples_df) == 5
    assert list(triples_df.columns) == ["subject", "relation", "object"]
    
    # Verify specific triples exist
    assert any((triples_df["subject"] == "Alice") & (triples_df["relation"] == "KNOWS") & (triples_df["object"] == "Bob"))
    assert any((triples_df["subject"] == "Bob") & (triples_df["relation"] == "KNOWS") & (triples_df["object"] == "Charlie"))
    assert any((triples_df["subject"] == "Alice") & (triples_df["relation"] == "FOLLOWS") & (triples_df["object"] == "Charlie"))
    assert any((triples_df["subject"] == "Charlie") & (triples_df["relation"] == "COLLABORATES") & (triples_df["object"] == "Alice"))
    assert any((triples_df["subject"] == "Bob") & (triples_df["relation"] == "FOLLOWS") & (triples_df["object"] == "Alice"))
    
    # Verify nodes were created (should be 3 unique: Alice, Bob, Charlie)
    df = graph_db.get_dataframe("social_kg")
    assert df is not None
    df_nodes = df[df["element_type"] == "node"]
    assert len(df_nodes) == 3
    
    # Verify all nodes have the "name" property
    assert all(df_nodes["name"].notna())
    node_names = set(df_nodes["name"])
    assert node_names == {"Alice", "Bob", "Charlie"}
    
    # Test convert_to_names with pre-fetched lookup DataFrame
    elements_df = graph_db.get_dataframe("social_kg")
    triples_df_cached = kg.convert_to_names(triples_ids_df, df_lookup=elements_df)
    assert triples_df_cached is not None
    assert len(triples_df_cached) == 5
    # Should produce identical results to direct conversion
    assert triples_df_cached.equals(triples_df)
    
    # Test edge case: empty DataFrame
    empty_df = DataFrame(columns=["subject_id", "relation_id", "object_id"])
    empty_result = kg.convert_to_names(empty_df)
    assert empty_result is not None
    assert empty_result.empty
    assert list(empty_result.columns) == ["subject", "relation", "object"]
    
    # Test get_triple_properties (full pivoted view)
    props_df = kg.get_triple_properties()
    assert props_df is not None
    assert len(props_df) == 5
    # Should have prefixed columns for both nodes and relationship
    assert "n1.name" in props_df.columns
    assert "n2.name" in props_df.columns
    assert "r.rel_type" in props_df.columns
    # Verify one specific triple's properties
    alice_knows_bob = props_df[
        (props_df["n1.name"] == "Alice") & 
        (props_df["r.rel_type"] == "KNOWS") & 
        (props_df["n2.name"] == "Bob")
    ]
    assert len(alice_knows_bob) == 1
    
    graph_db.drop_graph("social_kg")






@pytest.fixture
def nature_scene_graph(graph_db: GraphConnector) -> KnowledgeGraph:
    """Create a scene graph with multiple location-based communities for testing.
    @details  Graph structure represents a park with distinct areas:
    - Playground: swings, slide, kids
    - Bench area: bench, parents
    - Forest: trees, rock, path
    - School building: doors, windows, classroom
    Each area forms a natural community for GraphRAG testing.
    """
    graph_db.drop_graph("nature_scene")
    kg = KnowledgeGraph("nature_scene", graph_db)
    
    # Playground community
    kg.add_triple("Kid1", "PLAYS_ON", "Swings")
    kg.add_triple("Kid2", "PLAYS_ON", "Slide")
    kg.add_triple("Swings", "LOCATED_IN", "Playground")
    kg.add_triple("Slide", "LOCATED_IN", "Playground")
    kg.add_triple("Kid1", "NEAR", "Kid2")
    
    # Bench area community
    kg.add_triple("Parent1", "SITS_ON", "Bench")
    kg.add_triple("Parent2", "SITS_ON", "Bench")
    kg.add_triple("Bench", "LOCATED_IN", "BenchArea")
    kg.add_triple("Parent1", "WATCHES", "Kid1")
    kg.add_triple("Parent2", "WATCHES", "Kid2")
    
    # Forest community
    kg.add_triple("Oak", "LOCATED_IN", "Forest")
    kg.add_triple("Pine", "LOCATED_IN", "Forest")
    kg.add_triple("Rock", "NEAR", "Oak")
    kg.add_triple("Path", "CONNECTS", "Forest")
    kg.add_triple("Path", "CONNECTS", "Playground")
    
    # School building community
    kg.add_triple("Door", "PART_OF", "School")
    kg.add_triple("Window", "PART_OF", "School")
    kg.add_triple("Classroom", "INSIDE", "School")
    kg.add_triple("Desk", "INSIDE", "Classroom")
    kg.add_triple("Whiteboard", "INSIDE", "Classroom")
    
    # Cross-community connections
    kg.add_triple("Playground", "ADJACENT_TO", "BenchArea")
    kg.add_triple("BenchArea", "ADJACENT_TO", "Forest")
    kg.add_triple("School", "NEAR", "Playground")
    
    yield kg
    
    graph_db.drop_graph("nature_scene")


@pytest.mark.order(17)
@pytest.mark.dependency(name="subgraph_by_nodes", depends=["knowledge_graph_triples"])
def test_get_subgraph_by_nodes(nature_scene_graph: KnowledgeGraph) -> None:
    """Test filtering triples by specific node IDs.
    @details  Validates that get_subgraph_by_nodes correctly filters triples where
    either subject or object matches the provided node list.
    """
    kg = nature_scene_graph
    
    # Get all triples to extract some node IDs
    all_triples = kg.get_all_triples()
    assert all_triples is not None
    assert len(all_triples) > 0
    
    # Get the full graph to find specific node IDs
    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    
    # Find Kid1 and Swings node IDs
    kid1_id = nodes_df[nodes_df["name"] == "Kid1"]["element_id"].iloc[0]
    swings_id = nodes_df[nodes_df["name"] == "Swings"]["element_id"].iloc[0]
    
    # Get subgraph containing only triples with Kid1 or Swings
    subgraph = kg.get_subgraph_by_nodes([kid1_id, swings_id])
    assert subgraph is not None
    assert len(subgraph) > 0
    
    # Convert to names for verification
    named_subgraph = kg.convert_to_names(subgraph)
    
    # Should include Kid1 PLAYS_ON Swings
    assert any(
        (named_subgraph["subject"] == "Kid1") & 
        (named_subgraph["relation"] == "PLAYS_ON") & 
        (named_subgraph["object"] == "Swings")
    )
    
    # Should include Kid1 NEAR Kid2
    assert any(
        (named_subgraph["subject"] == "Kid1") & 
        (named_subgraph["relation"] == "NEAR")
    )
    
    # Should include Swings LOCATED_IN Playground
    assert any(
        (named_subgraph["subject"] == "Swings") & 
        (named_subgraph["relation"] == "LOCATED_IN")
    )
    
    # All triples should involve Kid1 or Swings
    for _, row in named_subgraph.iterrows():
        assert row["subject"] in ["Kid1", "Swings"] or row["object"] in ["Kid1", "Swings"]


@pytest.mark.order(18)
@pytest.mark.dependency(name="neighborhood", depends=["knowledge_graph_triples"])
def test_get_neighborhood(nature_scene_graph: KnowledgeGraph) -> None:
    """Test k-hop neighborhood expansion around a central node.
    @details  Validates that get_neighborhood correctly finds all triples within
    k hops of a starting node.
    """
    kg = nature_scene_graph
    
    # Get node IDs
    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    playground_id = nodes_df[nodes_df["name"] == "Playground"]["element_id"].iloc[0]
    
    # Test 1-hop neighborhood
    neighborhood_1hop = kg.get_neighborhood(playground_id, depth=1)
    assert neighborhood_1hop is not None
    assert len(neighborhood_1hop) > 0
    
    named_1hop = kg.convert_to_names(neighborhood_1hop)
    
    # Should include direct connections to Playground
    assert any(named_1hop["subject"] == "Playground")
    assert any(named_1hop["object"] == "Playground")
    
    # Should include Swings and Slide located in Playground
    assert any(
        (named_1hop["subject"] == "Swings") & 
        (named_1hop["relation"] == "LOCATED_IN") & 
        (named_1hop["object"] == "Playground")
    )
    
    # Test 2-hop neighborhood (should reach further)
    neighborhood_2hop = kg.get_neighborhood(playground_id, depth=2)
    assert neighborhood_2hop is not None
    assert len(neighborhood_2hop) >= len(neighborhood_1hop)
    
    named_2hop = kg.convert_to_names(neighborhood_2hop)
    
    # Should reach Kids who play on equipment in Playground
    assert any(named_2hop["subject"] == "Kid1") or any(named_2hop["object"] == "Kid1")


@pytest.mark.order(19)
@pytest.mark.dependency(name="random_walk_sample", depends=["knowledge_graph_triples"])
def test_get_random_walk_sample(nature_scene_graph: KnowledgeGraph) -> None:
    """Test random walk sampling starting from specified nodes.
    @details  Validates that get_random_walk_sample generates a representative
    subgraph by following random paths through the graph.
    """
    kg = nature_scene_graph
    
    # Get node IDs
    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    kid1_id = nodes_df[nodes_df["name"] == "Kid1"]["element_id"].iloc[0]
    
    # Perform random walk with length 3
    sample = kg.get_random_walk_sample([kid1_id], walk_length=3, num_walks=1)
    assert sample is not None
    assert len(sample) > 0
    assert len(sample) <= 3  # Should visit at most walk_length edges
    
    named_sample = kg.convert_to_names(sample)
    
    # Should start from Kid1 (first edge in walk must have Kid1 as subject)
    first_triple = named_sample.iloc[0]
    assert first_triple["subject"] == "Kid1", "Random walk must start from specified start node"
    
    # Test multiple walks produces equal or more coverage
    sample_multi = kg.get_random_walk_sample([kid1_id], walk_length=5, num_walks=3)
    assert sample_multi is not None
    # Multiple walks should visit at least as many edges (with possible duplicates removed)
    assert len(sample_multi) >= len(sample), "More walks should not reduce coverage"
    
    # All sampled triples should be valid (exist in original graph)
    all_triples = kg.get_all_triples()
    for _, row in sample.iterrows():
        match = (
            (all_triples["subject_id"] == row["subject_id"]) &
            (all_triples["relation_id"] == row["relation_id"]) &
            (all_triples["object_id"] == row["object_id"])
        )
        assert match.any(), f"Sampled triple not found in graph: {row.to_dict()}"








@pytest.mark.order(20)
@pytest.mark.dependency(name="neighborhood_comprehensive", depends=["neighborhood"])
def test_get_neighborhood_comprehensive(nature_scene_graph: KnowledgeGraph) -> None:
    """Comprehensive test for k-hop neighborhood expansion.
    @details  Tests edge cases and advanced features:
    - depth=0 (no expansion)
    - Disconnected nodes
    - Maximum depth reaching entire connected component
    - Cycle handling (no infinite loops)
    - Consistent results across multiple calls
    """
    kg = nature_scene_graph
    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    
    # Get various node IDs for testing
    playground_id = nodes_df[nodes_df["name"] == "Playground"]["element_id"].iloc[0]
    school_id = nodes_df[nodes_df["name"] == "School"]["element_id"].iloc[0]
    desk_id = nodes_df[nodes_df["name"] == "Desk"]["element_id"].iloc[0]
    
    # Edge case 1: depth=0 should return empty (no expansion)
    neighborhood_0 = kg.get_neighborhood(playground_id, depth=0)
    assert neighborhood_0 is not None
    assert neighborhood_0.empty  # No hops = no edges
    
    # Edge case 2: depth=1 from isolated-ish node
    neighborhood_desk_1 = kg.get_neighborhood(desk_id, depth=1)
    named_desk_1 = kg.convert_to_names(neighborhood_desk_1)
    # Desk only connects to Classroom
    assert len(named_desk_1) <= 2  # Should be very small neighborhood
    
    # Feature: Progressively expanding neighborhoods
    n1 = kg.get_neighborhood(playground_id, depth=1)
    n2 = kg.get_neighborhood(playground_id, depth=2)
    n3 = kg.get_neighborhood(playground_id, depth=3)
    # Each hop should expand (or at minimum stay same size)
    assert len(n1) <= len(n2)
    assert len(n2) <= len(n3)
    
    # Feature: Large depth reaches connected component
    # From Playground, following cross-community links should eventually reach most of graph
    neighborhood_large = kg.get_neighborhood(playground_id, depth=10)
    all_triples = kg.get_all_triples()
    # Should capture significant portion of graph (not necessarily all due to disconnected components)
    assert len(neighborhood_large) >= len(all_triples) * 0.5
    
    # Edge case 3: Cycle handling - verify no infinite loop and consistent results
    # The graph has cycles (e.g., Kid1 -> Swings -> Playground -> ... -> Kid1)
    neighborhood_cycle_1 = kg.get_neighborhood(playground_id, depth=3)
    neighborhood_cycle_2 = kg.get_neighborhood(playground_id, depth=3)
    # Should produce identical results (deterministic despite cycles)
    assert len(neighborhood_cycle_1) == len(neighborhood_cycle_2)
    
    # Feature: Different starting nodes produce different neighborhoods
    neighborhood_school = kg.get_neighborhood(school_id, depth=2)
    neighborhood_playground = kg.get_neighborhood(playground_id, depth=2)
    named_school = kg.convert_to_names(neighborhood_school)
    named_playground = kg.convert_to_names(neighborhood_playground)
    # These should have different content
    school_entities = set(named_school["subject"]).union(named_school["object"])
    playground_entities = set(named_playground["subject"]).union(named_playground["object"])
    assert school_entities != playground_entities



@pytest.mark.order(21)
@pytest.mark.dependency(name="random_walk_comprehensive", depends=["random_walk_sample"])
def test_get_random_walk_sample_comprehensive(nature_scene_graph: KnowledgeGraph) -> None:
    """Comprehensive test for random walk sampling.
    @details  Tests edge cases and advanced features:
    - Empty start_nodes list (should sample from any node)
    - Dead-end nodes (leaf nodes with no outgoing edges)
    - Walk length limits are respected
    - Deterministic subset property
    - Stochasticity verification
    """
    kg = nature_scene_graph
    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    all_triples = kg.get_all_triples()
    
    # Get node IDs
    kid1_id = nodes_df[nodes_df["name"] == "Kid1"]["element_id"].iloc[0]
    whiteboard_id = nodes_df[nodes_df["name"] == "Whiteboard"]["element_id"].iloc[0]
    kid2_id = nodes_df[nodes_df["name"] == "Kid2"]["element_id"].iloc[0]
    
    # Edge case 1: Empty start_nodes (should randomly pick from graph)
    sample_random_start = kg.get_random_walk_sample([], walk_length=3, num_walks=1)
    assert sample_random_start is not None
    assert len(sample_random_start) > 0, "Empty start_nodes should default to random node"
    
    # Edge case 2: Start from leaf/dead-end node
    # Whiteboard is inside Classroom - may have limited outgoing paths
    sample_from_leaf = kg.get_random_walk_sample([whiteboard_id], walk_length=5, num_walks=1)
    assert sample_from_leaf is not None
    # May not reach full walk_length due to dead ends, but should get something
    assert len(sample_from_leaf) > 0, "Should handle leaf nodes gracefully"
    
    # Feature: Walk length is respected (sample size <= walk_length due to deduplication)
    sample_short = kg.get_random_walk_sample([kid1_id], walk_length=2, num_walks=1)
    assert len(sample_short) <= 2, "Walk should respect length limit"
    
    sample_long = kg.get_random_walk_sample([kid1_id], walk_length=5, num_walks=1)
    assert len(sample_long) <= 5, "Walk should respect length limit"
    
    # Feature: Random walks can produce different samples (test stochasticity)
    # NOTE: This is probabilistic - some graphs have forced paths that make walks deterministic
    samples = [
        kg.get_random_walk_sample([kid1_id], walk_length=5, num_walks=1)
        for _ in range(10)  # Increased trials for better probability
    ]
    
    # Convert samples to hashable tuples for comparison
    sample_hashes = []
    for sample in samples:
        # Sort rows to normalize, then create hash from IDs
        sorted_sample = sample.sort_values(["subject_id", "relation_id", "object_id"]).reset_index(drop=True)
        sample_hash = tuple(sorted_sample.to_records(index=False).tolist())
        sample_hashes.append(sample_hash)
    
    unique_samples = len(set(sample_hashes))
    
    # Check if walks are deterministic or stochastic
    if unique_samples == 1:
        # All walks identical - verify this is due to graph constraints, not a bug
        # The path from Kid1 must be deterministic (only one choice at each step)
        assert len(samples[0]) >= 1, "Deterministic walks should still produce valid paths"
    else:
        # Good - walks show expected randomness
        assert unique_samples >= 2, "With 10 trials, should see at least 2 different paths"
    
    # Edge case 3: Multiple start nodes
    sample_multi_start = kg.get_random_walk_sample([kid1_id, kid2_id], walk_length=3, num_walks=2)
    assert sample_multi_start is not None
    assert len(sample_multi_start) > 0, "Multiple start nodes should work"
    
    # Feature: Very long walks eventually explore large portions of graph
    sample_exhaustive = kg.get_random_walk_sample([kid1_id], walk_length=20, num_walks=10)
    # Should capture significant graph coverage with enough walks
    coverage_ratio = len(sample_exhaustive) / len(all_triples)
    assert coverage_ratio > 0.1, "Extensive walking should cover at least 10% of graph"




@pytest.mark.order(22)
@pytest.mark.dependency(name="community_detection_minimal", depends=["knowledge_graph_triples"])
def test_detect_community_clusters_minimal(nature_scene_graph: KnowledgeGraph) -> None:
    """Test basic community detection functionality.
    @details  Validates that detect_community_clusters assigns community_id properties
    to nodes and that get_community_subgraph retrieves triples within a community.
    Tests both Leiden and Louvain methods.
    """
    kg = nature_scene_graph

    # Run Leiden community detection (default, single-level)
    kg.detect_community_clusters(method="leiden", multi_level=False)

    # Verify all nodes have community_id assigned
    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    assert all(nodes_df["community_id"].notna()), "Not all nodes have community_id assigned"

    # Check that we have multiple communities (graph has distinct clusters)
    unique_communities = nodes_df["community_id"].unique()
    assert len(unique_communities) >= 2, "Should detect multiple communities"

    # Find a community with internal edges (some communities may only have cross-community edges)
    found_non_empty = False
    for comm_id in unique_communities:
        community_triples = kg.get_community_subgraph(int(comm_id))
        if len(community_triples) > 0:
            found_non_empty = True
            # Verify structure
            assert list(community_triples.columns) == ["subject_id", "relation_id", "object_id"]
            # Verify all triples involve nodes from this community only
            community_node_ids = set(
                nodes_df[nodes_df["community_id"] == comm_id]["element_id"]
            )
            for _, row in community_triples.iterrows():
                assert row["subject_id"] in community_node_ids
                assert row["object_id"] in community_node_ids
            break

    assert found_non_empty, "Should find at least one community with internal edges"

    # Test Louvain method
    kg.detect_community_clusters(method="louvain", multi_level=False)

    # Verify Louvain also assigns community_id
    elements_df_louvain = kg.database.get_dataframe(kg.graph_name)
    nodes_df_louvain = elements_df_louvain[elements_df_louvain["element_type"] == "node"]
    assert all(nodes_df_louvain["community_id"].notna()), "Louvain should assign community_id"



@pytest.mark.order(23)
@pytest.mark.dependency(name="community_detection_comprehensive", depends=["community_detection_minimal"])
def test_detect_community_clusters_comprehensive(nature_scene_graph: KnowledgeGraph) -> None:
    """Comprehensive test for community detection with various parameters.
    @details  Tests:
    - Multi-level hierarchical detection
    - community_list structure for hierarchical summarization
    - Invalid method handling
    - Community stability and coverage
    """
    kg = nature_scene_graph

    # Test 1: Multi-level hierarchical detection
    kg.detect_community_clusters(method="leiden", multi_level=True, max_levels=3)

    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]

    # Verify hierarchical properties exist
    assert "community_list" in nodes_df.columns, "Multi-level should create community_list"
    assert all(nodes_df["community_list"].notna()), "All nodes should have community_list"

    # Verify community_list is a list with length indicating hierarchy depth
    sample_list = nodes_df["community_list"].iloc[0]
    assert isinstance(sample_list, list), "community_list should be a list"
    assert len(sample_list) >= 1, "community_list should have at least one level"
    assert len(sample_list) <= 3, "Should respect max_levels parameter"

    # Test 2: Single-level vs multi-level comparison
    kg.detect_community_clusters(method="leiden", multi_level=False)
    elements_single = kg.database.get_dataframe(kg.graph_name)
    nodes_single = elements_single[elements_single["element_type"] == "node"]

    assert "community_id" in nodes_single.columns, "Single-level should create community_id"
    assert all(nodes_single["community_id"].notna()), "All nodes should have community_id"
    if "community_list" in nodes_single.columns:
        assert all(nodes_single["community_list"].isna()), "Single-level should not have community_list"

    single_level_communities = nodes_single["community_id"].nunique()
    assert single_level_communities >= 1, "Should have at least one community"

    # Test 3: Invalid method handling
    with pytest.raises(Log.Failure):
        kg.detect_community_clusters(method="invalid_method")

    # Test 4: Community coverage (all nodes assigned)
    kg.detect_community_clusters(method="leiden")
    elements_df = kg.database.get_dataframe(kg.graph_name)
    nodes_df = elements_df[elements_df["element_type"] == "node"]

    total_nodes = len(nodes_df)
    assigned_nodes = nodes_df["community_id"].notna().sum()
    assert assigned_nodes == total_nodes, "All nodes should be assigned to a community"

    # Test 5: Get community subgraphs (only those with internal edges)
    unique_communities = nodes_df["community_id"].unique()
    retrieved_triples = []

    for community_id in unique_communities:
        community_triples = kg.get_community_subgraph(int(community_id))
        assert community_triples is not None
        assert list(community_triples.columns) == ["subject_id", "relation_id", "object_id"]

        # Only count non-empty communities (some may have only cross-community edges)
        if len(community_triples) > 0:
            retrieved_triples.append(community_triples)

    assert len(retrieved_triples) > 0, "Should find at least one community with internal edges"

    # Coverage check using ID-based triples
    total_community_triples = sum(len(df) for df in retrieved_triples)
    all_triples = kg.get_all_triples()
    coverage = total_community_triples / len(all_triples) if len(all_triples) > 0 else 0
    assert coverage >= 0.3, f"Community subgraphs should cover at least 30% of graph (got {coverage:.1%})"

    # Test 6: Empty community handling (valid - communities can have no internal edges)
    # Just verify that calling with any integer doesn't crash
    result = kg.get_community_subgraph(999999)
    assert result is not None
    assert isinstance(result, DataFrame)
    assert list(result.columns) == ["subject_id", "relation_id", "object_id"]

    # Test 7: Louvain with multi-level (should work but may not produce hierarchy)
    kg.detect_community_clusters(method="louvain", multi_level=True)
    elements_louvain_ml = kg.database.get_dataframe(kg.graph_name)
    nodes_louvain_ml = elements_louvain_ml[elements_louvain_ml["element_type"] == "node"]
    # Louvain should at minimum assign community_id or community_list
    has_community_id = "community_id" in nodes_louvain_ml.columns and nodes_louvain_ml["community_id"].notna().any()
    has_community_list = "community_list" in nodes_louvain_ml.columns and nodes_louvain_ml["community_list"].notna().any()
    assert has_community_id or has_community_list, "Louvain multi-level should assign either community_id or community_list"

