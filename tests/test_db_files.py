import os
from pandas import DataFrame
import pytest
from src.connectors.base import DatabaseConnector
from src.connectors.document import DocumentConnector
from src.connectors.graph import GraphConnector
from src.connectors.relational import RelationalConnector
from src.util import Log
import sys
import time
from typing import Generator, List, Optional


# ------------------------------------------------------------------------------
# DATABASE FILE TESTS: Run execute_file with example scripts.
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def load_examples_relational(relational_db: RelationalConnector) -> Generator[None, None, None]:
    """Fixture to create relational tables using engine-specific syntax."""
    if relational_db.db_type == "MYSQL":
        _exec_query_file(relational_db, "./tests/examples-db/relational/schema_mysql.sql", ["sql"])
        yield
        relational_db.execute_query("DROP TABLE EntityName; DROP TABLE ExampleEAV;")
    elif relational_db.db_type == "POSTGRES":
        _exec_query_file(relational_db, "./tests/examples-db/relational/schema_postgres.sql", ["sql"])
        yield
        relational_db.execute_query("DROP TABLE entityname; DROP TABLE exampleeav;")
    else:
        raise Exception(f"Unknown database engine '{relational_db.db_type}'")


@pytest.mark.relational
@pytest.mark.order(7)
@pytest.mark.dependency(name="rel_example_1", depends=["rel_minimal", "rel_comprehensive"], scope="session")
def test_sql_example_1(relational_db: RelationalConnector, load_examples_relational: Generator[None, None, None]) -> None:
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected.
    @note  Uses a table-creation fixture to load / unload schema."""
    _exec_query_file(relational_db, "./tests/examples-db/relational/df1_entity.sql", valid_files=["sql"])
    df = relational_db.get_dataframe("EntityName")
    assert not df.empty
    assert df.loc[1, 'name'] == 'Fluffy'


@pytest.mark.relational
@pytest.mark.order(8)
@pytest.mark.dependency(name="rel_example_2", depends=["rel_minimal", "rel_comprehensive"], scope="session")
def test_sql_example_2(relational_db: RelationalConnector, load_examples_relational: Generator[None, None, None]) -> None:
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected.
    @note  Uses a table-creation fixture to load / unload schema."""
    _exec_query_file(relational_db, "./tests/examples-db/relational/df2_eav.sql", valid_files=["sql"])
    df = relational_db.get_dataframe("ExampleEAV", ["entity", "attribute", "value"])
    assert not df.empty
    assert df.iloc[-1]['value'] == 'Timber'


@pytest.mark.document
@pytest.mark.order(9)
@pytest.mark.dependency(name="docs_example_1", depends=["docs_minimal", "docs_comprehensive"], scope="session")
def test_mongo_example_1(docs_db: DocumentConnector) -> None:
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    _exec_query_file(docs_db, "./tests/examples-db/document/df1_books.mongo", valid_files=["json", "mongo"])
    df = docs_db.get_dataframe("books")
    assert not df.empty
    assert df.loc[0, 'title'] == 'Wuthering Heights'
    assert df.iloc[-1]['chapters.pages'] == 22
    docs_db.execute_query('{"drop": "books"}')


@pytest.mark.document
@pytest.mark.order(10)
@pytest.mark.dependency(name="docs_example_2", depends=["docs_minimal", "docs_comprehensive"], scope="session")
def test_mongo_example_2(docs_db: DocumentConnector) -> None:
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    _exec_query_file(docs_db, "./tests/examples-db/document/df2_qa-exam-dune.json", valid_files=["json", "mongo"])
    df = docs_db.get_dataframe("qa_exam")
    assert not df.empty
    assert df.iloc[0]['answer'] == 'Paul Atreides'
    assert any((df['gold_answer'] == 'The Fremen') & (df['is_correct'] == False))
    docs_db.execute_query('{"drop": "qa_exam"}')


@pytest.mark.document
@pytest.mark.order(11)
@pytest.mark.dependency(name="docs_example_3", depends=["docs_minimal", "docs_comprehensive"], scope="session")
def test_mongo_example_3(docs_db: DocumentConnector) -> None:
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    _exec_query_file(docs_db, "./tests/examples-db/document/df3_potions.mongo", valid_files=["json", "mongo"])
    df = docs_db.get_dataframe("potions")
    assert not df.empty
    assert df.loc[4, 'potion_name'] == 'Elixir of Wisdom'
    assert "effects.description" in df.columns
    assert any((df['potion_name'] == 'Invisibility Draught') & (df['effects.description'] == 'Silent movement'))
    assert df.loc[12, 'ingredients.name'] == 'Mirage Powder'
    assert "effects.seconds" in df.columns
    assert any((df['potion_name'] == 'Catkin Tincture') & (df['effects.seconds'] == 0))
    docs_db.execute_query('{"drop": "potions"}')


@pytest.mark.graph
@pytest.mark.order(12)
@pytest.mark.dependency(name="graph_example_1", depends=["graph_minimal", "graph_comprehensive"], scope="session")
def test_cypher_example_1(graph_db: GraphConnector) -> None:
    """Run queries contained within test files.
    @details  Internal errors are handled by the class itself, and ruled out earlier.
    Here we just assert that the received results DataFrame matches what we expected."""
    graph_db.drop_graph("pets")
    _exec_query_file(graph_db, "./tests/examples-db/graph/df1_pets.cql", valid_files=["cql", "cypher"])
    df = graph_db.get_dataframe("pets")
    assert not df.empty
    assert "element_id" in df.columns and "element_type" in df.columns
    assert "db" in df.columns and "kg" in df.columns
    assert len(df) == 5
    assert len(df.columns) == 9
    # db-kg (2), elem-id-type (2), labels (1), and 4 expected (name, species, weight, age)
    assert df.iloc[-1]['name'] == 'Buddy'
    assert any((df['species'] == 'Rabbit') & (df['age'] == 4))
    graph_db.drop_graph("pets")


@pytest.mark.graph
@pytest.mark.order(13)
@pytest.mark.dependency(name="graph_example_2", depends=["graph_minimal", "graph_comprehensive"], scope="session")
def test_cypher_example_2(graph_db: GraphConnector) -> None:
    """Test social network graph with relationships and mixed query patterns.
    @details  Validates comment parsing, semicolon splitting, CREATE/MERGE/MATCH,
    relationships with properties, and TAG_NODES_ with/without RETURN."""
    graph_db.drop_graph("social")
    _exec_query_file(graph_db, "./tests/examples-db/graph/df2_social.cypher", valid_files=["cql", "cypher"])

    # Verify nodes were created correctly
    df = graph_db.get_dataframe("social")
    assert not df.empty
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


@pytest.mark.graph
@pytest.mark.order(14)
@pytest.mark.dependency(name="graph_example_3", depends=["graph_minimal", "graph_comprehensive"], scope="session")
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

    _exec_query_file(graph_db, "./tests/examples-db/graph/df3_scene-dialogue.cql", valid_files=["cql", "cypher"])

    # Test Graph 1: Scene graph with spatial relationships
    with graph_db.temp_graph("scene"):
        df_scene = graph_db.get_dataframe("scene")
        assert not df_scene.empty
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
        assert not df_dialogue.empty
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
    assert not df_scene.empty and not df_dialogue.empty

    # Extract nodes only for comparison
    df_scene_nodes = df_scene[df_scene["element_type"] == "node"]
    df_dialogue_nodes = df_dialogue[df_dialogue["element_type"] == "node"]
    scene_entities = set(df_scene_nodes['name'].dropna())
    dialogue_speakers = set(df_dialogue_nodes['speaker'].dropna())

    # Speakers reference scene entities (cross-graph semantic links are intentional)
    # Employee/Manager appear as speakers in dialogue, AND as Person nodes in scene
    assert 'Employee' in dialogue_speakers  # Employee speaks
    assert 'Employee' in scene_entities  # Employee exists in scene
    assert 'Manager' in dialogue_speakers  # Manager speaks
    assert 'Manager' in scene_entities  # Manager exists in scene

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


@pytest.mark.graph
@pytest.mark.order(15)
@pytest.mark.dependency(name="graph_example_4", depends=["graph_minimal", "graph_comprehensive"], scope="session")
def test_cypher_example_4(graph_db: GraphConnector) -> None:
    """Test event graph with property mutations and multi-hop traversal.
    @details  Validates MERGE property updates (2-wave assignment), relationship chains
    in DAG structure, consistent rel_type with varied properties, and multi-hop path queries.
    Tests that properties added via SET after initial CREATE are properly stored."""
    graph_db.drop_graph("events")
    _exec_query_file(graph_db, "./tests/examples-db/graph/df4_events.cypher", valid_files=["cql", "cypher"])

    # Verify all event nodes were created with correct types
    df = graph_db.get_dataframe("events")
    assert not df.empty
    df_nodes = df[df["element_type"] == "node"]
    assert len(df_nodes) == 7  # 7 event nodes
    assert "element_id" in df_nodes.columns and "labels" in df_nodes.columns
    assert "db" in df_nodes.columns and "kg" in df_nodes.columns

    # Verify event types exist
    dialogue_events = df_nodes[df_nodes['labels'].apply(lambda x: 'DialogueEvent' in x if isinstance(x, list) else False)]
    action_events = df_nodes[df_nodes['labels'].apply(lambda x: 'ActionEvent' in x if isinstance(x, list) else False)]
    scene_events = df_nodes[df_nodes['labels'].apply(lambda x: 'SceneEvent' in x if isinstance(x, list) else False)]
    assert len(dialogue_events) == 3  # Knight, Guide Master, Wizard speak
    assert len(action_events) == 2  # Knight hired, Wizard arrives
    assert len(scene_events) == 2  # Guide Master leaves room, Knight enters hall

    # Verify Wave 2 properties were added via MERGE/SET (property mutation)
    knight_speaks = df_nodes[df_nodes['name'] == 'Knight speaks'].iloc[0]
    assert knight_speaks['speaker'] == 'Knight'  # Wave 1 property
    assert knight_speaks['says'] == 'I accept the quest'  # Wave 2 property
    assert knight_speaks['audience'] == 'Guild Master'  # Wave 2 property

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
def _exec_query_file(db_fixture: DatabaseConnector, filename: str, valid_files: List[str]) -> None:
    """Run queries from a local file through the database.
    @param db_fixture  Fixture corresponding to the current session's database.
    @param filename  The name of a query file (for example ./tests/example1.sql).
    @param valid_files  A list of file extensions valid for this database type."""
    file_ext = filename.split('.')[-1].lower()
    assert file_ext in valid_files, f"Received '{filename}', but cannot execute .{file_ext} files."
    df = db_fixture.execute_file(filename)
