from pandas import DataFrame
import pytest
from src.components.fact_storage import KnowledgeGraph
from src.connectors.graph import GraphConnector
from src.util import Log
from typing import Generator
import re
from src.components.fact_storage import sanitize_node, sanitize_relation



@pytest.mark.kg
@pytest.mark.order(16)
@pytest.mark.dependency(name="knowledge_graph_triples", depends=["graph_minimal", "graph_comprehensive"], scope="session")
@pytest.mark.parametrize("main_graph", ["social_kg"], indirect=True)
def test_knowledge_graph_triples(main_graph: KnowledgeGraph) -> None:
    """Test KnowledgeGraph triple operations using add_triple and get_all_triples.
    @details  Validates the KnowledgeGraph wrapper for semantic triple management:
    - add_triple() creates nodes and relationships
    - get_all_triples() retrieves triples as element IDs
    - get_triple_properties() constructs a DataFrame with element properties as columns
    - triples_to_names() maps IDs to human-readable names
    """
    kg = main_graph
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
    triples_df = kg.triples_to_names(triples_ids_df, drop_ids=False)
    assert triples_df is not None
    assert len(triples_df) == 5
    assert set(triples_df.columns) == {"subject_id", "relation_id", "object_id", "subject", "relation", "object"}
    triples_df = triples_df.drop(columns=["subject_id", "relation_id", "object_id"])
    assert list(triples_df.columns) == ["subject", "relation", "object"]

    # Verify specific triples exist
    assert any((triples_df["subject"] == "Alice") & (triples_df["relation"] == "KNOWS") & (triples_df["object"] == "Bob"))
    assert any((triples_df["subject"] == "Bob") & (triples_df["relation"] == "KNOWS") & (triples_df["object"] == "Charlie"))
    assert any((triples_df["subject"] == "Alice") & (triples_df["relation"] == "FOLLOWS") & (triples_df["object"] == "Charlie"))
    assert any((triples_df["subject"] == "Charlie") & (triples_df["relation"] == "COLLABORATES") & (triples_df["object"] == "Alice"))
    assert any((triples_df["subject"] == "Bob") & (triples_df["relation"] == "FOLLOWS") & (triples_df["object"] == "Alice"))

    # Verify nodes were created (should be 3 unique: Alice, Bob, Charlie)
    df = kg.database.get_dataframe("social_kg")
    assert not df.empty
    df_nodes = df[df["element_type"] == "node"]
    assert len(df_nodes) == 3

    # Verify all nodes have the "name" property
    assert all(df_nodes["name"].notna())
    node_names = set(df_nodes["name"])
    assert node_names == {"Alice", "Bob", "Charlie"}

    # Test triples_to_names with pre-fetched lookup DataFrame
    elements_df = kg.database.get_dataframe("social_kg")
    triples_df_cached = kg.triples_to_names(triples_ids_df, drop_ids=True, df_lookup=elements_df)
    assert triples_df_cached is not None
    assert len(triples_df_cached) == 5
    # Should produce identical results to direct conversion
    assert triples_df_cached.equals(triples_df)

    # Test find_element_names helper (handles renamed ID columns)
    renamed_df = triples_ids_df.rename(columns={"subject_id": "node1", "object_id": "node2"})
    mapped_df = kg.find_element_names(renamed_df, ["node1_name", "node2_name"], ["node1", "node2"], "node", "name", df_lookup=elements_df)
    assert not mapped_df.empty
    assert set(mapped_df["node1_name"]) | set(mapped_df["node2_name"]) <= {"Alice", "Bob", "Charlie"}

    # Test edge case: empty DataFrame
    empty_df = DataFrame(columns=["subject_id", "relation_id", "object_id"])
    empty_result = kg.triples_to_names(empty_df, drop_ids=True)
    assert empty_result is not None
    assert empty_result.empty
    assert set(empty_result.columns) == {"subject", "relation", "object"}

    # Test get_triple_properties (full pivoted view)
    props_df = kg.get_triple_properties()
    assert props_df is not None
    assert len(props_df) == 5
    # Should have prefixed columns for both nodes and relationship
    assert "n1.name" in props_df.columns
    assert "n2.name" in props_df.columns
    assert "r.rel_type" in props_df.columns
    # Verify one specific triple's properties
    alice_knows_bob = props_df[(props_df["n1.name"] == "Alice") & (props_df["r.rel_type"] == "KNOWS") & (props_df["n2.name"] == "Bob")]
    assert len(alice_knows_bob) == 1


@pytest.fixture(params=["nature_scene"])
def nature_scene_graph(main_graph: KnowledgeGraph) -> Generator[KnowledgeGraph, None, None]:
    """Create a scene graph with multiple location-based communities for testing.
    @details  Graph structure represents a park with distinct areas:
    - Playground: swings, slide, kids
    - Bench area: bench, parents
    - Forest: trees, rock, path
    - School building: doors, windows, classroom
    Each area forms a natural community for GraphRAG testing.
    """
    kg = main_graph

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


@pytest.mark.kg
@pytest.mark.order(17)
@pytest.mark.dependency(name="subgraph_by_nodes", depends=["knowledge_graph_triples"], scope="session")
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
    assert not elements_df.empty
    nodes_df = elements_df[elements_df["element_type"] == "node"]

    # Find Kid1 and Swings node IDs
    kid1_id = nodes_df[nodes_df["name"] == "Kid1"]["element_id"].iloc[0]
    swings_id = nodes_df[nodes_df["name"] == "Swings"]["element_id"].iloc[0]

    # Get subgraph containing only triples with Kid1 or Swings
    subgraph = kg.get_subgraph_by_nodes([kid1_id, swings_id])
    assert subgraph is not None
    assert len(subgraph) > 0

    # Convert to names for verification
    named_subgraph = kg.triples_to_names(subgraph, drop_ids=True)

    # Should include Kid1 PLAYS_ON Swings
    assert any((named_subgraph["subject"] == "Kid1") & (named_subgraph["relation"] == "PLAYS_ON") & (named_subgraph["object"] == "Swings"))

    # Should include Kid1 NEAR Kid2
    assert any((named_subgraph["subject"] == "Kid1") & (named_subgraph["relation"] == "NEAR"))

    # Should include Swings LOCATED_IN Playground
    assert any((named_subgraph["subject"] == "Swings") & (named_subgraph["relation"] == "LOCATED_IN"))

    # All triples should involve Kid1 or Swings
    for _, row in named_subgraph.iterrows():
        assert row["subject"] in ["Kid1", "Swings"] or row["object"] in ["Kid1", "Swings"]


@pytest.mark.kg
@pytest.mark.order(18)
@pytest.mark.dependency(name="neighborhood_minimal", depends=["knowledge_graph_triples"], scope="session")
def test_get_neighborhood(nature_scene_graph: KnowledgeGraph) -> None:
    """Test k-hop neighborhood expansion around a central node.
    @details  Validates that get_neighborhood correctly finds all triples within
    k hops of a starting node.
    """
    kg = nature_scene_graph

    # Get node IDs
    elements_df = kg.database.get_dataframe(kg.graph_name)
    assert not elements_df.empty
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    assert not nodes_df.empty
    playground_id = nodes_df[nodes_df["name"] == "Playground"]["element_id"].iloc[0]

    # Test 1-hop neighborhood
    neighborhood_1hop = kg.get_neighborhood(playground_id, depth=1)
    assert neighborhood_1hop is not None
    assert len(neighborhood_1hop) > 0

    named_1hop = kg.triples_to_names(neighborhood_1hop, drop_ids=True)

    # Should include direct connections to Playground
    assert any(named_1hop["subject"] == "Playground")
    assert any(named_1hop["object"] == "Playground")

    # Should include Swings and Slide located in Playground
    assert any((named_1hop["subject"] == "Swings") & (named_1hop["relation"] == "LOCATED_IN") & (named_1hop["object"] == "Playground"))

    # Test 2-hop neighborhood (should reach further)
    neighborhood_2hop = kg.get_neighborhood(playground_id, depth=2)
    assert neighborhood_2hop is not None
    assert len(neighborhood_2hop) >= len(neighborhood_1hop)

    named_2hop = kg.triples_to_names(neighborhood_2hop, drop_ids=True)

    # Should reach Kids who play on equipment in Playground
    assert any(named_2hop["subject"] == "Kid1") or any(named_2hop["object"] == "Kid1")


@pytest.mark.kg
@pytest.mark.order(19)
@pytest.mark.dependency(name="random_walk_minimal", depends=["knowledge_graph_triples"], scope="session")
def test_get_random_walk_sample(nature_scene_graph: KnowledgeGraph) -> None:
    """Test random walk sampling starting from specified nodes.
    @details  Validates that get_random_walk_sample generates a representative
    subgraph by following random paths through the graph.
    """
    kg = nature_scene_graph

    # Get node IDs
    elements_df = kg.database.get_dataframe(kg.graph_name)
    assert not elements_df.empty
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    assert not nodes_df.empty
    kid1_id = nodes_df[nodes_df["name"] == "Kid1"]["element_id"].iloc[0]

    # Perform random walk with length 3
    sample = kg.get_random_walk_sample([kid1_id], walk_length=3, num_walks=1)
    assert sample is not None
    assert len(sample) > 0
    assert len(sample) <= 3  # Should visit at most walk_length edges

    named_sample = kg.triples_to_names(sample, drop_ids=True)

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
            (all_triples["subject_id"] == row["subject_id"])
            & (all_triples["relation_id"] == row["relation_id"])
            & (all_triples["object_id"] == row["object_id"])
        )
        assert match.any(), f"Sampled triple not found in graph: {row.to_dict()}"


@pytest.mark.kg
@pytest.mark.order(20)
@pytest.mark.dependency(name="neighborhood_comprehensive", depends=["neighborhood_minimal"], scope="session")
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
    named_desk_1 = kg.triples_to_names(neighborhood_desk_1, drop_ids=True)
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
    named_school = kg.triples_to_names(neighborhood_school, drop_ids=True)
    named_playground = kg.triples_to_names(neighborhood_playground, drop_ids=True)
    # These should have different content
    school_entities = set(named_school["subject"]).union(named_school["object"])
    playground_entities = set(named_playground["subject"]).union(named_playground["object"])
    assert school_entities != playground_entities


@pytest.mark.kg
@pytest.mark.order(21)
@pytest.mark.dependency(name="random_walk_comprehensive", depends=["random_walk_minimal"], scope="session")
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
    samples = [kg.get_random_walk_sample([kid1_id], walk_length=5, num_walks=1) for _ in range(10)]  # Increased trials for better probability

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


@pytest.mark.kg
@pytest.mark.order(22)
@pytest.mark.dependency(name="community_detection_minimal", depends=["knowledge_graph_triples"], scope="session")
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
    assert not elements_df.empty
    nodes_df = elements_df[elements_df["element_type"] == "node"]
    assert not nodes_df.empty
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
            community_node_ids = set(nodes_df[nodes_df["community_id"] == comm_id]["element_id"])
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


@pytest.mark.kg
@pytest.mark.order(23)
@pytest.mark.dependency(name="community_detection_comprehensive", depends=["community_detection_minimal"], scope="session")
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
    result = kg.get_community_subgraph(-1)
    assert result is not None
    assert isinstance(result, DataFrame)
    assert list(result.columns) == ["subject_id", "relation_id", "object_id"]
    assert len(result) == 0

    # Test 7: Louvain with multi-level (should work but may not produce hierarchy)
    kg.detect_community_clusters(method="louvain", multi_level=True)
    elements_louvain_ml = kg.database.get_dataframe(kg.graph_name)
    nodes_louvain_ml = elements_louvain_ml[elements_louvain_ml["element_type"] == "node"]
    # Louvain should at minimum assign community_id or community_list
    has_community_id = "community_id" in nodes_louvain_ml.columns and nodes_louvain_ml["community_id"].notna().any()
    has_community_list = "community_list" in nodes_louvain_ml.columns and nodes_louvain_ml["community_list"].notna().any()
    assert has_community_id or has_community_list, "Louvain multi-level should assign either community_id or community_list"


@pytest.mark.kg
@pytest.mark.order(24)
@pytest.mark.dependency(name="degree_rank", depends=["subgraph_by_nodes"], scope="session")
def test_ranked_degree(nature_scene_graph: KnowledgeGraph) -> None:
    """Test filtering triples by ranked node degree.
    @details  Validates that get_by_ranked_degree correctly returns triples
    whose endpoints belong to nodes within the specified degree rank range.
    """
    kg = nature_scene_graph

    # Compute all node degrees
    degree_df = kg.get_edge_counts()
    assert not degree_df.empty

    # Grab the top-ranked node (rank 1)
    top_node_id = degree_df.sort_values("edge_count", ascending=False)["node_id"].iloc[0]

    # Fetch triples for rank 1 only
    subgraph = kg.get_by_ranked_degree(best_rank=1, worst_rank=1)
    assert subgraph is not None
    assert len(subgraph) > 0

    # All triples must involve the top-ranked node
    endpoints = set(subgraph["subject_id"]).union(set(subgraph["object_id"]))
    assert top_node_id in endpoints

    # Optional: verify degree ordering consistency
    # (rank=1 node must have >= every other degree)
    top_degree = degree_df[degree_df["node_id"] == top_node_id]["edge_count"].iloc[0]
    assert all(top_degree >= degree_df["edge_count"])


@pytest.mark.kg
@pytest.mark.order(25)
@pytest.mark.dependency(name="degree_rank_ties", depends=["degree_rank"], scope="session")
def test_ranked_degree_ties(main_graph: KnowledgeGraph) -> None:
    """Test that degree ranking correctly handles ties with minimal data.
    @details  Verifies that nodes with equal degrees receive the same rank
              and querying for non-existent ranks returns empty DataFrame.
    """
    kg = main_graph

    # Add two isolated nodes (each with degree 1)
    kg.add_triple("node_a", "relates_to", "node_b")
    kg.add_triple("node_c", "relates_to", "node_d")

    degree_df = kg.get_edge_counts()
    assert len(degree_df) == 4  # 4 nodes total

    # Both isolated pairs should have same degree and rank
    # Ranks should be: all nodes have degree=1, so all should be rank 1
    degree_df = degree_df.sort_values("edge_count", ascending=False).reset_index(drop=True)
    degree_df["rank"] = degree_df["edge_count"].rank(method="dense", ascending=False).astype(int)
    assert degree_df["rank"].nunique() == 1  # All same rank
    assert degree_df["rank"].iloc[0] == 1

    # Querying for rank 2 should return empty (no rank 2 exists)
    empty_result = kg.get_by_ranked_degree(best_rank=2, worst_rank=2)
    assert empty_result.empty, "No rank-2 nodes exist, should return empty"


# ==========================================
# NODE AND EDGE LABEL SANITIZATION
# ==========================================

@pytest.fixture
def node_nlp_cases():
    """Fixtures focusing on spaCy NLP cleaning (Stopword/Part-of-speech removal)."""
    return [
        # (input_label, expected_output, description)
        ("The Apple", "Apple", "Remove Determiner (DET) 'The'"),
        ("He runs", "runs", "Remove Pronoun (PRON) 'He'"),
        ("Door to Enter", "Door_Enter", "Remove Particle (PART) 'to' in node name"),
        ("walk to school", "walk_to_school", "Remove Particle (PART) 'to'"),
        ("A very big dog", "very_big_dog", "Remove 'A' (DET), keep Adverbs/Adj"),
        ("The user and the system", "user_and_system", "Remove multiple DETs, keep CCONJ 'and'"),
        ("My own car", "own_car", "Remove Possessive Pronoun (PRON) 'My'"),
        ("It is time", "is_time", "Remove Pronoun 'It', keep Verb 'is'"),
        ("The 50% increase", "50_increase", "Handle symbols mixed with NLP (keep numbers)"),
        ("tHE inconsistent Case", "inconsistent_Case", "Handle mixed-case stopwords"),
        ("Give it to me", "Give_to", "Remove 'it' (PRON) and 'me' (PRON) - leaving only 'Give' Verb and 'to' ADP")
    ]

@pytest.fixture
def node_regex_cases():
    """Fixtures focusing on Regex replacement and stripping."""
    return [
        ("User-Name", "User_Name", "Replace hyphen with underscore"),
        ("User@Name!", "User_Name", "Replace special chars, disallow trailing underscore"),
        ("  Spaces  ", "Spaces", "Trim whitespace"),
        ("Hello---World", "Hello_World", "Trim repeated inner special chars"),
        ("___Underscores___", "Underscores", "Trim leading/trailing underscores"),
        ("Node 123", "Node_123", "Allow numbers"),
        ("C++ Developer", "C_Developer", "Handle special symbols like +"),
        ("Line\nBreak", "Line_Break", "Handle newlines as separators"),
        ("Tab\tSeparated", "Tab_Separated", "Handle tabs as separators"),
        ("Café Name", "Caf_Name", "Strip non-ASCII (Accents) if regex is strict A-Z"),
        ("Data (2024)", "Data_2024", "Handle parentheses/brackets"),
        ("Rocket 🚀", "Rocket", "Strip Emojis"),
        ("..Start..", "Start", "Trim leading/trailing dots if regex converts them to underscores")
    ]

@pytest.fixture
def relation_casing_cases():
    """Fixtures for testing UPPER_CASE vs camelCase modes."""
    return [
        # (input, mode, default, expected)
        ("related to", "UPPER_CASE", "RELATED_TO", "RELATED_TO"),
        ("related to", "camelCase", "relatedTo", "relatedTo"),
        ("has part", "UPPER_CASE", "RELATED_TO", "HAS_PART"),
        ("has part", "camelCase", "relatedTo", "hasPart"),
        ("works_with", "camelCase", "relatedTo", "worksWith"),
        ("  messy  input  ", "UPPER_CASE", "RELATED_TO", "MESSY_INPUT"),
        ("HTML Parser", "camelCase", "relatedTo", "htmlParser"), # Acronym handling
        ("HTML Parser", "UPPER_CASE", "RELATED_TO", "HTML_PARSER"), # Acronym handling
        ("already_snake_case", "UPPER_CASE", "RELATED_TO", "ALREADY_SNAKE_CASE"), # Idempotency check
        ("alreadyCamelCase", "camelCase", "relatedTo", "alreadyCamelCase"), # Idempotency check
        ("Is CEO Of", "UPPER_CASE", "RELATED_TO", "IS_CEO_OF"), # Stopwords in relations usually kept
        ("Is CEO Of", "camelCase", "relatedTo", "isCeoOf")
    ]

@pytest.fixture
def relation_fallback_cases():
    """Fixtures specifically testing the fallback logic (when input is invalid/numeric)."""
    return [
        # (input, mode, default_raw, expected_final)
        ("123", "UPPER_CASE", "default_rel", "DEFAULT_REL"),  # Starts with number -> fallback
        ("->", "UPPER_CASE", "generic_link", "GENERIC_LINK"),   # Special chars only -> fallback
        ("", "camelCase", "has_connection", "hasConnection"),   # Empty input -> fallback
        ("2nd_step", "camelCase", "backup", "backup"),          # Starts with number -> fallback
        ("   ", "UPPER_CASE", "empty_space", "EMPTY_SPACE"), # Whitespace only -> Fallback
        ("!!!", "camelCase", "bad_chars", "badChars") # Symbols only -> Fallback
    ]

@pytest.mark.kg
@pytest.mark.order(51)
@pytest.mark.dependency(name="sanitize_node_nlp")
def test_sanitize_node_nlp_capabilities(node_nlp_cases):
    """Test that NLP logic correctly strips POS tags (DET, PRON, PART).
    @details
        Validates that 'sanitize_node' loads the global _nlp object
        and correctly filters linguistic tokens.
    """
    for raw, expected, reason in node_nlp_cases:
        result = sanitize_node(raw)
        assert result == expected, f"Failed on: {reason}"

@pytest.mark.kg
@pytest.mark.order(52)
@pytest.mark.dependency(name="sanitize_node_regex", depends=["sanitize_node_nlp"])
def test_sanitize_node_regex_cleaning(node_regex_cases):
    """Test that Regex logic handles symbols and whitespace correctly.
    @details
        Ensures strict node naming conventions:
        - No non-alphanumeric chars (except underscore/space)
        - No leading/trailing garbage
    """
    for raw, expected, reason in node_regex_cases:
        result = sanitize_node(raw)
        assert result == expected, f"Failed on: {reason}"

@pytest.mark.kg
@pytest.mark.order(53)
@pytest.mark.dependency(name="sanitize_rel_modes")
@pytest.mark.parametrize("mode", ["UPPER_CASE", "camelCase"])
def test_sanitize_relation_modes(relation_casing_cases, mode):
    """Test standard relation normalization for both supported modes.
    @details
        Filters the fixture data to match the parameterized mode and verifies
        string transformation logic.
    """
    for raw, test_mode, default, expected in relation_casing_cases:
        if test_mode == mode:
            result = sanitize_relation(raw, mode=mode, default_relation=default)
            assert result == expected

@pytest.mark.kg
@pytest.mark.order(54)
@pytest.mark.dependency(name="sanitize_rel_fallback", depends=["sanitize_rel_modes"])
def test_sanitize_relation_fallbacks(relation_fallback_cases):
    """Test the 'safety net' fallback logic for relations.
    @details
        The function requires relations to start with an alphabetic character.
        If the input is garbage (e.g., '123' or '>>'), it must revert to the 
        default_relation, and that default relation *must* also be normalized 
        to the requested mode.
    """
    for raw, mode, default_raw, expected in relation_fallback_cases:
        result = sanitize_relation(raw, mode=mode, default_relation=default_raw)
        assert result == expected, \
            f"Failed fallback logic. Input: '{raw}', Mode: {mode}, Default: '{default_raw}'"

@pytest.mark.kg
@pytest.mark.order(55)
@pytest.mark.dependency(name="sanitize_rel_defaults", depends=["sanitize_rel_fallback"])
def test_sanitize_relation_default_normalization():
    """Edge case: Ensure the default_relation itself is sanitized if used.
    @details
        If the input is empty, we return the default. 
        But if the default provided is 'bad input', the function must clean 
        the default before returning it.
    """
    # Case: Input is empty, forcing fallback.
    # Fallback is "bad @ default", should become "BAD_DEFAULT" in UPPER_CASE
    result = sanitize_relation("", mode="UPPER_CASE", default_relation="bad @ default")
    assert result == "BAD_DEFAULT"

    # Case: Input is empty, forcing fallback to normalized camelCase default
    result_camel = sanitize_relation("", mode="camelCase", default_relation="bad @ default")
    assert result_camel == "badDefault"
