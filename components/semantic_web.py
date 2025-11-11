from components.fact_storage import GraphConnector
from pandas import DataFrame, option_context
from src.util import Log
from typing import Any, List, Optional, Tuple, Dict
import re

class KnowledgeGraph:
    """Manages a single graph within Neo4j.
    @details
        - Handles safe conversion of LLM output to structured triples.
        - Provides helper functions to add and retrieve triples.
    """

    def __init__(self, name: str, database: GraphConnector, verbose: bool = False) -> None:
        ## The name of this graph. Matches node.kg for all nodes in the graph database.
        self.graph_name = name
        ## Reference to a pre-configured graph database wrapper.
        self.database = database
        ## Whether to print debug messages.
        self.verbose = verbose

    def add_triple(self, subject: str, relation: str, object_: str) -> None:
        """Add a semantic triple to the graph using raw Cypher.
        @param subject  A string representing the entity performing an action.
        @param relation  A string describing the action.
        @param object_  A string representing the entity being acted upon.
        @note  LLM output should be pre-normalized using @ref KnowledgeGraph.normalize_triples.
        @throws Log.Failure  If the triple cannot be added to our graph database.
        """

        # Normalize already-cleaned inputs for Cypher safety
        relation = re.sub(r"[^A-Za-z0-9_]", "_", relation).upper().strip("_")
        subject = re.sub(r"[^A-Za-z0-9_ ]", "_", subject).strip("_ ")
        object_ = re.sub(r"[^A-Za-z0-9_ ]", "_", object_).strip("_ ")
        if not relation or not subject or not object_:
            raise Log.Failure(Log.gr_db + Log.kg, f"Invalid triple: ({subject})-[:{relation}]->({object_})")

        # Temporarily switch to this graph's context for the operation
        with self.database.temp_graph(self.graph_name):
            # Merge subject/object and connect via relation
            query = f"""
            MERGE (s {{name: '{subject}', db: '{self.database.database_name}', kg: '{self.graph_name}'}})
            MERGE (o {{name: '{object_}', db: '{self.database.database_name}', kg: '{self.graph_name}'}})
            MERGE (s)-[r:{relation}]->(o)
            RETURN s, r, o
            """

            try:
                df = self.database.execute_query(query, _filter_results=False)
                if df is not None:
                    Log.success(Log.gr_db + Log.kg, f"Added triple: ({subject})-[:{relation}]->({object_})", self.verbose)
            except Exception as e:
                raise Log.Failure(Log.gr_db + Log.kg, f"Failed to add triple: ({subject})-[:{relation}]->({object_})") from e
    
    def get_triple_properties(self) -> Optional[DataFrame]:
        """Pivot the graph elements DataFrame to expose node and relationship properties as columns.
        @details
        - Builds a joined view of properties from both nodes (n1, n2) and the relationship (r).
        - Removes redundant fields such as: db, kg, element_type, start_node_id, and end_node_id.
        - Usage: n1.element_id, r.rel_type, n2.name, etc.
        @return  DataFrame where each row represents one triple (n1, r, n2).
        @throws Log.Failure  If the elements DataFrame cannot be loaded or pivoting fails.
        """
        try:
            elements_df = self.database.get_dataframe(self.graph_name)
            if elements_df is None or elements_df.empty:
                raise Log.Failure(Log.kg + Log.gr_rag, Log.bad_triples(self.graph_name))
    
            # Split nodes and relationships
            nodes = elements_df[elements_df["element_type"] == "node"].drop(
                columns=["element_type", "db", "kg"], errors="ignore"
            )
            rels = elements_df[elements_df["element_type"] == "relationship"].drop(
                columns=["element_type", "db", "kg"], errors="ignore"
            )
    
            # Join relationship to its start (n1) and end (n2) nodes
            triples_df = (
                rels.merge(nodes.add_prefix("n1."), left_on="start_node_id", right_on="n1.element_id")
                    .merge(nodes.add_prefix("n2."), left_on="end_node_id", right_on="n2.element_id")
            )
    
            triples_df = triples_df.drop(columns=["start_node_id", "end_node_id"], errors="ignore")
            return triples_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to pivot triple properties: {e}") from e


    def print_nodes(self, max_rows: int = 20, max_col_width: int = 50) -> None:
        """Print all nodes and edges in the current pseudo-database with row/column formatting."""
        nodes_df = self.database.get_dataframe(self.graph_name)
        if nodes_df is None:
            return

        # Set pandas display options only within scope
        with option_context("display.max_rows", max_rows, "display.max_colwidth", max_col_width):
            print(f"Graph nodes ({len(nodes_df)} total):")
            print(nodes_df)

    def print_triples(self, max_rows: int = 20, max_col_width: int = 50) -> None:
        """Print all nodes and edges in the current pseudo-database with row/column formatting."""
        triples_df = self.get_all_triples()
        if triples_df is None:
            return

        # Set pandas display options only within scope
        with option_context("display.max_rows", max_rows, "display.max_colwidth", max_col_width):
            print(f"Graph triples ({len(triples_df)} total):")
            print(triples_df)



    # ------------------------------------------------------------------------
    # Subgraph Selection
    # ------------------------------------------------------------------------
    
    def get_all_triples(self) -> DataFrame:
        """Return all triples in the specified graph as a pandas DataFrame.
        @return  Returns (subject, relation, object) columns only.
        @throws Log.Failure  If the query fails to retrieve or process the DataFrame.
        """
        try:
            triples_df = self.get_triple_property_df()
            cols = ["subject", "relation", "object"]

            if triples_df is None or triples_df.empty:
                Log.success(Log.gr_db + Log.kg, "Found 0 triples in graph.", self.verbose)
                return DataFrame(columns=cols)

            # Extract and rename columns
            triples_df = triples_df[["n1.name", "r.rel_type", "n2.name"]].rename(
                columns={"n1.name": "subject", "r.rel_type": "relation", "n2.name": "object"}
            )
            Log.success(Log.gr_db + Log.kg, f"Found {len(triples_df)} triples in graph.", self.verbose)
            return triples_df


    def get_subgraph_by_nodes(self, node_ids: List[str]) -> DataFrame:
        """Return all triples where subject or object is in the specified node list.
        @param node_ids  List of node element IDs to filter by.
        @return  DataFrame with columns: subject, relation, object
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        pass
    
    def get_neighborhood(self, node_id: str, depth: int = 1) -> DataFrame:
        """Get k-hop neighborhood around a central node.
        @details  Returns all triples within k hops of the specified node. A 1-hop neighborhood
        includes all direct neighbors, 2-hop includes neighbors-of-neighbors, etc.
        @param node_id  The element ID of the central node.
        @param depth  Number of hops to traverse (default: 1).
        @return  DataFrame with columns: subject, relation, object
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        pass
    
    def get_random_walk_sample(self, start_nodes: List[str], walk_length: int, num_walks: int = 1) -> DataFrame:
        """Sample subgraph using random walk traversal starting from specified nodes.
        @details  Performs random walks to sample a representative subgraph. More diverse than
        degree-based filtering and better preserves graph structure. Each walk starts from a
        randomly selected node in start_nodes and continues for walk_length steps.
        @param start_nodes  List of node IDs to use as starting points.
        @param walk_length  Number of steps in each random walk.
        @param num_walks  Number of random walks to perform (default: 1).
        @return  DataFrame with columns: subject, relation, object
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        pass
    
    def get_community_subgraph(self, community_id: str) -> DataFrame:
        """Return all triples belonging to a specific GraphRAG community.
        @details
        - Communities are densely connected subgraphs detected via clustering algorithms.
        - This enables GraphRAG-style hierarchical summarization where each community can be
        summarized independently. Requires nodes to have a 'community_id' property assigned.
        - Afterwards, you may run a summary step which generates community summaries for each cluster (as described in the paper).
        @param community_id  The identifier of the community to retrieve.
        @return  DataFrame with columns: subject, relation, object
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame or community detection has not been run.
        """
        try:
            triples_df = self.get_triple_properties()
            if triples_df is None:
                raise Log.Failure(Log.kg + Log.gr_rag, Log.bad_triples(self.graph_name))

            # Only nodes are tagged. Include triples where both nodes match community ID.
            triples_df = triples_df.query("n1.community_id == @community_id and n2.community_id == @community_id")
            if triples_df.empty:
                raise Log.Failure(Log.kg + Log.gr_rag, f"No triples found for community_id {community_id}")

            triples_df = triples_df[["n1.name", "r.rel_type", "n2.name"]].rename(
                columns={"n1.name": "subject", "r.rel_type": "relation", "n2.name": "object"}
            )
            return triples_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to retrieve community subgraph") from e

    def detect_community_clusters(self) -> None:
        """Run community detection on the graph as describe by GraphRAG paper.
        @details
        - Assigns a `community_id` property to all nodes.
        - Partitions the graph's nodes into topic-coherent communities.
        Typical algorithms include Leiden (recommended) or Louvain. For weighted undirected graphs
        the approach assigns each node a `community_id` property, and builds (optionally) a hierarchy of
        community levels (larger communities containing sub-communities) for summarization at different granularities.
        - Afterwards, you can call `get_community_subgraph()` to extract each community’s triples for summarization.
        @throws Log.Failure  If the graph has not been constructed or loaded, or if the community detection algorithm fails.
        """
        pass
    
    # ------------------------------------------------------------------------
    # Verbalization Formats
    # ------------------------------------------------------------------------
    
    def to_triple_string(self, triples_df: Optional[DataFrame] = None, format: str = "natural") -> str:
        """Convert triples to string representation in various formats.
        @details  Supports multiple output formats for LLM consumption:
        - "natural": Human-readable sentences (e.g., "Alice knows Bob.")
        - "triple": Raw triple format (e.g., "Alice KNOWS Bob")
        - "json": JSON array of objects with s/r/o keys
        @param triples_df  DataFrame with subject/relation/object columns. If None, uses all triples from this graph.
        @param format  Output format: "natural", "triple", or "json" (default: "natural").
        @return  String representation of triples in the specified format.
        @throws ValueError  If format is not recognized.
        """
        pass
    
    def to_contextualized_string(self, focus_nodes: Optional[List[str]] = None, top_n: int = 5) -> str:
        """Convert triples to contextualized string grouped by focus nodes.
        @details  Groups triples by subject nodes and formats them with context headers.
        This provides better structure for LLM comprehension compared to flat triple lists.
        If focus_nodes is None, uses the top_n most connected nodes.
        Example output:
            Facts about Alice:
              - knows Bob
              - works_at Company
              - lives_in City
        @param focus_nodes  List of node names to group by. If None, uses top_n by degree.
        @param top_n  Number of top nodes to use if focus_nodes is None (default: 5).
        @return  Formatted string with contextualized triple groups.
        """
        pass
    
    def to_narrative(self, strategy: str = "path", start_node: Optional[str] = None, max_triples: int = 50) -> str:
        """Convert graph to narrative text using specified strategy.
        @details  Transforms structured triples into natural language narrative:
        - "path": Follow edges sequentially from start_node, creating a story-like flow
        - "cluster": Group related entities and describe them thematically
        - "summary": High-level overview of graph contents and structure
        @param strategy  Narrative generation strategy: "path", "cluster", or "summary" (default: "path").
        @param start_node  Starting node for "path" strategy. If None, uses highest-degree node.
        @param max_triples  Maximum number of triples to include (default: 50).
        @return  Natural language narrative describing the graph.
        @throws ValueError  If strategy is not recognized.
        """
        pass
    
    # ------------------------------------------------------------------------
    # Graph Statistics and Metadata
    # ------------------------------------------------------------------------
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Return summary statistics about the graph structure.
        @details  Provides metadata useful for LLM context, including:
        - node_count: Total number of nodes
        - edge_count: Total number of relationships
        - relation_types: List of unique relationship types
        - avg_degree: Average node degree (edges per node)
        - top_nodes: List of most connected nodes (top 5 by degree)
        - density: Graph density (actual edges / possible edges)
        @return  Dictionary containing graph statistics.
        """
        pass

    def get_edge_counts(self, top_n: int = 10) -> DataFrame:
        """Return node names and their edge counts, ordered by edge count descending.
        @param top_n  Number of top nodes to return (by edge count). Default is 10.
        @return  DataFrame with columns: node_name, edge_count
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        df = self.database.get_dataframe(self.graph_name)
        if df is None or df.empty:
            raise Log.Failure(Log.gr_db + Log.kg, f"Failed to fetch edge_counts DataFrame.")
        
        # Filter to nodes only
        df_nodes = df[df["element_type"] == "node"].copy()
        df_rels = df[df["element_type"] == "relationship"].copy()
        
        # Count edges per node by matching element_id with start_node_id/end_node_id
        edge_counts = {}
        for _, node in df_nodes.iterrows():
            node_id = node["element_id"]
            node_name = node.get("name", None)
            if node_name is None:
                continue
            # Count relationships where this node is start or end
            count = len(df_rels[(df_rels["start_node_id"] == node_id) | (df_rels["end_node_id"] == node_id)])
            edge_counts[node_name] = count
        
        # Convert to DataFrame and sort
        result_df = DataFrame(list(edge_counts.items()), columns=["node_name", "edge_count"])
        result_df = result_df.sort_values("edge_count", ascending=False).head(top_n)
        
        Log.success(Log.gr_db + Log.kg, f"Found top-{top_n} most popular nodes.", self.verbose)
        return result_df
    
    def get_node_context(self, node_name: str, include_neighbors: bool = True) -> str:
        """Return natural language description of a node and its relationships.
        @details  Generates a human-readable summary of a single node suitable for LLM context.
        Example: "Alice is connected to 5 entities. She knows Bob and Charlie, works at Company, 
        lives in City, and follows Dave."
        @param node_name  The name of the node to describe.
        @param include_neighbors  Whether to list neighbor names (default: True).
        @return  Natural language description of the node.
        @throws Log.Failure  If the node does not exist in the graph.
        """
        pass
    
    def get_relation_summary(self) -> DataFrame:
        """Return summary of relationship types and their frequencies.
        @details  Provides an overview of what types of relationships exist in the graph
        and how common each type is. Useful for understanding graph schema.
        @return  DataFrame with columns: relation_type, count, example_triple
        """
        pass
