from components.fact_storage import GraphConnector
from pandas import DataFrame, option_context, concat
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
            raise Log.Failure(Log.kg + Log.gr_rag, f"Invalid triple: ({subject})-[:{relation}]->({object_})")

        # Merge subject/object and connect via relation
        query = f"""
        MERGE (s {{name: '{subject}', kg: '{self.graph_name}'}})
        MERGE (o {{name: '{object_}', kg: '{self.graph_name}'}})
        MERGE (s)-[r:{relation}]->(o)
        RETURN s, r, o
        """
        try:
            df = self.database.execute_query(query)
            if df is not None:
                Log.success(Log.kg + Log.gr_rag, f"Added triple: ({subject})-[:{relation}]->({object_})", self.verbose)
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to add triple: ({subject})-[:{relation}]->({object_})") from e
    
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
            rels = (
                elements_df[elements_df["element_type"] == "relationship"]
                .drop(columns=["element_type", "db", "kg"], errors="ignore")
                .add_prefix("r.")
            )
    
            # Join relationship to its start (n1) and end (n2) nodes
            triples_df = (
                rels.merge(nodes.add_prefix("n1."), left_on="r.start_node_id", right_on="n1.element_id")
                    .merge(nodes.add_prefix("n2."), left_on="r.end_node_id", right_on="n2.element_id")
            )
    
            triples_df = triples_df.drop(columns=["r.start_node_id", "r.end_node_id"], errors="ignore")
            return triples_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to pivot triple properties") from e

    def convert_triple_names(self, df_ids: DataFrame, df_lookup: Optional[DataFrame] = None) -> DataFrame:
        """Maps a DataFrame containing element ID columns (subject_id, relation_id, object_id) to human-readable names.
        @note
        - Requires that nodes and relationships still exist in the graph database; otherwise must specify df_lookup.
        @param df_ids  DataFrame with columns: subject_id, relation_id, object_id.
        @param df_lookup  DataFrame fetched from @ref components.fact_storage.GraphConnector.get_dataframe with columns: element_id, name, and rel_type.
        @return  DataFrame with columns: subject, relation, object.
        @raises Log.Failure  If mapping fails or required IDs are missing.
        """
        try:
            if df_ids is None:
                return None
            if df_ids.empty:  # Ensure the DataFrame has correct columns even if no data. For legacy compatibility
                return DataFrame(columns=["subject", "relation", "object"])
    
            # Use provided lookup DataFrame if given, otherwise query the connector
            elements_df = df_lookup if df_lookup is not None else self.database.get_dataframe(self.graph_name)
            if elements_df is None or elements_df.empty:
                raise Log.Failure(Log.kg + Log.gr_rag, Log.bad_triples(self.graph_name))
    
            # Build lookup dictionaries for efficiency. Can now use df.map(dict)
            node_map = (
                elements_df[elements_df["element_type"] == "node"]
                .set_index("element_id")["name"]
                .to_dict()
            )
            rel_map = (
                elements_df[elements_df["element_type"] == "relationship"]
                .set_index("element_id")["rel_type"]
                .to_dict()
            )
    
            # Map IDs to readable names
            df_named = df_ids.assign(
                subject=df_ids["subject_id"].map(node_map),
                relation=df_ids["relation_id"].map(rel_map),
                object=df_ids["object_id"].map(node_map)
            )
            # Drop ID columns and reorder
            df_named = df_named.drop(columns=["subject_id", "relation_id", "object_id"], errors="ignore")[
                ["subject", "relation", "object"]
            ]

            return df_named
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to convert triple names") from e


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
            triples_df = self.get_triple_properties()
            cols = ["subject_id", "relation_id", "object_id"]

            if triples_df is None or triples_df.empty:
                Log.success(Log.kg + Log.gr_rag, "Found 0 triples in graph.", self.verbose)
                return DataFrame(columns=cols)

            # Extract and rename columns
            triples_df = triples_df[["n1.element_id", "r.element_id", "n2.element_id"]].rename(
                columns={"n1.element_id": "subject_id", "r.element_id": "relation_id", "n2.element_id": "object_id"}
            )
            Log.success(Log.kg + Log.gr_rag, f"Found {len(triples_df)} triples in graph.", self.verbose)
            return triples_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to retrieve triples") from e


    def get_subgraph_by_nodes(self, node_ids: List[str]) -> DataFrame:
        """Return all triples where subject or object is in the specified node list.
        @param node_ids  List of node element IDs to filter by.
        @return  DataFrame with columns: subject_id, relation_id, object_id
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        try:
            triples_df = self.get_all_triples()
            if triples_df is None or triples_df.empty:
                raise Log.Failure(Log.kg + Log.gr_rag, f"No triples in graph {self.graph_name}")

            # Filter triples where either endpoint is in node_ids
            sub_df = triples_df[
                triples_df["subject_id"].isin(node_ids) | triples_df["object_id"].isin(node_ids)
            ].reset_index(drop=True)

            Log.success(Log.kg + Log.gr_rag, f"Found {len(sub_df)} triples for given nodes.", self.verbose)
            return sub_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to get subgraph by nodes") from e
    
    def get_neighborhood(self, node_id: str, depth: int = 1) -> DataFrame:
        """Get k-hop neighborhood around a central node.
        @details  Returns all triples within k hops of the specified node. A 1-hop neighborhood
        includes all direct neighbors, 2-hop includes neighbors-of-neighbors, etc.
        @param node_id  The element ID of the central node.
        @param depth  Number of hops to traverse (default: 1).
        @return  DataFrame with columns: subject_id, relation_id, object_id
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        try:
            triples_df = self.get_all_triples()
            if triples_df is None or triples_df.empty:
                raise Log.Failure(Log.kg + Log.gr_rag, f"No triples in graph {self.graph_name}")

            current = {node_id}
            visited = set()
            all_edges = []

            # Perform k-hop expansion
            for _ in range(depth):
                neighbors = triples_df[
                    triples_df["subject_id"].isin(current) | triples_df["object_id"].isin(current)
                ]
                if neighbors.empty:
                    break
                all_edges.append(neighbors)
                visited |= current
                current = set(neighbors["subject_id"]).union(neighbors["object_id"]) - visited

            result_df = (
                DataFrame() if not all_edges else concat(all_edges, ignore_index=True).drop_duplicates()
            )

            Log.success(Log.kg + Log.gr_rag, f"Found {len(result_df)} triples in {depth}-hop neighborhood.", self.verbose)
            return result_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to get neighborhood") from e
    
    def get_random_walk_sample(self, start_nodes: List[str], walk_length: int, num_walks: int = 1) -> DataFrame:
        """Sample subgraph using directed random walk traversal starting from specified nodes.
        @details
        - More diverse than degree-based filtering (nodes with many edges) and better preserves graph structure.
        - Each walk starts from a random node in start_nodes and continues for walk_length steps.
        @param start_nodes  List of node IDs to use as starting points.
        @param walk_length  Number of steps in each random walk.
        @param num_walks  Number of random walks to perform (default: 1).
        @return  DataFrame with columns: subject_id, relation_id, object_id
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        try:
            import random
            triples_df = self.get_all_triples()
            if triples_df is None or triples_df.empty:
                raise Log.Failure(Log.kg + Log.gr_rag, f"No triples available in graph {self.graph_name}")

            # Build directed adjacency: subject -> [rows where subject_id == subject]
            adjacency = {}
            for _, row in triples_df.iterrows():
                s = row["subject_id"]
                adjacency.setdefault(s, []).append(row)

            if not adjacency:
                raise Log.Failure(Log.kg + Log.gr_rag, f"Graph has no directed edges")

            sampled_edges = []
            valid_starts = [n for n in start_nodes if n in adjacency] or list(adjacency.keys())

            for _ in range(num_walks):
                current = random.choice(valid_starts)
                for _ in range(walk_length):
                    neighbors = adjacency.get(current, [])
                    if not neighbors:
                        break  # dead end
                    edge = random.choice(neighbors)
                    sampled_edges.append(edge)
                    current = edge["object_id"]  # move along directed edge

            if not sampled_edges:
                raise Log.Failure(Log.kg + Log.gr_rag, f"No triples visited during random walk")

            result_df = (
                DataFrame(sampled_edges)[["subject_id", "relation_id", "object_id"]]
                .drop_duplicates()
                .reset_index(drop=True)
            )

            Log.success(
                Log.kg + Log.gr_rag,
                f"Directed random-walk sampled {len(result_df)} triples ({num_walks} walks × length {walk_length}).",
                self.verbose,
            )
            return result_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to perform directed random walk sample") from e





    
    def get_community_subgraph(self, community_id: int) -> DataFrame:
        """Return all triples belonging to a specific GraphRAG community.
        @details
        - Communities are densely connected subgraphs detected via clustering algorithms.
        - This enables GraphRAG-style hierarchical summarization where each community can be
        summarized independently. Requires nodes to have a 'community_id' property assigned.
        - Afterwards, you may run a summary step which generates community summaries for each cluster (as described in the paper).
        @param community_id  The identifier of the community to retrieve.
        @return  DataFrame with columns: subject_id, relation_id, object_id
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame or community detection has not been run.
        """
        try:
            triples_df = self.get_triple_properties()
            if triples_df is None:
                raise Log.Failure(Log.kg + Log.gr_rag, Log.bad_triples(self.graph_name))

            # Only nodes are tagged. Include triples where both nodes match community ID.
            triples_df = triples_df[
                (triples_df["n1.community_id"] == community_id) & 
                (triples_df["n2.community_id"] == community_id)
            ]
            if triples_df.empty:
                Log.warn(Log.kg + Log.gr_rag, f"No triples found for community_id {community_id}")
                return triples_df

            triples_df = triples_df[["n1.name", "r.rel_type", "n2.name"]].rename(
                columns={"n1.name": "subject", "r.rel_type": "relation", "n2.name": "object"}
            )
            return triples_df
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to retrieve community subgraph") from e

    
    def detect_community_clusters(self, method: str = "leiden", multi_level: bool = False, max_levels: int = 10) -> None:
        """Run community detection on the graph as described by the GraphRAG paper.
        @details
        - Assigns a `community_id` property to all nodes, and optionally `level_id`.
        - Partitions the graph's nodes into topic-coherent communities.
        - Afterwards, you can call `get_community_subgraph()` to extract each community's triples for summarization.
        Clustering Methods
        - Leiden (recommended) - improvement of Louvain ensuring well-connected, stable communities; supports multi-level hierarchy.
        - Louvain - quickly groups nodes but may yield fragmented subcommunities.
        @param method  The community detection algorithm to run. Options: "leiden" (default) or "louvain".
        @param multi_level  Whether to record hierarchical levels (`level_id`) for multi-scale summarization.
        @param max_levels  Maximum hierarchy depth to compute (default: 10).
        @throws Log.Failure  If GDS is unavailable or any query fails."""
        try:
            method = method.lower().strip()
            if method not in {"leiden", "louvain"}:
                raise Log.Failure(Log.kg + Log.gr_rag, f"Unsupported community detection method: {method}")
    
            # Creates an in-memory projection using native projection with property filters
            query_setup_gds = f"""
            CALL gds.graph.project(
                '{self.graph_name}',
                '*',
                {{
                    ALL: {{
                        type: '*',
                        orientation: 'UNDIRECTED'
                    }}
                }}
            ) YIELD graphName, nodeCount, relationshipCount
            """
            
            # Runs the selected community detection algorithm.
            if multi_level:
                options = f"writeProperty: 'community_list', includeIntermediateCommunities: true"
                if method == "leiden":
                    options += f", maxLevels: {max_levels}"
            else:
                options = f"writeProperty: 'community_id'"
            query_detect_communities = f"""
            CALL gds.{method}.write(
                '{self.graph_name}',
                {{ {options} }}
            )
            """
            # Cleans up the temporary projection.
            query_drop_gds = f"CALL gds.graph.drop('{self.graph_name}') YIELD graphName"
    
            # --- Execute sequentially ---
            try:  # Drop any existing projection (in case of previous failure)
                self.database.execute_query(query_drop_gds)
            except:
                pass  # Graph didn't exist, that's fine
            self.database.execute_query(query_setup_gds)
            self.database.execute_query(query_detect_communities)
            self.database.execute_query(query_drop_gds)
    
            # Clean up GDS-generated metadata: keep only community_id or community_list, remove everything else.
            self.database.execute_query("""
            MATCH (n)
            REMOVE n.communityLevel, n.community_level
            SET n.level_id = CASE WHEN n.community_list IS NOT NULL THEN size(n.community_list) - 1 ELSE 0 END
            """)
    
            Log.success(Log.kg + Log.gr_rag, f"Community detection ({method}) complete.", self.database.verbose)
        except Exception as e:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to run community detection") from e


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
            raise Log.Failure(Log.kg + Log.gr_rag, f"Failed to fetch edge_counts DataFrame.")
        
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
        
        Log.success(Log.kg + Log.gr_rag, f"Found top-{top_n} most popular nodes.", self.verbose)
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
