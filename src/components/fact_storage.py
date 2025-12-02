from pandas import concat, DataFrame, option_context
import random
import re
from src.connectors.graph import GraphConnector
from src.util import Log
from typing import Any, Dict, List, Optional, Tuple, TypedDict
import spacy

nlp = None  # module-level cache for lazy-loaded NLP model (used by sanitize_node)

class Triple(TypedDict):
    s: str
    r: str
    o: str


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
        ## Flag to drop any existing graph when the first triple is added.
        self._first_insert = True

    def add_triple(self, subject: str, relation: str, object_: str) -> None:
        """Add a semantic triple to the graph using raw Cypher.
        @param subject  A string representing the entity performing an action.
        @param relation  A string describing the action.
        @param object_  A string representing the entity being acted upon.
        @note  LLM output should be pre-normalized using @ref src.connectors.llm.LLMConnector.normalize_triples.
        @throws Log.Failure  If the triple cannot be added to our graph database.
        """
        if self._first_insert:
            self._first_insert = False
            if self.database.graph_exists(self.graph_name):
                self.database.drop_graph(self.graph_name)

        # Normalize already-cleaned inputs for extra Cypher safety
        if not subject or not relation or not object_:
            Log.warn(Log.kg, f"Invalid triple: ({subject})-[:{relation}]->({object_})", verbose)
            return
        relation = sanitize_relation(relation)
        subject = sanitize_node(subject)
        object_ = sanitize_node(object_)

        # Merge subject/object and connect via relation
        query = f"""
        MERGE (s {{name: '{subject}', kg: '{self.graph_name}'}})
        MERGE (o {{name: '{object_}', kg: '{self.graph_name}'}})
        MERGE (s)-[r:{relation}]->(o)
        RETURN s, r, o
        """  # NOTE: this query has a DIRECTED relationship! And in log messages.
        try:
            df = self.database.execute_query(query)
            if df is not None:
                Log.success(Log.kg, f"Added triple: ({subject})-[:{relation}]->({object_})", self.verbose)
        except Exception as e:
            raise Log.Failure(Log.kg, f"Failed to add triple: ({subject})-[:{relation}]->({object_})") from e

    def add_triples_json(self, triples_json: List[Triple]) -> None:
        """Add several semantic triples to the graph from pre-verified JSON.
        @note  JSON should be pre-normalized using @ref src.connectors.llm.normalize_triples.
        @param triples_json  A list of Triple dictionaries containing keys: 's', 'r', and 'o'.
        @throws Log.Failure  If any triple cannot be added to the graph database.
        """
        for triple in triples_json:
            subj = triple["s"]
            rel = triple["r"]
            obj = triple["o"]
            self.add_triple(subj, rel, obj)

    def get_all_triples(self) -> DataFrame:
        """Return all triples in the specified graph as a pandas DataFrame.
        @return  Returns (subject, relation, object) columns only.
        @throws Log.Failure  If the query fails to retrieve or process the DataFrame.
        """
        triples_df = self.get_triple_properties()
        cols = ["subject_id", "relation_id", "object_id"]

        if triples_df is None or triples_df.empty:
            Log.success(Log.kg, "Found 0 triples in graph.", self.verbose)
            return DataFrame(columns=cols)

        # Extract and rename columns
        triples_df = triples_df[["n1.element_id", "r.element_id", "n2.element_id"]].rename(
            columns={"n1.element_id": "subject_id", "r.element_id": "relation_id", "n2.element_id": "object_id"}
        )
        Log.success(Log.kg, f"Found {len(triples_df)} triples in graph.", self.verbose)
        return triples_df

    def get_triple_properties(self) -> Optional[DataFrame]:
        """Pivot the graph elements DataFrame to expose node and relationship properties as columns.
        @details
        - Builds a joined view of properties from both nodes (n1, n2) and the relationship (r).
        - Removes redundant fields such as: db, kg, element_type, start_node_id, and end_node_id.
        - Usage: n1.element_id, r.rel_type, n2.name, etc.
        @return  DataFrame where each row represents one triple (n1, r, n2).
        @throws Log.Failure  If the elements DataFrame cannot be loaded or pivoting fails.
        """
        elements_df = self.database.get_dataframe(self.graph_name)
        if elements_df is None or elements_df.empty:
            raise Log.Failure(Log.kg, Log.msg_bad_triples(self.graph_name))

        # Split nodes and relationships
        nodes = elements_df[elements_df["element_type"] == "node"].drop(columns=["element_type", "db", "kg"], errors="ignore")
        rels = elements_df[elements_df["element_type"] == "relationship"].drop(columns=["element_type", "db", "kg"], errors="ignore").add_prefix("r.")
        # Join relationship to its start (n1) and end (n2) nodes
        triples_df = rels.merge(nodes.add_prefix("n1."), left_on="r.start_node_id", right_on="n1.element_id").merge(
            nodes.add_prefix("n2."), left_on="r.end_node_id", right_on="n2.element_id"
        )
        triples_df = triples_df.drop(columns=["r.start_node_id", "r.end_node_id"], errors="ignore")
        return triples_df

    def triples_to_names(self, df_ids: DataFrame, drop_ids: bool = False, df_lookup: Optional[DataFrame] = None) -> DataFrame:
        """Maps a DataFrame containing element ID columns to human-readable names.
        @note
        - Requires the provided nodes to still exist in the graph database; otherwise must specify df_lookup.
        @param df_ids  DataFrame with added columns: subject_id, relation_id, object_id.
        @param drop_ids  Whether to remove columns from results: subject_id, relation_id, object_id.
        @param df_lookup  Optional DataFrame fetched from @ref src.connectors.graph.GraphConnector.get_dataframe with required columns: element_id, elemenet_type, name, and rel_type.
        @return  DataFrame with added columns: subject, relation, object.
        @throws Log.Failure  If mapping fails or required IDs are missing.
        """
        df_ids = self.find_element_names(df_ids, ["subject", "object"], ["subject_id", "object_id"], "node", "name", drop_ids, df_lookup)
        df_ids = self.find_element_names(df_ids, ["relation"], ["relation_id"], "relationship", "rel_type", drop_ids, df_lookup)
        # Keep minimum of 2 database queries, but must correct the column order.
        name_cols_ordered = ["subject", "relation", "object"]
        return df_ids[[col for col in df_ids.columns if col not in name_cols_ordered] + name_cols_ordered]

    def find_element_names(
        self,
        df_ids: DataFrame,
        name_columns: List[str],
        id_columns: List[str],
        element_type: str,
        name_property: str,
        drop_ids: bool = False,
        df_lookup: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Helper function which maps element IDs to human-readable names.
        @note
        - Requires the provided nodes or edges to still exist in the graph database; otherwise must specify df_lookup.
        @param df_ids  DataFrame with required columns: *id_columns*.
        @param name_columns  Required list of column names to create.
        @param id_columns  Required list of columns containing element IDs.
        @param element_type  Whether to match nodes or relationships. Value must be "node" or "relationship".
        @param name_property  Required element property from *df_lookup* to use as the display name.
        @param drop_ids  Whether to remove *id_columns* from results.
        @param df_lookup  Optional DataFrame fetched from @ref src.connectors.graph.GraphConnector.get_dataframe with required columns: element_id, elemenet_type, and *name_property*.
        @return  DataFrame with added columns: *name_columns*.
        @throws Log.Failure  If mapping fails or required IDs are missing.
        """
        if df_ids is None:
            return None
        if df_ids.empty:  # Ensure the DataFrame has correct columns even if no data. For legacy compatibility
            out_cols = [col for col in df_ids.columns if not (drop_ids and col in id_columns)] + name_columns
            return DataFrame(columns=out_cols)
        if len(name_columns) != len(id_columns):
            raise Log.Failure(Log.kg, f"name_columns (size {len(name_columns)}) and id_columns (size {len(id_columns)}) must have the same length.")

        # Filter out non-existent columns
        valid_cols = set(df_ids.columns)
        filtered_pairs = [(name_col, id_col) for name_col, id_col in zip(name_columns, id_columns) if id_col in valid_cols]
        if not filtered_pairs:  # If no valid columns are left, return the original DataFrame
            return df_ids
        # Re-assign the filtered column lists
        name_columns, id_columns = map(list, zip(*filtered_pairs))
        name_columns = list(name_columns)
        id_columns = list(id_columns)

        # Use provided DataFrame if given, otherwise query the connector
        df_lookup = df_lookup if df_lookup is not None else self.database.get_dataframe(self.graph_name)
        if df_lookup is None or df_lookup.empty:
            raise Log.Failure(Log.kg, Log.msg_bad_triples(self.graph_name))

        # Build ID-to-name dictionary for lookup efficiency
        id_map = df_lookup[df_lookup["element_type"] == element_type].set_index("element_id")[name_property].to_dict()
        df_named = df_ids.copy()
        for name_col, id_col in zip(name_columns, id_columns):
            df_named[name_col] = df_named[id_col].map(id_map)

        # Reorder columns, and optionally drop ID columns
        cols_to_keep = [col for col in df_ids.columns if not (drop_ids and col in id_columns)]
        ordered_cols = cols_to_keep + name_columns
        return df_named[ordered_cols]

    # ------------------------------------------------------------------------
    # Subgraph Selection
    # ------------------------------------------------------------------------
    # TODO: add to_names and drop_ids flags to each of these
    # user can avoid calling triples_to_names each time
    # TODO: better approach to id_columns for unidirectional vs bidirectional, fan-in vs fan-out

    def get_subgraph_by_nodes(self, node_ids: List[str], id_columns: List[str] = ["subject_id", "object_id"]) -> DataFrame:
        """Return all triples where subject or object is in the specified node list.
        @param node_ids  List of node element IDs to filter by.
        @param id_columns  List of columns to compare against. Can be 'subject_id', 'object_id', or both.
        @return  DataFrame with columns: subject_id, relation_id, object_id
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        @throws KeyError  If the provided column names are invalid.
        """
        # TODO: Update pytest for id_columns
        triples_df = self.get_all_triples()
        if triples_df is None or triples_df.empty:
            raise Log.Failure(Log.kg + Log.sub_gr, Log.msg_bad_triples(self.graph_name))

        missing = [k for k in id_columns if k not in triples_df.columns]
        if missing:
            raise KeyError(f"The provided key columns are not in triples_df: {missing}")

        # Filter triples where either endpoint is in node_ids
        mask = triples_df[id_columns].isin(node_ids).any(axis=1)
        sub_df = triples_df[mask].reset_index(drop=True)

        Log.success(Log.kg + Log.sub_gr, f"Found {len(sub_df)} triples for given nodes.", self.verbose)
        return sub_df

    def get_neighborhood(self, node_id: str, depth: int = 1) -> DataFrame:
        """Get k-hop neighborhood around a central node.
        @details  Returns all triples within k hops of the specified node. A 1-hop neighborhood
        includes all direct neighbors, 2-hop includes neighbors-of-neighbors, etc.
        @param node_id  The element ID of the central node.
        @param depth  Number of hops to traverse (default: 1).
        @return  DataFrame with columns: subject_id, relation_id, object_id
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        triples_df = self.get_all_triples()
        if triples_df is None or triples_df.empty:
            raise Log.Failure(Log.kg + Log.sub_gr, Log.msg_bad_triples(self.graph_name))

        new_nodes = {node_id}
        visited_nodes = set()
        all_edges = []
        # Perform k-hop expansion
        for _ in range(depth):
            neighbors = triples_df[triples_df["subject_id"].isin(new_nodes) | triples_df["object_id"].isin(new_nodes)]
            if neighbors.empty:
                break
            all_edges.append(neighbors)
            visited_nodes |= new_nodes
            new_nodes = set(neighbors["subject_id"]).union(neighbors["object_id"]) - visited_nodes

        # Clean the filtered triples DataFrame
        if not all_edges:
            return DataFrame()
        result_df = concat(all_edges, ignore_index=True).drop_duplicates()

        Log.success(Log.kg + Log.sub_gr, f"Found {len(result_df)} triples in {depth}-hop neighborhood.", self.verbose)
        return result_df

    def get_degree_range(self, min_degree: int = 1, max_degree: int = -1, id_columns: List[str] = ["subject_id", "object_id"]) -> DataFrame:
        """Return triples associated with nodes whose degree lies within the specified bounds.
        @details
            - Degree is defined as the number of relationships where a node appears as
              start_node_id or end_node_id.
            - Selects all nodes satisfying min_degree <= degree <= max_degree
              and returns triples incident to those nodes.
        @param max_degree  Maximum number of edges allowed for a node to be included (-1 = infer highest edge count).
        @param min_degree  Minimum number of edges required for a node to be included.
        @param id_columns  List of columns to compare against. Can be 'subject_id', 'object_id', or both.
        @return  DataFrame containing an arbitrary number of triples for nodes in the specified degree range.
        @throws Log.Failure  If the graph fails to load or degree computation fails.
        @throws ValueError   If min_degree or max_degree values are invalid.
        """
        pass

    def get_by_ranked_degree(
        self, best_rank: int = 1, worst_rank: int = -1, enforce_count: bool = False, id_columns: List[str] = ["subject_id", "object_id"]
    ) -> DataFrame:
        """Return triples associated with nodes whose degree rank lies in the specified range.
        @details
            - Computes degree (edge count) for all nodes.
            - Sorts nodes by degree descending, assigns ranks, and selects those with
              best_rank <= rank <= worst_rank.
            - Returns all triples where subject_id or object_id matches a selected node.
        @param best_rank  Minimum degree rank. Inclusive.
        @param worst_rank  Maximum degree rank (-1 = maximum degree) to include. Inclusive.
        @param enforce_count  Always return (worst_rank - best_rank + 1) rows (fallback to node_id order).
        @param id_columns  List of columns to compare against. Can be 'subject_id', 'object_id', or both.
        @return  DataFrame containing the triples for ranked nodes; columns:
                 subject_id, relation_id, object_id.
        @throws Log.Failure  If the graph cannot be queried.
        @throws ValueError   If best_rank or worst_rank values are invalid.
        """
        if best_rank < 1:
            raise ValueError("best_rank must be >= 1")
        if worst_rank != -1 and worst_rank < best_rank:
            raise ValueError("worst_rank must be >= best_rank, or -1 for no upper bound")

        edge_df = self.get_edge_counts()
        if edge_df is None or edge_df.empty:
            raise Log.Failure(Log.kg, "Failed to compute edge counts.")
        # Sort and assign rank (1 = highest degree)
        edge_df = edge_df.sort_values(["edge_count", "node_id"], ascending=[False, True]).reset_index(drop=True)

        if enforce_count:
            # Take exact number of nodes by position, ignoring rank gaps
            start_idx = best_rank - 1  # Convert 1-indexed rank to 0-indexed position
            end_idx = worst_rank if worst_rank != -1 else len(edge_df)
            ranked_nodes = edge_df.iloc[start_idx:end_idx]
        else:
            # Dense ranking: tied nodes get same rank, next rank is consecutive
            edge_df["rank"] = edge_df["edge_count"].rank(method="dense", ascending=False).astype(int)
            # Determine actual worst_rank
            if worst_rank == -1:
                worst_rank = int(edge_df["rank"].max())
            # Filter nodes by rank
            ranked_nodes = edge_df[(edge_df["rank"] >= best_rank) & (edge_df["rank"] <= worst_rank)]

        if ranked_nodes.empty:
            return DataFrame(columns=["subject_id", "relation_id", "object_id"])
        node_ids = ranked_nodes["node_id"].tolist()

        triples_df = self.get_subgraph_by_nodes(node_ids, id_columns=id_columns)
        return triples_df

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
        triples_df = self.get_all_triples()
        if triples_df is None or triples_df.empty:
            raise Log.Failure(Log.kg + Log.sub_gr, Log.msg_bad_triples(self.graph_name))

        # Find adjacent nodes where 'subject_id' in [start_nodes]
        # 1. Initialize with all outgoing edges
        rows_outgoing: Dict[str, List[Any]] = {}
        for _, row in triples_df.iterrows():
            subject_id = row["subject_id"]
            rows_outgoing.setdefault(subject_id, []).append(row)
        if not rows_outgoing:
            return DataFrame()
        # 2. Filter out disconnected nodes (no outgoing edges)
        valid_starts = [n for n in start_nodes if n in rows_outgoing]
        # 3. Any node in the graph can be used if start_nodes has no valid outgoing edges.
        if not valid_starts:
            valid_starts = list(rows_outgoing.keys())

        sampled_edges = []
        for _ in range(num_walks):
            # Choose random start
            current = random.choice(valid_starts)
            for _ in range(walk_length):
                neighbors = rows_outgoing.get(current, [])
                if not neighbors:
                    break  # dead end
                edge = random.choice(neighbors)  # Choose random edge from this node
                sampled_edges.append(edge)
                current = edge["object_id"]  # move along directed edge

        # Clean the filtered triples DataFrame
        if not sampled_edges:
            return DataFrame()
        result_df = DataFrame(sampled_edges)[["subject_id", "relation_id", "object_id"]].drop_duplicates().reset_index(drop=True)

        Log.success(
            Log.kg + Log.sub_gr,
            f"The directed random-walk sampled {len(result_df)} triples ({num_walks} walks × length {walk_length}).",
            self.verbose,
        )
        return result_df

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
        triples_df = self.get_triple_properties()
        if triples_df is None:
            raise Log.Failure(Log.kg + Log.gr_rag, Log.msg_bad_triples(self.graph_name))

        # Only nodes are tagged. Include triples where both nodes match community ID.
        triples_df = triples_df[(triples_df["n1.community_id"] == community_id) & (triples_df["n2.community_id"] == community_id)]
        if triples_df.empty:
            Log.warn(Log.kg + Log.gr_rag, f"No triples found for community_id {community_id}")
            # Return empty DataFrame with correct schema
            return DataFrame(columns=["subject_id", "relation_id", "object_id"])

        triples_df = triples_df[["n1.element_id", "r.element_id", "n2.element_id"]].rename(
            columns={"n1.element_id": "subject_id", "r.element_id": "relation_id", "n2.element_id": "object_id"}
        )
        return triples_df

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
        method = method.lower().strip()
        if method not in {"leiden", "louvain"}:
            raise Log.Failure(Log.kg + Log.gr_rag, f"Unsupported community detection method: {method}")

        # --- Build GDS queries ---
        # 1. Creates an in-memory projection of the graph
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
        # 2. Runs the selected community detection algorithm.
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
        # 3. Cleans up the temporary projection.
        query_drop_gds = f"CALL gds.graph.drop('{self.graph_name}') YIELD graphName"

        # --- Execute sequentially ---
        try:  # Drop any existing projection (in case of previous failure)
            self.database.execute_query(query_drop_gds)
        except:
            pass  # Graph didn't exist, that's fine
        self.database.execute_query(query_setup_gds)
        self.database.execute_query(query_detect_communities)
        self.database.execute_query(query_drop_gds)

        # --- Clean up GDS-generated metadata ---
        # 1. Keep only community_id or community_list, remove everything else.
        query_rm_props = """MATCH (n)
            WHERE n.communityLevel IS NOT NULL
            REMOVE n.communityLevel"""
        # 2. Remove the property we're NOT using in this mode
        if multi_level:
            query_rm_other = """MATCH (n)
                WHERE n.community_id IS NOT NULL
                REMOVE n.community_id"""
        else:
            query_rm_other = """MATCH (n)
            WHERE n.community_list IS NOT NULL
            REMOVE n.community_list"""
        self.database.execute_query(query_rm_props)
        self.database.execute_query(query_rm_other)

        Log.success(Log.kg + Log.gr_rag, f"Community detection ({method}) complete.", self.database.verbose)

    # ------------------------------------------------------------------------
    # Verbalization Formats
    # ------------------------------------------------------------------------

    def to_triples_string(self, triple_names_df: Optional[DataFrame] = None, mode: str = "triple") -> str:
        """Convert triples to string representation in various formats.
        @details  Supports multiple output formats for LLM consumption:
        - "natural": Human-readable sentences (e.g., "Alice employed by Bob.")
        - "triple": Raw triple format (e.g., "Alice employedBy Bob")
        - "json": JSON array of objects with s/r/o keys
        @param triple_names_df  DataFrame with subject, relation, object columns. If None, uses all triples from this graph.
        @param mode  Output format: "natural", "triple", or "json" (default: "triple").
        @return  String representation of triples in the specified format.
        @throws ValueError  If format is not recognized.
        """
        accepted_modes = ["natural", "triple", "json"]
        if mode not in accepted_modes:
            raise ValueError(f"Invalid triple string format {mode}; expected {accepted_modes}")

        if triple_names_df is None:
            triples_df = self.get_all_triples()
            triple_names_df = self.triples_to_names(triples_df, drop_ids=True)

        # TODO: Simplify & add other modes
        if mode != "triple":
            return "TODO"

        triples_string = ""
        for _, triple in triple_names_df.iterrows():
            subj = triple["subject"]
            rel = triple["relation"]
            obj = triple["object"]
            triples_string += f"{subj} {rel} {obj}\n"
        return triples_string

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

    def get_edge_counts(self, top_n: int = -1) -> DataFrame:
        """Return node names and their edge counts, ordered by edge count descending.
        @param top_n  Number of top nodes to return (by edge count). Default is -1 (all nodes).
        @return  DataFrame with columns: node_id, edge_count
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        df = self.database.get_dataframe(self.graph_name)
        if df is None or df.empty:
            raise Log.Failure(Log.kg, f"Failed to fetch edge_counts DataFrame.")

        # Filter to nodes only
        df_nodes = df[df["element_type"] == "node"].copy()
        df_rels = df[df["element_type"] == "relationship"].copy()

        # Count edges per node by matching element_id with start_node_id/end_node_id
        edge_counts = {}
        for _, node in df_nodes.iterrows():
            node_id = node["element_id"]
            if node_id is None:
                continue
            # Count relationships where this node is start or end
            count = len(df_rels[(df_rels["start_node_id"] == node_id) | (df_rels["end_node_id"] == node_id)])
            edge_counts[node_id] = count

        # Convert to DataFrame and sort
        result_df = DataFrame(list(edge_counts.items()), columns=["element_id", "edge_count"])
        result_df = result_df.rename(columns={"element_id": "node_id"})
        result_df = result_df.sort_values("edge_count", ascending=False).reset_index(drop=True)
        if top_n > 0:
            result_df = result_df.head(top_n)

        Log.success(Log.kg, f"Found top-{top_n} most popular nodes.", self.verbose)
        return result_df

    def get_node_context(self, node_id: str, include_neighbors: bool = True) -> str:
        """Return natural language description of a node and its relationships.
        @details  Generates a human-readable summary of a single node suitable for LLM context.
        Example: "Alice is connected to 5 entities. She knows Bob and Charlie, works at Company,
        lives in City, and follows Dave."
        @param node_id  The element ID of the node to describe.
        @param include_neighbors  Whether to list neighbor node IDs (default: True).
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




def sanitize_node(label: str) -> str:
    """Clean node name for Cypher safety.
    @details
        - Joins lists/tuples into single string
        - Replaces invalid characters with underscores
        - Trims leading/trailing underscores and spaces
        Used by KG systems before inserting nodes.
    @param label  Raw node name (subject / object)
    @return  Sanitized string suitable for node property
    @throws ValueError  If result is empty after sanitization
    """
    # NLP-based cleaning: remove determiners, pronouns, particles
    global nlp
    if nlp is None:
        # Auto-download if missing (Self-healing)
        try:
            nlp = spacy.load("en_core_web_sm")
        except OSError:
            print("Spacy model 'en_core_web_sm' not found. Downloading...")
            spacy.cli.download("en_core_web_sm")
            nlp = spacy.load("en_core_web_sm")

    doc = nlp(label)
    tokens = [
        token.text for token in doc 
        if token.pos_ not in {"DET", "PRON", "PART"}  # determiners, pronouns, particles
    ]
    cleaned = " ".join(tokens)
    if not cleaned:  # Revert back to input: a messy label is better than nothing.
        cleaned = label
    
    # Regex: collapse consecutive non-alphanumeric to single underscore, strip edges
    sanitized = re.sub(r"[^A-Za-z0-9]+", "_", cleaned).strip("_")
    if not sanitized:
        raise ValueError(f"Node name cannot be empty after sanitization: '{label}' -> '{cleaned}'")
    return sanitized


def sanitize_relation(label: str, mode: str = "UPPER_CASE", default_relation: str = "RELATED_TO") -> str:
    """Clean and normalize relation label for knowledge graphs.
    @details
        Supports two output modes:
        - UPPER_CASE: Neo4j convention (e.g., RELATED_TO)
        - camelCase: OWL/RDF convention (e.g., relatedTo)
        
        Process:
        - Replaces invalid characters with underscores
        - Applies mode-specific casing rules
        - Falls back to normalized default if empty or invalid start
        
        Relations must start with alphabetic character.
        Default relation is automatically normalized to match mode.
    @param label  Raw relation label (string)
    @param mode  Output format: "UPPER_CASE" or "camelCase"
    @param default_relation  Fallback relation name (auto-normalized to mode)
    @return  Sanitized relation label in specified mode
    @throws ValueError  If mode is invalid
    """
    # 1. PRE-PROCESS: Inject underscores between CamelCase (e.g., "hasPart" -> "has_Part")
    # This ensures re.split below sees distinct words.
    pre_split = re.sub(r'([a-z])([A-Z])', r'\1_\2', label)

    # 2. CLEAN: Replace invalid chars, split on underscores/spaces
    cleaned = re.sub(r"[^A-Za-z0-9_ ]", "_", pre_split)
    words = [w for w in re.split(r"[_ ]+", cleaned) if w]
    
    # Normalize default_relation according to mode
    default_cleaned = re.sub(r"[^A-Za-z0-9_ ]", "_", default_relation)
    default_words = [w for w in re.split(r"[_ ]+", default_cleaned) if w]
    
    if mode == "UPPER_CASE":
        # Neo4j convention: SCREAMING_SNAKE_CASE
        normalized_default = "_".join(w.upper() for w in default_words) if default_words else "RELATED_TO"
        if not words:
            return normalized_default
        
        sanitized = "_".join(w.upper() for w in words)
        if not sanitized or not sanitized[0].isalpha():
            sanitized = normalized_default
            
    elif mode == "camelCase":
        # OWL convention: lowerCamelCase
        normalized_default = (
            default_words[0].lower() + "".join(w.capitalize() for w in default_words[1:])
            if default_words else "relatedTo"
        )
        if not words:
            return normalized_default

        sanitized = words[0].lower() + "".join(w.capitalize() for w in words[1:])
        if not sanitized or not sanitized[0].isalpha():
            sanitized = normalized_default
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'UPPER_CASE' or 'camelCase'")
    return sanitized
