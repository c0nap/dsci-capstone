from components.fact_storage import GraphConnector
from pandas import DataFrame, option_context
from src.util import Log
from typing import Any, List, Optional, Tuple
import re

class KnowledgeGraph:
    """Manages a single graph within Neo4j.
    @details
        - Handles safe conversion of LLM output to structured triples.
        - Provides helper functions to add and retrieve triples.
    """

    def __init__(self, name: str, connector: GraphConnector) -> None:
        ##
        self.graph_name = name
        ##
        self.connector = connector

    # ------------------------------------------------------------------------
    # Knowledge Graph helpers for Semantic Triples
    # ------------------------------------------------------------------------
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
        with self.connector.temp_graph(self.graph_name):
            # Merge subject/object and connect via relation
            query = f"""
            MERGE (s {{name: '{subject}', db: '{self.connector.database_name}', kg: '{self.graph_name}'}})
            MERGE (o {{name: '{object_}', db: '{self.connector.database_name}', kg: '{self.graph_name}'}})
            MERGE (s)-[r:{relation}]->(o)
            RETURN s, r, o
            """

            try:
                df = self.connector.execute_query(query, _filter_results=False)
                if df is not None:
                    Log.success(Log.gr_db + Log.kg, f"Added triple: ({subject})-[:{relation}]->({object_})", self.connector.verbose)
            except Exception as e:
                raise Log.Failure(Log.gr_db + Log.kg, f"Failed to add triple: ({subject})-[:{relation}]->({object_})") from e

    @staticmethod
    def normalize_triples(data: Any) -> List[Tuple[str, str, str]]:
        """Normalize flexible LLM output into a list of clean (subject, relation, object) triples.
        @details
            - Accepts dicts, lists of dicts, tuples, or dicts-of-lists.
            - Joins list values, trims, and sanitizes for Cypher safety.
            - Enforces uppercase underscore-safe relation labels.
        @param data  Raw LLM output to normalize.
        @return  List of sanitized (s, r, o) triples ready for insertion.
        @throws ValueError  If input format cannot be parsed.
        """

        def _sanitize_node(value: Any) -> str:
            """Clean a node name for Cypher safety.
            @param value  Raw subject/object value.
            @return  Sanitized string suitable for node property.
            """
            if isinstance(value, (list, tuple)):  # Join list/tuple into single string
                value = " ".join(map(str, value))
            elif not isinstance(value, str):  # Convert non-str types
                value = str(value)
            # Replace invalid chars, trim edges
            return re.sub(r"[^A-Za-z0-9_ ]", "_", value).strip("_ ")

        def _sanitize_rel(value: Any) -> str:
            """Clean and normalize a relation label.
            @param value  Raw relation value.
            @return  Uppercase, underscore-safe relation label.
            """
            if isinstance(value, (list, tuple)):  # Join list/tuple into one label
                value = " ".join(map(str, value))
            elif not isinstance(value, str):
                value = str(value)
            rel = re.sub(r"[^A-Za-z0-9_]", "_", value.upper()).strip("_")
            # Fallback if empty or invalid start char
            if not rel or not rel[0].isalpha():
                rel = "RELATED_TO"
            return rel

        def _as_list(x: Any) -> List[Any]:
            """Ensure value is returned as a list.
            @param x  Any input type.
            @return  List wrapping the input if needed.
            """
            return list(x) if isinstance(x, (list, tuple)) else [x]

        def _extract(data: Any) -> List[Tuple[str, str, str]]:
            """Extract raw triples from any supported LLM format.
            @param data  Raw triple input (dict, list, etc.).
            @return  List of unprocessed (s, r, o) tuples.
            """
            # List of dicts [{s,r,o}, ...]
            if isinstance(data, list) and data and isinstance(data[0], dict):
                return [
                    (d.get("s") or d.get("subject"), d.get("r") or d.get("relation"), d.get("o") or d.get("object") or d.get("object_")) for d in data
                ]
            # Single dict (scalars or lists)
            if isinstance(data, dict):
                s = data.get("s") or data.get("subject")
                r = data.get("r") or data.get("relation")
                o = data.get("o") or data.get("object") or data.get("object_")
                S, R, O = _as_list(s), _as_list(r), _as_list(o)
                # Expand 1-element lists to match longest list length
                n = max(len(S), len(R), len(O))
                if len(S) == 1 and n > 1:
                    S *= n
                if len(R) == 1 and n > 1:
                    R *= n
                if len(O) == 1 and n > 1:
                    O *= n
                m = min(len(S), len(R), len(O))
                return list(zip(S[:m], R[:m], O[:m]))
            # Single list/tuple triple
            if isinstance(data, (list, tuple)) and len(data) == 3 and not isinstance(data[0], dict):
                return [tuple(data)]
            raise ValueError("Unrecognized triple format")

        # Extract and sanitize all triples
        raw_triples = _extract(data)
        clean_triples = []
        for s, r, o in raw_triples:
            s_clean, r_clean, o_clean = _sanitize_node(s), _sanitize_rel(r), _sanitize_node(o)
            # Only include fully valid triples
            if all([s_clean, r_clean, o_clean]):
                clean_triples.append((s_clean, r_clean, o_clean))
        return clean_triples

    def get_edge_counts(self, top_n: int = 10) -> DataFrame:
        """Return node names and their edge counts, ordered by edge count descending.
        @param top_n  Number of top nodes to return (by edge count). Default is 10.
        @return  DataFrame with columns: node_name, edge_count
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame.
        """
        df = self.connector.get_dataframe(self.graph_name)
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
        
        Log.success(Log.gr_db + Log.kg, f"Found top-{top_n} most popular nodes.", self.connector.verbose)
        return result_df

    def get_all_triples(self) -> DataFrame:
        """Return all triples in the specified graph as a pandas DataFrame.
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame."""
        df = self.connector.get_dataframe(self.graph_name)
        
        # Always return a DataFrame with the 3 desired columns, even if empty or None
        cols = ["subject", "relation", "object"]
        if df is None or df.empty:
            result_df = DataFrame(columns=cols)
            Log.success(Log.gr_db + Log.kg, f"Found 0 triples in graph.", self.connector.verbose)
            return result_df
        
        # Filter to relationships only
        df_rels = df[df["element_type"] == "relationship"].copy()
        df_nodes = df[df["element_type"] == "node"].copy()
        
        # Build triples by matching relationship endpoints to node names
        triples = []
        for _, rel in df_rels.iterrows():
            start_id = rel.get("start_node_id")
            end_id = rel.get("end_node_id")
            rel_type = rel.get("rel_type")
            
            # Find subject and object names
            subject_node = df_nodes[df_nodes["element_id"] == start_id]
            object_node = df_nodes[df_nodes["element_id"] == end_id]
            
            if not subject_node.empty and not object_node.empty:
                subject_name = subject_node.iloc[0].get("name")
                object_name = object_node.iloc[0].get("name")
                if subject_name and object_name and rel_type:
                    triples.append({
                        "subject": subject_name,
                        "relation": rel_type,
                        "object": object_name
                    })
        
        result_df = DataFrame(triples, columns=cols)
        Log.success(Log.gr_db + Log.kg, f"Found {len(result_df)} triples in graph.", self.connector.verbose)
        return result_df

    def print_nodes(self, max_rows: int = 20, max_col_width: int = 50) -> None:
        """Print all nodes and edges in the current pseudo-database with row/column formatting."""
        nodes_df = self.connector.get_dataframe(self.graph_name)
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


