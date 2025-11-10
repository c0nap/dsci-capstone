from components.connectors import DatabaseConnector
from contextlib import contextmanager
from neomodel import config, db
import os
from pandas import DataFrame, Series, option_context
import re
from src.util import check_values, df_natural_sorted, Log
from typing import Any, Dict, Generator, List, Optional, Tuple


class GraphConnector(DatabaseConnector):
    """Connector for Neo4j (graph database).
    @details
        - Uses neomodel to abstract some operations, but raw CQL is required for many tasks.
        - Neo4j does not support multiple logical databases in community edition, so we emulate them.
        - This is achieved by using a 'db' property (database name) and 'kg' property (graph name) on nodes.
    """

    def __init__(self, verbose: bool = False) -> None:
        """Creates a new Neo4j connector.
        @param verbose  Whether to print success and failure messages."""
        super().__init__(verbose)
        database = os.environ["DB_NAME"]
        super().configure("NEO4J", database)
        # Connect neomodel - URL never needs to change for Neo4j
        config.DATABASE_URL = self.connection_string

        ## The name of the current graph. Matches node.kg for all nodes in the graph.
        self.graph_name: Optional[str] = "default"

    def change_database(self, new_database: str) -> None:
        """Update the connection URI to reference a different database in the same engine.
        @note  Neo4j does not accept database names routed through the connection string.
        @param new_database  The name of the database to connect to.
        """
        Log.success(Log.gr_db + Log.swap_db, Log.msg_swap_db(self.database_name, new_database), self.verbose)
        self.database_name = new_database
        self.graph_name = "default"
        self.connection_string = f"{self.db_engine}://{self.username}:{self.password}@{self.host}:{self.port}"

    @contextmanager
    def temp_graph(self, graph_name: str) -> Generator[None, None, None]:
        """Temporarily inspect the specified graph, then swap back when finished.
        @param graph_name  The name of a graph in the current database."""
        old = self.graph_name
        self.graph_name = graph_name
        try:
            yield
        finally:
            self.graph_name = old

    def test_connection(self, raise_error: bool = True) -> bool:
        """Establish a basic connection to the Neo4j database, and test full functionality.
        @details  Can be configured to fail silently, which enables retries or external handling.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete.
        """
        # Check if connection string is valid
        if self.check_connection(Log.test_conn, raise_error) == False:
            return False

        try:  # Run universal test queries
            result, _ = db.cypher_query("RETURN 1")
            if check_values([result[0][0]], [1], self.verbose, Log.gr_db, raise_error) == False:
                return False
            result = self.execute_query("RETURN 'TWO'", _filter_results=False)
            if check_values([result.iloc[0, 0]], ["TWO"], self.verbose, Log.gr_db, raise_error) == False:
                return False
            result = self.execute_query("RETURN 5, 6", _filter_results=False)
            if check_values([result.iloc[0, 0], result.iloc[0, 1]], [5, 6], self.verbose, Log.gr_db, raise_error) == False:
                return False
        except Exception as e:
            if not raise_error:
                return False
            raise Log.Failure(Log.gr_db + Log.test_conn + Log.test_basic, Log.msg_unknown_error) from e

        try:  # Display useful information on existing databases
            databases = self.get_unique(key="db")
            Log.success(Log.gr_db, Log.msg_result(databases), self.verbose)
            graphs = self.get_unique(key="kg")
            Log.success(Log.gr_db, Log.msg_result(graphs), self.verbose)
        except Exception as e:
            if not raise_error:
                return False
            raise Log.Failure(Log.gr_db + Log.test_conn + Log.test_info, Log.msg_unknown_error) from e

        try:  # Create nodes, insert dummy data, and use get_dataframe
            with self.temp_graph("test_graph"):
                query = f"MATCH (n:TestPerson {self.SAME_DB_KG_()}) WHERE {self.NOT_DUMMY_()} DETACH DELETE n"
                self.execute_query(query, _filter_results=False)
                query = f"""CREATE (n1:TestPerson {{kg: '{self.graph_name}', name: 'Alice', age: 30}})
                            CREATE (n2:TestPerson {{kg: '{self.graph_name}', name: 'Bob', age: 25}}) RETURN n1, n2"""
                self.execute_query(query, _filter_results=False)
                df = self.get_dataframe(self.graph_name)
                #### TODO: remove once error handling is fixed
                # check_values will raise, so this never became an issue until now
                if df is None:
                    raise Log.Failure(Log.gr_db + Log.test_conn + Log.test_df, "DataFrame fetched from graph 'test_graph' is None")
                if check_values([len(df)], [2], self.verbose, Log.gr_db, raise_error) == False:
                    return False
                query = f"MATCH (n:TestPerson {self.SAME_DB_KG_()}) WHERE {self.NOT_DUMMY_()} DETACH DELETE n"
                self.execute_query(query, _filter_results=False)
        except Exception as e:
            if not raise_error:
                return False
            raise Log.Failure(Log.gr_db + Log.test_conn + Log.test_df, Log.msg_unknown_error) from e

        try:  # Test create/drop functionality with tmp database
            tmp_db = "test_conn"  # Do not use context manager: interferes with traceback
            working_database = self.database_name
            if self.database_exists(tmp_db):
                self.drop_database(tmp_db)
            self.create_database(tmp_db)
            self.change_database(tmp_db)
            self.execute_query("RETURN 1")
            self.change_database(working_database)
            self.drop_database(tmp_db)
        except Exception as e:
            if not raise_error:
                return False
            raise Log.Failure(Log.gr_db + Log.test_conn + Log.test_tmp_db, Log.msg_unknown_error) from e

        # Finish with no errors = connection test successful
        Log.success(Log.gr_db, Log.msg_db_connect(self.database_name), self.verbose)
        return True

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @details  Connect to Neo4j executing a query: db.cypher_query()
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
        try:
            # Automatically connected, just try a basic query
            db.cypher_query("RETURN 1")
        except Exception:  # These errors are usually nasty, so dont print the original.
            if not raise_error:
                return False
            raise Log.Failure(Log.gr_db + log_source + Log.bad_addr, Log.msg_bad_addr(self.connection_string)) from None
        Log.success(Log.gr_db + log_source, Log.msg_db_connect(self.database_name), self.verbose)
        return True

    def execute_query(self, query: str, _filter_results: bool = True) -> Optional[DataFrame]:
        """Send a single Cypher query to Neo4j.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @param _filter_results  If True, limit results to the current database. Needed for internal helper functions.
        @return  DataFrame containing the result of the query, or None
        @throws Log.Failure  If the query fails to execute.
        """
        self.check_connection(Log.run_q, raise_error=True)
        # The base class will handle the multi-query case, so prevent a 2nd duplicate query
        result = super().execute_query(query)
        if not self._is_single_query(query):
            return result
        # Derived classes MUST implement single-query execution.
        try:
            elements, meta = db.cypher_query(query)
            # Re-tag nodes and edges with self.database_name using a second query.
            # Must also re-fetch from Neo4j to ensure our copy is tagged with 'db'
            q = query.lower()
            if "create" in q or "merge" in q:
                self._execute_retag_db()
                elements, meta = self._fetch_latest(elements)
            
            returns_data = self._returns_data(query)
            parsable_to_df = self._parsable_to_df((elements, meta))
            df = _elements_to_df(elements, meta) if returns_data and parsable_to_df else None
            if df is None or df.empty:
                Log.success(Log.gr_db + Log.run_q, Log.msg_good_exec_q(query), self.verbose)
                return None
            else:
                # Return nodes from the current database ONLY, despite what the query wants.
                if _filter_results:
                    df = _filter_to_db(df, self.database_name)
                Log.success(Log.gr_db + Log.run_q, Log.msg_good_exec_qr(query, df), self.verbose)
                return df
        except Exception as e:
            raise Log.Failure(Log.gr_db + Log.run_q, Log.msg_bad_exec_q(query)) from e

    def _split_combined(self, multi_query: str) -> List[str]:
        """Divides a string into non-divisible CQL queries, ignoring comments.
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        # 1. Remove single-line comments
        multi_query = re.sub(r'//.*', '', multi_query)
        # 2. Remove block comments
        multi_query = re.sub(r'/\*.*?\*/', '', multi_query, flags=re.DOTALL)
        # 3. Split by ; outside of strings and strip, respecting escapes
        parts, buf, in_str, prev_backslash = [], '', None, False
        for ch in multi_query:
            if prev_backslash:
                buf += ch
                prev_backslash = False
            elif ch == '\\':
                buf += ch
                prev_backslash = True
            elif ch in "\"'" and not prev_backslash:
                in_str = None if in_str == ch else (ch if in_str is None else in_str)
                buf += ch
            elif ch == ';' and not in_str:
                if buf.strip():
                    parts.append(buf.strip())
                buf = ''
            else:
                buf += ch
        if buf.strip():
            parts.append(buf.strip())
        return parts

    def _returns_data(self, query: str) -> bool:
        """Checks if a query is structured in a way that returns real data, and not status messages.
        @details  Determines whether a Cypher query should yield records.
        - RETURN must be present as a keyword (not in a string value) to return data.
        - YIELD is used for stored procedures, and might return data.
        @param query  A single pre-validated CQL query string.
        @return  Whether the query is intended to fetch data (true) or might return a status message (false).
        """
        q = query.strip().upper()
        # Remove quoted strings to avoid false positives (e.g., {name: "Return of the Jedi"})
        q_no_strings = re.sub(r"'[^']*'|\"[^\"]*\"", "", q)
        # Check if RETURN exists as a keyword (word boundary)
        if re.search(r'\b(RETURN|YIELD)\b', q_no_strings):
            return True
        return False  # Normally we would let execution handle ambiguous cases, but this basic check is exhaustive.

    def _parsable_to_df(self, result: Tuple[Any, Any]) -> bool:
        """Checks if the result of a Neo4j query is valid (i.e. can be converted to a Pandas DataFrame).
        @details
          - Validates shape: (records, meta)
          - Validates content: rows are iterable, elements are dict-like or have .__properties__ (NeoModel Node/Rel)
        @param result  The result of a Cypher query.
        @return  Whether the object is parsable to DataFrame.
        """
        # Handle tuple: (results, meta)
        if not isinstance(result, tuple) or len(result) != 2:
            return False
        records, meta = result
        # Must have row data
        if not records or not isinstance(records, (list, tuple)):
            return False
        # Meta must describe columns
        if not isinstance(meta, (list, tuple, dict)) or len(meta) == 0:
            return False

        # Defensive content validation
        row = records[0]
        if isinstance(row, (list, tuple)):
            # Each cell should be dict-like, scalar, or have .__properties__ (NeoModel Node/Rel)
            for x in row:
                if not (
                    x is None
                    or isinstance(x, (dict, str, int, float, bool))
                    or hasattr(x, "__properties__")  # NeoModel property map
                ):
                    return False
        elif not isinstance(row, (dict, str, int, float, bool)):
            # Single-column result with complex object
            if row is not None and not hasattr(row, "__properties__"):
                return False
        # Structural success only — semantic validation handled by filter_valid (or DataFrame-level filters)
        return True

    def get_dataframe(self, name: str, columns: List[str] = []) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified Knowledge Graph collection.
        @details
            - Fetches all nodes and relationships belonging to the active database + graph name.
            - Includes public attributes, element_id, labels, and element_type.
            - Uses execute_query() for DataFrame conversion and filtering.
            - Does not explode lists or nested values.
        @param name  The name of an existing graph or subgraph.
        @param columns  A list of column names to keep.
        @return  DataFrame containing the requested data, or None.
        @throws Log.Failure  If we fail to create the requested DataFrame for any reason.
        """
        self.check_connection(Log.get_df, raise_error=True)

        with self.temp_graph(name):
            # Fetch both nodes and relationships belonging to this graph and DB
            query = f"""MATCH (n {self.SAME_DB_KG_()})
                OPTIONAL MATCH (n)-[r {self.SAME_DB_KG_()}]->(m)
                RETURN n, r, m"""
            df = self.execute_query(query)

        if df is not None and not df.empty:
            df = df_natural_sorted(df, ignored_columns=['db', 'kg', 'node_id', 'labels'], sort_columns=columns)
            df = df[columns] if columns else df
            Log.success(Log.gr_db + Log.get_df, Log.msg_good_graph(name, df), self.verbose)
            return df

        # If not found, warn but do not fail
        Log.warn(Log.gr_db + Log.get_df, Log.msg_bad_graph(name), self.verbose)
        return None

    def get_unique(self, key: str) -> List[str]:
        """Retrieve all unique values for a specified node property.
        @details  Queries all nodes in the database and extracts distinct values for the given key.
        @param key  The node property name to extract unique values from (e.g. 'db' or 'kg').
        @return  A list of unique values for the specified key, or an empty list if none exist.
        @throws Log.Failure  If the query fails to execute."""
        self.check_connection(Log.get_unique, raise_error=True)

        query = f"""MATCH (n) WHERE n.{key} IS NOT NULL AND {self.NOT_DUMMY_()}
            RETURN DISTINCT n.{key} AS {key} ORDER BY {key}"""
        df = self.execute_query(query, _filter_results=False)
        if df is None or df.empty:
            return []
        unique_values = df[key].tolist()

        Log.success(Log.gr_db + Log.get_unique, Log.msg_result(unique_values), self.verbose)
        return unique_values

    def create_database(self, database_name: str) -> None:
        """Create a fresh pseudo-database if it does not already exist.
        @note  This change will apply to any new nodes created after @ref components.connectors.DatabaseConnector.change_database is called.
        @param database_name  A database ID specifying the pseudo-database.
        @throws Log.Failure  If we fail to create the requested database for any reason."""
        self.check_connection(Log.create_db, raise_error=True)
        super().create_database(database_name)  # Check if exists
        try:
            # Insert a dummy node to resolve self.database_exists()
            query = f"CREATE ({{db: '{database_name}', _init: true}})"
            self.execute_query(query)

            Log.success(Log.gr_db + Log.create_db, Log.msg_success_managed_db("created", database_name), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.gr_db + Log.create_db, Log.msg_fail_manage_db("create", database_name, self.connection_string)) from e

    def drop_database(self, database_name: str) -> None:
        """Delete all nodes stored under a particular database name.
        @param database_name  A database ID specifying the pseudo-database.
        @throws Log.Failure  If we fail to drop the target database for any reason."""
        self.check_connection(Log.drop_db, raise_error=True)
        super().drop_database(database_name)  # Check if exists
        try:
            # Result includes multiple collections & any dummy nodes
            query = f"MATCH (n) WHERE n.db = '{database_name}' DETACH DELETE n"
            self.execute_query(query)

            Log.success(Log.gr_db + Log.drop_db, Log.msg_success_managed_db("dropped", database_name), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.gr_db + Log.drop_db, Log.msg_fail_manage_db("drop", database_name, self.connection_string)) from e

    def drop_graph(self, graph_name: str) -> None:
        """Delete all nodes stored under a particular graph name.
        @param graph_name  The name of a graph in the current database.
        @throws Log.Failure  If we fail to drop the target graph for any reason."""
        self.check_connection(Log.drop_gr, raise_error=True)
        try:
            with self.temp_graph(graph_name):
                # Result includes any dummy nodes
                query = f"MATCH (n {self.SAME_DB_KG_()}) DETACH DELETE n"
                self.execute_query(query)

            Log.success(Log.gr_db + Log.drop_gr, Log.msg_success_managed_gr("dropped", graph_name), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.gr_db + Log.drop_gr, Log.msg_fail_manage_gr("drop", graph_name, self.connection_string)) from e

    def database_exists(self, database_name: str) -> bool:
        """Search for an existing database using the provided name.
        @param database_name  The name of a database to search for.
        @return  Whether the database is visible to this connector."""
        query = f"""MATCH (n)
            WHERE n.db = '{database_name}'
            RETURN count(n) AS count"""
        # Result includes multiple collections & any dummy nodes
        result = self.execute_query(query, _filter_results=False)
        if result is None:
            return False
        count = result.iloc[0, 0]
        return count > 0

    def delete_dummy(self) -> None:
        """Delete the initial dummy node from the database.
        @note  Never use this. Enables the existence of an "empty" database."""
        query = f"MATCH (n) WHERE {self.IS_DUMMY_()} DETACH DELETE n"
        self.execute_query(query, _filter_results=False)

    def _execute_retag_db(self) -> None:
        """Sweeps the database for untagged nodes and relationships, and adds a 'db' attribute."""
        db.cypher_query(
            f"""MATCH (n) WHERE n.db IS NULL
            SET n.db = '{self.database_name}'"""
        )
        db.cypher_query(
            f"""MATCH ()-[r]->() WHERE r.db IS NULL
            SET r.db = '{self.database_name}'"""
        )

    def _fetch_latest(self, results: List[Tuple[Any, ...]]) -> Tuple[Optional[List[Tuple[Any, ...]]], Optional[List[str]]]:
        """Re-fetch nodes and edges after changing the remote copy in Neo4j.
        @param results Original list of untagged tuples from db.cypher_query().
        @return Latest version of results fetched from the database."""
        if not results:
            return (None, None)
        # Gather element IDs for all nodes and relationships
        node_ids, rel_ids = [], []
        for row in results:
            for obj in (row if isinstance(row, (list, tuple)) else [row]):
                if not hasattr(obj, "element_id"):
                    continue
                if hasattr(obj, "start_node") or hasattr(obj, "nodes"):  # relationship
                    rel_ids.append(obj.element_id)
                else:  # node
                    node_ids.append(obj.element_id)
        if not node_ids and not rel_ids:
            return (None, None)

        # Build a single query to get all necessary elements
        parts = []
        if node_ids:
            node_ids_str = "[" + ", ".join(f"'{i}'" for i in node_ids) + "]"
            parts.append(f"MATCH (n) WHERE elementId(n) IN {node_ids_str} RETURN n")
        if rel_ids:
            rel_ids_str = "[" + ", ".join(f"'{i}'" for i in rel_ids) + "]"
            parts.append(f"MATCH ()-[r]->() WHERE elementId(r) IN {rel_ids_str} RETURN r")
        union_query = " UNION ALL ".join(parts)

        fresh_results, fresh_meta = db.cypher_query(union_query)
        return fresh_results, fresh_meta

    # ------------------------------------------------------------------------
    # Knowledge Graph helpers for Semantic Triples
    # ------------------------------------------------------------------------
    def add_triple(self, subject: str, relation: str, object_: str) -> None:
        """Add a semantic triple to the graph using raw Cypher.
        @param subject  A string representing the entity performing an action.
        @param relation  A string describing the action.
        @param object_  A string representing the entity being acted upon.
        @note  LLM output should be pre-normalized using @ref components.fact_storage.GraphConnector.normalize_triples.
        @throws Log.Failure  If the triple cannot be added to our graph database.
        """

        # Normalize already-cleaned inputs for Cypher safety
        relation = re.sub(r"[^A-Za-z0-9_]", "_", relation).upper().strip("_")
        subject = re.sub(r"[^A-Za-z0-9_ ]", "_", subject).strip("_ ")
        object_ = re.sub(r"[^A-Za-z0-9_ ]", "_", object_).strip("_ ")
        if not relation or not subject or not object_:
            raise Log.Failure(Log.gr_db + Log.kg, f"Invalid triple: ({subject})-[:{relation}]->({object_})")

        # Merge subject/object and connect via relation
        query = f"""
        MERGE (s {{name: '{subject}', db: '{self.database_name}', kg: '{self.graph_name}'}})
        MERGE (o {{name: '{object_}', db: '{self.database_name}', kg: '{self.graph_name}'}})
        MERGE (s)-[r:{relation}]->(o)
        RETURN s, r, o
        """

        try:
            df = self.execute_query(query, _filter_results=False)
            if df is not None:
                Log.success(Log.gr_db + Log.kg, f"Added triple: ({subject})-[:{relation}]->({object_})", self.verbose)
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
        query = f"""
        MATCH (n {self.SAME_DB_KG_()})
        WHERE {self.NOT_DUMMY_()}
        OPTIONAL MATCH (n)-[r]-()
        WITH n.name as node_name, count(r) as edge_count
        ORDER BY edge_count DESC, rand()
        LIMIT {top_n}
        RETURN node_name, edge_count"""
        try:
            df = self.execute_query(query, _filter_results=False)
            Log.success(Log.gr_db + Log.kg, f"Found top-{top_n} most popular nodes.", self.verbose)
            return df
        except Exception as e:
            raise Log.Failure(Log.gr_db + Log.kg, f"Failed to fetch edge_counts DataFrame.") from e

    def get_all_triples(self, graph_name: Optional[str] = None) -> DataFrame:
        """Return all triples in the specified graph as a pandas DataFrame.
        @param graph_name  The graph to query. If None, uses self.graph_name.
        @throws Log.Failure  If the query fails to retrieve the requested DataFrame."""

        target_graph = graph_name if graph_name is not None else self.graph_name
        with self.temp_graph(target_graph):
            # No need to apply the DB-KG pattern to relationships - relaxes query requirements.
            query = f"""
            MATCH (s {self.SAME_DB_KG_()})-[r]->(o {self.SAME_DB_KG_()})
            WHERE {self.NOT_DUMMY_('s')} AND {self.NOT_DUMMY_('o')}
            RETURN s.name AS subject, type(r) AS relation, o.name AS object
            """
            try:
                df = self.execute_query(query, _filter_results=False)
                # Always return a DataFrame with the 3 desired columns, even if empty or None
                cols = ["subject", "relation", "object"]
                if df is None:
                    df = DataFrame()
                df = df.reindex(columns=cols)

                Log.success(Log.gr_db + Log.kg, f"Found {len(df)} triples in graph.", self.verbose)
                return df
            except Exception as e:
                raise Log.Failure(Log.gr_db + Log.kg, f"Failed to fetch all_triples DataFrame.") from e

    def print_nodes(self, max_rows: int = 20, max_col_width: int = 50) -> None:
        """Print all nodes and edges in the current pseudo-database with row/column formatting."""
        nodes_df = self.get_dataframe(self.graph_name)
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

    def IS_DUMMY_(self, alias: str = 'n') -> str:
        """Generates Cypher code to select dummy nodes inside a WHERE clause.
        @details  Usage: MATCH (n) WHERE {self.IS_DUMMY_('n')};
        @return  A string containing Cypher code.
        """
        return f"({alias}._init IS NOT NULL AND {alias}._init = true)"

    def NOT_DUMMY_(self, alias: str = 'n') -> str:
        """Generates Cypher code to select non-dummy nodes inside a WHERE clause.
        @details  Usage: MATCH (n) WHERE {self.NOT_DUMMY_('n')};
        @return  A string containing Cypher code.
        """
        return f"({alias}._init IS NULL OR {alias}._init = false)"

    def SAME_DB_KG_(self) -> str:
        """Generates a Cypher pattern dictionary to match nodes by current database and graph name.
        @details  Usage: MATCH (n {self.SAME_DB_KG_()})
        @return  A string containing Cypher code.
        """
        return f"{{db: '{self.database_name}', kg: '{self.graph_name}'}}"



def _filter_to_db(self, df: DataFrame) -> DataFrame:
    """Filter a DataFrame by database context.
    @details
        - Keeps nodes where 'db' matches the current database name and _init is False/absent.
        - Keeps relationships if either endpoint node (by elementId) matches the same db.
        - Works on unflattened frames where cells are dicts (node/rel maps).
        - Safely ignores missing fields; drops a top-level '_init' column if present.
    @param df  DataFrame containing node and relationship rows.
    @return  Filtered DataFrame restricted to the active database.
    """
    if df is None or df.empty:
        return df

    # --- Helpers for unflattened (dict-in-cell) rows ---
    def node_ok(d: Any) -> bool:
        return (
            isinstance(d, dict)
            and d.get("db") == self.database_name
            and not d.get("_init", False)
        )

    def elem_id_from_node_map(d: dict) -> Any:
        # Be flexible about id field names
        return d.get("element_id") or d.get("node_id") or d.get("id")

    # Build a set of "good" node elementIds present anywhere in the frame
    good_node_ids = set()
    for col in df.columns:
        for v in df[col]:
            if node_ok(v):
                eid = elem_id_from_node_map(v)
                if eid is not None:
                    good_node_ids.add(eid)

    def rel_touches_good(v: Any) -> bool:
        """True if a relationship dict touches any good node by elementId."""
        if not isinstance(v, dict):
            return False

        # Direct endpoint id fields commonly seen in custom packing
        for k in ("start_id", "end_id", "startElementId", "endElementId"):
            if v.get(k) in good_node_ids:
                return True

        # Nested endpoint node maps
        for k in ("start", "end", "start_node", "end_node"):
            n = v.get(k)
            if isinstance(n, dict) and elem_id_from_node_map(n) in good_node_ids:
                return True

        # List/tuple of endpoint nodes (e.g., 'nodes': [start, end])
        nodes = v.get("nodes")
        if isinstance(nodes, (list, tuple)):
            for n in nodes:
                if isinstance(n, dict) and elem_id_from_node_map(n) in good_node_ids:
                    return True

        return False

    # Row-wise decisions (unflattened): keep if any node_ok OR any rel touches good node
    any_node_ok = df.apply(lambda row: any(node_ok(v) for v in row), axis=1)
    any_rel_ok  = df.apply(lambda row: any(rel_touches_good(v) for v in row), axis=1)

    keep = (any_node_ok | any_rel_ok)
    filtered = df.loc[keep].drop(columns="_init", errors="ignore")
    return filtered


def _tuples_to_df(elements: List[Dict[str, Any]], meta: List[str] = None) -> DataFrame:
    """Convert Neo4j query results (nodes and relationships) into a Pandas DataFrame.
    @details
        - Accepts the `elements` output of db.cypher_query().
        - Automatically unwraps NeoModel Node/Relationship objects into plain dicts via `.__properties__`.
        - Adds an 'element_type' property distinguishing nodes vs relationships.
    Example Query: MATCH (a) (r) (b) RETURN a AS node_1, r AS edge, b AS node_2;
    Result: DataFrame with `node_1` and `node_2` columns containing Nodes converted to dicts,
        and an `edge` column containing a dict-cast relationship (element_type: relation).
    @param tuples  List of Neo4j query result tuples.
    @param meta  List of element aliases returned by the query, and used here as column names.
    @return  DataFrame with requested columns.
    """

def _normalize_elements(df: DataFrame) -> DataFrame:
    """Convert Neo4j query results (nodes and relationships) into a Pandas DataFrame.
    @details
        - Accepts the DataFrame output of @ref components.fact_storage.GraphConnector.execute_query.
        - Explodes dict-cast elements from columns into rows, resulting in 1 node or relation per row.
        - Normalizes node and relation properties as columns. `element_id`, `element_type` are shared.
        - Node-only properties (e.g. labels) are None for relationships, and likewise for relations (e.g. start_node).
        - Returns an empty DataFrame for no results.
    @param df  DataFrame containing dict-cast nodes and relationships.
    @return  DataFrame suitable for downstream filtering and analysis.
    """
    if not elements:
        return DataFrame()

    # --- Step 1: Normalize NeoModel objects to plain dicts ---
    normalized = []
    element_types = []  # track node/rel/scalar per row
    for row in elements:
        new_row = []
        element_type = None
        for x in row:
            if x is None:
                continue
            elif hasattr(x, "__properties__"):
                # NeoModel Node/Rel: extract property map and preserve IDs/labels
                props = dict(x.__properties__)
                props["element_id"] = getattr(x, "element_id", None)
                props["labels"] = list(getattr(x, "labels", []))
                # Identify if this is a Node or Relationship object
                if hasattr(x, "start_node") or hasattr(x, "end_node") or hasattr(x, "nodes"):
                    element_type = "relationship"
                else:
                    element_type = "node"
                new_row.append(props)
            elif isinstance(x, dict):
                new_row.append(x)
                element_type = "dict"
            elif isinstance(x, list):
                new_row.append(x)
                element_type = "list"
            else:
                # Scalars or unrecognized types — keep as-is
                new_row.append(x)
                element_type = "scalar"
        normalized.append(new_row)
        element_types.append("unknown")

    # --- Step 2: Construct the DataFrame ---
    df = DataFrame(normalized, columns=meta if meta else None)
    df["element_type"] = element_types
    return df
