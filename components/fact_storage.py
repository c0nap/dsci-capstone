from components.connectors import DatabaseConnector
from src.util import Log, check_values, df_natural_sorted
from typing import List, Optional
from pandas import DataFrame
from neomodel import config, db
from time import time
import os
import re


class GraphConnector(DatabaseConnector):
    """Connector for Neo4j (graph database).
    @details
        - Uses neomodel to abstract some operations, but raw CQL is required for many tasks.
        - Neo4j does not support multiple logical databases in community edition, so we emulate them.
        - This is achieved by using a 'db' property (database name) and 'kg' property (graph name) on nodes.
    """

    def __init__(self, verbose=False):
        """Creates a new Neo4j connector.
        @param verbose  Whether to print success and failure messages."""
        super().__init__(verbose)
        self._route_db_name = False
        """@brief  Whether to use the database name in the connection string.
        @note  Neo4j is the exception; the free version has no concept of databases."""
        self._auth_suffix = ""
        """@brief  Additional options appended to the connection string. Not used here."""
        database = os.getenv("DB_NAME")
        super().configure("NEO4J", database)
        # Connect neomodel - URL never needs to change for Neo4j
        config.DATABASE_URL = self.connection_string
        
        ## The name of the current graph. Matches node.kg for all nodes in the graph.
        self.graph_name = None
        self.change_graph("default")

        # Add a dummy node to ensure at least 1 valid database exists
        if not self.database_exists(database):
            self.create_database(database)

    def change_database(self, new_database: str):
        """Update the connection URI to reference a different database in the same engine.
        @note  Neo4j does not accept database names routed through the connection string.
        @param new_database  The name of the database to connect to.
        """
        Log.success(Log.gr_db + Log.swap_db, Log.msg_swap_db(self.database_name, new_database), self.verbose)
        self.database_name = new_database
        self.graph_name = "default"
        self.connection_string = f"{self.db_engine}://{self.username}:{self.password}@{self.host}:{self.port}"

    def change_graph(self, graph_name: str):
        """Sets graph_name to create new a Knowledge Graph (collection of triples).
        @details  Similar to creating tables in SQL and collections in Mongo.
        @note  This change will apply to any new nodes created.
        @param graph_name  A string corresponding to the 'kg' node attribute."""
        Log.success(Log.gr_db + Log.swap_kg, Log.msg_swap_kg(self.graph_name, graph_name), self.verbose)
        self.graph_name = graph_name


    def test_connection(self, raise_error=True) -> bool:
        """Establish a basic connection to the Neo4j database.
        @details  By default, Log.fail will raise an exception.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            # Check if connection string is valid
            if self.check_connection(Log.test_conn, raise_error) == False:
                return False
    
            try:    # Run universal test queries
                result, _ = db.cypher_query("RETURN 1")
                if check_values([result[0][0]], [1], self.verbose, Log.gr_db, raise_error) == False:
                    return False
                result, _ = db.cypher_query("RETURN 'TWO'")
                if check_values([result[0][0]], ["TWO"], self.verbose, Log.gr_db, raise_error) == False:
                    return False
                result, _ = db.cypher_query("RETURN 5, 6")
                if check_values([result[0][0], result[0][1]], [5, 6], self.verbose, Log.gr_db, raise_error) == False:
                    return False
            except Exception as e:
                Log.fail(Log.gr_db + Log.test_conn + Log.test_basic, Log.msg_unknown_error, raise_error, e)
                return False
    
            try:   # Display useful information on existing databases
                databases = self.get_unique(key="db")
                Log.success(Log.gr_db, Log.msg_result(databases), self.verbose)
                graphs = self.get_unique(key="kg")
                Log.success(Log.gr_db, Log.msg_result(graphs), self.verbose)
            except Exception as e:
                Log.fail(Log.gr_db + Log.test_conn + Log.test_info, Log.msg_unknown_error, raise_error, e)
                return False
    
            try:   # Create nodes, insert dummy data, and use get_dataframe
                tmp_graph = "test_graph"
                query = f"MATCH (n:TestPerson {{db: '{self.database_name}', kg: '{tmp_graph}'}}) WHERE {self.NOT_DUMMY_()} DETACH DELETE n"
                self.execute_query(query)
                query = f"""CREATE (n1:TestPerson {{db: '{self.database_name}', kg: '{tmp_graph}', name: 'Alice', age: 30}})
                            CREATE (n2:TestPerson {{db: '{self.database_name}', kg: '{tmp_graph}', name: 'Bob', age: 25}}) RETURN n1, n2"""
                self.execute_query(query)
                df = self.get_dataframe(tmp_graph)
                if check_values([len(df)], [2], self.verbose, Log.gr_db, raise_error) == False:
                    return False
                query = f"MATCH (n:TestPerson {{db: '{self.database_name}', kg: '{tmp_graph}'}}) WHERE {self.NOT_DUMMY_()} DETACH DELETE n"
                self.execute_query(query)
            except Exception as e:
                Log.fail(Log.gr_db + Log.test_conn + Log.test_df, Log.msg_unknown_error, raise_error, e)
                return False
    
            try:   # Test create/drop functionality with tmp database
                tmp_db = f"test_db_{int(time())}"
                working_database = self.database_name
                if self.database_exists(tmp_db):
                    self.drop_database(tmp_db)
                self.create_database(tmp_db)
                self.change_database(tmp_db)
                self.execute_query("RETURN 1")
                self.change_database(working_database)
                self.drop_database(tmp_db)
            except Exception as e:
                Log.fail(Log.gr_db + Log.test_conn + Log.test_tmp_db, Log.msg_unknown_error, raise_error, e)
                return False
    
        except Exception as e:
            Log.fail(Log.gr_db + Log.test_conn, Log.msg_unknown_error, raise_error, e)
            return False
        # Finish with no errors = connection test successful
        Log.success(Log.gr_db, Log.msg_db_connect(self.database_name), self.verbose)
        return True

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @details  Connect to Neo4j using ######
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            # Automatically connected, just try a basic query
            db.cypher_query("RETURN 1")
        except Exception as e:
            Log.fail(Log.gr_db + log_source + Log.bad_addr, Log.msg_bad_addr(self.connection_string), raise_error, e)
            return False
        Log.success(Log.gr_db + log_source, Log.msg_db_connect(self.database_name), self.verbose)
        return True

    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single Cypher query to Neo4j.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        @raises RuntimeError  If the query fails to execute.
        """
        # The base class will handle the multi-query case, so prevent a 2nd duplicate query
        result = super().execute_query(query)
        if not self._is_single_query(query):
            return result
        # Derived classes MUST implement single-query execution.
        self.check_connection(Log.run_q, raise_error=True)
        try:
            results, meta = db.cypher_query(query)
            if not results:
                return None
            df = DataFrame(results, columns=[m for m in meta])

            if df is None or df.empty:
                Log.success(Log.gr_db + Log.run_q, Log.msg_good_exec_q(query), self.verbose)
                return None
            else:
                Log.success(Log.gr_db + Log.run_q, Log.msg_good_exec_qr(query, df), self.verbose)
                return df
        except Exception as e:
            Log.fail(Log.gr_db + Log.run_q, Log.msg_bad_exec_q(query), raise_error=True, other_error=e)


    def _split_combined(self, multi_query: str) -> List[str]:
        """Divides a string into non-divisible CQL queries, ignoring comments.
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        # 1. Remove single-line comments
        multi_query = re.sub(r'//.*', '', multi_query)
        # 2. Remove block comments
        multi_query = re.sub(r'/\*.*?\*/', '', multi_query, flags=re.DOTALL)
        # 3. Split by ; and strip
        return [q.strip() for q in multi_query.split(";") if q.strip()]


    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified Knowledge Graph collection.
        @details
            - Fetches all public node attributes, the internal ID, and all labels (e.g. :Person :Character)
            - Does not explode lists or nested values
            - Different approach than DocumentConnector because our node attributes are usually flat key:value already.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None
        @raises RuntimeError  If we fail to create the requested DataFrame for any reason."""
        if name == "":
            name = self.graph_name
        self.check_connection(Log.get_df, raise_error=True)
        try:
            working_graph = self.graph_name
            self.change_graph(name)
            # Get all nodes in the specified graph
            query = f"MATCH (n {self.SAME_DB_KG_()}) WHERE {self.NOT_DUMMY_()} RETURN n"
            results, _ = db.cypher_query(query)
            self.change_graph(working_graph)
            
            # Create a row for each node with attributes as columns - might be unbalanced
            rows = []
            for record in results:
                node = record[0]
                # 1) Public properties, 2) internal ID, and 3) labels
                row = dict(node)
                row["node_id"] = node.element_id
                row["labels"] = list(node.labels)
                rows.append(row)
            # Pandas will fill in NaN where necessary
            df = DataFrame(rows)
            df = df_natural_sorted(df, ignored_columns=['db', 'kg'])

            Log.success(Log.gr_db + Log.get_df, Log.msg_good_graph(name, df), self.verbose)
            return df
        except Exception as e:
            Log.fail(Log.gr_db + Log.get_df, Log.msg_unknown_error, raise_error=True, other_error=e)
        # If not found, warn but do not fail
        Log.warn(Log.gr_db + Log.get_df, Log.msg_bad_graph(name), self.verbose)
        return None


    def get_unique(self, key: str) -> List[str]:
        """Retrieve all unique values for a specified node property.
        @details  Queries all nodes in the database and extracts distinct values for the given key.
        @param key  The node property name to extract unique values from (e.g. 'db' or 'kg').
        @return  A list of unique values for the specified key, or an empty list if none exist.
        @raises RuntimeError  If the query fails to execute."""
        self.check_connection(Log.get_unique, raise_error=True)
        try:
            query = f"""MATCH (n) WHERE n.{key} IS NOT NULL AND {self.NOT_DUMMY_()}
                RETURN DISTINCT n.{key} AS {key} ORDER BY {key}"""
            df = self.execute_query(query)
            if df is None or df.empty:
                return []
            unique_values = df[key].tolist()
            
            Log.success(Log.gr_db + Log.get_unique, Log.msg_result(unique_values), self.verbose)
            return unique_values
        except Exception as e:
            Log.fail(Log.gr_db + Log.get_unique, Log.msg_unknown_error, raise_error=True, other_error=e)


    def create_database(self, database_name: str):
        """Create a fresh pseudo-database if it does not already exist.
        @note  This change will apply to any new nodes created after @ref components.connectors.DatabaseConnector.change_database is called.
        @param database_name  A database ID specifying the pseudo-database.
        @raises RuntimeError  If we fail to create the requested database for any reason."""
        super().create_database(database_name)  # Check if exists
        self.check_connection(Log.create_db, raise_error=True)
        try:
            # Insert a dummy node to resolve self.database_exists()
            query = f"CREATE ({{db: '{database_name}', _init: true}})"
            self.execute_query(query)

            Log.success(Log.gr_db + Log.create_db, Log.msg_success_managed_db("created", database_name), self.verbose)
        except Exception as e:
            Log.fail(Log.gr_db + Log.create_db, Log.msg_fail_manage_db("create", database_name, self.connection_string), raise_error=True, other_error=e)

    def drop_database(self, database_name: str):
        """Delete all nodes stored under a particular database name.
        @param database_name  A database ID specifying the pseudo-database.
        @raises RuntimeError  If we fail to drop the target database for any reason."""
        super().drop_database(database_name)  # Check if exists
        self.check_connection(Log.drop_db, raise_error=True)
        try:
            # Result includes multiple collections & any dummy nodes
            query = f"MATCH (n) WHERE n.db = '{database_name}' DETACH DELETE n"
            self.execute_query(query)

            Log.success(Log.gr_db + Log.create_db, Log.msg_success_managed_db("dropped", database_name), self.verbose)
        except Exception as e:
            Log.fail(Log.gr_db + Log.create_db, Log.msg_fail_manage_db("drop", database_name, self.connection_string), raise_error=True, other_error=e)

    def database_exists(self, database_name: str) -> bool:
        """Search for an existing database using the provided name.
        @param database_name  The name of a database to search for.
        @return  Whether the database is visible to this connector."""
        query = f"""MATCH (n)
            WHERE n.db = '{database_name}'
            RETURN count(n) AS count"""
        # Result includes multiple collections & any dummy nodes
        count = self.execute_query(query).iloc[0, 0]
        return count > 0

    def delete_dummy(self):
        """Delete the initial dummy node from the database.
        @note  Call this method whenever real data is being added to avoid pollution."""
        query = f"MATCH (n) WHERE {self.IS_DUMMY_()} DETACH DELETE n"
        self.execute_query(query)



    # ------------------------------------------------------------------------
    # Knowledge Graph helpers for Semantic Triples
    # ------------------------------------------------------------------------
    def add_triple(self, subject: str, relation: str, object_: str, _delete_init: bool = True):
        """Add a semantic triple to the graph using raw Cypher.
        @details
            1. Finds nodes by exact match on `name` attribute.
            2. Creates a relationship between them with the given label.
        @param subject  A string representing the entity performing an action.
        @param relation  A string describing the action.
        @param object  A string representing the entity being acted upon.
        @param _delete_init  Whether to delete the dummy node added during database creation.
        @raises RuntimeError  If the triple cannot be added to our graph database."""
        if _delete_init:
            self.delete_dummy()

        # Keep only letters, numbers, underscores
        relation = re.sub(r"[^A-Za-z0-9_]", "_", relation)
        subject = re.sub(r"[^A-Za-z0-9_]", "_", subject)
        object_ = re.sub(r"[^A-Za-z0-9_]", "_", object_)

        # Finds or creates 2 nodes with correct attributes, and connects with an edge.
        query = f"""
        MERGE (s {{name: '{subject}', db: '{self.database_name}', kg: '{self.graph_name}'}})
        MERGE (o {{name: '{object_}', db: '{self.database_name}', kg: '{self.graph_name}'}})
        MERGE (s)-[r:{relation}]->(o)
        RETURN s, r, o"""

        try:
            df = self.execute_query(query)
            Log.success(Log.gr_db + Log.kg, f"Added triple: ({subject})-[:{relation}]->({object_})", self.verbose)
            return df
        except Exception as e:
            Log.fail(Log.gr_db + Log.kg, f"Failed to add triple: ({subject})-[:{relation}]->({object_})", raise_error=True, other_error=e)


    def get_edge_counts(self, top_n: int = 10) -> DataFrame:
        """Return node names and their edge counts, ordered by edge count descending.
        @param top_n  Number of top nodes to return (by edge count). Default is 10.
        @return  DataFrame with columns: node_name, edge_count
        @raises RuntimeError  If the query fails to retrieve the requested DataFrame.
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
            df = self.execute_query(query)
            Log.success(Log.gr_db + Log.kg, f"Found top-{top_n} most popular nodes.", self.verbose)
            return df
        except Exception as e:
            Log.fail(Log.gr_db + Log.kg, f"Failed to fetch edge_counts DataFrame.", raise_error=True, other_error=e)


    def get_all_triples(self) -> DataFrame:
        """Return all triples in the current pseudo-database as a pandas DataFrame.
        @raises RuntimeError  If the query fails to retrieve the requested DataFrame."""
        query = f"""
        MATCH (s {self.SAME_DB_KG_()})-[r]->(o {self.SAME_DB_KG_()})
        WHERE {self.NOT_DUMMY_('s')} AND {self.NOT_DUMMY_('o')}
        RETURN s.name AS subject, type(r) AS relation, o.name AS object
        """
        try:
            df = self.execute_query(query)
            # Always return a DataFrame with the 3 desired columns, even if empty or None
            cols = ["subject", "relation", "object"]
            if df is None:
                df = DataFrame()
            df = df.reindex(columns=cols)

            Log.success(Log.gr_db + Log.kg, f"Found {len(df)} triples in graph.", self.verbose)
            return df
        except Exception as e:
            Log.fail(Log.gr_db + Log.kg, f"Failed to fetch all_triples DataFrame.", raise_error=True, other_error=e)



    def print_nodes(self, max_rows: int = 20, max_col_width: int = 50):
        """Print all nodes and edges in the current pseudo-database with row/column formatting."""
        nodes_df = self.get_dataframe()

        # Set pandas display options only within scope
        with option_context("display.max_rows", max_rows,
            "display.max_colwidth", max_col_width):
            print(f"Graph nodes ({len(nodes_df)} total):")
            print(nodes_df)

    def print_triples(self, max_rows: int = 20, max_col_width: int = 50):
        """Print all nodes and edges in the current pseudo-database with row/column formatting."""
        triples_df = self.get_all_triples()

        # Set pandas display options only within scope
        with option_context("display.max_rows", max_rows,
            "display.max_colwidth", max_col_width):
            print(f"Graph triples ({len(triples_df)} total):")
            print(triples_df)




    def IS_DUMMY_(self, alias: str = 'n'):
        """Generates Cypher code to select dummy nodes inside a WHERE clause.
        @details  Usage: MATCH (n) WHERE {self.IS_DUMMY_('n')};
        @return  A string containing Cypher code.
        """
        return f"({alias}._init = true)"

    def NOT_DUMMY_(self, alias: str = 'n'):
        """Generates Cypher code to select non-dummy nodes inside a WHERE clause.
        @details  Usage: MATCH (n) WHERE {self.NOT_DUMMY_('n')};
        @return  A string containing Cypher code.
        """
        return f"({alias}._init IS NULL OR {alias}._init = false)"
    
    def SAME_DB_KG_(self):
        """Generates a Cypher pattern dictionary to match nodes by current database and graph name.
        @details  Usage: MATCH (n {self.SAME_DB_KG_()})
        @return  A string containing Cypher code.
        """
        return f"{{db: '{self.database_name}', kg: '{self.graph_name}'}}"

