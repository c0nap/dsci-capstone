import os
from time import time
from pandas import DataFrame
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, text, Table, MetaData, select
from sqlalchemy.exc import NoSuchTableError
from dotenv import load_dotenv
from typing import List
from sqlparse import parse as sql_parse
from src.util import Log

# Read environment variables at compile time
load_dotenv(".env")


class Connector(ABC):
    """Abstract base class for external connectors.
    @note  Credentials are specified in the .env file.
    @details
        Derived classes should implement:
        - __init__
        - @ref components.connectors.Connector.configure
        - @ref components.connectors.Connector.test_connection
        - @ref components.connectors.Connector.execute_query
        - @ref components.connectors.Connector.execute_file
    """

    @abstractmethod
    def configure(self, DB: str, database_name: str):
        """Read connection settings from the .env file.
        @param DB  The prefix of fetched credentials.
        @param database_name  The specific service to connect to."""
        pass

    @abstractmethod
    def test_connection(self, raise_error=True) -> bool:
        """Establish a basic connection to the database.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete."""
        pass

    @abstractmethod
    def execute_query(self, query: str) -> object:
        """Send a single command through the connection.
        @param query  A single query to perform on the database.
        @return  The result of the query, or None
        """
        pass

    @abstractmethod
    def execute_file(self, filename: str) -> list:
        """Run several commands from a file.
        @param filename  The path to a specified query or prompt file (.sql, .txt).
        @return  Whether the query was performed successfully."""
        pass


class DatabaseConnector(Connector):
    """Abstract base class for database engine connectors.
    @details
        Derived classes should implement:
        - @ref components.connectors.DatabaseConnector.__init__
        - @ref components.connectors.DatabaseConnector.test_connection
        - @ref components.connectors.DatabaseConnector.execute_query
        - @ref components.connectors.DatabaseConnector._split_combined
        - @ref components.connectors.DatabaseConnector.get_dataframe
        - @ref components.connectors.DatabaseConnector.create_database
        - @ref components.connectors.DatabaseConnector.drop_database
    """

    def __init__(self, verbose=False):
        """Initialize the connector.
        @param verbose  Whether to print debug messages.
        @note  Attributes will be set to None until @ref components.connectors.DatabaseConnector.configure() is called.
        """
        ## The common name for the type of database as observed in the .env prefixes (MYSQL, POSTGRES, MONGO, or NEO4J).
        self.db_type = None
        ## The protocol specifying the database type, syntax is usually dialect+driver.
        self.db_engine = None
        ## The username used to access the database.
        self.username = None
        ## The password used to access the database.
        self.password = None
        ## The IP address where the database service is hosted.
        self.host = None
        ## The port number where the database service is hosted.
        self.port = None
        ## The collection being modified by this connector (Optional since Neo4j does not have one).
        self.database_name = None
        ## URI of the database connection: syntax is engine://username:password@host:port/database.
        self.connection_string = None
        ## Whether to use the database name in the connection string.
        self._route_db_name = None
        ## Whether to print debug messages.
        self.verbose = verbose

    def configure(self, DB: str, database_name: str):
        """Read connection settings from the .env file.
        @param DB  The prefix of fetched database credentials.
        @param database_name  The name of the database to connect to.
        """
        self.db_type = DB
        # The .env file contains multiple credentials.
        # Here we select environment variables corresponding to our database engine.
        self.db_engine = os.getenv(f"{DB}_ENGINE")
        self.username = os.getenv(f"{DB}_USERNAME")
        self.password = os.getenv(f"{DB}_PASSWORD")
        self.host = os.getenv(f"{DB}_HOST")
        self.port = os.getenv(f"{DB}_PORT")
        # Condense the above variables into a connection string
        self.change_database(database_name)

    def change_database(self, new_database: str):
        """Update the connection URI to reference a different database in the same engine.
        @param new_database  The name of the database to connect to.
        @param _route_db_name  Whether to use the database name in the connection string.
        """
        self.database_name = new_database
        if self._route_db_name:
            self.connection_string = f"{self.db_engine}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"
        else:
            self.connection_string = f"{self.db_engine}://{self.username}:{self.password}@{self.host}:{self.port}"

    @abstractmethod
    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single command through the connection.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        """
        # Perform basic error checks.
        query = query.strip()  # Remove whitespace
        if not query:
            return None  # Check if empty
        if not self._is_single_query(query):
            results = self.execute_combined(query)
            if len(results) == 0:
                return None
            # Return the final result if several are found.
            if len(results) > 1 and self.verbose:
                # Warn when earlier results are ignored
                Log.fail(Log.db_conn_abc + Log.run_q, Log.msg_multiple_query(len(results), query), raise_error=False)
            return results[-1]
        # Derived classes MUST implement single-query execution.
        pass

    def execute_combined(self, multi_query: str) -> List[DataFrame]:
        """Run several database commands in sequence.
        @param multi_query  A string containing multiple queries.
        @return  A list of query results converted to DataFrames."""
        queries = self._split_combined(multi_query)
        results = []
        # No error handling - execute_query will take care of it
        for query in queries:
            df = self.execute_query(query)
            if df is not None:
                results.append(df)
        return results

    def execute_file(self, filename: str) -> List[DataFrame]:
        """Run several database commands from a file.
        @note  Loads the entire file into memory at once.
        @param filename  The path to a specified query file (.sql, .cql, .json).
        @return  Whether the query was performed successfully."""
        
        try:  # Read the entire file as a multi-query string
            with open(filename, "r") as file:
                multi_query = file.read()
                if self.verbose:
                    Log.success(Log.db_conn_abc + Log.run_f, Log.msg_good_path(filename))
        except Exception as e:
            Log.fail(Log.db_conn_abc + Log.run_f, Log.msg_bad_path(filename), raise_error=True, e)
        
        try:  # Attempt to run the multi-query
            results = self.execute_combined(multi_query)
            if self.verbose:
                Log.success(Log.db_conn_abc + Log.run_f, Log.msg_good_exec_f(filename))
            return results
        except Exception as e:
            Log.fail(Log.db_conn_abc + Log.run_f, Log.msg_bad_exec_f(filename), raise_error=True, e)

    @abstractmethod
    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified resource.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        pass

    @abstractmethod
    def create_database(self, database_name: str):
        """Use the current database connection to create a sibling database in this engine.
        @param database_name  The name of the new database to create."""
        self.drop_database(database_name)
        pass

    @abstractmethod
    def drop_database(self, database_name: str = ""):
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database."""
        if not database_name:
            database_name = self.database_name
        pass

    def _is_single_query(self, query: str) -> bool:
        """Checks if a string contains multiple queries.
        @param query  A single or combined query string.
        @return  Whether the query is single (true) or combined (false)."""
        queries = self._split_combined(query)
        return len(queries) == 1

    @abstractmethod
    def _split_combined(self, multi_query: str) -> List[str]:
        """Checks if a string contains multiple queries.
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        pass


class RelationalConnector(DatabaseConnector):
    """Connector for relational databases (MySQL, PostgreSQL).
    @details
        Uses SQLAlchemy to abstract complex database operations.
        Hard-coded queries are used for testing purposes, and depend on the specific engine.
    """

    def __init__(self, verbose, specific_queries: list):
        """Creates a new database connector. Use @ref components.connectors.RelationalConnector.from_env instead (this is called by derived classes).
        @param verbose  Whether to print success and failure messages.
        @param specific_queries  A list of helpful SQL queries.
        """
        super().__init__(verbose)
        self._route_db_name = True
        """@brief  Whether to use the database name in the connection string.
        @note  MySQL and PostgreSQL both ask for this. We avoided using databases that don't fit this pattern."""
        engine = os.getenv("DB_ENGINE")
        database = os.getenv("DB_NAME")
        super().configure(engine, database)

        self._specific_queries = specific_queries
        """@brief  Hard-coded queries which depend in the specific engine, and cannot be abstracted with SQLAlchemy.
        @note  This is set by derived classes e.g. 'mysqlConnector' for lanugage-sensitive syntax."""
        assert len(specific_queries) == 2

    @classmethod
    def from_env(cls, verbose=False):
        """Decides what type of relational connector to create using the .env file.
        @param verbose  Whether to print success and failure messages."""
        engine = os.getenv("DB_ENGINE")
        if engine == "MYSQL":
            return mysqlConnector(verbose)
        elif engine == "POSTGRES":
            return postgresConnector(verbose)
        Log.fail(
            f"Database engine '{engine}' not supported. Did you mean 'MYSQL' or 'POSTGRES'?"
        )

    def test_connection(self, raise_error=True) -> bool:
        """Establish a basic connection to the database.
        @details  By default, Log.fail will raise an exception.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            # Check if connection string is valid
            if self.check_connection(Log.test_conn, raise_error) == False:
                return False

            with engine.connect() as connection:
                try:    # Run universal test queries
                    result = connection.execute(text("SELECT 1")).fetchone()[0]
                    if self._check_values([result], [1], raise_error) == False:
                        return False
                    result = self.execute_query("SELECT 'TWO';").iloc[0, 0]
                    if self._check_values([result], ["TWO"], raise_error) == False:
                        return False
                    results = self.execute_combined("SELECT 3; SELECT 4;")
                    if self._check_values([results[0].iloc[0, 0], results[1].iloc[0, 0]], [3, 4], raise_error) == False:
                        return False
                    results = self.execute_query("SELECT 5, 6;")
                    if self._check_values([results[0].iloc[0, 0], results[0].iloc[0, 1]], [5, 6], raise_error) == False:
                        return False
                except Exception as e:
                    Log.fail(Log.rel_db + Log.test_conn + Log.test_basic, Log.msg_unknown_error, raise_error, e)
                    return False

                try:   # Display useful information on existing databases
                    db_name = self.execute_query(self._specific_queries[0]).iloc[0, 0]
                    self._check_values([db_name], [self.database_name], raise_error)
                    databases = self.execute_query(self._specific_queries[1])
                    if self.verbose:
                        Log.success(Log.rel_db, Log.msg_result(databases))
                except Exception as e:
                    Log.fail(Log.rel_db + Log.test_conn + Log.test_info, Log.msg_unknown_error, raise_error, e)
                    return False

                try:   # Create a table, insert dummy data, and use get_dataframe
                    tmp_table = f"test_table_{int(time())}"
                    self.execute_query(f"CREATE TABLE {tmp_table} (id INT PRIMARY KEY, name VARCHAR(255)); INSERT INTO {tmp_table} (id, name) VALUES (1, 'Alice');")
                    df = self.get_dataframe(f"{tmp_table}")
                    self._check_values([df.at[0, 'name']], ['Alice'])
                    self.execute_query(f"DROP TABLE {tmp_table};")
                except Exception as e:
                    Log.fail(Log.rel_db + Log.test_conn + Log.test_df, Log.msg_unknown_error, raise_error, e)
                    return False

                try:   # Test create/drop functionality with tmp database
                    tmp_db = f"test_db_{int(time())}"
                    working_database = self.database_name
                    self.create_database(tmp_db)
                    self.change_database(tmp_db)
                    self.drop_database(tmp_db)
                    self.change_database(working_database)
                except Exception as e:
                    Log.fail(Log.rel_db + Log.test_conn + Log.test_tmp_db, Log.msg_unknown_error, raise_error, e)
                    return False

        except Exception as e:
            Log.fail(Log.rel_db + Log.test_conn, Log.msg_unknown_error, raise_error, e)
            return False
        # Finish with no errors = connection test successful
        if self.verbose:
            Log.success(Log.rel_db, Log.msg_db_connect(self.database_name))
        return True


    def check_connection(log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @details  Connect to MongoDB using the low-level PyMongo handle.
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            engine = create_engine(self.connection_string)
            # SQLAlchemy will not create the connection until we send a query
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception as e:
            Log.fail(Log.rel_db + log_source + Log.bad_addr, Log.msg_bad_addr(self.connection_string), raise_error, e)
            return False
        if self.verbose:
            Log.success(Log.rel_db + log_source, Log.msg_db_connect(self.database_name))
        return True


    def _check_values(results, expected, raise_error):
        for i in range(len(results)):
            if self.verbose && result == expected[i]:
                Log.success(Log.rel_db + Log.good_val, Log.msg_compare(result, 1))
            elif results[i] != expected[i]:
                Log.fail(Log.rel_db + Log.bad_val, Log.msg_compare(result, 1), raise_error)
                return False
        return True


    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single command to the database connection.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        """
        super().execute_query(query)
        self.check_connection(Log.run_q, raise_error=True)
        # Derived classes MUST implement single-query execution.
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as connection:
                result = connection.execute(text(query))
                if result.returns_rows and result.keys():
                    result = DataFrame(result.fetchall(), columns=result.keys())
                connection.commit()
                if self.verbose:
                    Log.success(Log.rel_db + Log.run_q, Log.msg_good_exec_q(query, result))
                return result
        except Exception as e:
            Log.fail(Log.rel_db + Log.run_q, Log.msg_bad_exec_q(query), raise_error=True, e)

    def _split_combined(self, multi_query: str) -> List[str]:
        """Checks if a string contains multiple queries.
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        queries = []
        for query in sql_parse(multi_query):
            query = str(query).strip()
            if query:
                queries.append(query)
        return queries

    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified table using SQLAlchemy.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        super().get_dataframe(name)
        self.check_connection(Log.get_df, raise_error=True)
        for table_name in (name, name.lower()):
            try:
                engine = create_engine(self.connection_string)
                with engine.connect() as connection:
                    table = Table(table_name, MetaData(), autoload_with=engine)
                    result = connection.execute(select(table))
                    if result.returns_rows and result.keys():
                        result = DataFrame(result.fetchall(), columns=result.keys())
                    if self.verbose:
                        Log.success(Log.rel_db + Log.get_df, Log.msg_good_table(table_name))
                    return result
            except NoSuchTableError:
                # Postgres will auto-lowercase all table names. Give it one more try with the lowercase name.
                continue
            except Exception as e:
                Log.fail(Log.rel_db + Log.get_df, Log.msg_unknown_error, raise_error=True, e)
        Log.fail(Log.rel_db + Log.get_df, Log.msg_bad_table(name), raise_error=False)
        return None

    def create_database(self, database_name: str):
        """Use the current database connection to create a sibling database in this engine.
        @param database_name  The name of the new database to create."""
        super().create_database(database_name)
        self.check_connection(Log.create_db, raise_error=True)
        try:
            # Auto-commit is required for database management operations
            engine = create_engine(self.connection_string)
            with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as connection:
                connection.execute(text(f"CREATE DATABASE {database_name}"))

            if self.verbose:
                Log.success(Log.rel_db + Log.create_db, Log.msg_success_managed_db("created", database_name))
        except Exception as e:
            Log.fail(Log.rel_db + Log.create_db, Log.msg_fail_manage_db(self.connection_string, database_name, "create"), raise_error=True, e)

    def drop_database(self, database_name: str = ""):
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database."""
        super().drop_database(database_name)
        self.check_connection(Log.drop_db, raise_error=True)
        try:
            # Auto-commit is required for database management operations
            engine = create_engine(self.connection_string)
            with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as connection:
                connection.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))
            
            if self.verbose:
                Log.success(Log.rel_db + Log.create_db, Log.msg_success_managed_db("dropped", database_name))
        except Exception as e:
            Log.fail(Log.rel_db + Log.create_db, Log.msg_fail_manage_db(self.connection_string, database_name, "drop"), raise_error=True, e)


class mysqlConnector(RelationalConnector):
    """A relational database connector configured for MySQL.
    @note  Should be hidden from the user using a factory method."""

    def __init__(self, verbose=False):
        """Configures the relational connector.
        @param verbose  Whether to print success and failure messages."""
        super().__init__(
            verbose, self.specific_queries["MYSQL"]
        )

    # A list of basic test queries used by RelationalConnector
    specific_queries = {
        "MYSQL": [
            "SELECT DATABASE();",  # Single value, name of the current database.
            "SHOW DATABASES;",     # List of databases the secondary user can access.
        ]  # List of all databases in the database engine.
    }


class postgresConnector(RelationalConnector):
    """A relational database connector configured for PostgreSQL.
    @note  Should be hidden from the user using a factory method."""

    def __init__(self, verbose=False):
        """Configures the relational connector.
        @param verbose  Whether to print success and failure messages."""
        super().__init__(
            verbose, self.specific_queries["POSTGRES"]
        )

    # A list of basic test queries used by RelationalConnector
    specific_queries = {
        "POSTGRES": [
            "SELECT current_database();",  # Single value, name of the current database.
            "SELECT datname FROM pg_database;",  # List of ALL databases, even ones we cannot access.
        ]  # List of all databases in the database engine.
    }
















import json
from typing import List, Optional, Any, Dict
from dataclasses import dataclass, asdict, is_dataclass

import mongoengine
from mongoengine import (
    Document,
    DynamicDocument,
)

from pandas import DataFrame, json_normalize


class DocumentConnector(DatabaseConnector):
    """Connector for MongoDB (document database)
    @details
        - Uses mongoengine.connect(...) on-demand for connections.
        - Low-level operations use pymongo via mongoengine.get_db().
        - create_database uses an init collection insertion (MongoDB is lazy).
    """

    def __init__(self, verbose: bool = False):
        """Creates a new MongoDB connector.
        @param verbose  Whether to print debug messages.
        """
        super().__init__(verbose)
        self._route_db_name = True
        """@brief  Whether to use the database name in the connection string.
        @note  mongoengine.connect is used on-demand; we keep the convention of routing the DB name."""
        super().configure("MONGO", database_name="default")



    def test_connection(self, raise_error=True) -> bool:
        """Establish a basic connection to the database.
        @details  By default, Log.fail will raise an exception.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            # Check if connection string is valid
            if self.check_connection(Log.test_conn, raise_error) == False:
                return False

        except Exception as e:
            Log.fail(Log.doc_db + Log.test_conn, Log.msg_unknown_error, raise_error, e)
            return False
        # Finish with no errors = connection test successful
        if self.verbose:
            Log.success(Log.doc_db, Log.msg_db_connect(self.database_name))
        return True

    def check_connection(log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @details  Connect to MongoDB using the low-level PyMongo handle.
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            mongoengine.connect(host=self.connection_string)
        except Exception as e:
            Log.fail(Log.doc_db + log_source + Log.bad_addr, Log.msg_bad_addr(self.connection_string), raise_error, e)
            return False
        if self.verbose:
            Log.success(Log.rel_db + log_source, Log.msg_db_connect(self.database_name))
        return True

    


    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single MongoDB command using PyMongo.
        @details
          - The query must be a valid JSON command object (e.g. {"find": "users", "filter": {...}}).
          - Mongo shell syntax such as `db.users.find({...})` or `.js` files will NOT work.
          - If a result is returned, it will be converted to a DataFrame.
        """
        self.check_connection(Log.run_q, raise_error=True)
        super().execute_query(query)
    
        try:
            # Connect to MongoDB using the low-level PyMongo handle
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()

            # Queries must be valid JSON
            try:
                cmd_obj = json.loads(query)
            except json.JSONDecodeError:
                Log.fail(Log.doc_db + Log.run_q, Log.msg_fail_parse("query", "JSON command object", query), raise_error=True, e)
    
            # Execute via PyMongo
            results = db.command(cmd_obj)
    
            # Mongo queries can return a dict or list
            # Standardize everything to a list of documents
            docs = []
            if isinstance(results, dict):
                if "cursor" in results:
                    docs = results["cursor"].get("firstBatch", [])
                elif "firstBatch" in results:
                    docs = results["firstBatch"]
                else:
                    # wrap single dict as list
                    docs = [results]
            elif isinstance(results, list):
                docs = results
            
            # Convert document list to DataFrame if any docs exist
            result = self._docs_to_df(docs)
            if self.verbose:
                Log.success(Log.rel_db + Log.run_q, Log.msg_good_exec_q(query, result))
            return result
        except Exception as e:
            Log.fail(Log.rel_db + Log.run_q, Log.msg_bad_exec_q(query), raise_error=True, e)


    def _split_combined(self, multi_query: str) -> list[str]:
        """Split combined MongoDB commands by semicolons, ignoring comments and semicolons inside JSON.
        @details
        Example Input:
            {"ping": 1}; {"aggregate": "users", "pipeline": [...]};
        Output:
            One command per string:
            - '{"ping": 1}'
            - '{"aggregate": "users", "pipeline": [...]}'
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        queries = []
        buffer = ""
        depth = 0
        for line in multi_query.splitlines():
            line = line.strip()
            # Skip comments
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            for c in line:
                buffer += c
                # Unpack nested brackets
                if c in "{[":
                    depth += 1
                elif c in "}]":
                    depth = max(0, depth - 1)
                elif c == ";" and depth == 0:
                    queries.append(buffer.strip().rstrip(";"))
                    buffer = ""
            if depth > 0:
                buffer += " "  # keep spacing between lines
        if buffer.strip():
            queries.append(buffer.strip().rstrip(";"))
        return queries


    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified collection.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        super().get_dataframe(name)
        self.check_connection(Log.get_df, raise_error=True)
        try:
            # Connect to MongoDB using the low-level PyMongo handle
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()
            # Results will be a list of documents
            docs = list(db[name].find({}))
            result = self._docs_to_df(docs)

            if self.verbose:
                Log.success(Log.doc_db + Log.get_df, Log.msg_good_coll(name))
            return result
        except Exception as e:
            Log.fail(Log.doc_db + Log.get_df, Log.msg_unknown_error, raise_error=True, e)
        # If not found, warn but do not fail
        Log.fail(Log.doc_db + Log.get_df, Log.msg_bad_coll(name), raise_error=False)
        return None


    def create_database(self, database_name: str):
        """Use the current database connection to create a sibling database in this engine.
        @note  Forces MongoDB to actually create it by inserting a small init document.
        @param database_name  The name of the new database to create."""
        super().create_database(database_name)
        self.check_connection(Log.create_db, raise_error=True)
        try:
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()
            # Create the database by adding dummy data
            if "init" not in db.list_collection_names():
                db.create_collection("init")
            db["init"].insert_one({"initialized_at": time()})

            if self.verbose:
                Log.success(Log.doc_db + Log.create_db, Log.msg_success_managed_db("created", database_name))
        except Exception as e:
            Log.fail(Log.doc_db + Log.create_db, Log.msg_fail_manage_db(self.connection_string, database_name, "create"), raise_error=True, e)


    def drop_database(self, database_name: str):
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database."""
        super().drop_database(database_name)
        self.check_connection(Log.drop_db, raise_error=True)
        try:
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()
            # Drop the entire database
            db.client.drop_database(database_name)

            if self.verbose:
                Log.success(Log.rel_db + Log.create_db, Log.msg_success_managed_db("dropped", database_name))
        except Exception as e:
            Log.fail(Log.rel_db + Log.create_db, Log.msg_fail_manage_db(self.connection_string, database_name, "drop"), raise_error=True, e)


    # Reuse the dataframe parsing logic
    def _docs_to_df(self, docs: List[Dict]) -> DataFrame:
        """Convert raw MongoDB documents to a Pandas DataFrame.
        @details
        Steps:
          1. Convert ObjectId fields (_id) to strings so Pandas can handle them.
          2. Flatten nested JSON structures using Pandas.json_normalize.
        Example input:
          docs = [
              {"_id": ObjectId("650f..."), "name": "Alice", "age": 30}, ...}
          ]
        Example output:
          DataFrame([
              {"_id": "650f...", "name": "Alice", "age": 30, "address.city": None, "address.zip": None},
              {"_id": "650f...", "name": "Bob", "age": None, "address.city": "NY", "address.zip": "10001"}
          ])"""
        # 1. Convert MongoDB ObjectId fields to strings
        for document in docs:
            if "_id" in document:
                try:
                    document["_id"] = str(document["_id"])
                except Exception:
                    # Fail if str() raises - probably corrupted data
                    Log.fail_parse(trace="_id field", expected_type="str", bad_value=document["_id"])
    
        # 2. Use Pandas to normalize nested JSON into flat columns
        try:
            return json_normalize(docs)
        except Exception:
            # Fallback: create a DataFrame directly if normalization fails
            # Pandas DataFrames can salvage messy nesting, but json_normalize requires all docs to be balanced dicts
            return DataFrame(docs)


