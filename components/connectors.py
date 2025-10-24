from abc import ABC, abstractmethod
from dotenv import load_dotenv
import os
from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, select, Table, text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.pool import NullPool
from sqlparse import parse as sql_parse
from src.util import check_values, df_natural_sorted, Log
from time import time
from typing import List, Optional


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
    def configure(self, DB: str, database_name: str) -> None:
        """Read connection settings from the .env file.
        @param DB  The prefix of fetched credentials.
        @param database_name  The specific service to connect to."""
        pass

    @abstractmethod
    def test_connection(self, raise_error: bool = True) -> bool:
        """Establish a basic connection to the database.
        @details  Can be configured to fail silently, which enables retries or external handling.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws RuntimeError  If raise_error is True and the connection test fails to complete."""
        pass

    @abstractmethod
    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single command through the connection.
        @param query  A single query to perform on the database.
        @return  The result of the query, or None
        """
        pass

    @abstractmethod
    def execute_file(self, filename: str) -> List[Optional[DataFrame]]:
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
        - @ref components.connectors.DatabaseConnector.change_database
        - @ref components.connectors.DatabaseConnector.database_exists
    """

    def __init__(self, verbose: bool = False) -> None:
        """Initialize the connector.
        @param verbose  Whether to print debug messages.
        @note  Attributes will be set to None until @ref components.connectors.DatabaseConnector.configure() is called.
        """
        ## The common name for the type of database as observed in the .env prefixes (MYSQL, POSTGRES, MONGO, or NEO4J).
        self.db_type: Optional[str] = None
        ## The protocol specifying the database type, syntax is usually dialect+driver.
        self.db_engine: Optional[str] = None
        ## The username used to access the database.
        self.username: Optional[str] = None
        ## The password used to access the database.
        self.password: Optional[str] = None
        ## The IP address where the database service is hosted.
        self.host: Optional[str] = None
        ## The port number where the database service is hosted.
        self.port: Optional[str] = None
        ## The collection being modified by this connector (Optional since Neo4j does not have one).
        self.database_name: Optional[str] = None
        ## URI of the database connection: syntax is engine://username:password@host:port/database.
        self.connection_string: Optional[str] = None
        ## Whether to print debug messages.
        self.verbose = verbose

    def configure(self, DB: str, database_name: str) -> None:
        """Read connection settings from the .env file.
        @param DB  The prefix of fetched database credentials.
        @param database_name  The name of the database to connect to.
        """
        self.db_type = DB
        # The .env file contains multiple credentials.
        # Here we select environment variables corresponding to our database engine.
        self.db_engine = os.environ[f"{DB}_ENGINE"]
        self.username = os.environ[f"{DB}_USERNAME"]
        self.password = os.environ[f"{DB}_PASSWORD"]
        self.host = os.environ[f"{DB}_HOST"]
        self.port = os.environ[f"{DB}_PORT"]
        # Condense the above variables into a connection string
        self.change_database(database_name)

    @abstractmethod
    def change_database(self, new_database: str) -> None:
        """Update the connection URI to reference a different database in the same engine.
        @param new_database  The name of the database to connect to.
        """
        pass

    # Avoid making this abstract, even though derived classes treat it like one.
    # Otherwise MyPy will complain about the partial logic inside.
    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single command through the connection.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        @raises RuntimeError  If the query fails to execute.
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
            if len(results) > 1:
                # Warn when earlier results are ignored
                Log.warn(Log.db_conn_abc + Log.run_q, Log.msg_multiple_query(len(results), query), self.verbose)
            return results[-1]
        # Derived classes MUST implement single-query execution.
        return None

    def execute_combined(self, multi_query: str) -> List[Optional[DataFrame]]:
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

    def execute_file(self, filename: str) -> List[Optional[DataFrame]]:
        """Run several database commands from a file.
        @note  Loads the entire file into memory at once.
        @param filename  The path to a specified query file (.sql, .cql, .json).
        @return  Whether the query was performed successfully.
        @raises RuntimeError  If any query in the file fails to execute."""

        try:  # Read the entire file as a multi-query string
            with open(filename, "r") as file:
                multi_query = file.read()
                Log.success(Log.db_conn_abc + Log.run_f, Log.msg_good_path(filename), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.db_conn_abc + Log.run_f, Log.msg_bad_path(filename)) from e

        try:  # Attempt to run the multi-query
            results = self.execute_combined(multi_query)
            Log.success(Log.db_conn_abc + Log.run_f, Log.msg_good_exec_f(filename), self.verbose)
            return results
        except Exception as e:
            raise Log.Failure(Log.db_conn_abc + Log.run_f, Log.msg_bad_exec_f(filename)) from e

    @abstractmethod
    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified resource.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        pass

    @abstractmethod
    def create_database(self, database_name: str) -> None:
        """Use the current database connection to create a sibling database in this engine.
        @param database_name  The name of the new database to create.
        @raises RuntimeError  If the database already exists."""
        if self.database_exists(database_name):
            raise Log.Failure(Log.db_conn_abc + Log.create_db, Log.msg_db_exists(database_name))
        pass

    @abstractmethod
    def drop_database(self, database_name: str) -> None:
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database.
        @raises RuntimeError  If the database does not exist."""
        if not self.database_exists(database_name):
            raise Log.Failure(Log.db_conn_abc + Log.drop_db, Log.msg_db_not_found(database_name, self.connection_string))
        if database_name == self.database_name:
            raise Log.Failure(Log.db_conn_abc + Log.drop_db, Log.msg_db_current(database_name))
        pass

    @abstractmethod
    def database_exists(self, database_name: str) -> bool:
        """Search for an existing database using the provided name.
        @param database_name  The name of a database to search for.
        @return  Whether the database is visible to this connector."""
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

    def __init__(self, verbose: bool, specific_queries: List[str]) -> None:
        """Creates a new database connector. Use @ref components.connectors.RelationalConnector.from_env instead (this is called by derived classes).
        @param verbose  Whether to print success and failure messages.
        @param specific_queries  A list of helpful SQL queries.
        """
        super().__init__(verbose)
        engine = os.environ["DB_ENGINE"]
        database = os.environ["DB_NAME"]
        super().configure(engine, database)

        self._specific_queries: List[str] = specific_queries
        """@brief  Hard-coded queries which depend in the specific engine, and cannot be abstracted with SQLAlchemy.
        @note  This is set by derived classes e.g. 'mysqlConnector' for lanugage-sensitive syntax."""
        assert len(specific_queries) == 2

    @classmethod
    def from_env(cls, verbose: bool = False) -> "RelationalConnector":
        """Decides what type of relational connector to create using the .env file.
        @param verbose  Whether to print success and failure messages.
        @raises RuntimeError  If the .env file contains an invalid DB_ENGINE value."""
        engine = os.environ["DB_ENGINE"]
        if engine == "MYSQL":
            return mysqlConnector(verbose)
        elif engine == "POSTGRES":
            return postgresConnector(verbose)
        raise Log.Failure(Log.rel_db + "FROM_ENV: ", f"Database engine '{engine}' not supported. Did you mean 'MYSQL' or 'POSTGRES'?")

    def change_database(self, new_database: str) -> None:
        """Update the connection URI to reference a different database in the same engine.
        @param new_database  The name of the database to connect to.
        """
        Log.success(Log.rel_db + Log.swap_db, Log.msg_swap_db(self.database_name, new_database), self.verbose)
        self.database_name = new_database
        self.connection_string = f"{self.db_engine}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"

    def test_connection(self, raise_error: bool = True) -> bool:
        """Establish a basic connection to the database.
        @details  Can be configured to fail silently, which enables retries or external handling.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete.
        """
        # Check if connection string is valid
        if self.check_connection(Log.test_conn, raise_error) == False:
            return False

        engine = create_engine(self.connection_string, poolclass=NullPool)
        with engine.begin() as connection:
            try:  # Run universal test queries
                result = connection.execute(text("SELECT 1")).fetchone()
                if check_values([result[0]], [1], self.verbose, Log.rel_db, raise_error) == False:
                    return False
                result = self.execute_query("SELECT 'TWO';")
                if check_values([result.iloc[0, 0]], ["TWO"], self.verbose, Log.rel_db, raise_error) == False:
                    return False
                results = self.execute_combined("SELECT 3; SELECT 4;")
                if check_values([results[0].iloc[0, 0], results[1].iloc[0, 0]], [3, 4], self.verbose, Log.rel_db, raise_error) == False:
                    return False
                result = self.execute_query("SELECT 5, 6;")
                if check_values([result.iloc[0, 0], result.iloc[0, 1]], [5, 6], self.verbose, Log.rel_db, raise_error) == False:
                    return False
            except Exception as e:
                if not raise_error:
                    return False
                raise Log.Failure(Log.rel_db + Log.test_conn + Log.test_basic, Log.msg_unknown_error) from e

            try:  # Display useful information on existing databases
                db_name = self.execute_query(self._specific_queries[0])
                check_values([db_name.iloc[0, 0]], [self.database_name], self.verbose, Log.rel_db, raise_error)
                databases = self.execute_query(self._specific_queries[1])
                Log.success(Log.rel_db, Log.msg_result(databases), self.verbose)
            except Exception as e:
                if not raise_error:
                    return False
                raise Log.Failure(Log.rel_db + Log.test_conn + Log.test_info, Log.msg_unknown_error) from e

            try:  # Create a table, insert dummy data, and use get_dataframe
                tmp_table = f"test_table_{int(time())}"
                self.execute_query(f"DROP TABLE IF EXISTS {tmp_table} CASCADE;")
                self.execute_query(
                    f"CREATE TABLE {tmp_table} (id INT PRIMARY KEY, name VARCHAR(255)); INSERT INTO {tmp_table} (id, name) VALUES (1, 'Alice');"
                )
                df = self.get_dataframe(f"{tmp_table}")
                check_values([df.at[0, 'name']], ['Alice'], self.verbose, Log.rel_db, raise_error)
                self.execute_query(f"DROP TABLE {tmp_table};")
            except Exception as e:
                if not raise_error:
                    return False
                raise Log.Failure(Log.rel_db + Log.test_conn + Log.test_df, Log.msg_unknown_error) from e

            try:  # Test create/drop functionality with tmp database
                tmp_db = f"test_db_{int(time())}"
                working_database = str(self.database_name)
                if self.database_exists(tmp_db):
                    self.drop_database(tmp_db)
                self.create_database(tmp_db)
                self.change_database(tmp_db)
                self.execute_query("SELECT 1")
                self.change_database(working_database)
                self.drop_database(tmp_db)
            except Exception as e:
                if not raise_error:
                    return False
                raise Log.Failure(Log.rel_db + Log.test_conn + Log.test_tmp_db, Log.msg_unknown_error) from e

        # Finish with no errors = connection test successful
        Log.success(Log.rel_db, Log.msg_db_connect(self.database_name), self.verbose)
        return True

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @details  Connect to our relational database using SQLAlchemy's engine.begin()
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            # SQLAlchemy will not create the connection until we send a query
            engine = create_engine(self.connection_string, poolclass=NullPool)
            with engine.begin() as connection:
                connection.execute(text("SELECT 1"))
        except Exception:  # These errors are usually nasty, so dont print the original.
            if not raise_error:
                return False
            raise Log.Failure(Log.rel_db + log_source + Log.bad_addr, Log.msg_bad_addr(self.connection_string)) from None
        Log.success(Log.rel_db + log_source, Log.msg_db_connect(self.database_name), self.verbose)
        return True

    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single command to the database connection.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        @raises RuntimeError  If the query fails to execute."""
        # The base class will handle the multi-query case, so prevent a 2nd duplicate query
        result = super().execute_query(query)
        if not self._is_single_query(query):
            return result
        # Derived classes MUST implement single-query execution.
        self.check_connection(Log.run_q, raise_error=True)
        try:
            engine = create_engine(self.connection_string, poolclass=NullPool)
            with engine.begin() as connection:
                result = connection.execute(text(query))
                if result.returns_rows and result.keys():
                    df = DataFrame(result.fetchall(), columns=result.keys())
                    Log.success(Log.rel_db + Log.run_q, Log.msg_good_exec_qr(query, df), self.verbose)
                    return df
                else:
                    Log.success(Log.rel_db + Log.run_q, Log.msg_good_exec_q(query), self.verbose)
                    return None
        except Exception as e:
            raise Log.Failure(Log.rel_db + Log.run_q, Log.msg_bad_exec_q(query)) from e

    def _split_combined(self, multi_query: str) -> List[str]:
        """Divides a string into non-divisible SQL queries using `sqlparse`.
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
        @return  Sorted DataFrame containing the requested data, or None
        @raises RuntimeError  If we fail to create the requested DataFrame for any reason."""
        self.check_connection(Log.get_df, raise_error=True)

        # Postgres will auto-lowercase all table names.
        if self.db_type == "POSTGRES":
            name = name.lower()

        engine = create_engine(self.connection_string, poolclass=NullPool)
        with engine.begin() as connection:
            table = Table(name, MetaData(), autoload_with=engine)
            result = connection.execute(select(table))
            df = DataFrame(result.fetchall(), columns=result.keys())
            df = df_natural_sorted(df)

            if df is not None and not df.empty:
                Log.success(Log.rel_db + Log.get_df, Log.msg_good_table(name, df), self.verbose)
                return df
        # If not found, warn but do not fail
        Log.warn(Log.rel_db + Log.get_df, Log.msg_bad_table(name), self.verbose)
        return None

    def create_database(self, database_name: str) -> None:
        """Use the current database connection to create a sibling database in this engine.
        @param database_name  The name of the new database to create.
        @raises RuntimeError  If we fail to create the requested database for any reason."""
        super().create_database(database_name)
        self.check_connection(Log.create_db, raise_error=True)
        try:
            engine = create_engine(self.connection_string, poolclass=NullPool)
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                connection.execute(text(f"CREATE DATABASE {database_name}"))

            Log.success(Log.rel_db + Log.create_db, Log.msg_success_managed_db("created", database_name), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.rel_db + Log.create_db, Log.msg_fail_manage_db("create", database_name, self.connection_string)) from e

    def drop_database(self, database_name: str = "") -> None:
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database.
        @raises RuntimeError  If we fail to drop the target database for any reason."""
        super().drop_database(database_name)
        self.check_connection(Log.drop_db, raise_error=True)
        try:
            engine = create_engine(self.connection_string, poolclass=NullPool)
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                connection.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))

            Log.success(Log.rel_db + Log.create_db, Log.msg_success_managed_db("dropped", database_name), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.rel_db + Log.create_db, Log.msg_fail_manage_db("drop", database_name, self.connection_string)) from e

    def database_exists(self, database_name: str) -> bool:
        """Search for an existing database using the provided name.
        @param database_name  The name of a database to search for.
        @return  Whether the database is visible to this connector."""
        result = self.execute_query(self._specific_queries[1])
        if result is None:
            return False
        databases = result.iloc[:, 0].tolist()
        return database_name in databases


class mysqlConnector(RelationalConnector):
    """A relational database connector configured for MySQL.
    @note  Should be hidden from the user using a factory method."""

    def __init__(self, verbose: bool = False) -> None:
        """Configures the relational connector.
        @param verbose  Whether to print success and failure messages."""
        super().__init__(verbose, self.specific_queries["MYSQL"])

    # A list of basic test queries used by RelationalConnector
    specific_queries = {
        "MYSQL": [
            "SELECT DATABASE();",  # Single value, name of the current database.
            "SHOW DATABASES;",  # List of databases the secondary user can access.
        ]  # List of all databases in the database engine.
    }


class postgresConnector(RelationalConnector):
    """A relational database connector configured for PostgreSQL.
    @note  Should be hidden from the user using a factory method."""

    def __init__(self, verbose: bool = False) -> None:
        """Configures the relational connector.
        @param verbose  Whether to print success and failure messages."""
        super().__init__(verbose, self.specific_queries["POSTGRES"])

    # A list of basic test queries used by RelationalConnector
    specific_queries = {
        "POSTGRES": [
            "SELECT current_database();",  # Single value, name of the current database.
            "SELECT datname FROM pg_database;",  # List of ALL databases, even ones we cannot access.
        ]  # List of all databases in the database engine.
    }
