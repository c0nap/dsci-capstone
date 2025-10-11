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
    def test_connection(self) -> bool:
        """Establish a basic connection to the database.
        @return  Whether the connection test was successful."""
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
    def execute_query(self, query: str) -> DataFrame:
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
            if len(results) > 1:
                if self.verbose:
                    Log.fail(
                        "A combined query was executed as a single query. Some results are hidden."
                    )
            return results[-1]
        # Derived classes MUST implement single-query execution.
        pass

    def execute_combined(self, multi_query: str) -> List[DataFrame]:
        """Run several database commands in sequence.
        @param multi_query  A string containing multiple queries.
        @return  A list of query results converted to DataFrames."""
        queries = self._split_combined(multi_query)
        results = []
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
        except Exception as e:
            if self.verbose:
                Log.file_read_error(filename)
            raise
        try:  # Attempt to run the multi-query
            results = self.execute_combined(multi_query)
            if self.verbose:
                Log.success(f'Finished executing "{filename}"\n')
            return results
        except Exception as e:
            if self.verbose:
                Log.fail(f'Failed to execute file "{filename}"')
            raise

    @abstractmethod
    def get_dataframe(self, name: str) -> DataFrame:
        """Automatically generate a query for the specified resource.
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
        raise

    def test_connection(self, print_results=False) -> bool:
        """Establish a basic connection to the database.
        @param print_results  Whether to display the retrieved test DataFrames
        @return  Whether the connection test was successful."""
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as connection:
                # 1. Basic connectivity test
                result = connection.execute(text("SELECT 1")).fetchone()[0]
                if print_results:
                    print(result)
                if result != 1:
                    Log.incorrect_result(result, 1)
                    return False
    
                # 2. Multi-statement execution test
                result = self.execute_combined("SELECT 1; SELECT 2;")[1].iloc[0, 0]
                if print_results:
                    print(result)
                if result != 2:
                    Log.incorrect_result(result, 2)
                    return False
    
                # 3. Current database name
                db_name = self.execute_query(self._specific_queries[0]).iloc[0, 0]
                if print_results:
                    print(db_name)
    
                # 4. List all databases
                databases = self.execute_query(self._specific_queries[1])
                if print_results:
                    print(databases)
    
                # 5. Ensure working database exists
                working_database = os.getenv("DB_NAME")
                if working_database not in databases.iloc[:, 0].tolist():
                    try:
                        self.create_database(working_database)
                        if self.verbose:
                            print(f"Created database: {working_database}")
                    except Exception as e:
                        Log.connect_fail(f"Failed to create database {working_database}")
                        print(e)
                        return False
    
                # 6. Test create/drop database functionality with a temporary DB
                temp_db = f"temp_test_db_{int(time())}"
                try:
                    self.create_database(temp_db)
                    if self.verbose:
                        print(f"Created temporary database: {temp_db}")
                    self.drop_database(temp_db)
                    if self.verbose:
                        print(f"Dropped temporary database: {temp_db}")
                except Exception as e:
                    Log.connect_fail(f"Failed to create/drop temporary database {temp_db}")
                    print(e)
                    return False
    
                # 7. Test connection to the working database
                self.change_database(working_database)
                result = self.execute_query("SELECT 1").iloc[0, 0]
                if result != 1:
                    Log.incorrect_result(result, 1)
                    return False
    
                # Log overall success
                if self.verbose:
                    Log.connect_success(working_database)
        except Exception as e:
            if self.verbose:
                Log.connect_fail(self.connection_string)
            print(e)
            return False
        return True

    def execute_query(self, query: str) -> DataFrame:
        """Send a single command to the database connection.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        """
        super().execute_query(query)
        # Derived classes MUST implement single-query execution.
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as connection:
                result = connection.execute(text(query))
                if result.returns_rows and result.keys():
                    result = DataFrame(result.fetchall(), columns=result.keys())
                connection.commit()
                return result
        except Exception as e:
            if self.verbose:
                Log.connect_fail(self.connection_string)
            raise

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

    def get_dataframe(self, name: str) -> DataFrame:
        """Automatically generate a query for the specified table using SQLAlchemy.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as connection:
                table = Table(name, MetaData(), autoload_with=engine)
                result = connection.execute(select(table))
                if result.returns_rows and result.keys():
                    result = DataFrame(result.fetchall(), columns=result.keys())
                return result
        except NoSuchTableError:
            # Postgres will auto-lowercase all table names.
            return self.get_dataframe(name.lower())
        except Exception as e:
            if self.verbose:
                Log.connect_fail(self.connection_string)
            raise

    def create_database(self, database_name: str):
        """Use the current database connection to create a sibling database in this engine.
        @note  Auto-commit is required for database management.
        @param database_name  The name of the new database to create."""
        super().create_database(database_name)
        try:
            engine = create_engine(self.connection_string)
            with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as connection:
                connection.execute(text(f"CREATE DATABASE {database_name}"))
            if self.verbose:
                Log.success_manage_db(database_name, "Created new")
        except Exception as e:
            if self.verbose:
                Log.fail_manage_db(self.connection_string, database_name, "create")
            raise

    def drop_database(self, database_name: str = ""):
        """Delete all data stored in a particular database.
        @note  Auto-commit is required for database management.
        @param database_name  The name of an existing database."""
        super().drop_database(database_name)
        try:
            engine = create_engine(self.connection_string)
            with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as connection:
                connection.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))
            if self.verbose:
                Log.success_manage_db(database_name, "Dropped")
        except Exception as e:
            if self.verbose:
                Log.fail_manage_db(self.connection_string, database_name, "drop")
            raise


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
            "SHOW DATABASES;",
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
            "SELECT datname FROM pg_database;",
        ]  # List of all databases in the database engine.
    }
