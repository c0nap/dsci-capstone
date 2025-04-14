import os
from pandas import DataFrame
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv
from typing import List
from sqlparse import parse as sql_parse
from util import Log

## Read environment variables at compile time
load_dotenv(".env")

class Connector(ABC):
    """Abstract base class for database engine connectors.

    Derived classes should implement:
    - @ref __init__
    - @ref test_connection
    - @ref execute_query
    - @ref split_combined_query
    - @ref get_dataframe
    - @ref create_database
    - @ref drop_database
    """

    def __init__(self, verbose=False):
        """Creates a new database connector.
        @param verbose  Whether to print success and failure messages.
        """
        ## The common name for the type of database as seen in the .env prefixes.
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
        ## URI of the database connection, syntax is engine://username:password@host:port/database.
        self.connection_string = None
        ## Whether to print success and failure messages.
        self.verbose = verbose

    def configure(self, DB: str, database_name: str):
        """Read connection settings from the .env file.
        @param DB  The prefix of fetched database credentials.
        @param name  The name of the database to connect to."""
        self.db_type = DB
        ## The .env file contains multiple credentials.
        ## Select environment variables corresponding to our database engine.
        self.db_engine = os.getenv(f"{DB}_ENGINE")
        self.username = os.getenv(f"{DB}_USERNAME")
        self.password = os.getenv(f"{DB}_PASSWORD")
        self.host = os.getenv(f"{DB}_HOST")
        self.port = os.getenv(f"{DB}_PORT")
        ## Condense these variables into a connection string
        self.change_database(database_name)
    
    def change_database(self, new_database: str):
        """Update the connection URI to reference a different database in the same engine.
        @param new_database  The name of the database to connect to."""
        self.database_name = new_database
        self.connection_string = f"{self.db_engine}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"

    @abstractmethod
    def test_connection(self) -> bool:
        """Establish a basic connection to the database.
        @return  Whether the connection test was successful."""
        pass

    @abstractmethod
    def execute_query(self, query: str) -> DataFrame:
        """Send a single command to the database connection.
        If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        """
        ## Perform basic error checks.
        query = query.strip()  # Remove whitespace
        if not query: return None  # Check if empty
        if not self.is_single_query(query):
            results = self.execute_combined(query)
            if len(results) == 0:
                return None
            ## Return the final result if several are found.
            if len(results) > 1:
                if self.verbose: Log.fail("A combined query was executed as a single query. Some results are hidden.")
            return results[-1]
        ## Derived classes MUST implement single-query execution.
        pass

    def execute_combined(self, multi_query: str) -> List[DataFrame]:
        """Run several commands in sequence.
        @param multi_query  A string containing multiple queries.
        @return  A list of query results converted to DataFrames."""
        queries = self.split_combined_query(multi_query)
        results = []
        for query in queries:
            df = self.execute_query(query)
            if df is not None:
                results.append(df)
        return results

    def execute_file(self, filename: str) -> List[DataFrame]:
        """Run several commands from a file.
        Loads the entire file into memory at once.
        @param filename  The path to a specified query file (.sql, .cql, .json).
        @return  Whether the query was performed successfully."""
        try:   ## Read the entire file as a multi-query string
            with open(filename, 'r') as file:
                multi_query = file.read()
        except Exception as e:
            if self.verbose: Log.file_read_error(filename)
            raise
        try:   ## Attempt to run the multi-query
            results = self.execute_combined(multi_query)
            if self.verbose: Log.success(f"Finished executing \"{filename}\"\n")
            return results
        except Exception as e:
            if self.verbose: Log.fail(f"Failed to execute file \"{filename}\"")
            raise

    def is_single_query(self, query: str) -> bool:
        """Checks if a string contains multiple queries.
        @param query  A single or combined query string.
        @return  Whether the query is single (true) or combined (false)."""
        queries = self.split_combined_query(query)
        return len(queries) == 1

    @abstractmethod
    def split_combined_query(self, multi_query: str) -> List[str]:
        """Checks if a string contains multiple queries.
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        pass

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
        if not database_name: database_name = self.database_name
        pass







class RelationalConnector(Connector):
    """Connector for relational databases (MySQL, PostgreSQL)."""

    def __init__(self, verbose=False):
        """Creates a new database connector.
        @param verbose  Whether to print success and failure messages.
        """
        super().__init__(verbose)
        engine = os.getenv("DB_ENGINE")
        database = os.getenv("DB_NAME")  # "" for Neo4j
        super().configure(engine, database)

    ## A dictionary for basic test queries. Keys are db_type.
    test_queries = {
        "MYSQL": ["SELECT DATABASE();", "SHOW DATABASES;"],
        "POSTGRES": ["SELECT current_database();", "SELECT datname FROM pg_database;"],
    }
    def test_connection(self, print_results=False) -> bool:
        """Establish a basic connection to the database.
        @param print_results  Whether to display the retrieved test DataFrames
        @return  Whether the connection test was successful."""
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))   ## This query should work in all relational databases.
                result = connection.execute(text(self.test_queries[self.db_type][0]))
                if self.verbose: Log.connect_success(result.fetchone()[0])
        except Exception as e:
            if self.verbose: Log.connect_fail(self.connection_string)
            return False
        result = self.execute_query(self.test_queries[self.db_type][1])
        if print_results: print(result)
        return True
    
    def execute_query(self, query: str) -> DataFrame:
        """Send a single command to the database connection.
        If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        """
        super().execute_query(query)
        ## Derived classes MUST implement single-query execution.
        ## The 
        try:
            engine = create_engine(self.connection_string)
            with engine.connect() as connection:
                result = connection.execute(text(query))
                if result.returns_rows and result.keys():
                    result = DataFrame(result.fetchall(), columns=result.keys())
                connection.commit()
                return result
        except Exception as e:
            if self.verbose: Log.fail_connect(self.connection_string)
            raise

    def split_combined_query(self, multi_query: str) -> List[str]:
        """Checks if a string contains multiple queries.
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        queries = []
        for query in sql_parse(multi_query):
            query = str(query).strip()
            if query: queries.append(query)
        return queries

    def get_dataframe(self, name: str) -> DataFrame:
        """Automatically generate a query for the specified resource.
        For more complex queries, SQLAlchemy's ORM can be used.
        e.g. Table(name, MetaData(), autoload_with=engine), select([table])
        But this is overkill for simple statements like we have here.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        return self.execute_query(f"SELECT * FROM {name}")

    def create_database(self, database_name: str):
        """Use the current database connection to create a sibling database in this engine.
        Auto-commit is required for database management.
        @param database_name  The name of the new database to create."""
        super().create_database(database_name)
        try:
            engine = create_engine(self.connection_string)
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                connection.execute(text(f"CREATE DATABASE {database_name}"))
            if self.verbose: Log.success_manage_db(database_name, "Created new")
        except Exception as e:
            if self.verbose: Log.fail_manage_db(self.connection_string, database_name, "create")
            raise

    def drop_database(self, database_name: str = ""):
        """Delete all data stored in a particular database.
        Auto-commit is required for database management.
        @param database_name  The name of an existing database."""
        super().drop_database(database_name)
        try:
            engine = create_engine(self.connection_string)
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                connection.execute(text(f"DROP DATABASE IF EXISTS {database_name}"))
            if self.verbose: Log.success_manage_db(database_name, "Dropped")
        except Exception as e:
            if self.verbose: Log.fail_manage_db(self.connection_string, database_name, "drop")
            raise

