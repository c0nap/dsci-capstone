import os
import pandas as pd
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv
from typing import List
from util import Log

## Read environment variables at compile time
load_dotenv(".env")

class Connector(ABC):
    """Abstract base class for database engine connectors.

    Derived classes must implement:
    - @ref __init__
    - @ref test_connection
    - @ref execute_query
    - @ref is_single_query
    - @ref get_dataframe
    - @ref clear_database
    """

    def __init__(self, verbose=False):
        """Creates a new database connector.
        @param verbose  Whether to print success and failure messages.
        """
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
        @param name  The name of the database to connect to.
        """
        ## The .env file contains multiple credentials.
        ## Select environment variables corresponding to our database engine.
        db_engine = os.getenv(f"{DB}_ENGINE")
        username = os.getenv(f"{DB}_USERNAME")
        password = os.getenv(f"{DB}_PASSWORD")
        host = os.getenv(f"{DB}_HOST")
        port = os.getenv(f"{DB}_PORT")
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
        if not is_single_query(query):
            queries = self.split_combined_query(query)
            results = self.execute_combined(queries)
            if len(results) == 0:
                return None
            ## Return the final result if several are found.
            if len(results) > 1:
                Log.print_fail("A combined query was executed as a single query. Some results are hidden.")
            return results[-1]
        ## Derived classes MUST implement single-query execution.
        pass

    def execute_combined(self, queries: List[str]) -> List[DataFrame]:
        """Run several commands in sequence.
        @param queries  A list of single-query strings.
        @return  A list of query results converted to DataFrames."""
        results = []
        for query in queries:
            df = self.execute_query(query):
            if df is not None:
                results.append(df)
        return results

    def execute_file(self, filename: str) -> List[DataFrame]:
        """Run several commands from a file.
        Loads the entire file into memory at once.
        @param filename  The path to a specified query file (.sql, .cql, .json).
        @return  Whether the query was performed successfully."""
        try:
            with open(filename, 'r') as file:
                queries = file.read()
        except Exception as e:
            if (self.verbose) Log.file_read_error(filename)
            raise
        return self.execute_combined(queries)

    @abstractmethod
    def is_single_query(self, query: str) -> bool:
        """Checks if a string contains multiple queries.
        @param query  A single or combined query string.
        @return  Whether the query is single (true) or combined (false)."""
        pass

    @abstractmethod
    def get_dataframe(self, name: str) -> DataFrame:
        """Automatically generate a query for the specified resource.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        pass

    @abstractmethod
    def clear_database(self, database_name: str = ""):
        """Delete all data stored in a particular database.
        @param database  The name of the database to clear."""
        if database_name != self.database_name:
            Log.fail(f"Attempted to clear database {database_name} in {engine}, but connected to the {self.database_name} database.")
        pass



class RelationalConnector(Connector):
	"""Connector for relational databases (MySQL, PostgreSQL)."""

	def __init__(self, verbose=False):
        """Creates a new database connector.
        @param verbose  Whether to print success and failure messages.
        """
        super().__init__()
        engine = os.getenv("DB_ENGINE")
        database = os.getenv("DB_NAME")  # "" for Neo4j
        super().configure(engine, database)




def test_connection(connection_string):
    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT DATABASE();"))
            Log.success(f"Successfully connected to database: {result.fetchone()[0]}\n")
        return True
    except Exception as e:
        msg = str(e).split('\n')[0]
        Log.fail(f"Failed to connect on {connection_string}:\n{msg}\n")
        return False

# Connect to "mysql" database and create the working database.
def create_database():
    try:
        engine = create_engine(connection_string("mysql"))
        with engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS {database};"))
            connection.execute(text(f"CREATE DATABASE {database};"))
        Log.success(f"Created new database \"{database}\"\n")
        return True
    except Exception as e:
        Log.fail(f"Failed to create database \"{database}\"\n{e}\n")
        return False

# Execute a .sql script
def execute_sql_file(path):
    try:    # Read from file
        with open(path, 'r') as file:
            sql_script = file.read()
    except Exception as e:
        Log.fail(f"Failed to read file \"{path}\"\n{e}\n")
        return False
    try:    # Execute SQL commands one-by-one
        engine = create_engine(connection_string())
        with engine.connect() as connection:
            sql_commands = sql_script.split(';')
            for command in sql_commands:
                # Ignore invalid queries
                if len(command.strip()) < 3: continue
                # Print query results
                result = connection.execute(text(command))
                # Print tables via pandas
                if result.returns_rows and result.keys():
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    print(df)
            connection.commit()
            Log.success(f"Finished executing \"{path}\"\n")
        return True
    except Exception as e:
        Log.fail(f"Error executing SQL commands: {e}")
        return False

# Connect to the main database and return the table as a Pandas DataFrame
def dataframe_from_table(name):
    try:
        engine = create_engine(connection_string())
        table = Table(name, MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM EntityName;"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        Log.fail(f"Failed to fetch table \"{name}\"\n{e}\n")
        return pd.DataFrame()