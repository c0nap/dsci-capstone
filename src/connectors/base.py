from abc import ABC, abstractmethod
from contextlib import contextmanager
from pandas import DataFrame
from src.util import Log
from typing import Any, Generator, List, Optional

class Connector(ABC):
    """Abstract base class for external connectors.
    @note  Credentials are specified in the .env file.
    @details
        Derived classes should implement:
        - __init__
        - @ref components.connectors.Connector.configure
        - @ref components.connectors.Connector.test_operations
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
    def test_operations(self, raise_error: bool = True) -> bool:
        """Establish a basic connection to the database, and test full functionality.
        @details  Can be configured to fail silently, which enables retries or external handling.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
        pass

    @abstractmethod
    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
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
        - @ref components.connectors.DatabaseConnector.test_operations
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

    @contextmanager
    def temp_database(self, database_name: str) -> Generator[None, None, None]:
        """Temporarily switch to a pseudo-database, creating and dropping it if needed.
        @details
            - If the target database does not exist, it will be created before yielding
              and dropped automatically afterward.
            - If it already exists, it will be left intact.
        @param database_name  The name of the pseudo-database to use temporarily.
        """
        old_db = self.database_name
        should_drop = not self.database_exists(database_name)
        if should_drop:
            self.create_database(database_name)
        self.change_database(database_name)

        try:
            yield
        finally:
            self.change_database(old_db)
            if should_drop:
                self.drop_database(database_name)

    # Avoid making this abstract, even though derived classes treat it like one.
    # Otherwise MyPy will complain about the partial logic inside.
    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single command through the connection.
        @note  If a result is returned, it will be converted to a DataFrame.
        @param query  A single query to perform on the database.
        @return  DataFrame containing the result of the query, or None
        @throws Log.Failure  If the query fails to execute.
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
        @throws Log.Failure  If any query in the file fails to execute."""

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
    def get_dataframe(self, name: str, columns: List[str] = []) -> DataFrame:
        """Automatically generate and run a query for the specified resource.
        @param name  The name of an existing table or collection in the database.
        @param columns  A list of column names to keep.
        @return  DataFrame containing the requested data"""
        pass

    @abstractmethod
    def create_database(self, database_name: str) -> None:
        """Use the current database connection to create a sibling database in this engine.
        @param database_name  The name of the new database to create.
        @throws Log.Failure  If the database already exists."""
        if self.database_exists(database_name):
            raise Log.Failure(Log.db_conn_abc + Log.create_db, Log.msg_db_exists(database_name))
        pass

    @abstractmethod
    def drop_database(self, database_name: str) -> None:
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database.
        @throws Log.Failure  If the database does not exist."""
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

    @abstractmethod
    def _returns_data(self, query: str) -> bool:
        """Checks if a query is structured in a way that returns real data, and not status messages.
        @param query  A single query string.
        @return  Whether the query is intended to fetch data (true) or might return a status message (false)."""
        pass

    @abstractmethod
    def _parsable_to_df(self, result: Any) -> bool:
        """Checks if the result of a query is valid (i.e. can be converted to a Pandas DataFrame).
        @param result  The result of a SQL, Cypher, or JSON query.
        @return  Whether the object is parsable to DataFrame."""
        pass
