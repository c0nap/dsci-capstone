import os
from dotenv import load_dotenv
from src.util import all_none
from components.connectors import RelationalConnector
from components.fact_storage import GraphConnector

# Read environment variables at compile time
load_dotenv(".env")


class Session:
    """Stores active database connections and configuration settings.
    @details
        - This class implements Singleton design, so only one session can be created.
        - However, the session config can still be updated using the normal constructor.
    """

    # TODO: this is bad design ^
    _instance = None

    def __new__(cls, *args, **kwargs):
        """Creates a new session at first access, otherwise uses the existing session."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, verbose=False):
        """Initializes the session using the .env file.
        @details
            - The relational database connector is created using a Factory Method, choosing mysql or postgres based on the .env file.
            - The document database connector is created normally since mongo is the only supported option.
            - The graph database connector is created normally since neo4j is the only supported option.
        """
        ## Enables or disables the components from printing debug info.
        self.verbose = verbose
        ## Stores RDF-compliant semantic triples.
        self.relational_db = RelationalConnector.from_env(verbose=verbose)
        # self.docs_db = DocumentConnector(verbose=False)  # Stores input text, pre-processed chunks, JSON intermediates, and final output.
        ## Main storage for entities (nodes) and relations (edges).
        self.graph_db = GraphConnector(verbose=verbose)

        # TODO: Do not interact with these directly, provide them to EAV Model and Knowledge Graph classes
        self.setup()

    def setup(self):
        """Configure the databases and verify they are working correctly."""
        self.relational_db.test_connection()

        self.setup1()

    def setup1(self):
        """Sanity check for Neo4j GraphConnector."""
        # Test connection to default "database" (really just a database_id)
        default_database = "default"  # Neo4j community doesn’t have real DBs
        self.graph_db.change_database(default_database)
        self.graph_db.test_connection()
        if self.verbose:
            print()

        # Test connection to working database ".env/DB_NAME" (stored as database_id)
        working_database = os.getenv("DB_NAME")
        self.graph_db.change_database(working_database)
        already_exists = self.graph_db.test_connection(print_results=self.verbose)

        # Ensures the working database was created (pseudo)
        if not already_exists:
            self.graph_db.change_database(default_database)
            self.graph_db.create_database(working_database)
            self.graph_db.change_database(working_database)
            self.graph_db.test_connection(print_results=self.verbose)

        # Test database management explicitly
        self.graph_db.drop_database(working_database)
        self.graph_db.create_database(working_database)

        # Test query execution
        self.graph_db.execute_query(
            f"""
        CREATE (n:Person {{name:'Alice', database_id:'{working_database}'}})
        RETURN n
        """
        )
        self.graph_db.execute_query(
            f"""
        CREATE (n:Person {{name:'Bob', database_id:'{working_database}'}})
        RETURN n
        """
        )

        # Test retrieving nodes as DataFrame
        df = self.graph_db.get_dataframe("Person")
        if self.verbose:
            print("Retrieved nodes:\n", df)

        # Cleanup
        self.graph_db.create_database(working_database)  # clears existing nodes

    def reset(self):
        """Deletes all created databases and tables."""
        # TODO


session = Session()
print()
