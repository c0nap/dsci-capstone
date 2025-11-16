from components.connectors import RelationalConnector
from components.document_storage import DocumentConnector
from components.fact_storage import GraphConnector
from components.semantic_web import KnowledgeGraph
from src.util import all_none



class Session:
    """Stores active database connections and configuration settings.
    @details
        - This class implements Singleton design, so only one session can be created.
        - However, the session config can still be updated using the normal constructor.
    """
    _instance = None
    _created = False

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
        if Session._created:
            return  # prevent reinitialization
        Session._created = True
        ## Enables or disables the components from printing debug info.
        self.verbose = verbose
        ## Stores RDF-compliant semantic triples.
        self.relational_db = RelationalConnector.from_env(verbose=verbose)
        ## Stores input text, pre-processed chunks, JSON intermediates, and final output.
        self.docs_db = DocumentConnector(verbose=verbose)
        ## Stores entities (nodes) and relations (edges).
        self.graph_db = GraphConnector(verbose=verbose)
        ## Main storage for initial pipeline.
        self.main_graph = KnowledgeGraph("main", self.graph_db)
        # TODO: Split into scene graph, event graph, and social graph.


# Global storage for the lazy singleton
_session_instance = None

def get_session(verbose: bool = False) -> Session:
    """ """
    global _session_instance
    if _session_instance is None:
        _session_instance = Session(verbose=verbose)
    return _session_instance





if __name__ == "__main__":
    session = Session()
