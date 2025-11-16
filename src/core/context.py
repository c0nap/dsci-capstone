from components.connectors import RelationalConnector
from components.document_storage import DocumentConnector
from components.fact_storage import GraphConnector
from components.semantic_web import KnowledgeGraph
from src.util import all_none
from typing import Any


class Session:
    """Stores active database connections and configuration settings.
    @details
        - This class implements Singleton design so only one session can be created.
    """
    _instance = None
    _created = False

    def __new__(cls, *args: Any, **kwargs: Any) -> Session:
        """Creates a new session at first access, otherwise returns the existing session.
        @param *args  Positional arguments forwarded to __init__().
        @param **kwargs  Keyword arguments forwarded to __init__().
        @return  The new global Session singleton."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, verbose: bool = False) -> None:
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


## Global storage for the lazy singleton
_session = None

def get_session(*args: Any, **kwargs) -> Session:
    """Lazily creates a session on first call, otherwise returns the existing session.
    @note  Will ignore any arguments passed after creation.
    @param *args  Positional arguments forwarded to Session().
    @param **kwargs  Keyword arguments forwarded to Session().
    @return  The global instance of the Session class."""
    global _session
    if _session is None:
        _session = Session(*args, **kwargs)
    return _session

@property
def session() -> Session:
    """Aesthetically pleasing accessor for the Session singleton.
    @details  Usage:
        from src.core.context import session
        session.graph_db.run_query(...)
    Behaves like a global variable but resolves lazily.
    @return  The global instance of the Session class."""
    return get_session()
