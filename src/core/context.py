from typing import Any, Self, Optional, TYPE_CHECKING

# 1. Avoid circular imports: compile-time imports only when type checking
if TYPE_CHECKING:
    from src.components.fact_storage import KnowledgeGraph
    from src.components.metrics import Metrics
    from src.connectors.document import DocumentConnector
    from src.connectors.graph import GraphConnector
    from src.connectors.relational import RelationalConnector


class Session:
    """Stores active database connections and configuration settings.
    @details
        - This class implements Singleton design so only one session can be created.
    """

    _instance = None
    _created = False
    _initialized = False

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
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
        if self._created:
            return  # Prevent reinitialization
        ## Flag to prevent duplicate calls to Session.__init__() / Session()
        self._created = True
        ## Flag to prevent duplicate calls to Session.setup()
        self._initialized = False
        ## Enables or disables the components from printing debug info.
        self.verbose = verbose

        # 2. Declare connectors and components ONLY
        #   Avoids initializing everything on session creation -> Cascading error for anyone importing session.
        #   We do NOT assign = None, so the type is strictly 'RelationalConnector'.
        #   "I promise this attribute exists and is this type."
        self.relational_db: "RelationalConnector"
        self.docs_db: "DocumentConnector"
        self.graph_db: "GraphConnector"
        self.main_graph: "KnowledgeGraph"
        self.metrics: "Metrics"

    def setup(self) -> None:
        """Loads heavy dependencies and initializes connections.
        @note  Must be called at application startup (main.py) or test setup (conftest.py).
        """
        if self._initialized:
            return  # Prevent reinitialization
        self._initialized = True

        # 3. Import connectors and components at runtime to avoid circular imports
        from src.components.fact_storage import KnowledgeGraph
        from src.components.metrics import Metrics
        from src.connectors.document import DocumentConnector
        from src.connectors.graph import GraphConnector
        from src.connectors.relational import RelationalConnector

        # 4. Initialize connectors and components
        ## Stores RDF-compliant semantic triples.
        self.relational_db = RelationalConnector.from_env(verbose=self.verbose)
        ## Stores input text, pre-processed chunks, JSON intermediates, and final output.
        self.docs_db = DocumentConnector(verbose=self.verbose)
        ## Stores entities (nodes) and relations (edges).
        self.graph_db = GraphConnector(verbose=self.verbose)
        ## Main storage for initial pipeline.
        self.main_graph = KnowledgeGraph("main", self.graph_db, self.verbose)
        ## The metrics class needs an instance to read the .env file.
        self.metrics = Metrics()
        # TODO: Split into scene graph, event graph, and social graph.


## The global instance of the singleton Session class.
# Do NOT assign = None since this should always exist after setup.
session: Session

## Internal module storage for the lazy session singleton
_session: Optional[Session] = None


def get_session(*args: Any, **kwargs: Any) -> Session:
    """Lazily creates a session on first call, otherwise returns the existing session.
    @note  Will ignore any arguments passed after creation.
    @param *args  Positional arguments forwarded to Session().
    @param **kwargs  Keyword arguments forwarded to Session().
    @return  The global instance of the Session class."""
    global _session
    if _session is None:
        _session = Session(*args, **kwargs)
    return _session


def __getattr__(name: str) -> Session:
    """Lazy attribute resolution for module-level imports.
    @details
        - Only called when normal attribute lookup fails (i.e., name not in module globals).
        - Enables lazy session creation: `from src.core.context import session`
        - Regular imports (Session, get_session, etc.) bypass this entirely.
    @param name  The attribute name being accessed.
    @return  The session singleton if 'session' is requested.
    @throws AttributeError  If an unknown/undefined attribute is requested."""
    if name == "session":
        return get_session()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
