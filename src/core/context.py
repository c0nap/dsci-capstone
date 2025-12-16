from typing import Any, Optional, Self, Dict, Generator, Type, TYPE_CHECKING
from contextlib import contextmanager
import os
from dotenv import load_dotenv

# 1. Avoid circular imports: compile-time imports only when type checking
if TYPE_CHECKING:
    from src.components.fact_storage import KnowledgeGraph
    from src.components.metrics import Metrics
    from src.connectors.document import DocumentConnector
    from src.connectors.graph import GraphConnector
    from src.connectors.relational import RelationalConnector
    from src.config import Config

    # External types for MyPy compliance
    import spacy.language
    from stanza.server import CoreNLPClient
    from sentence_transformers import SentenceTransformer, CrossEncoder
    from sklearn.feature_extraction.text import TfidfVectorizer
    from rouge_score.rouge_scorer import RougeScorer
    from bert_score import BERTScorer


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
        self.config: "Type[Config]"

        # 3. Declare Lazy-Loaded Models
        self.model_spacy: "spacy.language.Language"
        self.sentencizer_spacy: "spacy.language.Language"

        # Transformers use factory patterns and do not expose useful types
        self.tokenizer_rebel: Any
        self.model_rebel: Any
        
        self._client_openie: "CoreNLPClient"
        self.openie_persistent: bool

        self.model_nli: "CrossEncoder"
        self.vectorizer_salience: "TfidfVectorizer"
        self.model_bertscore_large: "BERTScorer"
        self.model_rouge_full: "RougeScorer"
        self.model_bertscore_distil: "BERTScorer"
        self.model_rouge_recall: "RougeScorer"
        self.model_sentence_coherence: "SentenceTransformer"

    def setup(self) -> None:
        """Loads heavy dependencies and initializes connections.
        @note  Must be called at application startup (main.py) or test setup (conftest.py).
        """
        if self._initialized:
            return  # Prevent reinitialization
        self._initialized = True

        # 4. Import connectors and components at runtime to avoid circular imports
        from src.components.fact_storage import KnowledgeGraph
        from src.components.metrics import Metrics
        from src.connectors.document import DocumentConnector
        from src.connectors.graph import GraphConnector
        from src.connectors.relational import RelationalConnector
        from src.config import Config

        # 5. Initialize connectors and components
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
        ## Static class containing global configuration settings.
        self.config = Config
        self.config.setup()
        # TODO: Split into scene graph, event graph, and social graph.

        self.load_models()
        self.load_metrics()
        if Config.relation_extractor_type == "openie":
            self.load_optional_openie()
        elif Config.relation_extractor_type == "rebel":
            self.load_optional_rebel()

    def load_models(self) -> None:
        import spacy
        name_spacy_model = "en_core_web_sm"
        
        try:  # Auto-download if missing (Self-healing)
            self.model_spacy = spacy.load(name_spacy_model)
        except OSError:
            print(f"Spacy model '{name_spacy_model}' not found. Downloading...")
            spacy.cli.download(name_spacy_model)  # type: ignore[attr-defined]
            self.model_spacy = spacy.load(name_spacy_model)

        ## Lightweight spaCy model to split text into sentences. Faster than full parsing model.
        self.sentencizer_spacy = spacy.blank("en")
        self.sentencizer_spacy.add_pipe("sentencizer")

    def load_optional_rebel(self) -> None:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        load_dotenv(".env")  # Needs HF_HUB_TOKEN
        name_rebel_model = "Babelscape/rebel-large"
        # TODO: different models
        # re_rst = "GAIR/rst-information-extraction-11b"
        # ner_renard = "compnet-renard/bert-base-cased-literary-NER"
        self.tokenizer_rebel = AutoTokenizer.from_pretrained(name_rebel_model)
        self.model_rebel = AutoModelForSeq2SeqLM.from_pretrained(name_rebel_model)

    def load_optional_openie(self, persistent: bool = False) -> None:
        """Ensures CoreNLP backend is installed. Can call early to pre-warm, but not required.
        @details
            Uses a context manager to spin up the Java server via CoreNLPClient.
            This ensures the heavy Java process (which requires ~4GB RAM) is
            terminated immediately after processing, freeing resources.
            """
        import stanza

        install_dir = os.path.expanduser("~/stanza_corenlp")  # Where to save the JAR files
        if not os.path.exists(install_dir):
            print("Installing CoreNLP backend...")
            stanza.install_corenlp()
        self._client_openie = None
        self.openie_persistent = persistent

    def stop_openie(self) -> None:
        """Stops Java CoreNLP server.
        @note  Only needed in persistent mode at pipeline end, otherwise auto-stops."""
        if self._client_openie is not None:
            self._client_openie.stop()
            self._client_openie = None

    @contextmanager
    def java_client_openie(self, config: Dict[str, Any]) -> Generator["CoreNLPClient", None, None]:
        """Context manager for CoreNLP client.
        @details  Usage - `with session.java_client_openie(config) as client:`
        @param config  CoreNLPClient configuration dict (annotators, memory, timeout, etc.)
        @yield  An active CoreNLPClient instance.
        """
        from stanza.server import CoreNLPClient
        
        # No error if load() wasnt called yet
        if not hasattr(self, '_client_openie') or self._client_openie is None:
            self.load_optional_openie()
            self._client_openie = CoreNLPClient(**config)
            self._client_openie.start()
        
        try:
            yield self._client_openie
        finally:
            if not self.openie_persistent:
                self.stop_openie()

    def load_metrics(self) -> None:
        from sentence_transformers import CrossEncoder
        from sklearn.feature_extraction.text import TfidfVectorizer
        from rouge_score import RougeScorer
        from bert_score import BERTScorer
        from sentence_transformers import SentenceTransformer

        self.model_nli = CrossEncoder('cross-encoder/nli-deberta-base', model_kwargs={"low_cpu_mem_usage": False})
        self.vectorizer_salience = TfidfVectorizer(max_features=1000)
        self.model_bertscore_large = BERTScorer(model_type="roberta-large", device="cpu")
        self.model_rouge_full = RougeScorer(['rouge1', 'rouge2', 'rougeL', 'rougeLsum'], use_stemmer=True)
        self.model_bertscore_distil = BERTScorer(model_type="distilroberta-base", lang="en", rescale_with_baseline=True, device="cpu")
        self.model_rouge_recall = RougeScorer(["rougeL"], use_stemmer=True)
        self.model_sentence_coherence = SentenceTransformer('all-MiniLM-L6-v2', model_kwargs={"low_cpu_mem_usage": False})


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
