from src.connectors.llm import LLMConnector
from src.components.relation_extraction import RelationExtractor
from typing import List, Any, Optional, Tuple, Dict
from src.core.context import session
import random

class Config:
    relation_extractor_type: str
    validation_llm_engine: str
    moderation_llm_engine: str
    moderation_strategy: str
    graph_lookup_mode: str
    verbalize_triples_mode: str
    summary_llm_engine: str

    source_text_visible: bool
    triples_visible: bool

    # gpt-5 only supports temperature 1
    temperature: float
    reasoning_effort: str
    model_name: str

    seed: int = 123
    chunk_selection_method: str = "random-30"
    configuration: str = "fast"

    # ================= BOSS CONFIGURATION =================
    # How many times to retry a failed chunk
    # Worker assignment POST failures are NOT counted here
    # Failures accumulate across tasks, e.g. 1 bookscore fail and 1 questeval fail = 2 total fails.
    MAX_RETRIES = 2

    # 'instant': Retry a chunk immediately when it reports failure.
    # 'deferred': Wait for all other chunks to finish, then bulk-retry failures.
    RETRY_STRATEGY = 'deferred'

    # 'chunk': Run pipeline_E immediately when a chunk completes.
    # 'story': Wait for ALL chunks to complete, then run pipeline_E on all of them.
    EVAL_SCOPE = 'chunk'

    @staticmethod
    def get_story_tracker_cols(only_tasks: bool = False) -> List[str]:
        """Get const column names using a method to hide attributes from @ref src.config.Config.to_dict.
        @param only_tasks  Set True to exclude chunk_id, story_id, etc.
        @return  List of column names in the Boss progress tracker."""
        tasks = ['preprocessing', 'chunking', 'summarization', 'metrics']
        if only_tasks:
            return tasks
        return ['story_id'] + tasks

    @staticmethod
    def get_chunk_tracker_cols(only_tasks: bool = False) -> List[str]:
        """Get const column names using a method to hide attributes from @ref src.config.Config.to_dict.
        @param only_tasks  Set True to exclude chunk_id, story_id, etc.
        @return  List of column names in the Boss progress tracker."""
        tasks = ['load_to_mongo',
            'relation_extraction',
            'llm_inference',
            'load_triples_to_neo4j',
            'graph_verbalization',
            'summarization',
            'metric_questeval',
            'metric_bookscore',
            'metrics_basic']
        if only_tasks:
            return tasks
        return ['chunk_id', 'story_id', 'retry_count'] + tasks

    @staticmethod
    def get_active_workers() -> List[str]:
        """Get const worker names using a method to hide attributes from @ref src.config.Config.to_dict.
        @return  List of worker names for the Boss to assign for this run."""
        return ["bookscore"]  #["questeval", "bookscore"]

    @staticmethod
    def get_worker_map(worker_type: str | None) -> Dict[str, str] | str:
        """Get const worker names using a method to hide attributes from @ref src.config.Config.to_dict.
        @param worker_type  Optionally get the task name using specified worker type.
        @return  Mapping of worker names to tracker-level task names."""
        worker_map = {'questeval': 'metric_questeval', 'bookscore': 'metric_bookscore'}
        if worker_type:
            return worker_map[worker_type]
        return worker_map
    # =================================================

    @staticmethod
    def to_dict() -> Dict[str, Any]:
        return {
            name: getattr(Config, name)
            for name in Config.__annotations__
            if hasattr(Config, name)
        }

    @staticmethod
    def load_values() -> None:
        """Load configuration values based on selected configuration mode.
        Validates configuration setting and dispatches to appropriate loader."""
        Config._check_val(Config.configuration, "configuration", ["fast", "best", "baseline"])
        if Config.configuration == "fast":
            Config.load_fast()
        if Config.configuration == "best":
            Config.load_best()
        if Config.configuration == "baseline":
            Config.load_baseline()

    @staticmethod
    def load_fast() -> None:
        """Load fast configuration preset.
        Uses lightweight models and minimal processing for quick evaluation."""
        Config.relation_extractor_type = "textacy"
        Config.validation_llm_engine = "openai"
        Config.moderation_llm_engine = "openai"
        Config.moderation_strategy = "drop"
        Config.graph_lookup_mode = "popular"
        Config.verbalize_triples_mode = "raw"
        Config.summary_llm_engine = "openai"

        Config.source_text_visible = False
        Config.triples_visible = True

        Config.temperature = 1
        Config.reasoning_effort = "minimal"
        Config.model_name = "gpt-5-nano"

    @staticmethod
    def load_best() -> None:
        """Load best configuration preset.
        Uses advanced models and comprehensive processing for highest quality."""
        Config.relation_extractor_type = "openie"
        Config.validation_llm_engine = "langchain"
        Config.moderation_llm_engine = "langchain"
        Config.moderation_strategy = "resolve"
        Config.graph_lookup_mode = "community"
        Config.verbalize_triples_mode = "context"
        Config.summary_llm_engine = "langchain"
        
        Config.source_text_visible = False
        Config.triples_visible = True

        Config.temperature = 1
        Config.reasoning_effort = "high"
        Config.model_name = "gpt-5"

    @staticmethod
    def load_baseline() -> None:
        """Load baseline configuration preset.
        Uses source text only without graph-based enhancements for comparison."""
        Config.relation_extractor_type = "textacy"
        Config.validation_llm_engine = "openai"
        Config.moderation_llm_engine = "openai"
        Config.moderation_strategy = "drop"
        Config.graph_lookup_mode = "popular"
        Config.verbalize_triples_mode = "raw"
        Config.summary_llm_engine = "openai"

        Config.source_text_visible = True
        Config.triples_visible = False

        Config.temperature = 1
        Config.reasoning_effort = "high"
        Config.model_name = "gpt-5"

    @staticmethod
    def load_pytest() -> None:
        """Load PyTest configuration preset."""
        Config.load_fast()
        Config.chunk_selection_method = "random-2"


    @staticmethod
    def get_moderation_thresholds() -> Dict[str, float]:
        """Get const moderation specs using a method to hide attributes from @ref src.config.Config.to_dict.
        @note  Setting any threshold to 0.0 will flag everything, since even safe text will have 1e-5 of each category.
        @return  Moderation thresholds for Gutenberg (historical fiction)."""
        return {
            "hate": 0.4,                      # Period racism in dialogue
            "harassment": 0.4,                # Victorian class conflict
            "violence": 0.5,                  # Gothic/adventure violence
            "sexual": 0.3,                    # Euphemistic Victorian content
            "self_harm": 0.3,                 # Tragic death scenes
            "hate_threatening": 0.2,          # Filter overt calls to violence
            "harassment_threatening": 0.2,
            "violence_graphic": 0.3,
            "sexual_minors": 0.01,            # Zero tolerance
            "self_harm_intent": 0.1,
            "self_harm_instructions": 0.1,
            "illicit": 0.05,
            "illicit_violent": 0.05,
        }

    @staticmethod
    def setup() -> None:
        """Initialize configuration system.
        Loads values and validates all configuration parameters."""
        Config.load_values()
        Config.check_values()

    @staticmethod
    def check_values() -> None:
        """Validate all configuration values.
        Ensures all settings conform to expected values."""
        Config._check_sample(Config.chunk_selection_method)
        Config._check_extractor(Config.relation_extractor_type)
        Config._check_llm_engine(Config.validation_llm_engine)
        Config._check_llm_engine(Config.moderation_llm_engine)
        Config._check_moderation_strategy(Config.moderation_strategy)
        Config._check_subgraph_mode(Config.graph_lookup_mode)
        Config._check_verbal_mode(Config.verbalize_triples_mode)
        Config._check_llm_engine(Config.summary_llm_engine)

    @staticmethod
    def _check_sample(value: Any) -> None:
        """Validate chunk selection method.
        @param value  Chunk selection strategy to validate."""
        Config._check_val(value, "extractor_type", ['all', 'random', 'first', 'index'])

    @staticmethod
    def _check_extractor(value: Any) -> None:
        """Validate relation extractor type.
        @param value  Extractor type to validate."""
        Config._check_val(value, "extractor_type", ['textacy', 'openie', 'rebel'])

    @staticmethod
    def _check_llm_engine(value: Any) -> None:
        """Validate LLM connector type.
        @param value  LLM engine type to validate."""
        Config._check_val(value, "llm_connector_type", ['langchain', 'openai'])

    @staticmethod
    def _check_moderation_strategy(value: Any) -> None:
        """Validate content moderation strategy.
        @param value  Moderation strategy to validate."""
        Config._check_val(value, "moderation_strategy", ['drop', 'resolve'])

    @staticmethod
    def _check_subgraph_mode(value: Any) -> None:
        """Validate graph lookup mode.
        @param value  Subgraph retrieval mode to validate."""
        Config._check_val(value, "subgraph_mode", ['popular', 'local', 'explore', 'community'])

    @staticmethod
    def _check_verbal_mode(value: Any) -> None:
        """Validate triple verbalization mode.
        @param value  Verbalization mode to validate."""
        Config._check_val(value, "verbalization_mode", ['raw', 'natural', 'json', 'context'])

    @staticmethod
    def _check_val(value: Any, name: str, allowed_values: List[Any]) -> None:
        """Validate a configuration value against allowed options.
        @param value  Value to validate.
        @param name  Name of configuration parameter for error messages.
        @param allowed_values  List of acceptable values."""
        if value not in allowed_values:
            if not any(allowed_val in str(value) for allowed_val in allowed_values):
                raise ValueError(f"Invalid {name}: {value}. Expected: {str(allowed_values)}")



    @staticmethod
    def get_extractor(extractor_type: str) -> RelationExtractor:
        """Get relation extractor instance by type.
        @param extractor_type  Type of extractor ('rebel', 'openie', or 'textacy').
        @return  Initialized RelationExtractor instance."""
        # TODO: move to session.extractor?
        if extractor_type == "rebel":
            from src.components.relation_extraction import RelationExtractorREBEL

            return RelationExtractorREBEL(max_tokens=1024)

        if extractor_type == "openie":
            from src.components.relation_extraction import RelationExtractorOpenIE

            # Initialize OpenIE wrapper (handles CoreNLP server internally)
            return RelationExtractorOpenIE(memory="4G")

        if extractor_type == "textacy":
            from src.components.relation_extraction import RelationExtractorTextacy

            # Initialize Textacy wrapper (pure Python backup)
            return RelationExtractorTextacy()
        raise ValueError(f"Unknown relation extractor type: {extractor_type}")


    @staticmethod
    def get_llm(llm_connector_type: str, system_prompt: str) -> LLMConnector:
        """Get LLM connector instance by type.
        @param llm_connector_type  Type of connector ('langchain' or 'openai').
        @param system_prompt  System prompt to initialize the LLM with.
        @return  Initialized LLMConnector instance."""
        # TODO: move to session.llm?
        if llm_connector_type == "langchain":
            from src.connectors.llm import LangChainConnector
            return LangChainConnector(
                model_name=Config.model_name,
                temperature=Config.temperature,
                reasoning_effort=Config.reasoning_effort,
                system_prompt=system_prompt,
            )
        if llm_connector_type == "openai":
            from src.connectors.llm import OpenAIConnector
            return OpenAIConnector(
                model_name=Config.model_name,
                temperature=Config.temperature,
                reasoning_effort=Config.reasoning_effort,
                system_prompt=system_prompt,
            )
        raise ValueError(f"Unknown LLM connector type: {llm_connector_type}")


    @staticmethod
    def get_subgraph(lookup_mode: str) -> Any:
        """Perform selected subgraph retrieval strategy.
        @param lookup_mode  Strategy for subgraph retrieval ('popular', 'local', 'explore', or 'community').
        @return  Retrieved subgraph data structure."""
        if lookup_mode == "popular":
            # FAST: Degree-based filtering for hub entities
            return session.main_graph.get_by_ranked_degree(
                worst_rank=5, 
                enforce_count=True, 
                id_columns=["subject_id"]
            )
        if lookup_mode == "local":
            # FAST: Multi-hop exploration from most connected node
            center_node = session.main_graph.get_node_most_popular()
            return session.main_graph.get_neighborhood(center_node, depth=2)
        if lookup_mode == "explore":
            # MEDIUM: Structural exploration via random walks
            start_nodes = session.main_graph.get_nodes_top_degree(k=3)
            return session.main_graph.get_random_walk(
                start_nodes, 
                walk_length=5, 
                num_walks=3
            )
        if lookup_mode == "community":
            # HEAVY: Community-based retrieval (run detection once, query many times)
            session.main_graph.detect_community_clusters(method="leiden")
            community_id = session.main_graph.get_community_largest()
            return session.main_graph.get_community_subgraph(community_id)
        raise ValueError(f"Unknown lookup mode: {lookup_mode}")

    @staticmethod
    def get_final_prompt(use_triples: bool, use_text: bool, triples_string: Optional[str], text: Optional[str]) -> str:
        """Construct final prompt for summary generation based on available inputs.
        @param use_triples  Whether to include semantic triples in prompt.
        @param use_text  Whether to include original text in prompt.
        @param triples_string  Formatted string of semantic triples.
        @param text  Original story chunk text.
        @return  Constructed prompt string."""
        if use_triples and use_text:
            prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
            prompt += f"And here is the original text:\n{text}\n\n"
            prompt += "Transform this data into a coherent, factual, and concise summary. Some relations may be irrelevant, so don't force yourself to include every single one.\n"
            prompt += "Output your generated summary and nothing else."
        elif use_triples:
            prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
            prompt += "Transform this data into a coherent, factual, and concise summary. Some relations may be irrelevant, so don't force yourself to include every single one.\n"
            prompt += "Output your generated summary and nothing else."
        elif use_text:
            prompt = f"Here is a story chunk:\n{text}\n"
            prompt += "Transform this into a coherent, factual, and concise summary. Some details may be irrelevant, so don't force yourself to include every single one.\n"
            prompt += "Output your generated summary and nothing else."
        else:
            raise ValueError(f"Invalid prompting strategy: Must specify triples or source text.")
        return prompt

    @staticmethod
    def get_chunks(chunking_mode: str, book_chunks: List[Any]) -> List[Any]:
        """Select chunks strategy based on use case.
        @param chunking_mode  Strategy for chunk selection ('all', 'random-N', 'first-N', or 'index-N').
        @param book_chunks  List of all available chunks.
        @return  Selected subset of chunks."""
        if chunking_mode == "all":
            return book_chunks
        if "random" in chunking_mode:
            n_chunks = int(chunking_mode.split('-')[1])
            return Config._sample_chunks(book_chunks, n_chunks)
        if "first" in chunking_mode:
            n_chunks = int(chunking_mode.split('-')[1])
            return book_chunks[:n_chunks]
        if "index" in chunking_mode:
            index = int(chunking_mode.split('-')[1])
            return [book_chunks[index]]
        raise ValueError(f"Unknown chunking mode: {chunking_mode}")

    @staticmethod
    def _sample_chunks(chunks: List[Any], n_sample: int) -> List[Any]:
        """Sample random chunks using configured seed for reproducibility.
        @param chunks  List of all available chunks.
        @param n_sample  Number of chunks to sample.
        @return  Randomly sampled list of chunks."""
        rng = random.Random(Config.seed)
        unique_numbers = rng.sample(range(len(chunks)), n_sample)
        sample = [chunks[i] for i in unique_numbers]
        #print(unique_numbers)
        return sample