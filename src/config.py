from src.connectors.llm import LLMConnector
from src.components.relation_extraction import RelationExtractor
from typing import List, Any, Optional, Tuple
from src.core.context import session

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

    chunk_selection_method: str = "first-3"
    configuration: str = "fast"

    @staticmethod
    def load_values():
        Config._check_val(Config.configuration, "configuration", ["fast", "best", "baseline"])
        if Config.configuration == "fast":
            Config.load_fast()
        if Config.configuration == "best":
            Config.load_best()
        if Config.configuration == "baseline":
            Config.load_baseline()

    @staticmethod
    def load_fast():
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
    def load_best():
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
    def load_baseline():
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

    # Moderation thresholds for Gutenberg (historical fiction)
    moderation_thresholds = {
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
    def setup():
        Config.load_values()
        Config.check_values()

    @staticmethod
    def check_values():
        Config._check_sample(Config.chunk_selection_method)
        Config._check_extractor(Config.relation_extractor_type)
        Config._check_llm_engine(Config.validation_llm_engine)
        Config._check_llm_engine(Config.moderation_llm_engine)
        Config._check_moderation_strategy(Config.moderation_strategy)
        Config._check_subgraph_mode(Config.graph_lookup_mode)
        Config._check_verbal_mode(Config.verbalize_triples_mode)
        Config._check_llm_engine(Config.summary_llm_engine)

    @staticmethod
    def _check_sample(value: Any):
        Config._check_val(value, "extractor_type", ['all', 'random', 'first', 'index'])

    @staticmethod
    def _check_extractor(value: Any):
        Config._check_val(value, "extractor_type", ['textacy', 'openie', 'rebel'])

    @staticmethod
    def _check_llm_engine(value: Any):
        Config._check_val(value, "llm_connector_type", ['langchain', 'openai'])

    @staticmethod
    def _check_moderation_strategy(value: Any):
        Config._check_val(value, "moderation_strategy", ['drop', 'resolve'])

    @staticmethod
    def _check_subgraph_mode(value: Any):
        Config._check_val(value, "subgraph_mode", ['popular', 'local', 'explore', 'community'])

    @staticmethod
    def _check_verbal_mode(value: Any):
        Config._check_val(value, "verbalization_mode", ['raw', 'natural', 'json', 'context'])

    @staticmethod
    def _check_val(value: Any, name: str, allowed_values: List[Any]) -> None:
        if value not in allowed_values:
            if not any(allowed_val in str(value) for allowed_val in allowed_values):
                raise ValueError(f"Invalid {name}: {value}. Expected: {str(allowed_values)}")



    @staticmethod
    def get_extractor(extractor_type: str) -> RelationExtractor:
        # TODO: move to session.extractor?
        if extractor_type == "rebel":
            from src.components.relation_extraction import RelationExtractorREBEL

            # TODO: move to session.rel_extract
            re_rebel = "Babelscape/rebel-large"
            # TODO: different models
            # re_rst = "GAIR/rst-information-extraction-11b"
            # ner_renard = "compnet-renard/bert-base-cased-literary-NER"
            return RelationExtractorREBEL(model_name=re_rebel, max_tokens=1024)

        if extractor_type == "openie":
            from src.components.relation_extraction import RelationExtractorOpenIE

            # Initialize OpenIE wrapper (handles CoreNLP server internally)
            return RelationExtractorOpenIE(memory="4G")

        if extractor_type == "textacy":
            from src.components.relation_extraction import RelationExtractorTextacy

            # Initialize Textacy wrapper (pure Python backup)
            return RelationExtractorTextacy()


    @staticmethod
    def get_llm(llm_connector_type: str, system_prompt: str) -> LLMConnector:
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


    @staticmethod
    def get_subgraph(lookup_mode):
        """Perform selected subgraph retrieval strategy."""
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

    @staticmethod
    def get_final_prompt(use_triples: bool, use_text: bool, triples_string: Optional[str], text: Optional[str]) -> Tuple[str, str]:
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
        return prompt

    @staticmethod
    def get_chunks(chunking_mode, book_chunks):
        """Select chunks strategy based on use case."""
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

    def _sample_chunks(chunks, n_sample):
        unique_numbers = random.sample(range(len(chunks)), n_sample)
        sample = []
        for i in unique_numbers:
            c = chunks[i]
            sample.append(c)
        return (unique_numbers, sample)
