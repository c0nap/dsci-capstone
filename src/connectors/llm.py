from abc import ABC, abstractmethod
from dotenv import load_dotenv
from langchain_core.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from langchain_openai import ChatOpenAI
from openai import BadRequestError, OpenAI
import os
import re
from src.connectors.base import Connector
from src.util import Log
from typing import Any, Dict, List, Tuple, Optional


class LLMConnector(Connector, ABC):
    """Connector for prompting and returning LLM output (raw text/JSON) via LLMs.
    @note  The method @ref src.connectors.llm.LLMConnector.execute_query simplifies the prompt process.
    @details  To implement various configurations, either set properties directly or create another LLMConnector instance.
        Useful config options: temperature, system_prompt, llm, model_name.
        We prefer creating a separate wrapper instance for reusable hard-coded configurations.
    """

    def __init__(self, temperature: float = 0, system_prompt: str = "You are a helpful assistant.", verbose: bool = True):
        """Initialize common LLM connector properties."""
        self.temperature: float = temperature
        self.system_prompt: str = system_prompt
        self.model_name: str = None
        self.verbose: bool = verbose

    def test_operations(self, raise_error: bool = True) -> bool:
        """Establish a basic connection to the database, and test full functionality.
        @details  Can be configured to fail silently, which enables retries or external handling.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the prompt executed successfully.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
        return self.check_connection(Log.test_ops, raise_error=raise_error)

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Send a trivial prompt to verify LLM connectivity.
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the prompt executed successfully.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
        result = self.execute_full_query("You are a helpful assistant.", "ping")
        success = result.strip().lower() == "pong"
        if not success and raise_error:
            raise Log.Failure(f"{log_source}: Connection check failed")
        return success

    def _load_env(self) -> None:
        """Load environment variables and set model name.
        @details Called by subclasses during configure() to ensure consistent env loading."""
        load_dotenv(".env")
        self.model_name = os.environ["LLM_MODEL"]

    @abstractmethod
    def configure(self) -> None:
        pass

    @abstractmethod
    def execute_full_query(self, system_prompt: str, human_prompt: str) -> str:
        """Send a single prompt to the LLM with separate system and human instructions."""
        pass

    def execute_query(self, query: str) -> str:
        """Send a single prompt through the connection and return raw LLM output.
        @param query  A single string prompt to send to the LLM.
        @return Raw LLM response as a string."""
        return self.execute_full_query(self.system_prompt, query)

    def execute_file(self, filename: str) -> List[str]:
        """Run a single prompt from a file.
        @details  Reads the entire file as a single string and sends it to execute_query.
        @param filename  Path to the prompt file (.txt)
        @return  Raw LLM response as a string."""
        with open(filename, "r", encoding="utf-8") as f:
            return [self.execute_query(f.read())]


class OpenAIConnector(LLMConnector):
    """Lightweight LLM interface for faster response times."""

    def __init__(self, temperature: float = 0, system_prompt: str = "You are a helpful assistant.", verbose: bool = True):
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        super().__init__(temperature, system_prompt)
        self.client: OpenAI = None
        self.configure()

    def configure(self) -> None:
        """Initialize the OpenAI client."""
        self._load_env()
        self.client = OpenAI()

    def execute_full_query(self, system_prompt: str, human_prompt: str) -> str:
        """Send a single prompt using the OpenAI client directly for speed.
        @param system_prompt  Instructions for the LLM.
        @param human_prompt  The user input or query.
        @return Raw LLM response as a string."""
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": human_prompt},
                ],
                temperature=self.temperature,
                reasoning_effort="minimal",
            )
        except BadRequestError:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": human_prompt},
                ],
                temperature=self.temperature,
            )
        return str(response.choices[0].message.content)


class LangChainConnector(LLMConnector):
    """Fully-featured API to prompt across various LLM providers."""

    def __init__(self, temperature: float = 0, system_prompt: str = "You are a helpful assistant.", verbose: bool = True):
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        super().__init__(temperature, system_prompt)
        self.client: ChatOpenAI = None
        self.configure()

    def configure(self) -> None:
        """Initialize the LangChain LLM using environment credentials.
        @details
            Reads:
                - OPENAI_API_KEY from .env for authentication
                - LLM_MODEL and LLM_TEMPERATURE to override defaults"""
        self._load_env()
        self.client = ChatOpenAI(model=self.model_name, temperature=self.temperature, reasoning={"effort": "minimal"})

    def execute_full_query(self, system_prompt: str, human_prompt: str) -> str:
        """Send a single prompt to the LLM with separate system and human instructions.
        @param system_prompt  Instructions for the LLM.
        @param human_prompt  The user input or query.
        @return Raw LLM response as a string."""
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("human", human_prompt),
            ]
        )
        try:
            response = self.client.invoke(prompt.format_messages())
        except BadRequestError:
            self.client = ChatOpenAI(model=self.model_name, temperature=self.temperature)
            response = self.client.invoke(prompt.format_messages())
        return str(response.content)


def clean_json_block(s: str) -> str:
    # Remove leading/trailing triple backticks and optional "json" label
    s = s.strip()
    s = re.sub(r"^```json\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    return s


def normalize_to_dict(data: Dict[str, str] | List[Dict[str, str]], keys: List[str]) -> List[Dict[str, str]]:
    """Normalize nested/compacted LLM output into flat dicts.
    @details
        Handles token-saving patterns:
        - Nested relation-object pairs: {"s":"X", [{"r":"R1","o":"O1"}, ...]}
        - List subjects with nested r-o: {"s":["X","Y"], [{"r":"R","o":"O"}, ...]}
        - Cartesian products: {"s":["X","Y"], "r":["R1","R2"], "o":["O1","O2"]}
        Assumes input is already parsed (json.loads called by caller).
    @param data  Parsed LLM output (dict or list of dicts)
    @param keys  Expected keys (e.g., ["s", "r", "o"])
    @return  List of flat dicts with all keys present
    @throws ValueError  If input format cannot be parsed
    """

    def _as_list(x: Any) -> List[Any]:
        """Coerce value to list for uniform handling.
        @param x  Any input value
        @return  List containing x, or x itself if already a list/tuple
        """
        return list(x) if isinstance(x, (list, tuple)) else [x]

    def _expand_nested_ro(item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Expand nested relation-object pairs pattern.
        @details
            Detects patterns like:
            - {"s":"X", [{"r":"R1","o":"O1"}, {"r":"R2","o":"O2"}]}
            - {"s":["X","Y"], [{"r":"R","o":"O"}]}
            Creates cartesian product of subjects × nested r-o pairs.
        @param item  Single dict potentially containing nested r-o list
        @return  List of expanded flat dicts, or [item] if no nesting found
        """
        subjects = _as_list(item.get("s") or item.get("subject"))

        # Find nested r-o pairs (not under a key, just in the dict values)
        nested_pairs = [v for v in item.values() if isinstance(v, list) and v and isinstance(v[0], dict)]

        if not nested_pairs:
            return [item]  # No nesting, return as-is

        # Cartesian product: each subject × each r-o pair
        results: List[Dict[str, Any]] = []
        for s in subjects:
            for pair in nested_pairs[0]:  # First nested list
                r = pair.get("r") or pair.get("relation")
                o = pair.get("o") or pair.get("object") or pair.get("object_")
                if r and o:
                    results.append({"s": s, "r": r, "o": o})
        return results

    def _expand_cartesian(item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Expand list values into flat combinations.
        @details
            Handles three cases:
            1. Same-length lists: zip them (parallel structure)
            2. Single + multi-value: broadcast the single value
            3. Multiple multi-values: full cartesian product
        @param item  Dict with potentially list-valued s/r/o
        @return  List of expanded flat dicts
        """
        s_vals = _as_list(item.get("s") or item.get("subject"))
        r_vals = _as_list(item.get("r") or item.get("relation"))
        o_vals = _as_list(item.get("o") or item.get("object") or item.get("object_"))

        # If lists have same length, zip them (not cartesian)
        if len(s_vals) == len(r_vals) == len(o_vals) and len(s_vals) > 1:
            return [{"s": s, "r": r, "o": o} for s, r, o in zip(s_vals, r_vals, o_vals)]

        # Otherwise, broadcast single values or create cartesian product
        max_len = max(len(s_vals), len(r_vals), len(o_vals))
        if len(s_vals) == 1:
            s_vals *= max_len
        if len(r_vals) == 1:
            r_vals *= max_len
        if len(o_vals) == 1:
            o_vals *= max_len

        # If all same length now, zip
        if len(s_vals) == len(r_vals) == len(o_vals):
            return [{"s": s, "r": r, "o": o} for s, r, o in zip(s_vals, r_vals, o_vals)]

        # Full cartesian product for mismatched lengths
        results: List[Dict[str, Any]] = []
        for s in s_vals:
            for r in r_vals:
                for o in o_vals:
                    results.append({"s": s, "r": r, "o": o})
        return results

    # Normalize input to list of dicts
    items: List[Dict[str, Any]] = data if isinstance(data, list) else [data]

    # Expand each item
    expanded: List[Dict[str, Any]] = []
    for item in items:
        # Try nested r-o expansion first
        nested = _expand_nested_ro(item)
        if len(nested) > 1 or nested[0] != item:
            expanded.extend(nested)
        else:
            # Try cartesian expansion
            expanded.extend(_expand_cartesian(item))

    return expanded



def moderate_texts(
    texts: List[str],
    thresholds: Dict[str, float],
) -> List[Tuple[bool, Optional[Dict[str, float]]]]:
    """Check texts for offensive content using OpenAI moderation API.
    @details
    - Returns flagged categories so caller can log/analyze violations
    - Config object encodes domain knowledge about acceptable thresholds
    - Batch processing amortizes API latency across multiple texts
    @param texts  List of text strings to moderate
    @param thresholds  Dict of category->threshold (e.g., {"hate": 0.4})
    @return List of (is_safe, flagged_categories) tuples
            is_safe=True if all scores below thresholds
            flagged_categories=dict of exceeded categories, None if safe
    """
    from openai import OpenAI

    if not texts:
        return []

    load_dotenv()
    client = OpenAI()

    batch_size = 32  # OpenAI's max per request
    results = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]

        try:
            response = client.moderations.create(input=batch)

            for result in response.results:
                scores = result.category_scores.model_dump()
                
                # Check each category against configured threshold
                flagged = {}
                for category, score in scores.items():
                    # Map API category names to config attributes
                    config_attr = category.replace('/', '_').replace('-', '_')
                    threshold = getattr(thresholds, config_attr, 0.01)
                    
                    if score > threshold:
                        flagged[category] = score
                
                is_safe = len(flagged) == 0
                results.append((is_safe, None if is_safe else flagged))

        except Exception as e:
            Log.warn(f"Moderation API failed: {e}, marking batch as safe")
            results.extend([(True, None)] * len(batch))

    return results


def moderate_triples(
    triples: List[Dict[str, str]],
    thresholds: Dict[str, float],
) -> Tuple[List[Dict[str, str]], List[Tuple[Dict[str, str], Dict[str, float]]]]:
    """Filter triples containing offensive content.
    @details
    - Enables logging bad triples for model improvement
    - Allows manual review of edge cases
    - Preserves violation details for analysis
    @param triples  List of dicts with 's', 'r', 'o' keys
    @param thresholds  Dict of category->threshold (e.g., {"hate": 0.4})
    @return Tuple of (safe_triples, bad_triples_with_reasons)
            bad_triples_with_reasons = [(triple, bad_categories), ...]
    """
    texts = [f"{t['s']} {t['r']} {t['o']}" for t in triples]
    moderation_results = moderate_texts(texts, thresholds)

    safe_triples = []
    bad_triples = []

    for triple, (is_safe, flagged_cats) in zip(triples, moderation_results):
        if is_safe:
            safe_triples.append(triple)
        else:
            bad_triples.append((triple, flagged_cats))

    filtered_count = len(triples) - len(safe_triples)
    Log.success(f"Moderation: filtered {filtered_count}/{len(triples)} triples")

    return safe_triples, bad_triples

