from abc import ABC, abstractmethod
from dotenv import load_dotenv
from langchain_core.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from langchain_openai import ChatOpenAI
from openai import OpenAI
import os
import re
from src.connectors.base import Connector
from src.components.relation_extraction import Triple
from src.util import Log
from typing import Any, Dict, List, Tuple, Optional
import json


class LLMConnector(Connector, ABC):
    """Connector for prompting and returning LLM output (raw text/JSON) via LLMs.
    @note  The method @ref src.connectors.llm.LLMConnector.execute_query simplifies the prompt process.
    @details  To implement various configurations, either set properties directly or create another LLMConnector instance.
        Useful config options: temperature, system_prompt, llm, model_name.
        We prefer creating a separate wrapper instance for reusable hard-coded configurations.
    """

    def __init__(self, model_name: str, temperature: float = 0, reasoning_effort: str = None, system_prompt: str = "You are a helpful assistant.", verbose: bool = True):
        """Initialize common LLM connector properties."""
        self.model_name: str = model_name
        self.temperature: float = temperature
        self.reasoning_effort: str = reasoning_effort
        self.system_prompt: str = system_prompt
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

    def __init__(self, model_name: str, temperature: float = 0, reasoning_effort: str = None, system_prompt: str = "You are a helpful assistant.", verbose: bool = True):
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        super().__init__(model_name, temperature, reasoning_effort, system_prompt, verbose)
        self.client: OpenAI = None
        self.configure()

    def configure(self) -> None:
        """Initialize the OpenAI client."""
        load_dotenv(".env")
        self.client = OpenAI()

    def execute_full_query(self, system_prompt: str, human_prompt: str) -> str:
        """Send a single prompt using the OpenAI client directly for speed.
        @param system_prompt  Instructions for the LLM.
        @param human_prompt  The user input or query.
        @return Raw LLM response as a string."""
        if self.reasoning_effort is None:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": human_prompt},
                ],
                temperature=self.temperature,
            )
        else:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": human_prompt},
                ],
                temperature=self.temperature,
                reasoning_effort=self.reasoning_effort,
            )
        return str(response.choices[0].message.content)


class LangChainConnector(LLMConnector):
    """Fully-featured API to prompt across various LLM providers."""

    def __init__(self, model_name: str, temperature: float = 0, reasoning_effort: str = None, system_prompt: str = "You are a helpful assistant.", verbose: bool = True):
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        super().__init__(model_name, temperature, reasoning_effort, system_prompt, verbose)
        self.client: ChatOpenAI = None
        self.configure()

    def configure(self) -> None:
        """Initialize the LangChain LLM using environment credentials.
        @details
            Reads:
                - OPENAI_API_KEY from .env for authentication"""
        load_dotenv(".env")
        if self.reasoning_effort is None:
            self.client = ChatOpenAI(model=self.model_name, temperature=self.temperature)
        else:
            self.client = ChatOpenAI(model=self.model_name, temperature=self.temperature, reasoning={"effort": self.reasoning_effort})

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
    thresholds: Dict[str, float]
) -> List[Dict[str, float]]:
    """Check texts for offensive content using OpenAI moderation API.
    @details
    - Returns flagged categories so caller can log/analyze violations
    - Config object encodes domain knowledge about acceptable thresholds
    - Batch processing amortizes API latency across multiple texts
    @param texts  List of text strings to moderate
    @param thresholds  Dict of category->threshold (e.g., {"hate": 0.4})
    @return  List corresponding to inputs. 
    Empty dict {} = Safe. Populated dict = Unsafe (contains scores).
    """
    if not texts:
        return []
    
    # Initialize client locally as requested
    client = OpenAI()

    batch_size = 32
    all_violations: List[Dict[str, float]] = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        
        try:
            response = client.moderations.create(input=batch)
            
            for result in response.results:
                scores = result.category_scores.model_dump()
                batch_violations = {}

                for category, score in scores.items():
                    # Sanitize category name: "sexual/minors" -> "sexual_minors"
                    config_key = category.replace('/', '_').replace('-', '_')
                    
                    # Use .get() default to 1.0 (loose) to avoid false positives on missing keys
                    limit = thresholds.get(config_key, 1.0)

                    if score > limit:
                        batch_violations[category] = score
                
                all_violations.append(batch_violations)

        except Exception as e:
            print(f"Moderation API failed: {e}. Marking batch as safe.")
            # Fail-open: assume safe if API fails to prevent blocking pipeline
            all_violations.extend([{} for _ in batch])

    return all_violations



def flag_triples(
    triples: List[Triple], 
    thresholds: Dict[str, float]
) -> Tuple[List[Triple], List[Tuple[Triple, Dict[str, float]]]]:
    """
    Filter triples containing offensive content.
    Returns: (safe_triples, bad_triples_with_reasons)
    """
    # 1. Flatten to strings for the API
    texts = [f"{t['s']} {t['r']} {t['o']}" for t in triples]
    
    # 2. Get scores (delegates to your existing moderate_texts)
    results = moderate_texts(texts, thresholds)

    safe_triples = []
    bad_triples = []

    # 3. Split based on results
    for triple, violations in zip(triples, results):
        if not violations:
            safe_triples.append(triple)
        else:
            bad_triples.append((triple, violations))

    return safe_triples, bad_triples


def parse_llm_triples(llm_output: str) -> List[Triple]:
    """Load normalized triples from LLM strings."""
    # TODO: rely on robust LLM connector logic to assume json
    cleaned = clean_json_block(llm_output)
    json_triples = json.loads(cleaned)
    # TODO: should LLM connector run sanitization internally?
    return normalize_to_dict(json_triples, keys=["s", "r", "o"])
