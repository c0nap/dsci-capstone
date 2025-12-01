from dotenv import load_dotenv
from langchain_core.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from langchain_openai import ChatOpenAI
import os
import re
from src.connectors.base import Connector
from typing import Any, List, Tuple


class LLMConnector(Connector):
    """Connector for prompting and returning LLM output (raw text/JSON) via LangChain.
    @note  The method @ref src.connectors.llm.LLMConnector.execute_query simplifies the prompt process.
    @details  To implement various configurations, either set properties directly or create another LLMConnector instance.
        Useful config options: temperature, system_prompt, llm, model_name.
        We prefer creating a separate wrapper instance for reusable hard-coded configurations.
    """

    def __init__(
        self,
        temperature: float = 0,
        system_prompt: str = "You are a helpful assistant.",
    ) -> None:
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        # Read environment variables at runtime
        load_dotenv(".env")
        self.model_name: str = None
        self.temperature: float = temperature
        self.system_prompt: str = system_prompt
        self.llm: ChatOpenAI = None
        self.configure()

    def configure(self) -> None:
        """Initialize the LangChain LLM using environment credentials.
        @details
            Reads:
                - OPENAI_API_KEY from .env for authentication
                - LLM_MODEL and LLM_TEMPERATURE to override defaults"""
        self.model_name = os.environ["LLM_MODEL"]
        self.llm = ChatOpenAI(model=self.model_name, temperature=self.temperature)

    def test_operations(self, raise_error: bool = True) -> bool:
        """Establish a basic connection to the database, and test full functionality.
        @details  Can be configured to fail silently, which enables retries or external handling.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the prompt executed successfully.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
        result = self.execute_full_query("You are a helpful assistant.", "ping")
        return result.strip() == "pong"
        # TODO

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Send a trivial prompt to verify LLM connectivity.
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the prompt executed successfully.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
        result = self.execute_full_query("You are a helpful assistant.", "ping")
        return result.strip() == "pong"

    def execute_full_query(self, system_prompt: str, human_prompt: str) -> str:
        """Send a single prompt to the LLM with separate system and human instructions."""
        self.system_prompt = system_prompt

        # Build prompt template
        prompt = ChatPromptTemplate.from_messages(
            [
                SystemMessagePromptTemplate.from_template(system_prompt),
                HumanMessagePromptTemplate.from_template(human_prompt),
            ]
        )

        formatted_prompt = prompt.format_prompt()  # <-- returns ChatPromptValue
        response = self.llm.invoke(formatted_prompt.to_messages())  # <-- to_messages() returns list of BaseMessage
        return str(response.content)

    def execute_query(self, query: str) -> str:
        """Send a single prompt through the connection and return raw LLM output.
        @param query  A single string prompt to send to the LLM.
        @return Raw LLM response as a string."""
        return self.execute_full_query(self.system_prompt, query)

    def execute_file(self, filename: str) -> str:  # type: ignore[override]
        """Run a single prompt from a file.
        @details  Reads the entire file as a single string and sends it to execute_query.
        @param filename  Path to the prompt file (.txt)
        @return  Raw LLM response as a string."""
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
        return self.execute_query(content)


@staticmethod
def normalize_to_dict(data: Union[Dict[str, Any], List[Dict[str, Any]]], keys: List[str]) -> List[Dict[str, Any]]:
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


@staticmethod
def normalize_triples(data: Any) -> List[Tuple[str, str, str]]:
    """Normalize flexible LLM output into clean (subject, relation, object) triples.
    @details
        Handles token-saving patterns from LLMs:
        - Nested relation-object pairs
        - List subjects/objects for efficiency
        - Cartesian products of s/r/o lists
        Uses normalize_to_dict for structure parsing, then sanitizes values.
        Accepts JSON strings or pre-parsed structures.
    @param data  Raw LLM output (JSON string, dict, or list)
    @return  List of sanitized (s, r, o) tuples ready for Neo4j
    @throws ValueError  If format cannot be parsed
    """
    
    def _sanitize_node(value: Any) -> str:
        """Clean node name for Cypher safety.
        @details
            - Joins lists/tuples into single string
            - Replaces invalid characters with underscores
            - Trims leading/trailing underscores
        @param value  Raw subject/object value
        @return  Sanitized string suitable for node property
        """
        if isinstance(value, (list, tuple)):
            value = " ".join(map(str, value))
        elif not isinstance(value, str):
            value = str(value)
        return re.sub(r"[^A-Za-z0-9_ ]", "_", value).strip("_ ")
    
    def _sanitize_rel(value: Any) -> str:
        """Clean and normalize relation label.
        @details
            - Converts to UPPERCASE for Neo4j convention
            - Replaces invalid characters with underscores
            - Falls back to RELATED_TO if empty or invalid
        @param value  Raw relation value
        @return  Uppercase, underscore-safe relation label
        """
        if isinstance(value, (list, tuple)):
            value = " ".join(map(str, value))
        elif not isinstance(value, str):
            value = str(value)
        rel = re.sub(r"[^A-Za-z0-9_]", "_", value.upper()).strip("_")
        if not rel or not rel[0].isalpha():
            rel = "RELATED_TO"
        return rel
    
    # Parse JSON if needed
    if isinstance(data, str):
        data = json.loads(data)
    
    # Normalize to list of dicts
    dicts = normalize_to_dict(data, ["s", "r", "o"])
    
    # Sanitize and filter
    triples: List[Tuple[str, str, str]] = []
    for d in dicts:
        s = _sanitize_node(d["s"])
        r = _sanitize_rel(d["r"])
        o = _sanitize_node(d["o"])
        if all([s, r, o]):
            triples.append((s, r, o))
    
    return triples
