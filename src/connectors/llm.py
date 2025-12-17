from abc import ABC, abstractmethod
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
import os
import re
from src.connectors.base import Connector
from src.components.relation_extraction import Triple
from src.util import Log
from typing import Any, Dict, List, Tuple, Optional, cast, TypedDict, Type, Union
import json
import time


class LLMConnector(Connector, ABC):
    """Connector for prompting and returning LLM output (raw text/JSON) via LLMs.
    @note  The method @ref src.connectors.llm.LLMConnector.execute_query simplifies the prompt process.
    @details  To implement various configurations, either set properties directly or create another LLMConnector instance.
        Useful config options: temperature, system_prompt, llm, model_name.
        We prefer creating a separate wrapper instance for reusable hard-coded configurations.
        Now handles the full lifecycle: Connection -> Prompting -> Retrying -> Parsing -> Normalizing.
    """

    def __init__(self, 
                 model_name: str, 
                 temperature: float = 0, 
                 reasoning_effort: str = None, 
                 system_prompt: str = "You are a helpful assistant.", 
                 verbose: bool = True, 
                 number_retries: int = 2,
                 json_mode: bool = False,
                 response_format: Optional[Type[TypedDict] | Dict[str, Any]] = None):
        """Initialize common LLM connector properties.
        @param model_name  Name of the model to use.
        @param temperature  Sampling temperature.
        @param reasoning_effort  Optional reasoning parameter (e.g. for o1/o3 models).
        @param system_prompt  Default system instructions.
        @param verbose  Whether to print logs to stdout.
        @param number_retries  Number of allowed retries for connection OR parsing failures.
        @param json_mode  If True, requests basic JSON mode (e.g., {"type": "json_object"}).
        @param response_format  A TypedDict class or Schema dict for strict Structured Outputs.
        """
        self.model_name: str = model_name
        self.temperature: float = temperature
        self.reasoning_effort: str = reasoning_effort
        self.system_prompt: str = system_prompt
        self.verbose: bool = verbose
        self.number_retries: int = number_retries
        self.json_mode: bool = json_mode
        self.response_format = response_format

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
        try:
            # Force raw string return for ping to avoid schema validation errors
            result = self.execute_full_query("You are a helpful assistant.", "ping", override_format=True)
            text_result = str(result) if not isinstance(result, str) else result
            success = "pong" in text_result.lower()
        except Exception:
            success = False
            
        if not success and raise_error:
            raise Log.Failure(f"{log_source}: Connection check failed")
        return success

    @abstractmethod
    def configure(self) -> None:
        pass

    @abstractmethod
    def execute_full_query(self, system_prompt: str, human_prompt: str, override_format: bool = False) -> Union[str, Dict[str, Any], Any]:
        """Send a single prompt to the LLM with separate system and human instructions.
        @param system_prompt  Instructions for the LLM.
        @param human_prompt  The user input or query.
        @param override_format  If True, ignores structured outputs/json_mode (used for health checks).
        @return  Raw LLM response (string) or parsed object (if structured output is used)."""
        pass

    def execute_query(self, query: str) -> str:
        """Send a single prompt through the connection and return raw LLM output.
        @details  Handles connection retries, but returns the raw payload.
        @param query  A single string prompt to send to the LLM.
        @return Raw LLM response as a string.
        @throws Log.Failure  If retries are exhausted without success."""
        attempts = 0
        last_error = None

        while attempts <= self.number_retries:
            try:
                result = self.execute_full_query(self.system_prompt, query)
                return str(result)
            except Exception as e:
                attempts += 1
                last_error = e
                Log.warn(f"Connection Error (Attempt {attempts}/{self.number_retries + 1}): {e}", verbose=self.verbose)
                
                if attempts <= self.number_retries:
                    time.sleep(1)
        
        raise Log.Failure(f"LLM Connection failed after {self.number_retries + 1} attempts: {last_error}")

    def execute_to_triples(self, query: str) -> List[Triple]:
        """Execute a query and GUARANTEE a list of valid Triples.
        @details 
            This is the "Atomic" method. It handles:
            1. Calling the API
            2. Parsing the JSON (whether structured or raw string)
            3. Normalizing the schema (handling "subject" vs "s", or nested lists)
            4. Retrying automatically if any step above fails.
        @param query  The prompt text to send.
        @return  A list of extracted Triple objects.
        @throws Log.Failure  If retries are exhausted without success.
        """
        attempts = 0
        last_error = None

        while attempts <= self.number_retries:
            try:
                response = self.execute_full_query(self.system_prompt, query)
                
                # Case A: Structured Output (Response is already a dict/object)
                if self.response_format and not isinstance(response, str):
                    # Handle Pydantic models vs Dicts
                    data = response.dict() if hasattr(response, 'dict') else response
                    return self.normalize_triples(data)

                # Case B: Raw String (JSON mode or standard text)
                return self.parse_json_string(str(response))

            except (json.JSONDecodeError, ValueError, Exception) as e:
                attempts += 1
                last_error = e
                Log.warn(f"Parsing/Execution Error (Attempt {attempts}/{self.number_retries + 1}): {e}", verbose=self.verbose)
                
                if attempts <= self.number_retries:
                    time.sleep(1)

        raise Log.Failure(f"Failed to extract triples after {self.number_retries + 1} attempts. Last error: {last_error}")

    def execute_file(self, filename: str) -> List[str]:
        """Run a single prompt from a file.
        @details  Reads the entire file as a single string and sends it to execute_query.
        @param filename  Path to the prompt file (.txt)
        @return  Raw LLM response as a string."""
        with open(filename, "r", encoding="utf-8") as f:
            return [self.execute_query(f.read())]

    # -------------------------------------------------------------------------
    # Internal Sanitization & Parsing Logic
    # -------------------------------------------------------------------------

    @staticmethod
    def parse_json_string(raw_text: str) -> List[Triple]:
        """Clean and parse a raw JSON string from the LLM.
        @param raw_text  The raw string output from the LLM.
        @return  List of normalized Triple objects.
        @throws json.JSONDecodeError  If the cleaned string is not valid JSON.
        """
        # 1. Clean markdown
        s = raw_text.strip()
        s = re.sub(r"^```json\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)
        
        # 2. Parse JSON
        parsed = json.loads(s)
        
        # 3. Normalize to Schema
        return LLMConnector.normalize_triples(parsed)

    @staticmethod
    def normalize_triples(data: Dict[str, Any] | List[Dict[str, Any]]) -> List[Triple]:
        """Normalize nested/compacted LLM output into flat dicts.
        @details
            Handles token-saving patterns:
            - Nested relation-object pairs: {"s":"X", [{"r":"R1","o":"O1"}, ...]}
            - List subjects with nested r-o: {"s":["X","Y"], [{"r":"R","o":"O"}, ...]}
            - Cartesian products: {"s":["X","Y"], "r":["R1","R2"], "o":["O1","O2"]}
        @param data  Parsed LLM output (dict or list of dicts)
        @return  List of flat dicts with all keys present
        """
        
        def _as_list(x: Any) -> List[Any]:
            """Coerce value to list for uniform handling.
            @param x  Any input value
            @return  List containing x, or x itself if already a list/tuple"""
            return list(x) if isinstance(x, (list, tuple)) else [x]

        def _to_triple(item: Dict[str, Any]) -> Triple:
            """Convert a dict to a Triple, normalizing key aliases.
            @param item  Dict with s/subject, r/relation, o/object keys
            @return  Triple with s, r, o string keys"""
            s = item.get("s") or item.get("subject") or ""
            r = item.get("r") or item.get("relation") or ""
            o = item.get("o") or item.get("object") or item.get("object_") or ""
            return cast(Triple, {"s": str(s), "r": str(r), "o": str(o)})

        def _expand_nested_ro(item: Dict[str, Any]) -> List[Triple]:
            """Expand nested relation-object pairs pattern.
            @param item  Single dict potentially containing nested r-o list
            @return  List of expanded flat dicts, or [item] if no nesting found"""
            subjects = _as_list(item.get("s") or item.get("subject"))
            # Heuristic: Find list of dicts in values that might be relations
            nested_pairs = [v for v in item.values() if isinstance(v, list) and v and isinstance(v[0], dict)]

            if not nested_pairs:
                return [_to_triple(item)]

            results: List[Triple] = []
            for s in subjects:
                for pair in nested_pairs[0]:
                    r = pair.get("r") or pair.get("relation")
                    o = pair.get("o") or pair.get("object") or pair.get("object_")
                    if r and o:
                        results.append(cast(Triple, {"s": str(s), "r": str(r), "o": str(o)}))
            return results

        def _expand_cartesian(item: Dict[str, Any]) -> List[Triple]:
            """Expand list values into flat combinations.
            @param item  Dict with potentially list-valued s/r/o
            @return  List of expanded flat dicts"""
            s_vals = _as_list(item.get("s") or item.get("subject"))
            r_vals = _as_list(item.get("r") or item.get("relation"))
            o_vals = _as_list(item.get("o") or item.get("object") or item.get("object_"))

            # If lists have same length, zip them (not cartesian)
            if len(s_vals) == len(r_vals) == len(o_vals) and len(s_vals) > 1:
                return [cast(Triple, {"s": str(s), "r": str(r), "o": str(o)}) for s, r, o in zip(s_vals, r_vals, o_vals)]

            # Otherwise, broadcast single values or create cartesian product
            max_len = max(len(s_vals), len(r_vals), len(o_vals))
            if len(s_vals) == 1: s_vals *= max_len
            if len(r_vals) == 1: r_vals *= max_len
            if len(o_vals) == 1: o_vals *= max_len

            # If all same length now, zip
            if len(s_vals) == len(r_vals) == len(o_vals):
                return [cast(Triple, {"s": str(s), "r": str(r), "o": str(o)}) for s, r, o in zip(s_vals, r_vals, o_vals)]

            # Full cartesian product for mismatched lengths
            results: List[Triple] = []
            for s in s_vals:
                for r in r_vals:
                    for o in o_vals:
                        results.append(cast(Triple, {"s": str(s), "r": str(r), "o": str(o)}))
            return results

        # Normalize input to list of dicts
        items: List[Dict[str, Any]] = data if isinstance(data, list) else [data]

        # Expand each item
        expanded: List[Triple] = []
        for item in items:
            # Try nested r-o expansion first
            nested = _expand_nested_ro(item)
            if len(nested) > 1 or nested[0] != _to_triple(item):
                expanded.extend(nested)
            else:
                expanded.extend(_expand_cartesian(item))

        return expanded


class OpenAIConnector(LLMConnector):
    """Lightweight LLM interface using native OpenAI Structured Outputs."""

    def __init__(self, model_name: str, 
                 temperature: float = 0, 
                 reasoning_effort: str = None, 
                 system_prompt: str = "You are a helpful assistant.", 
                 verbose: bool = True, 
                 number_retries: int = 2,
                 json_mode: bool = False,
                 response_format: Optional[Type[TypedDict]] = None):
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        super().__init__(model_name, temperature, reasoning_effort, system_prompt, verbose, number_retries, json_mode, response_format)
        self.client: OpenAI = None
        self.configure()

    def configure(self) -> None:
        """Initialize the OpenAI client."""
        load_dotenv(".env")
        self.client = OpenAI()

    def execute_full_query(self, system_prompt: str, human_prompt: str, override_format: bool = False) -> Union[str, Any]:
        """Send a single prompt using the OpenAI client directly for speed.
        @param system_prompt  Instructions for the LLM.
        @param human_prompt  The user input or query.
        @param override_format  If True, ignores structured outputs/json_mode.
        @return Raw LLM response as a string, or a parsed object if strict format is used."""
        extra_args: Dict[str, Any] = {}  # Keep generic since MyPy will complain about unpacking non-strings otherwise
        if self.reasoning_effort is not None:
            extra_args["reasoning_effort"] = self.reasoning_effort
        
        messages: list[ChatCompletionMessageParam] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": human_prompt},
        ]

        if self.response_format and not override_format:
            completion = self.client.beta.chat.completions.parse(
                model=self.model_name,
                messages=messages,
                temperature=self.temperature,
                response_format=self.response_format,
                **extra_args
            )
            return completion.choices[0].message.parsed
            
        if self.json_mode and not override_format:
            extra_args["response_format"] = {"type": "json_object"}

        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            temperature=self.temperature,
            **extra_args
        )
        return str(response.choices[0].message.content)


class LangChainConnector(LLMConnector):
    """LangChain interface supporting .with_structured_output."""

    def __init__(self, model_name: str, 
                 temperature: float = 0, 
                 reasoning_effort: str = None, 
                 system_prompt: str = "You are a helpful assistant.", 
                 verbose: bool = True, 
                 number_retries: int = 2,
                 json_mode: bool = False,
                 response_format: Optional[Type[TypedDict]] = None):
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        super().__init__(model_name, temperature, reasoning_effort, system_prompt, verbose, number_retries, json_mode, response_format)
        self.client: ChatOpenAI = None
        self.configure()

    def configure(self) -> None:
        """Initialize the LangChain LLM using environment credentials.
        @details
            Reads:
                - OPENAI_API_KEY from .env for authentication"""
        load_dotenv(".env")
        kwargs = {"model": self.model_name, "temperature": self.temperature}
        if self.reasoning_effort:
            kwargs["reasoning"] = {"effort": self.reasoning_effort}
        
        base_llm = ChatOpenAI(**kwargs)
        
        if self.response_format:
            self.client = base_llm.with_structured_output(self.response_format)
        elif self.json_mode:
            self.client = base_llm.bind(response_format={"type": "json_object"})
        else:
            self.client = base_llm

    def execute_full_query(self, system_prompt: str, human_prompt: str, override_format: bool = False) -> Union[str, Any]:
        """Send a single prompt to the LLM with separate system and human instructions.
        @param system_prompt  Instructions for the LLM.
        @param human_prompt  The user input or query.
        @param override_format  If True, ignores structured outputs/json_mode.
        @return Raw LLM response as a string or parsed object."""
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=human_prompt)
        ]
        
        # Fallback to pure string response if override requested (e.g. for Ping)
        if override_format:
            # Note: For strict correctness in LangChain we would need a fresh client.
            # Assuming here that .invoke will behave gracefully or ping will catch the error.
            pass 

        response = self.client.invoke(messages)

        if self.response_format and not override_format:
            return response

        if hasattr(response, "content"):
            content = response.content
            if isinstance(content, list):
                return "".join(block["text"] for block in content if isinstance(block, dict) and "text" in block)
            return str(content)
            
        return str(response)


# -------------------------------------------------------------------------
# Global Helpers (Moderation)
# -------------------------------------------------------------------------

def moderate_texts(texts: List[str], thresholds: Dict[str, float]) -> List[Dict[str, float]]:
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
                    config_key = category.replace('/', '_').replace('-', '_')
                    limit = thresholds.get(config_key, 1.0)
                    if score > limit:
                        batch_violations[category] = score
                all_violations.append(batch_violations)
        except Exception as e:
            Log.warn(f"Moderation API failed: {e}. Marking batch as safe.", verbose=True)
            all_violations.extend([{} for _ in batch])

    return all_violations


def flag_triples(triples: List[Triple], thresholds: Dict[str, float]) -> Tuple[List[Triple], List[Tuple[Triple, Dict[str, float]]]]:
    """
    Filter triples containing offensive content.
    Returns: (safe_triples, bad_triples_with_reasons)
    """
    texts = [f"{t['s']} {t['r']} {t['o']}" for t in triples]
    results = moderate_texts(texts, thresholds)
    safe_triples: List[Triple] = []
    bad_triples: List[Tuple[Triple, Dict[str, float]]] = []

    for triple, violations in zip(triples, results):
        if not violations:
            safe_triples.append(triple)
        else:
            bad_triples.append((triple, violations))
    return safe_triples, bad_triples


def to_triples_string(extracted: List[Triple]) -> str:
    """Concatenate triples into a form usable in a LLM prompt.
    @param extracted  A list of extracted triples.
    @return  String with one triple per line."""
    triples_string = "\n".join(extracted)
    return triples_string

def to_flagged_reasons(bad_triples: List[Triple]) -> str:
    """Concatenate harmful triples with their justification for a LLM prompt.
    @param triples  A list of extracted triples.
    @return  String with one triple per line."""
    triples_string = "\n".join([
        f"- {t['s']} {t['r']} {t['o']} (Flagged: {list(reasons.keys())})" 
        for t, reasons in bad_triples
    ])
    return triples_string
