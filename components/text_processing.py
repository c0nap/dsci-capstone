from dotenv import load_dotenv
import os
import re
import spacy
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


nlp = spacy.blank("en")  # blank English model, no pipeline
sentencizer = nlp.add_pipe("sentencizer")

# Read environment variables at runtime
load_dotenv(".env")


class RelationExtractor:
    def __init__(self, model_name="Babelscape/rebel-large", max_tokens=1024):
        os.environ["HF_HUB_TOKEN"] = os.environ["HF_HUB_TOKEN"]
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        self.max_tokens = max_tokens
        self.tuple_delim = "  "

    def extract(self, text: str, parse_tuples: bool = False):
        # Split into sentences: RE models generally output 1 relation per input.
        text = text.replace("\n", " ").strip()
        doc = nlp(text)
        sentences = [sent.text for sent in doc.sents]

        # Perform RE on each sentence individually
        out = []
        for sentence in sentences:
            inputs = self.tokenizer(
                sentence,
                return_tensors="pt",
                truncation=True,
                max_length=self.max_tokens,
            )
            outputs = self.model.generate(**inputs)
            decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            if parse_tuples:
                parts = [str(element).strip() for element in decoded.split(self.tuple_delim)]
                # group 3 at a time using zip
                for subj, obj, rel in zip(parts[0::3], parts[1::3], parts[2::3]):
                    out.append(tuple([subj, rel, obj]))
            else:  # raw REBEL text: 'subj  obj  rel'
                out.append(decoded)
        return out


from components.connectors import Connector
from langchain_core.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from langchain_openai import ChatOpenAI
from typing import Any, List, Tuple


class LLMConnector(Connector):
    """Connector for prompting and returning LLM output (raw text/JSON) via LangChain.
    @note  The method @ref components.text_processing.LLMConnector.execute_query simplifies the prompt process.
    """

    # TODO: we may want various models with different configurations

    def __init__(
        self,
        temperature: float = 0,
        system_prompt: str = "You are a helpful assistant.",
    ):
        """Initialize the connector.
        @note  Model name is specified in the .env file."""
        self.model_name = None
        self.temperature = temperature
        self.system_prompt = system_prompt
        self.llm = None
        self.configure()

    def configure(self):
        """Initialize the LangChain LLM using environment credentials.
        @details
            Reads:
                - OPENAI_API_KEY from .env for authentication
                - LLM_MODEL and LLM_TEMPERATURE to override defaults"""
        self.model_name = os.environ["LLM_MODEL"]
        self.llm = ChatOpenAI(model_name=self.model_name, temperature=self.temperature)

    def test_connection(self):
        """Send a trivial prompt to verify LLM connectivity.
        @return  Whether the prompt executed successfully."""
        result = self.execute_full_query("You are a helpful assistant.", query)
        return result.strip() == "pong"
        # TODO

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws Log.Failure  If raise_error is True and the connection test fails to complete."""
        result = self.execute_full_query("You are a helpful assistant.", query)
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
        return response.content

    def execute_query(self, query: str) -> str:
        """Send a single prompt through the connection and return raw LLM output.
        @param query  A single string prompt to send to the LLM.
        @return Raw LLM response as a string."""
        return self.execute_full_query(self.system_prompt, query)

    def execute_file(self, filename: str) -> str:
        """Run a single prompt from a file.
        @details  Reads the entire file as a single string and sends it to execute_query.
        @param filename  Path to the prompt file (.txt)
        @return  Raw LLM response as a string."""
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
        return self.execute_query(content)

    # TODO: Generalize this - normalize_to_dict(keys=["s","r","o"])
    @staticmethod
    def normalize_triples(data: Any) -> List[Tuple[str, str, str]]:
        """Normalize flexible LLM output into a list of clean (subject, relation, object) triples.
        @details
            - Accepts dicts, lists of dicts, tuples, or dicts-of-lists.
            - Joins list values, trims, and sanitizes for Cypher safety.
            - Enforces uppercase underscore-safe relation labels.
        @param data  Raw LLM output to normalize.
        @return  List of sanitized (s, r, o) triples ready for insertion.
        @throws ValueError  If input format cannot be parsed.
        """

        def _sanitize_node(value: Any) -> str:
            """Clean a node name for Cypher safety.
            @param value  Raw subject/object value.
            @return  Sanitized string suitable for node property.
            """
            if isinstance(value, (list, tuple)):  # Join list/tuple into single string
                value = " ".join(map(str, value))
            elif not isinstance(value, str):  # Convert non-str types
                value = str(value)
            # Replace invalid chars, trim edges
            return re.sub(r"[^A-Za-z0-9_ ]", "_", value).strip("_ ")

        def _sanitize_rel(value: Any) -> str:
            """Clean and normalize a relation label.
            @param value  Raw relation value.
            @return  Uppercase, underscore-safe relation label.
            """
            if isinstance(value, (list, tuple)):  # Join list/tuple into one label
                value = " ".join(map(str, value))
            elif not isinstance(value, str):
                value = str(value)
            rel = re.sub(r"[^A-Za-z0-9_]", "_", value.upper()).strip("_")
            # Fallback if empty or invalid start char
            if not rel or not rel[0].isalpha():
                rel = "RELATED_TO"
            return rel

        def _as_list(x: Any) -> List[Any]:
            """Ensure value is returned as a list.
            @param x  Any input type.
            @return  List wrapping the input if needed.
            """
            return list(x) if isinstance(x, (list, tuple)) else [x]

        def _extract(data: Any) -> List[Tuple[str, str, str]]:
            """Extract raw triples from any supported LLM format.
            @param data  Raw triple input (dict, list, etc.).
            @return  List of unprocessed (s, r, o) tuples.
            """
            # List of dicts [{s,r,o}, ...]
            if isinstance(data, list) and data and isinstance(data[0], dict):
                return [
                    (d.get("s") or d.get("subject"), d.get("r") or d.get("relation"), d.get("o") or d.get("object") or d.get("object_")) for d in data
                ]
            # Single dict (scalars or lists)
            if isinstance(data, dict):
                s = data.get("s") or data.get("subject")
                r = data.get("r") or data.get("relation")
                o = data.get("o") or data.get("object") or data.get("object_")
                S, R, O = _as_list(s), _as_list(r), _as_list(o)
                # Expand 1-element lists to match longest list length
                n = max(len(S), len(R), len(O))
                if len(S) == 1 and n > 1:
                    S *= n
                if len(R) == 1 and n > 1:
                    R *= n
                if len(O) == 1 and n > 1:
                    O *= n
                m = min(len(S), len(R), len(O))
                return list(zip(S[:m], R[:m], O[:m]))
            # Single list/tuple triple
            if isinstance(data, (list, tuple)) and len(data) == 3 and not isinstance(data[0], dict):
                return [tuple(data)]
            raise ValueError("Unrecognized triple format")

        # Extract and sanitize all triples
        raw_triples = _extract(data)
        clean_triples = []
        for s, r, o in raw_triples:
            s_clean, r_clean, o_clean = _sanitize_node(s), _sanitize_rel(r), _sanitize_node(o)
            # Only include fully valid triples
            if all([s_clean, r_clean, o_clean]):
                clean_triples.append((s_clean, r_clean, o_clean))
        return clean_triples
