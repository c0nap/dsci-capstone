from abc import ABC, abstractmethod
from dotenv import load_dotenv
import os
from typing import Any, List, Optional, TYPE_CHECKING, TypedDict


# Forward references for lazy-loaded modules
if TYPE_CHECKING:
    import spacy
    import spacy.language
    import transformers


class Triple(TypedDict):
    s: str
    r: str
    o: str


class RelationExtractor(ABC):
    """Abstract base class for Relation Extraction (RE) models.
    @details
    Derived classes must implement extract() to return a list of Triple dictionaries.
    Backends (Spacy, Stanza, Transformers) are lazy-loaded to avoid memory for unused models.
    """

    @abstractmethod
    def extract(self, text: str, parse_tuples: bool = True) -> List[Triple]:
        """Extract relations from the provided text.
        @param text  The raw input text to process.
        @param parse_tuples  Retained for API compatibility; extraction always returns structured Triples.
        @return  A list of Triple dictionaries {'s': ..., 'r': ..., 'o': ...}.
        """
        pass


class RelationExtractorREBEL(RelationExtractor):
    """Relation Extractor using the REBEL generative model (Seq2Seq).
    @note  Requires 'torch', 'transformers', and 'spacy' installed.
    @details
        REBEL treats RE as a translation task (Text -> Triples).
        It is powerful but can hallucinate or normalize entities (non-literal).
    """

    def __init__(self, model_name: str = "Babelscape/rebel-large", max_tokens: int = 1024) -> None:
        """Initialize the REBEL config.
        @note  Imports and model loading are deferred to the first extract() call.
        @param model_name  The HuggingFace hub path for the model.
        @param max_tokens  The maximum sequence length for the tokenizer.
        """
        self.model_name = model_name
        self.max_tokens = max_tokens

        # Internal delimiter used by the model for splitting generated text
        self._model_delim = " "

        # Placeholders for lazy loading
        self.nlp: Optional[spacy.language.Language] = None
        self.tokenizer: Optional[transformers.PreTrainedTokenizer] = None
        self.model: Any = None  # AutoModelForSeq2SeqLM.from_pretrained() return type - internal factory messes up typing

    def extract(self, text: str, parse_tuples: bool = True) -> List[Triple]:
        """Perform extraction on the text using the generative model.
        @details
            The text is first segmented into sentences because RE models degrade
            significantly in performance on long, multi-sentence paragraphs.
        @param text  The input narrative text.
        @param parse_tuples  Unused (Always parses to Triples).
        @return  A list of extracted relations.
        """
        # 1. Lazy Imports & Setup (Run once)
        if self.model is None or self.nlp is None:
            import spacy
            from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

            # Setup Spacy for basic sentence segmentation
            self.nlp = spacy.blank("en")
            self.nlp.add_pipe("sentencizer")

            # Load Model
            load_dotenv(".env")
            print(f"Loading REBEL model: {self.model_name}...")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForSeq2SeqLM.from_pretrained(self.model_name)

        # Split into sentences: RE models generally output 1 relation set per input sequence.
        # Cleaning newlines prevents tokenization artifacts.
        text = text.replace("\n", " ").strip()
        doc = self.nlp(text)
        sentences = [sent.text for sent in doc.sents]

        out: List[Triple] = []

        # Perform RE on each sentence individually
        for sentence in sentences:
            inputs = self.tokenizer(
                sentence,
                return_tensors="pt",
                truncation=True,
                max_length=self.max_tokens,
            )

            # Generate the linearized triples
            outputs = self.model.generate(**inputs)
            decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)

            # REBEL output format is specific; we split by the internal model delimiter
            parts = [str(element).strip() for element in decoded.split(self._model_delim)]

            # group 3 at a time using zip to form (subj, obj, rel)
            # Note: REBEL outputs Subj, Obj, Rel order in its raw decoding
            for subj, obj, rel in zip(parts[0::3], parts[1::3], parts[2::3]):
                # Filter out empty strings or malformed triples
                if subj and obj and rel:
                    out.append({'s': subj, 'r': rel, 'o': obj})

        return out


class RelationExtractorOpenIE(RelationExtractor):
    """Wrapper for Stanford OpenIE using the Stanza library.
    @note  Requires Java (JDK 8+) and 'stanza' python package.
    @details
        Ideal for "Exhaustive" and "Literal" extraction. Unlike generative models,
        this extracts spans directly from the text and handles coreference resolution internally.
    """

    def __init__(self, memory: str = '4G', timeout: float = 120) -> None:
        """Initialize the Stanza CoreNLP configuration.
        @details
            Configuration targets "Exhaustive" and "Coref-Resolved" extraction.
        @param memory  Java heap size string (e.g., '4G', '8G').
        @param timeout  Timeout for the Java server response in seconds.
        """
        self.timeout = timeout
        self.memory = memory

        # Pre-configure properties so they are ready for the context manager
        self.client_config = {
            'annotators': ['tokenize', 'ssplit', 'pos', 'lemma', 'ner', 'parse', 'coref', 'openie'],
            'properties': {
                'openie.resolve_coref': True,
                'openie.triple.strict': False,
            },
            'timeout': 1000 * timeout,
            'memory': memory,
            'be_quiet': True,
        }

    def extract(self, text: str, parse_tuples: bool = True) -> List[Triple]:
        """Extract triples using the Stanford OpenIE pipeline.
        @details
            Uses a context manager to spin up the Java server via CoreNLPClient.
            This ensures the heavy Java process (which requires ~4GB RAM) is
            terminated immediately after processing, freeing resources.
        @param text  The raw narrative text.
        @param parse_tuples  Unused (Always parses to Triples).
        @return  A list of extracted relations.
        """
        # Lazy Import
        import stanza
        from stanza.server import CoreNLPClient

        # Ensure CoreNLP backend is installed (Run once check)
        # This saves the jar files to ~/stanza_corenlp by default
        install_dir = os.path.expanduser("~/stanza_corenlp")
        if not os.path.exists(install_dir):
            print("Ensuring CoreNLP backend is installed...")
            stanza.install_corenlp()

        text = text.replace("\n", " ").strip()
        out: List[Triple] = []

        # We use a context manager to ensure the Java server is cleanly started / stopped.
        with CoreNLPClient(**self.client_config) as client:
            doc = client.annotate(text)

            # Iterate through sentences and their extracted triples
            for sentence in doc.sentence:
                for triple in sentence.openieTriple:
                    # We create a TypedDict for easy consumption
                    out.append({'s': triple.subject, 'r': triple.relation, 'o': triple.object})

        return out


class RelationExtractorTextacy(RelationExtractor):
    """Lightweight extraction using Spacy and Textacy (SVO).
    @note  Requires 'spacy' and 'textacy'.
    @details
        A pure-Python backup. Less exhaustive than OpenIE but faster and setup-free.
        Extracts Subject-Verb-Object patterns based on dependency parsing.
    """

    def __init__(self) -> None:
        """Initialize config only.
        @note  Spacy model loading is deferred to extract().
        """
        self.nlp: Optional[spacy.language.Language] = None
        self.model_name: str = "en_core_web_sm"

    def extract(self, text: str, parse_tuples: bool = True) -> List[Triple]:
        """Extract SVO triples.
        @param text  The raw input text.
        @param parse_tuples  Unused (Always parses to Triples).
        @return  A list of extracted relations.
        """
        # Lazy Imports
        import spacy
        import textacy

        # Load Model on first run
        if self.nlp is None:
            # Auto-download if missing (Self-healing)
            try:
                self.nlp = spacy.load(self.model_name)
            except OSError:
                print(f"Spacy model '{self.model_name}' not found. Downloading...")
                spacy.cli.download(self.model_name)  # type: ignore[attr-defined]
                self.nlp = spacy.load(self.model_name)

        doc = self.nlp(text)
        out: List[Triple] = []

        # Extract SVO (Subject-Verb-Object)
        # Textacy triples use token lists instead of strings ["Alberts", "brother"] vs "Alberts brother", so we must join them.
        for svo in textacy.extract.subject_verb_object_triples(doc):
            subj = " ".join([t.text for t in svo.subject])
            verb = " ".join([t.text for t in svo.verb])
            obj = " ".join([t.text for t in svo.object])
            out.append({'s': subj, 'r': verb, 'o': obj})

        return out
