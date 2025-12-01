from dotenv import load_dotenv
import os
from typing import List, Tuple, Union, Optional
from abc import ABC, abstractmethod

class RelationExtractor(ABC):
    """Abstract base class for Relation Extraction (RE) models.
    @details
        Derived classes must implement the extract method to process text
        and return a list of triples or raw strings.
        Backends (Spacy, Stanza, Transformers) should be lazy-loaded.
    """
    # Unified delimiter for "raw" string representation
    TUPLE_DELIM = "  "

    @abstractmethod
    def extract(self, text: str, parse_tuples: bool = True) -> List[Union[Tuple[str, str, str], str]]:
        """Extract relations from the provided text.
        @param text  The raw input text to process.
        @param parse_tuples  If False, returns a formatted string 'Subj  Rel  Obj'.
        @return  A list of triples (subj, rel, obj) or raw string outputs.
        """
        pass

    def _format_triples_to_strings(self, triples: List[Tuple[str, str, str]]) -> List[str]:
        """Helper to convert native tuples to standardized raw strings."""
        return [f"{s}{self.TUPLE_DELIM}{r}{self.TUPLE_DELIM}{o}" for s, r, o in triples]


class RelationExtractorREBEL(RelationExtractor):
    """Relation Extractor using the REBEL generative model (Seq2Seq).
    @note  Requires 'torch', 'transformers', and 'spacy' installed.
    @details
        REBEL treats RE as a translation task (Text -> Triples).
        It is powerful but can hallucinate or normalize entities (non-literal).
    """

    def __init__(self, model_name="Babelscape/rebel-large", max_tokens=1024) -> None:
        """Initialize the REBEL model and tokenizer.
        @note  Lazy imports are used to prevent heavy libraries from loading unless this class is instantiated.
        @param model_name  The HuggingFace hub path for the model.
        @param max_tokens  The maximum sequence length for the tokenizer.
        """
        # 1. Lazy Imports
        import spacy
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        # 2. Setup Spacy for basic sentence segmentation (faster than Transformers for splitting)
        self.nlp = spacy.blank("en") 
        self.sentencizer = self.nlp.add_pipe("sentencizer")

        # 3. Load Environment and Model
        # Ensure HF_HUB_TOKEN is available for gated models if necessary
        load_dotenv(".env")
        
        print(f"Loading REBEL model: {model_name}...")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        
        self.max_tokens = max_tokens
        self.tuple_delim = " "

    def extract(self, text: str, parse_tuples: bool = False) -> List[Union[Tuple[str, str, str], str]]:
        """Perform extraction on the text using the generative model.
        @details 
            The text is first segmented into sentences because RE models degrade 
            in performance on long, multi-sentence paragraphs.
        @param text  The input narrative text.
        @param parse_tuples  If True, parses the generated string into structured tuples.
        @return  A list of extracted relations.
        """
        # Split into sentences: RE models generally output 1 relation set per input sequence.
        # Cleaning newlines prevents tokenization artifacts.
        text = text.replace("\n", " ").strip()
        doc = self.nlp(text)
        sentences = [sent.text for sent in doc.sents]

        out: List[Union[Tuple[str, str, str], str]] = []

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
            
            if parse_tuples:
                # REBEL output format is specific; we split by the delimiter
                parts = [str(element).strip() for element in decoded.split(self.tuple_delim)]
                # group 3 at a time using zip to form (subj, obj, rel)
                # Note: REBEL outputs Subj, Obj, Rel order in its raw decoding
                for subj, obj, rel in zip(parts[0::3], parts[1::3], parts[2::3]):
                    # Filter out empty strings or malformed triples
                    if subj and obj and rel:
                        out.append((subj, rel, obj))
            else:
                # Return raw REBEL text: 'subj  obj  rel'
                out.append(str(decoded))
                
        return out


class RelationExtractorOpenIE(RelationExtractor):
    """Wrapper for Stanford OpenIE using the Stanza library.
    @note  Requires Java (JDK 8+) and 'stanza' python package.
    @details
        Ideal for "Exhaustive" and "Literal" extraction. Unlike generative models,
        this extracts spans directly from the text and handles coreference resolution internally.
    """

    def __init__(self, memory='4G') -> None:
        """Initialize the Stanza CoreNLP client interface.
        @details 
            Checks for the existence of the CoreNLP backend and installs it if missing.
            This is a blocking operation on the first run.
        @param memory  Java heap size string (e.g., '4G', '8G').
        """
        # 1. Lazy Imports: Only happen when you instantiate THIS class
        import stanza
        from stanza.server import CoreNLPClient

        # 2. Attach to self so other methods can use them
        self.stanza = stanza
        self.CoreNLPClient = CoreNLPClient

        # 3. Download CoreNLP backend if not present (Automatic Setup)
        # This saves the jar files to ~/stanza_corenlp by default
        print("Ensuring CoreNLP backend is installed...")
        install_dir = os.path.expanduser("~/stanza_corenlp")
        if not os.path.exists(install_dir):
            print("Installing CoreNLP backend...")
            self.stanza.install_corenlp()
        
        self.memory = memory

    def _get_client(self) -> "CoreNLPClient":
        """Configure and instantiate the CoreNLP Client.
        @details
            Configuration targets "Exhaustive" and "Coref-Resolved" extraction:
            - openie.resolve_coref: Uses the coref graph to replace pronouns (He -> Harry).
            - openie.triple.strict: False allows for more loose/exhaustive extractions.
            - openie.max_entailments_per_clause: Maximizes variations of triples returned.
        @return  An instance of stanza.server.CoreNLPClient.
        """
        properties = {
            'openie.resolve_coref': True,
            'openie.triple.strict': False,
            'openie.max_entailments_per_clause': 500
        }
        
        # Use self.CoreNLPClient (lazy loaded)
        return self.CoreNLPClient(
            annotators=['tokenize', 'ssplit', 'pos', 'lemma', 'ner', 'parse', 'coref', 'openie'],
            properties=properties,
            timeout=30000,
            memory=self.memory,
            be_quiet=True # Set to False for debugging Java output
        )

    def extract(self, text: str, parse_tuples: bool = True) -> List[Union[Tuple[str, str, str], str]]:
        """Extract triples using the Stanford OpenIE pipeline.
        @details
            Uses a context manager to spin up the Java server, process the text, 
            and tear it down immediately to free resources.
            For production / batch processing, you might want to keep the client alive longer
        @param text  The raw narrative text.
        @param parse_tuples  If False, concatenates the triples into a multi-line string.
        @return  A list of extracted relations.
        """
        text = text.replace("\n", " ").strip()
        out = []

        # We use a context manager to ensure the server spins up/down cleanly
        with self._get_client() as client:
            # Submit annotation request
            ann = client.annotate(text)
            
            # Iterate through sentences and their extracted triples
            for sentence in ann.sentence:
                for triple in sentence.openieTriple:
                    # formatting: (Subject, Relation, Object)
                    # We create a tuple for easy consumption
                    t = (triple.subject, triple.relation, triple.object)
                    out.append(t)
        # Delegate to base helper if raw strings are requested
        return out if parse_tuples else self._format_triples_to_strings(out)


class RelationExtractorTextacy(RelationExtractor):
    """Lightweight extraction using Spacy and Textacy (SVO).
    @note  Requires 'spacy' and 'textacy'.
    @details
        A pure-Python backup. Less exhaustive than OpenIE but faster and setup-free.
        Extracts Subject-Verb-Object patterns based on dependency parsing.
    """

    def __init__(self) -> None:
        """Initialize Spacy model for dependency parsing.
        @note  Defaults to 'en_core_web_sm'. Ensure this model is downloaded via `python -m spacy download en_core_web_sm`.
        """
        import spacy
        import textacy
        from spacy.cli import download

        # Auto-download if missing (Self-healing)
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            print("Spacy model 'en_core_web_sm' not found. Downloading...")
            download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
        
        # Attach textacy to self to avoid import errors later
        self.textacy = textacy
        
    def extract(self, text: str, parse_tuples: bool = True) -> List[Union[Tuple[str, str, str], str]]:
        """Extract SVO triples.
        @param text  The raw input text.
        @param parse_tuples  If False, concatenates the triples into a multi-line string.
        @return  A list of extracted relations.
        """
        doc = self.nlp(text)
        out = []

        # Extract SVO (Subject-Verb-Object)
        # SVO components are lists of tokens (e.g., [The, big, dog]), so we must join them.
        for svo in self.textacy.extract.subject_verb_object_triples(doc):
            subj = " ".join([t.text for t in svo.subject])
            verb = " ".join([t.text for t in svo.verb])
            obj =  " ".join([t.text for t in svo.object])
            out.append((subj, verb, obj))
            
        # Delegate to base helper if raw strings are requested
        return out if parse_tuples else self._format_triples_to_strings(out)

