from dotenv import load_dotenv
import os
from typing import List, Tuple
from abc import ABC, abstractmethod


class RelationExtractor(ABC):


class RelationExtractorREBEL(RelationExtractor)
    def __init__(self, model_name="Babelscape/rebel-large", max_tokens=1024):
        import spacy
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        self.nlp = spacy.blank("en")  # blank English model, no pipeline
        self.sentencizer = self.nlp.add_pipe("sentencizer")
        # Read environment variables at runtime
        load_dotenv(".env")
        os.environ["HF_HUB_TOKEN"] = os.environ["HF_HUB_TOKEN"]
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        self.max_tokens = max_tokens
        self.tuple_delim = "  "

    def extract(self, text: str, parse_tuples: bool = False) -> List[Tuple[str, str, str] | str]:
        # Split into sentences: RE models generally output 1 relation per input.
        text = text.replace("\n", " ").strip()
        doc = self.nlp(text)
        sentences = [sent.text for sent in doc.sents]

        # Perform RE on each sentence individually
        out: List[Tuple[str, str, str] | str] = []
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
                    out.append((subj, rel, obj))
            else:  # raw REBEL text: 'subj  obj  rel'
                out.append(str(decoded))
        return out


class RelationExtractorOpenIE:
    def __init__(self, memory='4G'):
        # 1. Lazy Imports: Only happen when you instantiate THIS class
        import stanza
        from stanza.server import CoreNLPClient

        # 2. Attach to self so other methods can use them
        self.stanza = stanza
        self.CoreNLPClient = CoreNLPClient

        # 3. Download CoreNLP backend if not present (Automatic Setup)
        # This saves the jar files to ~/stanza_corenlp by default
        print("Ensuring CoreNLP backend is installed...")
        stanza.install_corenlp()
        
        self.memory = memory

    def _get_client(self):
        # Configuration for "Exhaustive" and "Coref-Resolved" extraction
        # openie.resolve_coref: Uses the coref graph to replace pronouns in triples
        # openie.triple.strict: False allows for more loose/exhaustive extractions
        properties = {
            'openie.resolve_coref': True,
            'openie.triple.strict': False,
            'openie.max_entailments_per_clause': 500
        }
        
        # Use self.CoreNLPClient
        return self.CoreNLPClient(
            annotators=['tokenize', 'ssplit', 'pos', 'lemma', 'ner', 'parse', 'coref', 'openie'],
            properties=properties,
            timeout=30000,
            memory=self.memory,
            be_quiet=False
        )

    def extract(self, text: str) -> list[tuple[str, str, str]]:
        text = text.replace("\n", " ").strip()
        out = []

        # We use a context manager to ensure the server spins up/down cleanly
        # For production/batch processing, you might want to keep the client alive longer
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
        return out


        
        
# --- Usage Example ---
if __name__ == "__main__":
    extractor = OpenIERelationExtractor()
    
    # Narrative example with Coref requirement
    text = "Harry picked up his wand. He pointed it at the door and whispered a spell."
    
    triples = extractor.extract(text)
    
    print(f"Input: {text}\n")
    print("Extracted Triples:")
    for subj, rel, obj in triples:
        print(f"  ({subj}) -> [{rel}] -> ({obj})")




class TextacyExtractor:
    def __init__(self):
        import spacy
        import textacy

        self.nlp = spacy.load("en_core_web_sm")
    
    def extract(self, text):
        doc = self.nlp(text)
        triples = []
        # Extract SVO (Subject-Verb-Object)
        for svo in textacy.extract.subject_verb_object_triples(doc):
            triples.append((svo.subject, svo.verb, svo.object))
        return triples

