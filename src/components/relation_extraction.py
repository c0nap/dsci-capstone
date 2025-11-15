from dotenv import load_dotenv
import os
import spacy
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


class RelationExtractor:
    def __init__(self, model_name="Babelscape/rebel-large", max_tokens=1024):
    	self.nlp = spacy.blank("en")  # blank English model, no pipeline
		self.sentencizer = self.nlp.add_pipe("sentencizer")
        # Read environment variables at runtime
		load_dotenv(".env")
        os.environ["HF_HUB_TOKEN"] = os.environ["HF_HUB_TOKEN"]
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        self.max_tokens = max_tokens
        self.tuple_delim = "  "

    def extract(self, text: str, parse_tuples: bool = False):
        # Split into sentences: RE models generally output 1 relation per input.
        text = text.replace("\n", " ").strip()
        doc = self.nlp(text)
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