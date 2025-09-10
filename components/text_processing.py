from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import re
import os
from dotenv import load_dotenv
import spacy

nlp = spacy.blank("en")  # blank English model, no pipeline
sentencizer = nlp.add_pipe("sentencizer")

## Read environment variables at runtime
load_dotenv(".env")

class RelationExtractor:
	def __init__(self, model_name="Babelscape/rebel-large", max_tokens=1024):
		os.environ["HF_HUB_TOKEN"] = os.getenv("HF_HUB_TOKEN")
		self.tokenizer = AutoTokenizer.from_pretrained(model_name)
		self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
		self.max_tokens = max_tokens
		self.tuple_delim = "  "

	def extract(self, text :str, parse_tuples: bool = False):
		# Split into sentences: RE models generally output 1 relation per input.
		text = text.replace("\n", " ").strip()
		doc = nlp(text)
		sentences = [sent.text for sent in doc.sents]

		# Perform RE on each sentence individually
		out = []
		for sentence in sentences:
			inputs = self.tokenizer(sentence, return_tensors="pt", truncation=True, max_length=self.max_tokens)
			outputs = self.model.generate(**inputs)
			decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
			if parse_tuples:
				parts = [str(element).strip() for element in decoded.split(self.tuple_delim)]
				# group 3 at a time using zip
				for subj, obj, rel in zip(parts[0::3], parts[1::3], parts[2::3]):
					out.append(tuple([subj, rel, obj]))
			else:   # raw REBEL text: 'subj  obj  rel'
				out.append(decoded)
		return out


# === Example usage ===
if __name__ == "__main__":
	sample_text = "Alice met Bob in the forest. Bob then went to the village."
	extractor = RelationExtractor(model_name = "Babelscape/rebel-large")
	print(extractor.extract(sample_text))











from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from components.connectors import Connector

class LLMConnector(Connector):
	"""
	Connector for prompting and returning LLM output (raw text/JSON) via LangChain.
	Minimal base class: execute_query and execute_file abstract the prompt process.
	"""
	def __init__(self, temperature: float = 0, system_prompt: str = "You are a helpful assistant."):
		"""
		Initialize connector. Model name and temperature can be overridden by .env values.
		"""
		self.model_name = None
		self.temperature = temperature
		self.system_prompt = system_prompt
		self.llm = None
		self.configure()

	def configure(self):
		"""
		Initialize the LangChain LLM using environment credentials.
		Reads:
			- OPENAI_API_KEY from .env for authentication
			- LLM_MODEL and LLM_TEMPERATURE to override defaults
		"""
		# Read API key
		os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY", "")

		# Override model and temperature if set in .env
		self.model_name = os.getenv("LLM_MODEL")

		# Initialize LangChain LLM
		self.llm = ChatOpenAI(model_name=self.model_name, temperature=self.temperature)

	def test_connection(self):
		"""
		Send a trivial prompt to verify LLM connectivity.
		Returns True if successful.
		"""
		result = self.execute_full_query("You are a helpful assistant.", query)
		return result.strip() == "pong"


	def execute_full_query(self, system_prompt: str, human_prompt: str) -> str:
		"""
		Send a single prompt to the LLM with separate system and human instructions.
		"""
		self.system_prompt = system_prompt
	
		# Build prompt template
		prompt = ChatPromptTemplate.from_messages([
			SystemMessagePromptTemplate.from_template(system_prompt),
			HumanMessagePromptTemplate.from_template(human_prompt)
		])
	
		formatted_prompt = prompt.format_prompt()  # <-- returns ChatPromptValue
		response = self.llm.invoke(formatted_prompt.to_messages())  # <-- to_messages() returns list of BaseMessage
		return response.content



	def execute_query(self, query: str) -> str:
		"""
		Send a single prompt through the connection and return raw LLM output.
		
		Args:
			query: A single string prompt to send to the LLM.
		
		Returns:
			Raw LLM response (str)
		"""
		return self.execute_full_query(self.system_prompt, query)


	def execute_file(self, filename: str) -> str:
		"""
		Run a prompt from a file. Reads the entire file as a single string
		and sends it to execute_query.
	
		Args:
			filename: Path to the prompt file (.txt)
	
		Returns:
			Raw LLM response as a string.
		"""
		with open(filename, "r", encoding="utf-8") as f:
			content = f.read()
		return self.execute_query(content)





