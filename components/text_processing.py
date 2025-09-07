from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import re
import os
from dotenv import load_dotenv

## Read environment variables at runtime
load_dotenv(".env")

class RelationExtractor:
	def __init__(self, model_name="Babelscape/rebel-large", max_tokens=1024):
		os.environ["HF_HUB_TOKEN"] = os.getenv("HF_HUB_TOKEN")
		self.tokenizer = AutoTokenizer.from_pretrained(model_name)
		self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
		self.max_tokens = max_tokens

	def extract(self, chunk, parse_tuples: bool = False):
		text = chunk.text.replace("\n", " ").strip()
		inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=self.max_tokens)
		outputs = self.model.generate(**inputs)
		decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
		if not parse_tuples:
			return decoded  # raw REBEL text: 'subj[rel]obj'
		pattern = r'(.+?)\[(.+?)\](.+?)'
		return [tuple(m.groups()) for m in re.finditer(pattern, decoded)]










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
		result = self.execute_query("You are a helpful assistant.", query)
		return result.strip() == "pong"


	def execute_query(self, system_prompt: str, human_prompt: str) -> str:
		"""
		Send a single prompt to the LLM with separate system and human instructions.
	
		Args:
			system_prompt: Instructions to set the LLM's behavior.
			human_prompt: The actual prompt/query content.
	
		Returns:
			Raw LLM response as a string.
		"""
		self.system_prompt = system_prompt
		prompt = ChatPromptTemplate.from_messages([
			SystemMessagePromptTemplate.from_template(system_prompt),
			HumanMessagePromptTemplate.from_template(human_prompt)
		])
		response = self.llm(prompt.format())
		return response.content


	def execute_query(self, query: str) -> str:
		"""
		Send a single prompt through the connection and return raw LLM output.
		
		Args:
			query: A single string prompt to send to the LLM.
		
		Returns:
			Raw LLM response (str)
		"""
		return self.execute_query(self.system_prompt, query)


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





