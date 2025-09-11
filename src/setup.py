import os
from dotenv import load_dotenv
from src.util import all_none
from components.connectors import RelationalConnector
from components.fact_storage import GraphConnector

## Read environment variables at compile time
load_dotenv(".env")


class Session:
	"""Stores active database connections and configuration settings.
	This class implements Singleton design - Only one session can be created.
	However, the session config can still be updated using the normal constructor."""
	_instance = None
	def __new__(cls, *args, **kwargs):
		"""Creates a new session at first access, otherwise uses the existing session."""
		if cls._instance is None:
			cls._instance = super().__new__(cls)
		return cls._instance

	def __init__(self):
		"""Initializes the session using the .env file."""
		## The relational database connector is created using a Factory Method, choosing mysql or postgres based on the .env file.
		## Stores RDF-compliant semantic triples.
		self.relational_db = RelationalConnector.from_env(verbose=True)
		## The document database connector is created normally since mongo is the only supported option.
		## Stores input text, pre-processed chunks, JSON intermediates, and final output.
		#self.docs_db = DocumentConnector(verbose=False)
		## The graph database connector is created normally since neo4j is the only supported option.
		## Main storage for entities (nodes) and relations (edges).
		self.graph_db = GraphConnector(verbose=True)
		# TODO: Do not interact with these directly, provide them to EAV Model and Knowledge Graph classes
		self.setup()

	def setup(self):
		""" TODO: refactor to database_connectors """
		## Test connection to default database "mysql" on the MySQL engine
		default_database = self.relational_db._default_database
		self.relational_db.change_database(default_database)
		self.relational_db.test_connection()
		print()
		## Test connection to working database ".env/DB_NAME" on the MySQL engine
		working_database = os.getenv("DB_NAME")
		self.relational_db.change_database(working_database)
		already_exists = self.relational_db.test_connection(print_results=True)
		## Ensures the working database was created
		if not already_exists:
			self.relational_db.change_database(default_database)
			self.relational_db.create_database(working_database)
			self.relational_db.change_database(working_database)
			self.relational_db.test_connection(print_results=True)
		self.setup1()

	def setup1(self):
		"""Sanity check for Neo4j GraphConnector."""
		## Test connection to default "database" (really just a database_id)
		default_database = "default"  # Neo4j community doesn’t have real DBs
		self.graph_db.change_database(default_database)
		self.graph_db.test_connection()
		print()
	
		## Test connection to working database ".env/DB_NAME" (stored as database_id)
		working_database = os.getenv("DB_NAME")
		self.graph_db.change_database(working_database)
		already_exists = self.graph_db.test_connection(print_results=True)
	
		## Ensures the working database was created (pseudo)
		if not already_exists:
			self.graph_db.change_database(default_database)
			self.graph_db.create_database(working_database)
			self.graph_db.change_database(working_database)
			self.graph_db.test_connection(print_results=True)
	
		## Test database management explicitly
		self.graph_db.drop_database(working_database)
		self.graph_db.create_database(working_database)
	
		## Test query execution
		self.graph_db.execute_query(f"""
		CREATE (n:Person {{name:'Alice', database_id:'{working_database}'}})
		RETURN n
		""")
		self.graph_db.execute_query(f"""
		CREATE (n:Person {{name:'Bob', database_id:'{working_database}'}})
		RETURN n
		""")
	
		## Test retrieving nodes as DataFrame
		df = self.graph_db.get_dataframe("Person")
		print("Retrieved nodes:\n", df)
	
		## Test file execution (if you have a .cql file handy)
		# self.graph_db.execute_file("queries/example.cql")
	
		## Cleanup
		self.graph_db.drop_database(working_database)


	def reset(self):
		"""Deletes all created databases and tables."""
		# TODO



session = Session()
print()

