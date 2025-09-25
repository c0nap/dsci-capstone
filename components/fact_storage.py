from components.connectors import DatabaseConnector
from src.util import Log
from typing import List
from pandas import DataFrame
from neomodel import config, db
import os
import re
import pandas as pd

class GraphConnector(DatabaseConnector):
	"""Connector for Neo4j (graph database).
	Uses neomodel to abstract some operations, but raw CQL is required for many tasks.
	Neo4j does not support multiple logical databases in community edition,
	so we emulate them using a `database_id` property on nodes.
	"""

	def __init__(self, verbose=False):
		"""Creates a new Neo4j connector. Use @ref from_env instead.
		@param verbose  Whether to print success and failure messages.
		@param specific_queries  A list of helpful CQL queries for testing.
		"""
		super().__init__(verbose)
		super().configure("NEO4J", database_name = "default", route_database = False)

		# Connect neomodel
		config.DATABASE_URL = self.connection_string

		# TODO: Relic from RelationalConnector: some queries cannot be accomplished via NeoModel?
		self._specific_queries = [
			"RETURN 1;",                           # Basic health check
			"MATCH (n) RETURN count(n) AS nodes;"  # Count of all nodes
		]

	def test_connection(self, print_results=False) -> bool:
		"""Establish a basic connection to the Neo4j database.
		@param print_results  Whether to display the retrieved test DataFrames
		@return  Whether the connection test was successful.
		"""
		try:
			# Run the hard-coded queries
			for q in self._specific_queries:
				df = self.execute_query(q)
				if print_results: print(df)

			if self.verbose: Log.connect_success("NEO4J")
			return True
		except Exception as e:
			if self.verbose: Log.connect_fail(self.connection_string)
			print(e)
			return False

	def execute_query(self, query: str) -> DataFrame:
		"""Send a single Cypher query to Neo4j.
		If a result is returned, it will be converted to a DataFrame.
		"""
		super().execute_query(query)
		try:
			results, meta = db.cypher_query(query)
			if not results:
				return None
			return pd.DataFrame(results, columns=[m for m in meta])
		except Exception as e:
			if self.verbose: Log.fail(f"Failed query on {self.connection_string}")
			raise

	def _split_combined(self, multi_query: str) -> List[str]:
		"""Splits combined CQL queries by semicolons."""
		return [q.strip() for q in multi_query.split(";") if q.strip()]

	def get_dataframe(self, label: str) -> DataFrame:
		"""Return all nodes of a given label, filtered by database_id."""
		query = f"""
		MATCH (n:{label})
		WHERE n.database_id = '{self.database_name}'
		RETURN n
		"""
		return self.execute_query(query)

	def create_database(self, database_name: str):
		"""Create a pseudo-database by clearing nodes with the same database_id."""
		super().create_database(database_name)
		self.drop_database(database_name)
		if self.verbose: Log.success_manage_db(database_name, "Initialized (pseudo)")

	def drop_database(self, database_name: str = ""):
		"""Delete all nodes with the given database_id."""
		super().drop_database(database_name)
		if not database_name:
			database_name = self.database_name
		query = f"MATCH (n) WHERE n.database_id = '{database_name}' DETACH DELETE n"
		self.execute_query(query)
		if self.verbose: Log.success_manage_db(database_name, "Dropped (pseudo)")


	def add_triple(self, subject: str, relation: str, object_: str):
		"""Add a semantic triple to the graph using raw Cypher.
	
		1. Finds nodes by exact match on `name` and `database_id`.
		2. Creates a relationship between them with the given label.
		"""

		# Keep only letters, numbers, underscores
		relation = re.sub(r'[^A-Za-z0-9_]', '_', relation)
		subject  = re.sub(r'[^A-Za-z0-9_]', '_', subject)
		object_   = re.sub(r'[^A-Za-z0-9_]', '_', object_)

		query = f"""
		MERGE (from_node {{name: '{subject}', database_id: '{self.database_name}'}})
		MERGE (to_node {{name: '{object_}', database_id: '{self.database_name}'}})
		MERGE (from_node)-[r:{relation}]->(to_node)
		RETURN from_node, r, to_node
		"""
	
		try:
			df = self.execute_query(query)
			if self.verbose:
				print(f"Added triple: ({subject})-[:{relation}]->({object_})")
			return df
		except Exception as e:
			if self.verbose:
				print(f"Failed to add triple: ({subject})-[:{relation}]->({object_})")
			raise

	def get_edge_counts(self, top_n: int = 10) -> DataFrame:
		"""Return node names and their edge counts, ordered by edge count descending.
		
		Args:
			top_n: Number of top nodes to return (by edge count). Default is 10.
			
		Returns:
			DataFrame with columns: node_name, edge_count
		"""
		query = f"""
		MATCH (n)
		WHERE n.database_id = '{self.database_name}'
		OPTIONAL MATCH (n)-[r]-()
		WITH n.name as node_name, count(r) as edge_count
		ORDER BY edge_count DESC, rand()
		LIMIT {top_n}
		RETURN node_name, edge_count
		"""
		return self.execute_query(query)


	def get_all_triples(self) -> pd.DataFrame:
		"""Return all triples in the current pseudo-database as a pandas DataFrame."""
		db_id = self.database_name
	
		query = f"""
		MATCH (a {{database_id: '{db_id}'}})-[r]->(b {{database_id: '{db_id}'}})
		RETURN a.name AS subject, type(r) AS relation, b.name AS object
		"""
	
		df = self.execute_query(query)
		# Ensure we always return a DataFrame with the 3 desired columns
		if df is None or df.empty:
			df = pd.DataFrame(columns=["subject", "relation", "object"])
		else:
			# Rename columns safely
			df = df.rename(columns={df.columns[0]: "subject",
									df.columns[1]: "relation",
									df.columns[2]: "object"})
		return df
	
	
	def print_triples(self, max_rows: int = 20, max_col_width: int = 50):
		"""Print all nodes and edges in the current pseudo-database with row/column formatting."""
		triples_df = self.get_all_triples()
	
		# Set pandas display options temporarily
		with pd.option_context('display.max_rows', max_rows,
							   'display.max_colwidth', max_col_width):
			print(f"Graph triples ({len(triples_df)} total):")
			print(triples_df)



