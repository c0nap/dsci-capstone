from components.connectors import DatabaseConnector
from src.util import Log
from typing import List
from pandas import DataFrame
from neomodel import config, db
import os
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
			return pd.DataFrame(results, columns=[m[0] for m in meta])
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
