from components.connectors import DatabaseConnector
from src.util import Log
import os
from time import time
import json
from pandas import DataFrame, json_normalize
from typing import List, Optional, Dict
from dotenv import load_dotenv

import mongoengine
from mongoengine import (
    Document,
    DynamicDocument,
)

# Read environment variables at compile time
load_dotenv(".env")


class DocumentConnector(DatabaseConnector):
    """Connector for MongoDB (document database)
    @details
        - Uses mongoengine.connect(...) on-demand for connections.
        - Low-level operations use pymongo via mongoengine.get_db().
        - create_database uses an init collection insertion (MongoDB is lazy).
    """

    def __init__(self, verbose: bool = False):
        """Creates a new MongoDB connector.
        @param verbose  Whether to print debug messages.
        """
        super().__init__(verbose)
        self._route_db_name = True
        """@brief  Whether to use the database name in the connection string.
        @note  mongoengine.connect is used on-demand; we keep the convention of routing the DB name."""
        database = os.getenv("DB_NAME")
        super().configure("MONGO", database)



    def test_connection(self, raise_error=True) -> bool:
        """Establish a basic connection to the MongoDB database.
        @details  By default, Log.fail will raise an exception.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            # Check if connection string is valid
            if self.check_connection(Log.test_conn, raise_error) == False:
                return False

        except Exception as e:
            Log.fail(Log.doc_db + Log.test_conn, Log.msg_unknown_error, raise_error, e)
            return False
        # Finish with no errors = connection test successful
        if self.verbose:
            Log.success(Log.doc_db, Log.msg_db_connect(self.database_name))
        return True

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @details  Connect to MongoDB using MongoEnigine.connect()
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            mongoengine.connect(host=self.connection_string)
        except Exception as e:
            Log.fail(Log.doc_db + log_source + Log.bad_addr, Log.msg_bad_addr(self.connection_string), raise_error, e)
            return False
        if self.verbose:
            Log.success(Log.doc_db + log_source, Log.msg_db_connect(self.database_name))
        return True

    


    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single MongoDB command using PyMongo.
        @details
          - The query must be a valid JSON command object (e.g. {"find": "users", "filter": {...}}).
          - Mongo shell syntax such as `db.users.find({...})` or `.js` files will NOT work.
          - If a result is returned, it will be converted to a DataFrame.
        """
        self.check_connection(Log.run_q, raise_error=True)
        super().execute_query(query)
    
        try:
            # Connect to MongoDB using the low-level PyMongo handle
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()

            # Queries must be valid JSON
            try:
                cmd_obj = json.loads(query)
            except json.JSONDecodeError:
                Log.fail(Log.doc_db + Log.run_q, Log.msg_fail_parse("query", "JSON command object", query), raise_error=True, other_error=e)
    
            # Execute via PyMongo
            results = db.command(cmd_obj)
    
            # Mongo queries can return a dict or list
            # Standardize everything to a list of documents
            docs = []
            if isinstance(results, dict):
                if "cursor" in results:
                    docs = results["cursor"].get("firstBatch", [])
                elif "firstBatch" in results:
                    docs = results["firstBatch"]
                else:
                    # wrap single dict as list
                    docs = [results]
            elif isinstance(results, list):
                docs = results
            
            # Convert document list to DataFrame if any docs exist
            result = self._docs_to_df(docs)
            if self.verbose:
                Log.success(Log.doc_db + Log.run_q, Log.msg_good_exec_q(query, result))
            return result
        except Exception as e:
            Log.fail(Log.doc_db + Log.run_q, Log.msg_bad_exec_q(query), raise_error=True, other_error=e)


    def _split_combined(self, multi_query: str) -> list[str]:
        """Divides a string into non-divisible MongoDB commands, ignoring comments and semicolons inside JSON.
        @details
        Example Input:
            {"ping": 1}; {"aggregate": "users", "pipeline": [...]};
        Output:
            One command per string:
            - '{"ping": 1}'
            - '{"aggregate": "users", "pipeline": [...]}'
        @param multi_query  A string containing multiple queries.
        @return  A list of single-query strings."""
        queries = []
        buffer = ""
        depth = 0
        for line in multi_query.splitlines():
            line = line.strip()
            # Skip comments
            if not line or line.startswith("#") or line.startswith("//"):
                continue
            for c in line:
                buffer += c
                # Unpack nested brackets
                if c in "{[":
                    depth += 1
                elif c in "}]":
                    depth = max(0, depth - 1)
                elif c == ";" and depth == 0:
                    queries.append(buffer.strip().rstrip(";"))
                    buffer = ""
            if depth > 0:
                buffer += " "  # keep spacing between lines
        if buffer.strip():
            queries.append(buffer.strip().rstrip(";"))
        return queries


    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified collection.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None"""
        super().get_dataframe(name)
        self.check_connection(Log.get_df, raise_error=True)
        try:
            # Connect to MongoDB using the low-level PyMongo handle
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()
            # Results will be a list of documents
            docs = list(db[name].find({}))
            df = self._docs_to_df(docs)

            if self.verbose:
                Log.success(Log.doc_db + Log.get_df, Log.msg_good_coll(name))
            return df
        except Exception as e:
            Log.fail(Log.doc_db + Log.get_df, Log.msg_unknown_error, raise_error=True, other_error=e)
        # If not found, warn but do not fail
        Log.fail(Log.doc_db + Log.get_df, Log.msg_bad_coll(name), raise_error=False)
        return None


    def create_database(self, database_name: str):
        """Use the current database connection to create a sibling database in this engine.
        @note  Forces MongoDB to actually create it by inserting a small init document.
        @param database_name  The name of the new database to create."""
        super().create_database(database_name)
        self.check_connection(Log.create_db, raise_error=True)
        try:
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()
            # Create the database by adding dummy data
            if "init" not in db.list_collection_names():
                db.create_collection("init")
            db["init"].insert_one({"initialized_at": time()})

            if self.verbose:
                Log.success(Log.doc_db + Log.create_db, Log.msg_success_managed_db("created", database_name))
        except Exception as e:
            Log.fail(Log.doc_db + Log.create_db, Log.msg_fail_manage_db(self.connection_string, database_name, "create"), raise_error=True, other_error=e)


    def drop_database(self, database_name: str):
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database."""
        super().drop_database(database_name)
        self.check_connection(Log.drop_db, raise_error=True)
        try:
            mongoengine.connect(host=self.connection_string)
            db = mongoengine.get_db()
            # Drop the entire database
            db.client.drop_database(database_name)

            if self.verbose:
                Log.success(Log.doc_db + Log.create_db, Log.msg_success_managed_db("dropped", database_name))
        except Exception as e:
            Log.fail(Log.doc_db + Log.create_db, Log.msg_fail_manage_db(self.connection_string, database_name, "drop"), raise_error=True, other_error=e)


    # Reuse the dataframe parsing logic
    def _docs_to_df(self, docs: List[Dict]) -> DataFrame:
        """Convert raw MongoDB documents to a Pandas DataFrame.
        @details
        	- Will explode / flatten nested dicts only if json_normalize() is successful
            - Different approach than GraphConnector because our documents usually contain nesting.
        Steps:
          1. Convert ObjectId fields (_id) to strings so Pandas can handle them.
          2. Flatten nested JSON structures using Pandas.json_normalize.
        Example input:
          docs = [ {"_id": ObjectId("650f..."), "name": "Alice", "age": 30}, ...} ]
        Example output:
          DataFrame([
              {"_id": "650f...", "name": "Alice", "age": 30, "address.city": None, "address.zip": None},
              {"_id": "650f...", "name": "Bob", "age": None, "address.city": "NY", "address.zip": "10001"}
        ])"""
        # 1. Convert MongoDB ObjectId fields to strings
        for document in docs:
            if "_id" in document:
                try:
                    document["_id"] = str(document["_id"])
                except Exception:
                    # Fail if str() raises - probably corrupted data
                    Log.fail_parse(trace="_id field", expected_type="str", bad_value=document["_id"])
    
        # 2. Use Pandas to normalize nested JSON into flat columns
        try:
            return json_normalize(docs)
        except Exception:
            # Fallback: create a DataFrame directly if normalization fails
            # Pandas DataFrames can salvage messy nesting, but json_normalize requires all docs to be balanced dicts
            return DataFrame(docs)


