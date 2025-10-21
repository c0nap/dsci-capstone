from components.connectors import DatabaseConnector
from src.util import Log, check_values, df_natural_sorted
import os
from time import time
import json
from pandas import DataFrame, json_normalize
from typing import List, Optional, Dict, Set, Type
from dotenv import load_dotenv
from contextlib import contextmanager

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
        database = os.getenv("DB_NAME")
        super().configure("MONGO", database)

    def change_database(self, new_database: str):
        """Update the connection URI to reference a different database in the same engine.
        @note  Additional settings are appended as a suffix to the MongoDB connection string.
        @param new_database  The name of the database to connect to.
        """
        Log.success(Log.doc_db + Log.swap_db, Log.msg_swap_db(self.database_name, new_database), self.verbose)
        self.database_name = new_database
        self._auth_suffix = "?authSource=admin" + "&uuidRepresentation=standard"
        """@brief  Additional options appended to the connection string.
        @note  PyMongo requires a lookup location for user permissions, and MongoEngine will show warnings if 'uuidRepresentation' is not set."""
        self.connection_string = f"{self.db_engine}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}{self._auth_suffix}"


    def test_connection(self, raise_error=True) -> bool:
        """Establish a basic connection to the MongoDB database.
        @details  Can be configured to fail silently, which enables retries or external handling.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @raises RuntimeError  If raise_error is True and the connection test fails to complete.
        """
        # Check if connection string is valid
        if self.check_connection(Log.test_conn, raise_error) == False:
            return False
        
        with mongo_handle(host=self.connection_string, alias="test_conn") as db:
            try:    # Run universal test queries - some require admin
                result = db.command({"ping": 1})
                if check_values([result.get("ok")], [1.0], self.verbose, Log.doc_db, raise_error) == False:
                    return False
                status = db.command({"serverStatus": 1})
                if check_values([status.get("ok")], [1.0], self.verbose, Log.doc_db, raise_error) == False:
                    return False
                result = list(db.command({"listCollections": 1})["cursor"]["firstBatch"])
                if check_values([isinstance(result, list)], [True], self.verbose, Log.doc_db, raise_error) == False:
                    return False
            except Exception as e:
                if not raise_error: return False
                raise Log.Failure(Log.doc_db + Log.test_conn + Log.test_basic, Log.msg_unknown_error) from e
    
            try:   # Display useful information on existing databases
                databases = db.client.list_database_names()
                Log.success(Log.doc_db, Log.msg_result(databases), self.verbose)
            except Exception as e:
                if not raise_error: return False
                raise Log.Failure(Log.doc_db + Log.test_conn + Log.test_info, Log.msg_unknown_error) from e
    
            try:   # Create a collection, insert dummy data, and use get_dataframe
                tmp_collection = f"test_collection_{int(time())}"
                if tmp_collection in db.list_collection_names():
                    db.drop_collection(tmp_collection)
                db[tmp_collection].insert_one({"id": 1, "name": "Alice"})
                df = self.get_dataframe(tmp_collection)
                check_values([df.at[0, 'name']], ['Alice'], self.verbose, Log.doc_db, raise_error)
                db.drop_collection(tmp_collection)
            except Exception as e:
                if not raise_error: return False
                raise Log.Failure(Log.doc_db + Log.test_conn + Log.test_df, Log.msg_unknown_error) from e
    
            try:   # Test create/drop functionality with tmp database
                tmp_db = f"test_db_{int(time())}"
                working_database = self.database_name
                if self.database_exists(tmp_db):
                    self.drop_database(tmp_db)
                self.create_database(tmp_db)
                self.change_database(tmp_db)
                self.execute_query('{"ping": 1}')
                self.change_database(working_database)
                self.drop_database(tmp_db)
            except Exception as e:
                if not raise_error: return False
                raise Log.Failure(Log.doc_db + Log.test_conn + Log.test_tmp_db, Log.msg_unknown_error) from e
    
        # Finish with no errors = connection test successful
        Log.success(Log.doc_db, Log.msg_db_connect(self.database_name), self.verbose)
        return True
    
    
    

    def check_connection(self, log_source: str, raise_error: bool) -> bool:
        """Minimal connection test to determine if our connection string is valid.
        @details  Connect to MongoDB using MongoEnigine.connect()
        @param log_source  The Log class prefix indicating which method is performing the check.
        @param raise_error  Whether to raise an error on connection failure.
        @return  Whether the connection test was successful.
        @throws RuntimeError  If raise_error is True and the connection test fails to complete."""
        try:
            with mongo_handle(host=self.connection_string, alias="check_conn") as db:
                db.command({"ping": 1})
        except Exception:  # These errors are usually nasty, so dont print the original.
            if not raise_error: return False
            raise Log.Failure(Log.doc_db + log_source + Log.bad_addr, Log.msg_bad_addr(self.connection_string)) from None
        Log.success(Log.doc_db + log_source, Log.msg_db_connect(self.database_name), self.verbose)
        return True



    def execute_query(self, query: str) -> Optional[DataFrame]:
        """Send a single MongoDB command using PyMongo.
        @details
          - The query must be a valid JSON command object (e.g. {"find": "users", "filter": {...}}).
          - Mongo shell syntax such as `db.users.find({...})` or `.js` files will NOT work.
          - If a result is returned, it will be converted to a DataFrame.
        @raises RuntimeError  If the query fails to execute.
        """
        # The base class will handle the multi-query case, so prevent a 2nd duplicate query
        result = super().execute_query(query)
        if not self._is_single_query(query):
            return result
        # Derived classes MUST implement single-query execution.
        self.check_connection(Log.run_q, raise_error=True)
        try:
            with mongo_handle(host=self.connection_string, alias="exec_q") as db:
                # Queries must be valid JSON
                try:
                    json_cmd_doc = json.loads(query)
                except json.JSONDecodeError:
                    Log.warn(Log.doc_db + Log.run_q, Log.msg_fail_parse("query", query, "JSON command object"), self.verbose)
                    query = _sanitize_json(query)
                    try:
                        json_cmd_doc = json.loads(query)
                    except json.JSONDecodeError:
                        raise Log.Failure(Log.doc_db + Log.run_q, Log.msg_fail_parse("sanitized query", query, "JSON command object"))
        
                # Execute via PyMongo
                results = db.command(json_cmd_doc)
        
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
                df = _docs_to_df(docs)
                if df is None or df.empty:
                    Log.success(Log.doc_db + Log.run_q, Log.msg_good_exec_q(query), self.verbose)
                    return None
                else:
                    Log.success(Log.doc_db + Log.run_q, Log.msg_good_exec_qr(query, df), self.verbose)
                    return df
        except Exception as e:
            raise Log.Failure(Log.doc_db + Log.run_q, Log.msg_bad_exec_q(query)) from e



    def _split_combined(self, multi_query: str) -> list[str]:
        """Divides a string into non-divisible MongoDB commands by splitting on semicolons at depth 0.
        @details  Handles nested brackets and semicolons inside JSON strings.
        @param multi_query  A string containing multiple queries with possible comments.
        @return  A list of single-query strings (cleaned, ready for JSON parsing)."""
        queries = []
        buffer = ""
        depth = 0
        in_string = False
        escape_next = False
        
        # Remove all comments and normalize whitespace
        cleaned = _sanitize_json(multi_query)
        for c in cleaned:
            # Handle string escaping
            if escape_next:
                buffer += c
                escape_next = False
                continue
            if c == '\\' and in_string:
                buffer += c
                escape_next = True
                continue
            
            # Track whether we're inside a string
            if c == '"':
                in_string = not in_string
                buffer += c
                continue
            # Only track depth and semicolons outside of strings
            if not in_string:
                if c in "{[":
                    depth += 1
                    buffer += c
                elif c in "}]":
                    depth = max(0, depth - 1)
                    buffer += c
                elif c == ";" and depth == 0:
                    # End of query at top level
                    stripped = buffer.strip()
                    if stripped:
                        queries.append(stripped)
                    buffer = ""
                else:
                    buffer += c
            else:
                buffer += c
        
        # Append any remaining buffer
        stripped = buffer.strip()
        if stripped:
            queries.append(stripped)
        return queries


    def get_dataframe(self, name: str) -> Optional[DataFrame]:
        """Automatically generate and run a query for the specified collection.
        @param name  The name of an existing table or collection in the database.
        @return  DataFrame containing the requested data, or None
        @raises RuntimeError  If we fail to create the requested DataFrame for any reason."""
        self.check_connection(Log.get_df, raise_error=True)
        with mongo_handle(host=self.connection_string, alias="get_df") as db:
            # Results will be a list of documents
            docs = list(db[name].find({}))
            df = _docs_to_df(docs)
            df = df_natural_sorted(df, ignored_columns=['_id'])

            if df is not None and not df.empty:
                Log.success(Log.doc_db + Log.get_df, Log.msg_good_coll(name, df), self.verbose)
                return df
        # If not found, warn but do not fail
        Log.warn(Log.doc_db + Log.get_df, Log.msg_bad_coll(name), self.verbose)
        return None


    def create_database(self, database_name: str):
        """Use the current database connection to create a sibling database in this engine.
        @note  Forces MongoDB to actually create it by inserting a small init document.
        @param database_name  The name of the new database to create.
        @raises RuntimeError  If we fail to create the requested database for any reason."""
        super().create_database(database_name)  # Check if exists
        self.check_connection(Log.create_db, raise_error=True)
        working_database = self.database_name
        self.change_database(database_name)
        self.check_connection(Log.create_db, raise_error=True)
        try:
            with mongo_handle(host=self.connection_string, alias="create_db") as db:
                # Create the database by adding dummy data
                if "init" not in db.list_collection_names():
                    db.create_collection("init")
                db["init"].insert_one({"initialized_at": int(time())})
                db["init"].delete_many({})

                self.change_database(working_database)
                Log.success(Log.doc_db + Log.create_db, Log.msg_success_managed_db("created", database_name), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.doc_db + Log.create_db, Log.msg_fail_manage_db("create", database_name, self.connection_string)) from e


    def drop_database(self, database_name: str):
        """Delete all data stored in a particular database.
        @param database_name  The name of an existing database.
        @raises RuntimeError  If we fail to drop the target database for any reason."""
        super().drop_database(database_name)  # Check if exists
        self.check_connection(Log.drop_db, raise_error=True)
        try:
            with mongo_handle(host=self.connection_string, alias="drop_db") as db:
                # Drop the entire database
                db.client.drop_database(database_name)
                Log.success(Log.doc_db + Log.create_db, Log.msg_success_managed_db("dropped", database_name), self.verbose)
        except Exception as e:
            raise Log.Failure(Log.doc_db + Log.create_db, Log.msg_fail_manage_db("drop", database_name, self.connection_string)) from e


    def database_exists(self, database_name: str) -> bool:
        """Search for an existing database using the provided name.
        @param database_name  The name of a database to search for.
        @return  Whether the database is visible to this connector."""
        with mongo_handle(host=self.connection_string, alias="db_exists") as db:
            #result = self.execute_query('{"dbstats": 1}')
            #print(result)
            databases = db.client.list_database_names()
        #print(databases)
        return database_name in databases


    def delete_dummy(self):
        """Delete the initial dummy collection from the database.
        @note  Call this method whenever real data is being added to avoid pollution."""
        with mongo_handle(host=self.connection_string, alias="drop_dum") as db:
            if "init" in db.list_collection_names() and db.command("dbstats")["collections"] > 1:
                db["init"].drop()









@contextmanager
def mongo_handle(host: str, alias: str):
    """Establish a temporary connection to MongoDB.
    @param host  A valid MongoDB connection string.
    @param alias  A unique name for the usage of this connection.
    @details  Allows scoped access to the low-level PyMongo handle from MongoEngine.
    Usage:
        with mongo_handle(host=self.connection_string, alias="create_db") as db:
            (your code here...)
    This will disconnect all connections on the alias once finished.
    Helpful when test_connection wants to call execute_query, but continue using its existing db handle after execute_query disconnects.
    """
    mongoengine.connect(host=host, alias=alias)
    db = mongoengine.get_db(alias=alias)
    try:
        yield db  # <-- your code runs here
    finally:
        mongoengine.disconnect(alias=alias)



def _flatten_recursive(df: DataFrame) -> DataFrame:
    """Explode all list columns and flatten dict columns until only scalars remain.
    @details  Recursive Process:
        1. Find columns containing lists → explode to create new rows
        2. Find columns containing dicts → normalize to create new columns
        3. Repeat until no lists or dicts remain
    @param df  DataFrame with potentially nested structures.
    @return  Fully flattened DataFrame with only scalar values.
    """
    while True:
        # Find all columns that contain lists
        list_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, list)).any()]
        if list_cols:
            # Explode the first list column (creates new rows)
            col = list_cols[0]
            df = df.explode(col).reset_index(drop=True)
            continue

        # Find all columns that contain dicts
        dict_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, dict)).any()]
        if dict_cols:
            # Normalize the first dict column (creates new columns with parent prefix)
            col = dict_cols[0]
            normalized = json_normalize(df[col])
            # Prefix all new columns with the parent column name
            normalized.columns = [f"{col}.{subcol}" for subcol in normalized.columns]
            normalized.index = df.index
            df = df.drop(columns=[col]).join(normalized)
            continue

        # No more lists or dicts → fully flattened
        break
    return df


def _sanitize_json(text: str) -> str:
    """Remove comments and other non-JSON content from a MongoDB query string.
    @details  Removes the following elements:
        - Block comments /* ... */
        - Single-line comments //
        - Half-line comments ... //
        - Trailing commas before closing braces
        - Newlines and whitespace
    Preserves bad text inside JSON string values.
    @param text  Raw text that may contain comments.
    @return  Cleaned text suitable for JSON parsing."""
    result = []
    i = 0
    in_string = False
    last_was_space = False
    
    while i < len(text):
        c = text[i]

        # Handle backslash - check what it's escaping
        if c == '\\' and i < len(text) - 1:
            # Add the backslash and the next character
            result.append(c)
            result.append(text[i + 1])
            last_was_space = False
            i += 2
            continue
        # Track string boundaries (only unescaped quotes)
        if c == '"':
            in_string = not in_string
            result.append(c)
            last_was_space = False
            i += 1
            continue
        
        # Inside strings: preserve everything exactly
        if in_string:
            result.append(c)
            last_was_space = False
            i += 1
            continue

        # Outside strings: process comments and normalize whitespace
        # Check for block comment /* */
        if i < len(text) - 1 and text[i:i+2] == '/*':
            i += 2
            while i < len(text) - 1:
                if text[i:i+2] == '*/':
                    i += 2
                    break
                i += 1
            continue
        # Check for single-line comment //
        if i < len(text) - 1 and text[i:i+2] == '//':
            while i < len(text) and text[i] != '\n':
                i += 1
            if i < len(text):
                i += 1  # Skip the newline
            continue
        # Normalize whitespace outside strings
        if c in ' \t\n\r':
            # Collapse consecutive whitespace to single space
            if not last_was_space and result:  # Don't add leading spaces
                result.append(' ')
                last_was_space = True
            i += 1
            continue
        # Check for trailing comma before } or ]
        if c == ',':
            # Look ahead: skip whitespace to see if } or ] follows
            j = i + 1
            while j < len(text) and text[j] in ' \t\n\r':
                j += 1
            # If we find } or ], skip the comma (trailing comma)
            if j < len(text) and text[j] in '}]':
                i += 1
                continue
        
        # Regular character
        result.append(c)
        last_was_space = False
        i += 1
    
    # Strip trailing whitespace
    return ''.join(result).strip()









def _sanitize_document(doc: Dict, type_registry: Dict[str, Set[Type]]) -> Dict:
    """Normalize document fields to consistent types for DataFrame construction.
    @details  Converts all field values to lists and tracks type patterns.
              - ObjectId → string
              - Single value → [value]
              - Mixed types tracked in type_registry for conflict resolution
    @param doc  MongoDB document to sanitize.
    @param type_registry  Tracks observed types per field path (e.g., {"effects": {str, list}}).
    @return  Document with all fields as lists.
    """
    sanitized = {}
    
    for key, value in doc.items():
        # Convert ObjectId to string
        if key == "_id":
            try:
                sanitized[key] = [str(value)]
            except Exception:
                raise Log.Failure(Log.doc_db + "SANITIZE: ", Log.msg_fail_parse("_id field", value, "str"))
        else:
            # Track the original type before wrapping
            original_type = type(value)
            if key not in type_registry:
                type_registry[key] = set()
            type_registry[key].add(original_type)
            
            # Wrap everything as a list
            if value is None:
                sanitized[key] = []
            elif isinstance(value, list):
                sanitized[key] = value if value else []
            else:
                sanitized[key] = [value]
    
    return sanitized


def _docs_to_df(docs: List[Dict], merge_unspecified: bool = True) -> DataFrame:
    """Convert raw MongoDB documents to a Pandas DataFrame.
    @details  Handles schema inconsistencies by:
              1. First pass: identify all nested column names and their types
              2. Second pass: sanitize and wrap primitives using type-compatible nested columns
              3. Flatten structures into final DataFrame
    @param docs  List of MongoDB documents to convert.
    @param merge_unspecified  If True, merge primitives into type-compatible nested columns
                              using aggressive type casting (int→float, bool→int→float).
                              If False, keep as _unspecified_type columns.
    @raises RuntimeError  If parsing query results to JSON fails.
    """
    if not docs:
        return DataFrame()
    
    # First pass: discover nested columns and their value types
    nested_schema = {}  # Maps base field -> {nested_key: set of value types}
    for doc in docs:
        for key, value in doc.items():
            if isinstance(value, dict):
                if key not in nested_schema:
                    nested_schema[key] = {}
                for nested_key, nested_val in value.items():
                    if nested_key not in nested_schema[key]:
                        nested_schema[key][nested_key] = set()
                    nested_schema[key][nested_key].add(type(nested_val))
            elif isinstance(value, list) and value:
                for item in value:
                    if isinstance(item, dict):
                        if key not in nested_schema:
                            nested_schema[key] = {}
                        for nested_key, nested_val in item.items():
                            if nested_key not in nested_schema[key]:
                                nested_schema[key][nested_key] = set()
                            nested_schema[key][nested_key].add(type(nested_val))
    
    # Second pass: sanitize with type-aware column mapping
    type_registry = {}
    sanitized_docs = []
    
    for doc in docs:
        sanitized = {}
        for key, value in doc.items():
            # Convert ObjectId to string
            if key == "_id":
                try:
                    sanitized[key] = [str(value)]
                except Exception:
                    raise Log.Failure(Log.doc_db + "SANITIZE: ", Log.msg_fail_parse("_id field", value, "str"))
            else:
                # Track the original type
                original_type = type(value)
                if key not in type_registry:
                    type_registry[key] = set()
                type_registry[key].add(original_type)
                
                # Wrap values with type-aware nested column mapping
                if value is None:
                    sanitized[key] = []
                elif isinstance(value, list):
                    # If field has nested schema and list contains primitives, wrap in dicts
                    if key in nested_schema and value and not isinstance(value[0], dict):
                        target_key = _find_compatible_nested_key(
                            type(value[0]), nested_schema.get(key, {}), merge_unspecified
                        )
                        sanitized[key] = [{target_key: item} for item in value]
                    else:
                        sanitized[key] = value if value else []
                else:
                    # Single value: wrap in list, check for nested column mapping
                    if key in nested_schema and not isinstance(value, dict):
                        target_key = _find_compatible_nested_key(
                            type(value), nested_schema.get(key, {}), merge_unspecified
                        )
                        sanitized[key] = [{target_key: value}]
                    else:
                        sanitized[key] = [value]
        
        sanitized_docs.append(sanitized)
    
    # Create DataFrame and flatten
    df = DataFrame(sanitized_docs)
    df = _flatten_recursive(df)
    
    return df


def _find_compatible_nested_key(value_type: Type, nested_schema: Dict[str, Set[Type]], 
                                  merge_unspecified: bool) -> str:
    """Find a nested column compatible with the given primitive type.
    @details  Uses type compatibility hierarchy for aggressive merging:
              bool → int → float (numeric types)
              str (isolated, only matches str)
              Searches for exact match first, then compatible types.
    @param value_type  The type of the primitive value to map (e.g., str, int, float).
    @param nested_schema  Dict mapping nested keys to sets of observed types.
    @param merge_unspecified  Whether to attempt type-compatible merging.
    @return  The nested key name to use for wrapping the primitive.
    """
    if not merge_unspecified:
        return f"_unspecified_{value_type.__name__}"
    
    # Define type compatibility: value_type can be cast to these types
    type_compatibility = {
        int: [int, float],          # int can go to float
        float: [float],             # float only to float
        str: [str],                 # str only to str
        bool: [bool]                # bool only to bool
    }
    
    compatible_types = type_compatibility.get(value_type, [value_type])
    
    # Search for compatible columns in order of preference (exact match first)
    for target_type in compatible_types:
        for nested_key, observed_types in nested_schema.items():
            if target_type in observed_types:
                return nested_key
    
    # No compatible column found
    return f"_unspecified_{value_type.__name__}"