class Log:
    """The Log class standardizes console output."""

    ## Enable ANSI colors in output
    USE_COLORS = True
    ## ANSI code for green text
    GREEN = "\033[32m"
    ## ANSI code for red text
    RED = "\033[31m"
    ## ANSI code for yellow text
    YELLOW = "\033[33m"
    ## ANSI code for bright yellow / cream
    BRIGHT = "\033[93m"
    ## ANSI code to reset color
    WHITE = "\033[0m"

    ## ANSI color applied to the prefix of success messages
    SUCCESS_COLOR = GREEN
    ## ANSI color applied to the prefix of ignored fail messages
    WARNING_COLOR = YELLOW
    ## ANSI color applied to the prefix of critical fail messages
    FAILURE_COLOR = RED
    ## ANSI color applied to the body of every Log message
    MSG_COLOR = BRIGHT


    # These functions can be used to print the standard prefix
    # before your own print(), or a message can be specified.

    @staticmethod
    def success(prefix: str = "PASS", msg: str = ""):
        """A success message begins with a Green Plus.
        @param prefix  The context of the message.
        @param msg  The message to print."""
        text = f"{Log.SUCCESS_COLOR}{prefix}{Log.MSG_COLOR}{msg}{Log.WHITE}" if Log.USE_COLORS else f"{prefix}{msg}"
        print(text)

    @staticmethod
    def fail(prefix: str = "ERROR", msg: str = "", raise_error=True, other_error=None):
        """A failure message begins with a Red X.
        @param prefix  The context of the message.
        @param msg  The message to print.
        @param raise_error  Whether to raise an error.
        @param other_error  Another Exception resulting from this failure.
        @raises RuntimeError  If raise_error is True"""
        _FAIL_COLOR = Log.FAILURE_COLOR if raise_error else Log.WARNING_COLOR
        text = f"{_FAIL_COLOR}{prefix}{Log.MSG_COLOR}{msg}{Log.WHITE}" if Log.USE_COLORS else f"{prefix}{msg}"
        if raise_error:
            if isinstance(other_error, Exception):
                raise RuntimeError(text) from other_error
            else:
                raise RuntimeError(text)
        else:
            print(text)

    @staticmethod
    def success_legacy(msg: str = ""):
        """A legacy success message begins with a Green Plus.
        @param msg  The message to print."""
        if msg != "":
            Log.success(prefix="+", msg=f" - - {msg}")

    @staticmethod
    def fail_legacy(msg: str = ""):
        """A legacy failure message begins with a Red X.
        @param msg  The message to print."""
        if msg != "":
            Log.fail(prefix="X", msg=f" - - {msg}", raise_error=False)


    # --------- Builder Pattern ---------
    # Compose your own standardized error messages depending on the context
    # Everything in the prefix will have color - a few words to contextualize the error source.
    # The message body (msg_*) will be bright - easy to find inside long traceback.

    conn_abc = "BASE CONNECTOR: "
    db_conn_abc = "CONNECTOR: "
    rel_db = "REL DB: "
    gr_db = "GRAPH DB: "
    doc_db = "DOCS DB: "

    bad_addr = "BAD ADDRESS: "
    msg_bad_addr = lambda connection_string: f"Failed to connect on {connection_string}"

    bad_path = "FILE NOT FOUND: "
    msg_bad_path = lambda file_path: f"Failed to open file '{file_path}'"
    msg_good_path = lambda file_path: f"Reading contents of file '{file_path}'"

    msg_good_exec_f = lambda file_path: f"Finished executing queries from '{file_path}'"
    msg_bad_exec_f = lambda file_path: f"Error occurred while executing queries from '{file_path}'"

    msg_db_connect = lambda database_name: f"Successfully connected to database: {database_name}"

    good_val = "VALID RESULT: "
    bad_val = "INCORRECT RESULT: "
    msg_compare = lambda observed, expected: f"Expected {expected}, got {observed}"
    msg_result = lambda results: f"Query results:\n{results}"

    test_conn = "CONNECTION TEST: "
    test_basic = "BASIC: "
    test_info = "DB INFO: "
    test_df = "GET DF: "
    test_tmp_db = "CREATE DB: "

    msg_unknown_error = "An unhandled error occurred."

    get_df = "GET_DF: "
    create_db = "CREATE_DB: "
    drop_db = "DROP_DB: "
    run_q = "QUERY: "
    run_f = "FILE EXEC: "
    msg_bad_table = lambda name: f"Table '{name}' not found"
    msg_good_table = lambda name: f"Exported table '{name}' to DataFrame."
    msg_bad_coll = lambda name: f"Collection '{name}' not found"
    msg_good_coll = lambda name: f"Exported collection '{name}' to DataFrame."

    msg_success_managed_db = lambda managed, database_name: f"Successfully {managed} database '{database_name}'"
    """@brief  Handles various successful actions an admin could perform on a database.
    @param managed  Past-tense verb representing the database operation performed, e.g. Created, Dropped."""
    msg_fail_manage_db = lambda manage, database_name, connection_string: f"Failed to {manage} database '{database_name}' on connection {connection_string}"
    """@brief  Handles various failed actions an admin could perform on a database.
    @param manage  Present-tense verb representing the database operation performed, e.g. create, drop."""

    msg_fail_parse = lambda alias, bad_value, expected_type: f"Could not convert {alias} with value {bad_value} to type {expected_type}"

    msg_multiple_query = lambda n_queries, query: f"A combined query ({n_queries} results) was executed as a single query. Extra results were discarded. Query: {query}"
    msg_good_exec_q = lambda query: f"Executed successfully: '{query}'"
    msg_good_exec_qr = lambda query, results: f"Executed successfully: '{query}'\n{Log.msg_result(results)}"
    msg_bad_exec_q = lambda query: f"Failed to execute query: '{query}'"

    kg = "KG: "
    pytest_db = "PYTEST (DB): "

    db_exists = "DB_EXIST: "
    msg_db_exists = lambda database_name: f"Database '{database_name}' already exists."
    msg_db_not_found = lambda database_name, connection_string: f"Could not find database '{database_name}' using connection '{connection_string}'"
    msg_db_current = lambda database_name: f"Cannot drop database '{database_name}' while connected to it!"

    swap_db = "SWAP_DB: "
    swap_kg = "SWAP_GRAPH: "
    msg_swap_db = lambda old_db, new_db: f"Switched from database '{old_db}' to database '{new_db}'"
    msg_swap_kg = lambda old_kg, new_kg: f"Switched from graph '{old_kg}' to graph '{new_kg}'"


def all_none(*args):
    """Checks if all provided args are None."""
    return all(arg is None for arg in args)
