from contextlib import contextmanager
import functools
import inspect
from inspect import FrameInfo
from pandas import DataFrame, Series
import sys
import os
import time
from typing import Any, Callable, List, Optional, Tuple, Generator


class Log:
    """The Log class standardizes console output."""

    ## Enable ANSI colors in output
    USE_COLORS = True
    ## Enable time-logging with the 'Log.time' decorator
    RECORD_TIME = True
    ## Print the entire DataFrame to console
    FULL_DF = False

    ## ANSI code for green text
    GREEN = "\033[32m"
    ## ANSI code for red text
    RED = "\033[31m"
    ## ANSI code for yellow text
    YELLOW = "\033[33m"
    ## ANSI code for bright yellow / cream
    BRIGHT = "\033[93m"
    ## ANSI code for light blue
    CYAN = "\033[96m"
    ## ANSI code to reset color
    WHITE = "\033[0m"

    ## ANSI color applied to the prefix of success messages
    SUCCESS_COLOR = GREEN
    ## ANSI color applied to the prefix of ignored fail messages
    WARNING_COLOR = YELLOW
    ## ANSI color applied to the prefix of critical fail messages
    FAILURE_COLOR = RED
    ## ANSI color applied to the prefix of time-elapsed messages
    TIME_COLOR = CYAN
    ## ANSI color applied to the body of every Log message
    MSG_COLOR = BRIGHT

    # These functions can be used to print the standard prefix
    # before your own print(), or a message can be specified.

    @staticmethod
    def success(prefix: str = "PASS: ", msg: str = "", verbose: bool = True) -> None:
        """A success message begins with a green prefix.
        @param prefix  The context of the message.
        @param msg  The message to print.
        @param verbose  Whether to actually print. Saves space and reduces nested if statements."""
        if not verbose:
            return
        text = f"{Log.SUCCESS_COLOR}{prefix}{Log.MSG_COLOR}{msg}{Log.WHITE}" if Log.USE_COLORS else f"{prefix}{msg}"
        print(text)

    @staticmethod
    def warn(prefix: str = "WARN: ", msg: str = "", verbose: bool = True) -> None:
        """A warning message begins with a yellow prefix.
        @param prefix  The context of the message.
        @param msg  The message to print.
        @param verbose  Whether to actually print. Saves space and reduces nested if statements."""
        if not verbose:
            return
        text = f"{Log.WARNING_COLOR}{prefix}{Log.MSG_COLOR}{msg}{Log.WHITE}" if Log.USE_COLORS else f"{prefix}{msg}"
        print(text)

    @staticmethod
    def fail(prefix: str = "ERROR: ", msg: str = "", raise_error: bool = True, other_error: Optional[Exception] = None) -> None:
        """A failure message begins with a red prefix.
        @param prefix  The context of the message.
        @param msg  The message to print.
        @param raise_error  Whether to raise an error.
        @param other_error  Another Exception resulting from this failure.
        @throws Log.Failure  If raise_error is True"""
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
    def success_legacy(msg: str = "") -> None:
        """A legacy success message begins with a Green Plus.
        @param msg  The message to print."""
        if msg != "":
            Log.success(prefix="+", msg=f" - - {msg}")

    @staticmethod
    def fail_legacy(msg: str = "") -> None:
        """A legacy failure message begins with a Red X.
        @param msg  The message to print."""
        if msg != "":
            Log.fail(prefix="X", msg=f" - - {msg}", raise_error=False)

    # --------- Custom Exceptions ---------
    class Failure(RuntimeError):
        """User-facing base class for custom error handling.
        @details
        - Builder Pattern - User can combine and chain standard message strings from the Log class.
        - Prefixes (e.g., "GRAPH DB:", "FILE IO:") are redundant with tracebacks but improve
          readability by highlighting the semantic source of the error - not just a line number.
        - Enforces a consistent color scheme across all raised errors for quick scanning.
        """

        def __init__(self, prefix: str = "ERROR: ", msg: str = ""):
            self.prefix = prefix
            self.msg = msg if msg else Log.msg_unknown_error
            super().__init__(self.__str__())

        def __str__(self):
            if Log.USE_COLORS:
                return f"{Log.FAILURE_COLOR}{self.prefix}{Log.MSG_COLOR}{self.msg}{Log.WHITE}"
            else:
                return f"{self.prefix}{self.msg}"

    class BadAddressFailure(Failure):
        """Raised when a database connection string or address is invalid.
        @details
        - We support 4+ database engines and 2 endpoint frameworks (Blazor & Flask),
          each of which has a different error type when unable to connect.
        - To avoid flooding the console with these, this error should not be chained.
        - Usage: raise BadAddressFailure(source_prefix, connection_string) from None"""

        def __init__(self, source_prefix: str, connection_string: str):
            prefix = f"{source_prefix}{Log.bad_addr}"
            msg = Log.msg_bad_addr(connection_string)
            super().__init__(prefix=prefix, msg=msg)

    @staticmethod
    def time_message(prefix: str = "[TIME] ", msg: str = "", verbose: bool = True) -> None:
        """A time message begins with a light blue prefix.
        @param prefix  The context of the message.
        @param msg  The message to print.
        @param verbose  Whether to actually print. Saves space and reduces nested if statements."""
        if not verbose:
            return
        text = f"{Log.TIME_COLOR}{prefix}{Log.MSG_COLOR}{msg}{Log.WHITE}" if Log.USE_COLORS else f"{prefix}{msg}"
        print(text)

    @staticmethod
    def elapsed_time(name: str, seconds: float, call_chain: str, verbose: bool = True) -> None:
        """Print the time taken to complete a function.
        @param name  The name of the function.
        @param seconds  The number of seconds (will be rounded to 3 decimals)
        @param call_chain  The full stack of function calls (for record-keeping)
        @param verbose  Whether to actually print. Saves space and reduces nested if statements.
        """
        # Store timing result for later summary
        Log._timing_results.append((name, seconds, call_chain, Log.run_id))
        msg = Log.msg_elapsed_time(name, seconds)
        Log.time_message(msg=msg, verbose=verbose)

    msg_elapsed_time = lambda name, seconds: f"{name} took {seconds:.3f}s{Log.WHITE}"

    @staticmethod
    def format_call_chain(stack: List[FrameInfo], name: str) -> str:
        """Sanitize and concatenate the full call stack for console output.
        @param stack  The frame stack obtained by inspect.stack().
        @param name  The name of the caller function.
        @return  A string representing the full call chain."""
        call_chain_parts = []
        for frame_info in reversed(stack[1:]):
            func_name = frame_info.function
            # Skip internal and logging frames
            if func_name not in ['timer', 'time', 'wrapper', '<module>', '__enter__', '__exit__']:
                call_chain_parts.append(func_name)
        call_chain_parts.append(name)
        call_chain = " -> ".join(call_chain_parts)
        return call_chain

    # --------- Decorator Pattern ---------
    # Use the 'Log.time' tag to print a Time Elapsed message on every call to that function.
    @staticmethod
    def time(func: Callable[..., Any]) -> Callable[..., Any]:
        """Logs the time elapsed for a function call.
        @details
        - Uses an inner wrapper function to capture *args and **kwargs.
        @param func  The function to wrap.
        @return  The wrapped function that logs time and forwards the result.
        """

        # Preserve the original function's name and docstring.
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Wrapper around the original function to measure elapsed time.
            @param args  Positional arguments forwarded to the original function.
            @param kwargs  Keyword arguments forwarded to the original function.
            @return  The result of calling the original function.
            """
            call_chain = Log.format_call_chain(inspect.stack(), func.__name__)
            start = time.time()
            try:
                result = func(*args, **kwargs)
            except Exception:  # Fix traceback of the wrapped function...
                _, exc, trace = sys.exc_info()

                # Not feasible to completely remove 'wrapper' frame from the traceback
                # This makes it slightly less intrusive:           otherwise:  File "/pipeline/src/main.py", line 121, in <module>
                #   File "/pipeline/src/main.py", line 121, in <module>          chunks = pipeline_A(
                #     chunks = pipeline_A(                                                ^^^^^^^^^^^
                #              ^^^^^^^^^^^                                     File "/pipeline/src/util.py", line 167, in wrapper
                #   File "/pipeline/src/util.py", line 174, in wrapper           result = func(*args, **kwargs)
                #     raise exc.with_traceback(trace.tb_next)                             ^^^^^^^^^^^^^^^^^^^^^
                #   File "/pipeline/src/main.py", line 24, in pipeline_A       File "/pipeline/src/main.py", line 24, in pipeline_A
                #     tei_path = task_01_convert_epub(epub_path)                 tei_path = task_01_convert_epub(epub_path)
                #                ^^^^^^^^^^^^^^^^^^^^                                       ^^^^^^^^^^^^^^^^^^^^
                if trace.tb_next is not None:
                    raise exc.with_traceback(trace.tb_next)
                # Fallback: just allow the messy traceback - this should not happen
                raise
            finally:
                if Log.RECORD_TIME:
                    elapsed = time.time() - start
                    Log.elapsed_time(func.__name__, elapsed, call_chain)
            return result

        return wrapper

    # Use the 'Log.timer' context to print a Time Elapsed message once the function is finished.
    # Advantage over @Log.time: Cleaner traceback
    @staticmethod
    @contextmanager
    def timer(name: str = None) -> Generator[None, None, None]:
        """Context manager for recording the execution time of code blocks.
        @param name  Optional name for the timed block. If not provided, uses caller function name.
        Usage:
            with Log.timer():
                # your code here
        """
        if not Log.RECORD_TIME:
            yield
            return

        # Auto-detect function name if not provided
        stack = inspect.stack()
        if name is None:
            # Find the first frame that's not timer/__enter__/__exit__
            for frame_info in stack[1:]:
                func_name = frame_info.function
                if func_name not in ['timer', '__enter__', '__exit__']:
                    name = func_name
                    break

        call_chain = Log.format_call_chain(stack, name)
        start = time.time()
        try:
            yield  # If an exception happens here... (see below)
        finally:
            elapsed = time.time() - start  # Data recorded even on failure
            Log.elapsed_time(name, elapsed, call_chain)

    _timing_results: List[Tuple[str, float, str, int]] = []  # (func_name, elapsed, call_chain, run_id)
    run_id: int = 1

    @staticmethod
    def get_timing_summary() -> DataFrame:
        """Returns timing results as a pandas DataFrame.
        @return  DataFrame with columns: function, elapsed, call_chain, run_id
        """
        if not Log._timing_results:
            return DataFrame(columns=['function', 'elapsed', 'call_chain', "run_id"])
        df = DataFrame(Log._timing_results, columns=['function', 'elapsed', 'call_chain', "run_id"])
        return df

    @staticmethod
    def get_merged_timing(file_path: str = "./logs/elapsed_time.csv") -> DataFrame:
        """Reads the existing file, deletes rows matching this run_id, and adds current data.
        @return  DataFrame with columns: function, elapsed, call_chain, run_id
        """
        # Current run timing as DataFrame
        current_df = Log.get_timing_summary()

        # Read existing file if it exists
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            # Remove rows with the current run_id
            existing_df = existing_df[existing_df['run_id'] != Log._current_run_id]
        else:
            # No file yet
            existing_df = DataFrame(columns=['function', 'elapsed', 'call_chain', "run_id"])

        # Merge existing with current
        merged_df = pd.concat([existing_df, current_df], ignore_index=True)
        return merged_df

    @staticmethod
    def dump_timing_csv(file_path: str = "./logs/elapsed_time.csv") -> None:
        """Save timing results to a CSV file, appending if it already exists.
        @param file_path  Where the saved CSV will be located.
        @return  DataFrame with columns: function, elapsed, call_chain
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # Check if file exists to decide whether to write header
        file_exists = os.path.exists(file_path)

        df = Log.get_merged_timing()
        df.to_csv(file_path, mode="a", index=False, header=not file_exists)
        Log.time_message(prefix=Log.t_dump, msg=Log.msg_time_dump(file_path))
    
    t_dump = "[DUMP] "
    msg_time_dump = lambda file_path: f"Saved time records to '{file_path}'"

    @staticmethod
    def clear_timing_data():
        """Clears all recorded timing data."""
        Log._timing_results.clear()

    @staticmethod
    def print_timing_summary():
        """Prints a formatted timing summary grouped by function."""
        df = Log.get_timing_summary()
        if df.empty:
            print("No timing data recorded.")
            return
        print("\n=== TIMING SUMMARY ===")

        # Group by function and show stats
        grouped = df.groupby('function')['elapsed'].agg(['count', 'sum', 'mean', 'min', 'max'])
        grouped.columns = ['calls', 'total', 'avg', 'min', 'max']

        print(grouped.to_string())
        print(f"\nTotal execution time: {df['elapsed'].sum():.3f}s")

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

    msg_result = (
        lambda results: f"""Fetched results: {Log.WHITE if Log.USE_COLORS else ''}
    {DataFrame(results).to_string(max_rows=None, max_cols=None) if Log.FULL_DF else results} {Log.MSG_COLOR if Log.USE_COLORS else ''}"""
    )

    msg_good_table = (
        lambda name, df: f"""Exported table '{name}' to DataFrame:{Log.WHITE if Log.USE_COLORS else ''}
    {DataFrame(df).to_string(max_rows=None, max_cols=None) if Log.FULL_DF else df} {Log.MSG_COLOR if Log.USE_COLORS else ''}"""
    )

    msg_good_coll = (
        lambda name, df: f"""Exported collection '{name}' to DataFrame:{Log.WHITE if Log.USE_COLORS else ''}
    {DataFrame(df).to_string(max_rows=None, max_cols=None) if Log.FULL_DF else df} {Log.MSG_COLOR if Log.USE_COLORS else ''}"""
    )

    msg_good_graph = (
        lambda name, df: f"""Exported graph '{name}' to DataFrame:{Log.WHITE if Log.USE_COLORS else ''}
    {DataFrame(df).to_string(max_rows=None, max_cols=None) if Log.FULL_DF else df} {Log.MSG_COLOR if Log.USE_COLORS else ''}"""
    )

    msg_bad_table = lambda name: f"Table '{name}' not found"
    msg_bad_coll = lambda name: f"Collection '{name}' not found"
    msg_bad_graph = lambda name: f"Graph '{name}' not found"

    test_ops = "OPERATE: "
    test_basic = "CONNECT: "
    test_info = "DB INFO: "
    test_df = "GET DF: "
    test_tmp_db = "CREATE DB: "

    msg_unknown_error = "An unhandled error occurred."

    get_df = "GET_DF: "
    create_db = "CREATE_DB: "
    drop_db = "DROP_DB: "
    run_q = "QUERY: "
    run_f = "FILE EXEC: "

    drop_gr = "DROP_GRAPH: "

    msg_success_managed_db = lambda managed, database_name: f"Successfully {managed} database '{database_name}'"
    """@brief  Handles various successful actions an admin could perform on a database.
    @param managed  Past-tense verb representing the database operation performed, e.g. Created, Dropped."""
    msg_fail_manage_db = (
        lambda manage, database_name, connection_string: f"Failed to {manage} database '{database_name}' on connection {connection_string}"
    )
    """@brief  Handles various failed actions an admin could perform on a database.
    @param manage  Present-tense verb representing the database operation performed, e.g. create, drop."""

    msg_success_managed_gr = lambda managed, database_name: f"Successfully {managed} graph '{database_name}'"
    msg_fail_manage_gr = (
        lambda manage, database_name, connection_string: f"Failed to {manage} graph '{database_name}' on connection {connection_string}"
    )

    msg_fail_parse = lambda alias, bad_value, expected_type: f"Could not convert {alias} with value {bad_value} to type {expected_type}"

    msg_multiple_query = (
        lambda n_queries, query: f"A combined query ({n_queries} results) was executed as a single query. Extra results were discarded. Query:\n{query}"
    )
    msg_good_exec_q = lambda query: f"Executed successfully:\n'{query}'"
    msg_good_exec_qr = lambda query, results: f"Executed successfully:\n'{query}'\n{Log.msg_result(results)}"
    msg_bad_exec_q = lambda query: f"Failed to execute query:\n'{query}'"

    msg_good_df_parse = lambda df: Log.msg_result(df)
    msg_bad_df_parse = lambda query: f"Failed to convert query result to DataFrame:\n'{query}'"

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

    get_unique = "UNIQUE: "

    msg_none_df = lambda collection_type, collection_name: f"Unable to fetch DataFrame from {collection_type} '{collection_name}' - None"

    kg = "TRIPLES: "
    sub_gr = "SUBGRAPH: "
    gr_rag = "RAG: "
    msg_bad_triples = lambda graph_name: f"No triples found for graph {graph_name}"


def df_natural_sorted(df: DataFrame, ignored_columns: List[str] = [], sort_columns: List[str] = []) -> DataFrame:
    """Sort a DataFrame in natural order using only certain columns.
    @details
    - Column order is alphabetic too, for completely predictable behavior.
    - The provided DataFrame will not be modified, since inplace=False by default.
    - Existing row numbers will be deleted and regenerated to match the sorted order.
    @param df  The DataFrame containing unsorted rows.
    @param  ignored_columns  A list of column names to NOT sort by.
    @param  sort_columns  A list of column names to sort by FIRST."""

    # Exclude any column that contains list/dict values anywhere
    def is_hashable_col(col: Series) -> bool:
        return not col.map(lambda x: isinstance(x, (list, dict))).any()

    if df is None or df.empty:
        return df
    # Exclude non-hashable columns e.g. lists and dicts
    safe_cols = [c for c in df.columns if c not in ignored_columns and is_hashable_col(df[c])]
    sort_columns = [c for c in sort_columns if c in df.columns and is_hashable_col(df[c])]
    # If we have no columns to sort on, just reset the row indexing.
    if not safe_cols:
        return df.reset_index(drop=True)
    # Columns sorted alphabetically, with optional priority override
    safe_cols = sorted(c for c in safe_cols if c not in sort_columns)
    safe_cols = [c for c in sort_columns if c in df.columns] + safe_cols
    return df.sort_values(by=safe_cols, kind="stable").reset_index(drop=True)


def check_values(results: List[Any], expected: List[Any], verbose: bool, log_source: str, raise_error: bool) -> bool:
    """Safely compare two lists of values. Helper for @ref src.connectors.relational.RelationalConnector.test_operations
    @param results  A list of observed values from the database.
    @param expected  A list of correct values to compare against.
    @param verbose  Whether to print success messages.
    @param log_source  The Log class prefix indicating which method is performing the check.
    @param raise_error  Whether to raise an error on connection failure.
    @throws Log.Failure  If any result does not match what was expected."""
    for i in range(len(results)):
        if results[i] == expected[i]:
            Log.success(log_source + Log.good_val, Log.msg_compare(results[i], expected[i]), verbose)
        elif results[i] != expected[i]:
            if raise_error:
                raise Log.Failure(log_source + Log.bad_val, Log.msg_compare(results[i], expected[i])) from None
            return False
    return True
