class Log:
    """The Log class standardizes console output."""

    # These functions can be used to print the standard prefix
    # before your own print(), or a message can be specified.
    def success(msg: str = ""):
        """A success message begins with a Green Plus.
        @param msg  The message to print."""
        print("\033[32m+\033[0m - - ", end="")
        if msg != "":
            print(msg)

    def fail(msg: str = ""):
        """A failure message begins with a Red X.
        @param msg  The message to print."""
        print("\033[31mX\033[0m - - ", end="")
        if msg != "":
            print(msg)

    # The below methods build upon the standardized success / fail messages.
    def file_read_failure(filename: str):
        """Prints a failure message when a file cannot be opened.
        @param filename  The file which caused the error."""
        Log.fail(f'Failed to read file "{filename}"')

    def connect_success(database_name: str):
        """Prints a failure message when a database connection fails.
        @param database_name  The name of the database which was connected to."""
        Log.success(f"Successfully connected to database: {database_name}")

    def connect_fail(connection_string: str):
        """Prints a failure message when a database connection fails.
        @param connection_string  The connection string which caused the error."""
        Log.fail(f"Failed to connect on {connection_string}")

    def success_manage_db(database_name: str, managed: str):
        """Prints a success message when a new database is created or dropped.
        @param database_name  The name of the target database.
        @param managed  Past-tense verb representing the database operation performed."""
        Log.success(f'{managed} database "{database_name}"')

    def fail_manage_db(connection_string: str, database_name: str, manage: str):
        """Prints a failure message when database creation or deletion fails.
        @param connection_string  The database connector which had the error.
        @param database_name  The name of the database which could not be created or dropped.
        @param manage  Present-tense verb representing the database operation performed."""
        Log.fail(
            f'Failed to {manage} database "{database_name}" on connection {connection_string}'
        )

    def incorrect_result(observed, expected):
        """Prints a failure message when a value is not what we expected.
        @param observed  The value to check (must be same type as 'expected').
        @param expected  The correct value (can be any printable type)."""
        Log.fail(f"Incorrect result: Expected {expected}, got {observed}")

    def warn_parse(trace: str, expected_type: str, bad_value: str):
        """Prints a failure message when parsing fails.
        @param trace  Alias for the object to be converted.
        @param expected_type  The type we are trying to convert to.
        @param bad_value  Actual target value to be converted."""
        Log.fail(
            f"Could not convert {trace} with value {bad_value} to type {expected_type}"
        )


def all_none(*args):
    """Checks if all provided args are None."""
    return all(arg is None for arg in args)
