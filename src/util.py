
class Log:
    """The Log class standardizes console output."""

    ## These functions can be used to print the standard prefix
    ## before your own print(), or a message can be specified.
    def success(msg: str = ""):
        """A success message begins with a Green Plus.
        @param msg  The message to print."""
        print("\033[32m+\033[0m - - ", end="")
        if msg != "": print(msg)
    def fail(msg: str = ""):
        """A failure message begins with a Red X.
        @param msg  The message to print."""
        print("\033[31mX\033[0m - - ", end="")
        if msg != "": print(msg)

    def file_read_failure(filename: str):
        """Prints a failure message when a file cannot be opened.
        @param filename  The file which caused the error."""
        Log.fail(f"Failed to read file \"{filename}\"")

    def fail_connect(connection_string: str):
        """Prints a failure message when a database connection fails.
        @param filename  The connection string which caused the error."""
        Log.fail(f"Failed to connect on {connection_string}")

    def success_manage_db(database_name: str, managed: str):
        """Prints a success message when a new database is created or dropped.
        @param database_name  The name of the target database."""
        Log.success(f"{managed} database \"{database_name}\"")
    
    def fail_create_db(connection_string: str, database_name: str, manage: str):
        """Prints a failure message when database creation or deletion fails.
        @param connection_string  The database connector which had the error.
        @param database_name  The name of the database which could not be created or dropped."""
        Log.fail(f"Failed to {manage} database \"{database_name}\" on connection {connection_string}")