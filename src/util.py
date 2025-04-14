
class Log:
    """The Log class standardizes console output."""

    ## These functions can be used to print the standard prefix
    ## before your own print(), or a message can be specified.
    def success(msg=""):
        """A success message begins with a Green Plus."""
        print("\033[32m+\033[0m - - ", end="")
        if msg != "": print(msg)
    def fail(msg=""):
        """A failure message begins with a Red X."""
        print("\033[31mX\033[0m - - ", end="")
        if msg != "": print(msg)

    def file_read_failure(filename: str):
        """Prints a failure message when a file cannot be opened.
        @param filename  The file which caused the error."""
        Log.fail(f"Failed to read file \"{filename}\"")