
class Log:
    def print_pass(msg=""):
        print("\033[32m+\033[0m - - ", end="")
        if msg != "": print(msg)
    def print_fail(msg=""):
        print("\033[31mX\033[0m - - ", end="")
        if msg != "": print(msg)