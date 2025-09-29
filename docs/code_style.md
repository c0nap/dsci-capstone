
Data Science Capstone - Patrick Conan
---

### Introduction

Doxygen is an external tool which automatically generates code documentation. We include pre-generated docs in our `/docs/html` folder which can be viewed by opening `annotated.html` in your web browser.

### Guidelines for Understanding Doxygen-Style Code

#### Quick Tips

- Docstrings are the preferred way to specify class signatures. Some useful tags are `@brief`, `@details`, `@param`, `@return`, `@raises`, and `@ref`.

- Attributes defined in `__init__` can use Doxygen-style inline comments `##` before the declaration or alternatively a docstring after it.

- Low-level implemetation comments should not be given to Doxygen. Keep those as normal Python comments, or leave a note in the method header.

- In this project, all Python code is formatted using `black .` before merging code into main. This will break legacy-style Doxygen blocks such as `#** #* ... #*`.

- Since the Jetbrains Rider IDE is used for our C# code, we simply run the `Built-in: Reformat Code` routine.

- Doxygen tags like `@brief` may also be used in C# code with triple-slash blocks `///`.

#### Code Sample

```python
class DatabaseConnector:
    """
    @brief Abstract base class for database connectors.
    @details
        Provides basic structure for connecting, executing queries,
        and managing database sessions.

        Derived classes should implement:
        - @ref connect
        - @ref disconnect
    """

    def __init__(self, verbose: bool = False):
        """
        @brief Initialize the connector.
        @param verbose Whether to print debug messages.
        @note Attributes will be set to None until configure() is called.
        """
        ## Database type, e.g., 'postgres', 'mysql'
        self.db_type = None
        ## Hostname or IP of the database server
        self.host = None
        ## Verbose mode
        self.verbose = verbose
        """
        @brief Controls whether the connector prints messages to the console.
        @details
            - This attribute may be modified at runtime.
            - All other methods in this class should respect its current value
              when deciding whether to print debug or status messages.
            - Default is False; set to True to enable verbose output.
        """

    def connect(self):
        """
        @brief Establish a connection to the database.
        @raises ConnectionError If unable to connect.
        @return True if connection succeeds, False otherwise.
        """
        pass

    def disconnect(self):
        """
        @brief Close the database connection.
        @see connect
        """
        pass
```

### Generating Documentation with Doxygen

These instructions are based on a [tutorial](https://www.woolseyworkshop.com/2020/06/25/documenting-python-programs-with-doxygen/) created by John Woolsey in 2020.

#### 1. Install required packages

```bash
sudo apt install doxygen
sudo apt install graphviz
```

The `Graphviz` package is required by Doxygen to generate class diagrams.

#### 2. Create the Doxygen config file

```bash
cd docs
doxygen -g
```

#### 3. Configure the new Doxyfile

Preferences used in our setup:

- `PROJECT_NAME`           = "Data Science Capstone Project"
- `JAVADOC_AUTOBRIEF`      = YES
- `OPTIMIZE_OUTPUT_JAVA`   = YES
- `EXTRACT_ALL`            = YES
- `EXTRACT_PRIVATE`        = YES
- `EXTRACT_STATIC`         = YES
- `HIDE_SCOPE_NAMES`       = YES
- `INPUT`                  = ../src ../components ../tests ../web-app

#### 4. Generate the HTML and LaTeX folders

```bash
doxygen
```

Open `annotated.html` in your Chrome browser to view the generated documentation.

