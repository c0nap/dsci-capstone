
Data Science Capstone - Patrick Conan
---

### Introduction

Doxygen is an external tool which automatically generates code documentation. We include pre-generated docs in our `/docs/html` folder which can be viewed by opening `annotated.html` in your web browser.


## Guidelines for Understanding Doxygen-Style Code

### Project Notes

- Docstrings are the preferred way to specify class signatures. Some useful tags are `@brief`, `@details`, `@param`, `@return`, `@raises`, and `@ref`.

- Attributes defined in `__init__` can use Doxygen-style inline comments `##` before the declaration or alternatively a docstring after it.

- Low-level implemetation comments should not be given to Doxygen. Keep those as normal Python comments, or leave a note in the method header.

- Since `AUTOBRIEF` is enabled, you may omit `@brief` for the first line in a docstring. The convention in this project is to take advantage of `AUTOBRIEF` to enhance code readability. We additionally put an extra space between the tag and its text description, _e.g._ `param x  An integer`, and prefer condensed docstrings, _i.e._ minimal whitespace with triple-quotes placed on the same line as text.

- In Python, `@ref` requires a fully-qualified method name. Even if `from_env()` is contained within the same class or file, Doxygen requires something like `@ref components.connectors.RelationalConnector.from_env`.

- In this project, all Python code is formatted using `black .` before merging code into main. This will break legacy-style Doxygen blocks such as `#** #* ... #*`.

- Since the Jetbrains Rider IDE is used for our C# code, we simply run the `Built-in: Reformat Code` routine.

- Doxygen tags like `@brief` may also be used in C# code with triple-slash blocks `///`.

### Code Sample

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


## Manually Generating Documentation with Doxygen

These instructions are based on a [tutorial](https://www.woolseyworkshop.com/2020/06/25/documenting-python-programs-with-doxygen/) created by John Woolsey in 2020.

### 1. Install required packages

```bash
sudo apt install doxygen
sudo apt install graphviz
```

The `Graphviz` package is required by Doxygen to generate class diagrams.

### 2. Create the Doxygen config file

```bash
cd docs
doxygen -g
```

### 3. Configure the new Doxyfile

Preferences used in our setup:

- `PROJECT_NAME`           = "Data Science Capstone Project"
- `INPUT`                  = ../src ../components ../tests ../web-app
- `RECURSIVE`              = YES
- `JAVADOC_AUTOBRIEF`      = YES
- `OPTIMIZE_OUTPUT_JAVA`   = YES
- `PYTHON_DOCSTRING`       = NO
- `HIDE_SCOPE_NAMES`       = YES
- `EXTRACT_ALL`            = YES
- `EXTRACT_PRIVATE`        = YES
- `EXTRACT_STATIC`         = YES


### 4. Generate the HTML and LaTeX folders

If the `/docs/html/` and `/docs/latex/` folders already exist, you must delete their contents first.

```bash
rm -rf html/ latex/
```

Generate the latest code documentation:

```bash
doxygen
```

Open `index.html` or `annotated.html` with your Chrome browser to view the generated documentation pages.


## Automated Doxygen

The manual Doxygen instructions are useful for PRs. Contributors should regenerate the documentation locally, and fix any console warnings before review.

However, updating the canonical documentation on our [origin/docs](https://github.com/c0nap/dsci-capstone/tree/docs) branch is locked behind a GitHub Actions job which runs automatically after pushing to main.

To manually dispatch this workflow during development, visit [GitHub Actions](https://github.com/c0nap/dsci-capstone/actions/workflows/doxygen.yml), click `Run Workflow`, and select your development branch. NOTE: This will rewrite the public code documentation, and should be used sparingly.

Once the `docs` branch contains `index.html`, you're ready to set up [GitHub Pages](https://github.com/c0nap/dsci-capstone/settings/pages). This will create a new `github-pages` deployment based on the contents of `origin/docs`.

The updated code documentation can be found at the URL [c0nap.github.io/dsci-capstone](https://c0nap.github.io/dsci-capstone/).

