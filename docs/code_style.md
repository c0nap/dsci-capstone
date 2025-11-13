
Data Science Capstone - Patrick Conan
---

### Introduction

Doxygen is an external tool which automatically generates code documentation. We include pre-generated docs in our `/docs/html` folder which can be viewed by opening `annotated.html` in your web browser.


# Guidelines for Understanding Doxygen-Style Code

### Project Notes

- Docstrings are the preferred way to specify class signatures. Some useful tags are `@brief`, `@details`, `@param`, `@return`, `@raises`, and `@ref`.

- Attributes defined in `__init__` can use Doxygen-style inline comments `##` before the declaration or alternatively a docstring after it.

- Low-level implemetation comments should not be given to Doxygen. Keep those as normal Python comments, or leave a note in the method header.

- Doxygen cannot handle special characters like `\n`, or HTML tags `<div>` inside docstrings (since it will attempt to wrap the docstring in HTML).

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

### More Style Tips

- To avoid MyPy complaints, use **type hints** everywhere. All function parameters should have at least `arg1: Any`, void functions should return `None`, and sometimes local variables inside functions need typing.

- MyPy warnings about `Optional` types are disabled in `pyproject.toml`, but if you do care about nullables, use `Optional[str]` instead of `str | None` notation to support older Python versions (pre-3.10).

- **Fail loudly:** In PyTests, there should not be try-except blocks in most cases.

- Be very careful when creating objects outside of classes. PyTest will fail during collection which is more ugly than a runtime failure.

- To contain unwanted imports, they must be added to a method inside a class, since `from module1 import my_function` will pull along everything at module- or class-level. See `metrics.py` for an example.

- Since our `Session` class is used as a PyTest fixture, any failure in its `__init__`constructor will cause ALL PyTests to fail. Since `Session` creates a reference to `RelationalConnector`, `DocumentConnector`, and `GraphConnector`, these classes must also have clean constructors which never fail!

- Instead of `os.getenv()`, use `os.environ[]` to raise an exception if not found. This is useful because otherwise a downstream task could silently fail.

- **Basic function etiquette:** If a function is used exclusively by 1 class, but does not modify the state of that class, it should be converted to a module-scoped function outside the class. See `DocumentConnector` for an example.

- If a module contains multiple classes and the helper function wouldn't make sense under that module name (_e.g._ `parse_json` does not make sense under `database_connectors` module), it should be kept inside the class.

- Helper function names should be prefixed with an underscore _e.g._ `_parse_json`.


# Manually Generating Documentation with Doxygen

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



# Role of Generative AI

## Development

We use code-generation tools like ChatGPT and Claude in this project. Their scope is limited to developer-in-the-loop (Copy / Paste) and spiked project-planning discussions.

AI is uniquely helpful here due to the many moving parts in the pipeline, such as:
- Metrics implemented from scientific literature
- 4 messy datasets (Project Gutenberg, BookSum, NarrativeQA, LitBank)
- 3 database wrappers for 4 engine types (Neo4j, MongoDB, PostgreSQL, MySQL)
- 2 endpoint frameworks (Flask, Blazor) which also need database access
- Interactive UI in Blazor
- Optimized and scalable system architecture in Python
- NLP tooling with local HuggingFace models and LangChain API calls
- Deployment as container images (Docker, GitHub Actions)
- Code quality tools (Doxygen, MyPy)

We consulted with various sizes and configuration of LLM models during development, and each provider took a different role based on their individual strengths and weaknesses. These observations are noted below.

### ChatGPT

- **Company:** OpenAI
- **Models Used:** GPT-5, GPT-4o
- **Pricing Plan:** Plus (1-month trial) / Free

#### Key Points

- Great for quick questions, like syntax or best practices. Replaces the traditional Google Search debugging method which relied on Reddit forums and Stack Overflow posts.

- Can easily understand garbled sentences to an extent above and beyond basic typo detection.

- Usage limits feel very generous, and model seamlessly downgrades when reached, which often goes unnoticed.

- Has a tendency to become stuck in a silent "quiet quitting" loop. Not enough information to answer the question given the user's constraints, but never recognizes this or asks for clarification.

### Claude

- **Company:** Anthropic
- **Models Used:** Sonnet 4.5
- **Pricing Plan:** Pro ($20 for November)

#### Key Points

- Excellent integration for coding. Can link repository to automatically download code.

- Has a cleaner UI with extra formatting. Code blocks are rendered in the prompt window, which helps to avoid repetitively typing triple-backticks.

- The secondary "code artifacts" screen bloats the UI. Sometimes a single-line fix will get silently applied to the middle of a 300-line document.

- Tends to overachieve with code generation by adding defensive checks and unrequested structural changes. This is not ideal for a project where the intent is to understand all code being written.

- Has a strict cutoff when usage limits are reached. These are reached very quickly on the free plan, and only slightly relaxed on pro.

### Gemini

- **Company:** Google
- **Models Used:** 2.5 Flash
- **Pricing Plan:** Pro (free student subscription)

#### Key Points

- Strong ability to identify the cause of difficult bugs, even when ChatGPT and the strongest Claude models continue to fail.

- Tries to build the full context behind a prompt. Asks for more clarification than other models, and not ideal when the question can be misinterpreted.

- Great at being critical for code reviews. Fewer assumptions about your personality and implicit requests from the prompt's writing style. Suggests general-purpose best practices otherwise dismissed for irrelevancy.

- When using Chrome, easily accessible via Google Search in the URL bar.

### Prompting Tips

1. More code context up front usually corresponds to better output.
2. Always provide version numbers.
3. Inspect the target repository manually, and paste the relevant code. 
4. LLMs will struggle if a tool has limited online documentation.
5. Learn to recognize a quiet-quitting loop early. Signs: repetitive output, lack of direction, wants to change something major, frequent bugs like syntax, forgetting earlier requirements.
6. Break a silent loop: provide more context, change the task temporarily, approach it from a different angle, reset context with a new chat window, or try a different LLM provider.

### Shared Limitations

- **Copy / Paste:** All LLM providers are limited by their very basic chatbot interfaces; the user spends time formatting the prompt text instead of just pasting the relevant code context. The GitHub integration is what makes Claude so helpful for developers.
- **Tab Management:** A typical PR has 1 or 2 main chat windows, but there are also 5-10 throwaway chats which act as checkpoints and reminders for future PRs. Not to mention the typical GitHub open PR / issue pages, deployment dashboards, and project planning boards. This lack of organization could be addressed with a standalone application instead of relying on the web browser.
- **Discarded Chats:** Conversations are never repurposed, and just continue to build up and bloat the UI. Starting a new chat and repeating the context is always easier than finding a closed conversation.
- **Repetitive Prompts:** Many prompts have a consistent structure, but there is no framework to store or reuse them. Users are encouraged to find new ways to avoid the constant typing (_e.g._ few-stroke keywords like `be concise` / `explain that` / `minimal changes`, or omitting an explanation in favor of sending more code context) instead of building up a solid reusable approach.
- **Code Indentation:** When not using a dedicated IDE, the generated code never lines up with existing lines automatically, and must be manually shifted.
- **Style Mismatch:** Code changes are not isolated to just a few lines; the entire function is regenerated with interspersed modifications by default. This makes it more difficult to follow the reasoning behind individual fixes, promoting a hands-off "just let the chatbot handle it" approach.
- **Quiet Quitting:** The LLM ususally fails to recognize when it lacks sufficient information to answer the problem, proposing the "final revision that will 100% work this time" instead of stepping back to add more debug statements or to clarify version numbers. ChatGPT is most impacted by this, while Claude does in fact ask for clarification at times.

## Agents

Vibe coding was not used in this project.

This decision was based on our desire to preserve this capstone project both as a demonstration of software engineering proficiency and a personal learning experience.

Anecdotally, the AI-assisted workflow employed here may have doubled both productivity and personal comprehension. On the other hand, agentic coding feels like it halves how much is learned in order to achieve its massive 10x reduction in development time and developer overhead, making it more suitable for products you wish would "just exist" via any means necessary.

## Test-Driven Design (TDD)

LLMs saved a lot of time by creating PyTests for all the moving parts of this application. These tasks are usually monotonous and do not provide much potential for personal improvement, so LLMs are a good fit here.

When adding new features to a class, it is helpful to take things in stages:

1. Figure out the intended behavior of the class. Discuss which features are desired, standard for the domain, or already available using an external import.
2. Set limits on code generation by drafting docstrings and function signatures first. This allows feedback and clarification, and helps with understanding how things will fit together. Group related methods into sections and use similar naming conventions.
3. Choose one function to generate first, and verify code style. For example, type hints, docstring format, and error handling / defensive None-checks.
4. Always generate a minimal PyTest first for basic functionality, noting a preference for no edge cases yet.
5. Save time by generating several function bodies and their corresponding minimal tests in a single prompt. If you create comprehensive versions using this approach, make sure to evaluate whether a comprehensive version is actually needed, otherwise the LLM will bloat the test with obviously unnecessary asserts, adding nothing new or useful.
6. When several methods perform similar tasks, use a PyTest fixture to load the same data each time. Our database connector tests rely on SQL, JSON, or Cypher query files. This worked out well, but finding and opening separate files made copy / paste operations more time-consuming than a hard-coded query string inside a fixture. The file-reading functionality does not need to be tested 10+ times.
7. Use a distinct theme for the generated input data to make it more memorable for you and for the LLM. For example, we use a Scene graph, Social graph, and Event graph, rarely doubling-up with data fixtures. These examples provide a baseline for final intended usage; they essentially present a visualization to make comprehension and planning more seamless.


## Integration

LLMs also play a vital role in our text-processing pipeline.

Specific usage examples:
- **Relation Extraction** - In the early stages of development, the REBEL model was used for NLP, but gave bad results since it was trained on Wikipedia articles and intended for WikiData or news article applications. We used `gpt-5-nano` to 
- **BooookScore Metric** - The original paper implemented BookScore using many LLM prompts to judge summary coherence. Their work was designed for `gpt-4` - but in 2025 these legacy models are very expensive ($30 / 1M input - ended up as $0.80 per 1500-char chunk). We upgraded the model to `gpt-4o-mini` for testing, and `gpt-4o` for full production-level runs of the pipeline. However, the `gpt-5` models did not give reliable output, and the current code has no built-in stopping mechanism (we provide a 5-minute timeout instead).

Planned usage (TODO):
- Infer metaphors and other implicit triples
- Sanity check for triples - filter down OpenIE
- Structured data extraction (social relationships, events, dialogue attribution)

## Limitations

- As noted by the authors of BookScore, LLMs contain implicit knowledge about classical books - these full texts are in their training data. One way of getting around this is changing entity names, e.g. rename all instances of "Mary" to "Jane". Although some nuance may be lost in some situations, this is usually inconsequential. For example: `"What's your favorite food, Jane?" "Maple syrup, becuase it start with the letter 'M' just like 'Jane'!"`
- Without official funding for this project, the role of LLMs must be minimized to keep costs within budget. Testing and development should use smaller models, and full runs with 100+ books use a $10 budget each.


# Error Handling and Logging Design

The provided logging class in `src/util.py` writes success and warning messages to the console in addition to defining custom error classes.

## Custom Exception Design

We use a `Failure` base exception, which defines a **prefix** and a **message body** to match the style of the other custom logs in our application. The prefix is printed in a different color than the body.

The **Builder Pattern** is used for constructing these exceptions dynamically, letting developers add context incrementally without defining a new class for each error type.


#### Rationalle

- This design ensures consistency across subsystems. Each error message clearly shows its origin (_e.g._ `GRAPH_DB: TEST_CONN: Failed to connect to Neo4j using address 'neo4j_service:7687'`), while still preserving Python’s native stack trace for debugging when needed.

- The Builder pattern keeps the thrown code concise, allowing developers to express context in a single line rather than constructing verbose error hierarchies.

- This prefix format tells you exactly what happened without reading the full message, striking a balance between readability and detail.

- The centralized design ensures quick fixes to all usages of that prefix. For example, if we decide "GRAPH_DB" is too long for a prefix, we can change it to "GRAPH" or "GR_DB" while keeping diffs contained to the Log class only.

- The goal is not to hide errors but to **contextualize them**, providing a readable, layered indicator of where the failure occurred.

- Prefix is a conceptual traceback. If done improperly, it duplicates the built-in traceback — but a robust implementation only chains prefixes when it clarifies two distinct contexts. For example, `"DOCS_DB: TEST_CONN:"` should remain distinct from `"REL_DB: TEST_CONN:"`.

## Alternatives

Our design balances 3 different unsuitable approaches to exception handling:
1. **Catch built-in exceptions** - Research or test every library used, and hard-code except-blocks for each type.
2. **Specific micro-exceptions** - Defines a custom error class for every case: MongoSyntaxFailure, PostgresSyntaxFailure, MongoConnectionFailure, PostgresConnectionFailure. In a typical project this may be sufficient, but we interact with many different databases.
3. **Broadly scoped try-except blocks**

Each of these approaches has drawbacks:
1. **Built-in Exceptions** tightly couple code to third-party libraries, relying on our developers to know their internals and update handling logic when APIs change.
2. **Micro-Exceptions** scale poorly; you end up maintaining dozens of nearly identical subclasses and constructors just to preserve consistent formatting.
3. **Broad Try-Eexcept** quickly become noise: they catch too much, duplicate context, and bury the root cause several layers deep in logs.

By contrast, our prefix-based `Failure` model:
- Centralizes all formatting and coloring in one class.
- Allows semantic, human-readable context (`GRAPH_DB: TEST_CONN:`) instead of raw technical traces.
- Eliminates exception-type bloat.
- Keeps logs visually consistent and easy to parse across multiple database connectors.

In short, the `Failure` system captures the *clarity* of domain-specific messages without the maintenance overhead of a full exception hierarchy or the clutter of broad try/except usage.

Additionally, sub-errors like `BadAddressFailure` are used to consolidate — hiding long, complicated tracebacks when the issue is fixable with something simple on our end, such as forgetting to start the databases before running the tests.
