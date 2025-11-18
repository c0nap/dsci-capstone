
Author: Patrick Conan

Advisor: Ali Baheri

Rochester Institute of Technology

Capstone Project DSCI-601/602 w/ Travis Desell

Construction of a Flexible Fact Database for Fictional Stories

The goal of this project is to extract details from fiction books, and store facts in a knowledge database. The final output is a JSON-style list of entities and their relationships.

---

## Requirements

- Windows Subsystem for Linux (WSL), or another terminal
- Python 3.12
- Relational database (PostgreSQL or MySQL preferred)
- Document database (MongoDB)
- Graph database (Neo4j)
- Pandoc

If you need help installing your databases, follow the [database guides](docs/database_instructions.md).

#### Common Issues
- Each login should have a username and password (not always set by default).
- Remember to start the database server. `sudo service postgresql start`
- If your database is installed on your OS (Windows) and this code is running in WSL, localhost may not work. [How to Fix](docs/database_instructions.md)

---

## Project Structure

```
project/                          # Parent directory (optional)
├── venv/                         # Python virtual environment (not committed)
└── repository/
    ├── Makefile                  # Convenience commands for Docker + tests
    ├── docker-compose.yml        # Multi-container orchestration
    ├── pyproject.toml            # Settings for mypy / black / isort
    ├── pytest.ini                # Pytest configuration
    ├── conftest.py               # Shared PyTest fixtures
    ├── .env.example              # Template environment config
    ├── .env                      # Actual runtime config (not committed)
    ├── .gitignore
    ├── .gitattributes
    ├── .dockerignore
    │
    ├── src/                      # All application source code
    │   ├── main.py               # Entry point (CLI, orchestration)
    │   ├── util.py               # Shared helpers (logging, error handling)
    │   ├── core/                 # Orchestration and global runtime context
    │   │   ├── context.py        # Lazy singleton Session + accessors
    │   │   ├── boss.py           # Main orchestrator for pipeline tasks
    │   │   └── worker.py         # Worker routines / processing units
    │   │
    │   ├── connectors/           # Adapters for all DB + LLM backends
    │   │   ├── base.py           # Shared ABCs for all connectors
    │   │   ├── relational.py     # MySQL/Postgres connector (SQLAlchemy)
    │   │   ├── document.py       # MongoDB connector (MongoEngine)
    │   │   ├── graph.py          # Neo4j connector (NeoModel)
    │   │   └── llm.py            # LLM connector (LangChain)
    │   │
    │   └── components/           # Pipeline logic and data-processing modules
    │       ├── corpus.py
    │       ├── metrics.py
    │       ├── fact_storage.py
    │       ├── relation_extraction.py
    │       └── book_conversion.py
    │
    ├── tests/                    # Full pytest suite (ordered + dependent)
    │   ├── README.md
    │   ├── __init__.py
    │   ├── test_db_basic.py      # Minimal DB tests (relational/doc/graph)
    │   ├── test_db_files.py      # File-based example dataset tests
    │   ├── test_kg_triples.py    # KnowledgeGraph behaviors, parsing tests
    │   └── examples-db/          # Deterministic cql/json fixtures for tests
    │       ├── relational/
    │       ├── document/
    │       └── graph/
    │
    ├── datasets/                 # Local data or user-supplied corpora
    │   └── (empty or populated as-needed)
    ├── deps/                     # Lists of required Python packages
    │   ├── requirements.txt
    │   ├── development.txt
    │   ├── bookscore.txt
    │   └── (etc.)
    ├── docker/                   # All Dockerfiles + DB setup scripts
    │   ├── Dockerfile.python
    │   ├── Dockerfile.blazor
    │   ├── Dockerfile.bookscore
    │   ├── (etc.)
    │   └── db-init/
    │
    ├── docs/                     # Documentation (Doxygen + custom guides)
    │   ├── Doxyfile
    │   ├── html/                 # Auto-generated Doxygen HTML (not committed)
    │   └── latex/                # Auto-generated LaTeX (not committed)
    ├── logs/                     # Runtime logs / snapshots of output
    │   └── output.txt
    ├── web-app/                  # Blazor frontend component
    └── .github/                  # CI/CD workflows
        └── workflows/
```

## Setup (Windows)

1. Download the project folder. Create a parent folder directly under the `C:/` drive (for easy access from WSL), and navigate into it using your terminal.
```bash
cd /mnt/c/project
```

2. (Optional) Create a virtual environment and activate it. This prevents installed packages from interfering with other Python installations on your system.
```bash
python3 -m venv your-venv
```
```bash
source your-venv/bin/activate
```

3. Clone the repository and place the project folder inside your parent directory, next to your virtual environment.
```bash
git clone https://github.com/C0NAP/dsci-capstone.git /mnt/c/project/repository
```

4. **IMPORTANT!** You must be in the repository folder for the following steps.
```bash
cd /mnt/c/project/repository
```

5. Install required packages into your virtual environment.
```bash
python -m pip install -r requirements.txt
```

6. Copy `.env.example` to `.env`, and add your PostgreSQL credentials. Make sure `DB_ENGINE` is set to `POSTGRESQL`. If you have pre-installed engines running on Windows instead of WSL, use `hostname -I` to find the IP.

7. Run setup script to verify basic components are working. Scripts are treated as modules to simplify imports between folders.
```bash
python -m src.setup
```

8. Run pytests to load example tables into database.
```bash
pytest .
```

9. Install required system libraries: Pandoc is used for EPUB file conversion.
```bash
sudo apt install pandoc
```

## API Keys

Create OpenAI and HuggingFace API keys, and copy them into `OPENAI_API_KEY` and `HF_HUB_TOKEN` in your `.env` file. Usage and cost can be monitored on each website individually.

---

## 3 - Additional Information

### Database Connections / Network Troubleshooting

We have also compiled a [detailed guide](docs/database_instructions.md) addressing issues found when connecting the various components.

### Docker Hostname Resolution

Optionally, Docker can be used to run our exact setup with test datasets and minimal configurations. [Docker Guide](docs/docker_setup.md)

### Web Application

To extend the provided web app or to build your own, please reference our [Blazor UI Notes](docs/web_app_notes.md).

### Code Review Tools

- Doxygen
- Black
- isort
- mypy
- Gource
- git-story
- GitClear

### Code Documentation with Doxygen

Pre-compiled code diagrams can be accessed from `docs/html/annotated.html`. We discuss our standards and development process in more detail [here](docs/code_style.md).

### Design Justification

An overview of our implementation decisions can be found on the [Design Patterns page](docs/implementation_design.md).


## License
This project is licensed under **CC BY-NC-ND 4.0**.  
You may use the code for personal or research purposes only.  
Commercial use and derivative works are not permitted.  
See the full license in the [LICENSE](LICENSE) file.
