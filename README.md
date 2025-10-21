
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
project/		 # Parent directory (optional).
  ├── repository/
  │   ├── src/              # Entry point for the data pipeline.
  │   │   ├── main.py
  │   │   ├── setup.py
  │   │   └── util.py
  │   ├── components/       # Wrapper classes to abstract low-level data processing.
  │   │   ├── connectors.py
  │   │   ├── text_processing.py
  │   │   ├── fact_storage.py
  │   │   └── semantic_web.py
  │   ├── tests/            # Comprehensive tests for project components.
  │   │   ├── test_components.py
  │   │   └── (TODO: other tests)
  │   ├── datasets/         # Empty directory for user-downloaded or auto-downloaded datasets.
  │   ├── requirements.txt  # List of required Python packages for easier installation.
  │   └── .env.example      # Stores credentials and configuration (must be renamed to .env).
  └── venv/ 	 # Python virtual environment directory (optional, not committed).
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

#### Code Documentation with Doxygen

Pre-compiled code diagrams can be accessed from `docs/html/annotated.html`. We discuss our process in more detail [here](docs/code_style.md).

#### Database Connections / Network Troubleshooting

We have also compiled a [detailed guide](docs/database_instructions.md) addressing issues found when connecting the various components.

#### Docker Hostname Resolution

Optionally, Docker can be used to run our exact setup with test datasets and minimal configurations. [Docker Guide](docs/docker_setup.md)

#### Web Application

To extend the provided web app or to build your own, please reference our [Blazor UI Notes](docs/web_app_notes.md).