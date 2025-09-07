
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

```
huggingface-cli login
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

---

## 3 - Modules

#### Generating Documentation with Doxygen

[Helpful Guide](https://www.woolseyworkshop.com/2020/06/25/documenting-python-programs-with-doxygen/)

```bash
sudo apt install doxygen
sudo apt install graphviz
cd docs
doxygen -g  # Next: Configure the generated doxyfile with settings from the Guide link
doxygen     # Generate the HTML and LaTeX
```