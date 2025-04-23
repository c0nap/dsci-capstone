
Author: Patrick Conan

Advisor: Ali Baheri

Rochester Institute of Technology

Capstone Project DSCI-601/602 w/ Travis Desell

Construction of a Flexible Fact Database for Fictional Stories

The goal of this project is to extract details from fiction books, and store facts in a knowledge database. The final output is a JSON-style list of entities and their relationships.

---

## Requirements

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

## Project Setup

1. Create a new folder directly under `C:/` for easy access from WSL, and navigate into it using the terminal.
```bash
cd /mnt/c/<folder>
```

(TODO: Adding __init__.py to a folder makes it a module    Run modules using `python -m <folder>.<script>` instead of `python <folder>/<script>.py`    Benefit: import scripts across folders)

2. (Optional) Create a virtual environment and activate it. This prevents installed packages from interfering with other Python installations on your system.
```bash
python -m venv env-cap
```
```bash
source env-cap/bin/activate
```

3. Clone the repository and place the project folder inside your created directory (next to your virtual environment).
```bash
git clone https://github.com/C0NAP/dsci-capstone.git /mnt/c/<folder>/dsci-capstone
```

4. IMPORTANT! Enter the project folder for the following steps.
```bash
cd /mnt/c/<folder>/dsci-capstone
```

5. Install required packages into your virtual environment.
```bash
python -m pip install -r requirements.txt
```

6. Copy `.env.example` to `.env`, and add your PostgreSQL credentials. Make sure `DB_ENGINE` is set to `POSTGRESQL`.

7. Run setup script to verify basic components are working.
```bash
python src/setup.py
pytest tests/test_components.py
```

10. Load example JSON into database.

---

## 3 - Modules
