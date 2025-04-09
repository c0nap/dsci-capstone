
Author: Patrick Conan

Advisor: Ali Baheri

Rochester Institute of Technology

Capstone Project DSCI-601/602 w/ Travis Desell

### Requirements

- Python 3.12
- PostgreSQL

### Setup

```bash
cd <path>
```

(Optional) Create a virtual environment and activate it.
```bash
python -m venv env-cap
source env-cap/bin/activate
```

1. Install required packages:
```bash
python -m pip install -r requirements.txt
```

2. Install database engines (PostgreSQL) if you do not have them already:
```bash
sudo apt install postgresql
```

3. Start the database service.
```bash
sudo service mysql start
```

4. Copy `.env.example` to `.env`, and add your PostgreSQL credentials. Make sure `DB_ENGINE` is set to `POSTGRESQL`.

5. Run setup script to verify basic components are working.
```bash
python src/setup.py
```

6. Load example JSON into database.

