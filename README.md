
Author: Patrick Conan

Advisor: Ali Baheri

Rochester Institute of Technology

Capstone Project DSCI-601/602 w/ Travis Desell

Construction of a Flexible Fact Database for Fictional Stories

The goal of this project is to extract details from fiction books, and store facts in a knowledge database. The final output is a JSON-style list of entities and their relationships.

## 1 - Requirements

- Python 3.12
- PostgreSQL or MySQL
- MongoDB

## 2 - Setup

1. Create a new folder directly under `C:/` for easy access from WSL, and navigate into it using the terminal.
```bash
cd /mnt/c/<folder>
```

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

6. Install database engines if you do not have them already.
```bash
sudo apt install postgresql
```
```bash
sudo -i -u postgres
psql
CREATE USER yourusername WITH PASSWORD 'yourpassword';
ALTER USER yourusername CREATEDB;
```
```bash

```

7. Start the database service.
```bash
sudo service mysql start
```

8. Copy `.env.example` to `.env`, and add your PostgreSQL credentials. Make sure `DB_ENGINE` is set to `POSTGRESQL`.

9. Run setup script to verify basic components are working.
```bash
python src/setup.py
```

10. Load example JSON into database.

