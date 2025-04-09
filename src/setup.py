from util import Log
import os
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv

# Configure database connection
load_dotenv(".env")
DB = os.getenv("DB_ENGINE")   # Select credentials
db_engine = os.getenv(f"{DB}_ENGINE")
username = os.getenv(f"{DB}_USERNAME")
password = os.getenv(f"{DB}_PASSWORD")
host = os.getenv(f"{DB}_HOST")
port = os.getenv(f"{DB}_PORT")
database = os.getenv("DB_NAME")

def connection_string(database_name=database):
    return f"{db_engine}://{username}:{password}@{host}:{port}/{database_name}"

def test_connection(connection_string):
    try:
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT DATABASE();"))
            Log.print_pass(f"Successfully connected to database: {result.fetchone()[0]}\n")
        return True
    except Exception as e:
        msg = str(e).split('\n')[0]
        Log.print_fail(f"Failed to connect on {connection_string}:\n{msg}\n")
        return False

# Connect to "mysql" database and create the working database.
def create_database():
    try:
        engine = create_engine(connection_string("mysql"))
        with engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS {database};"))
            connection.execute(text(f"CREATE DATABASE {database};"))
        Log.print_pass(f"Created new database \"{database}\"\n")
        return True
    except Exception as e:
        Log.print_fail(f"Failed to create database \"{database}\"\n{e}\n")
        return False

# Execute a .sql script
def execute_sql_file(path):
    try:    # Read from file
        with open(path, 'r') as file:
            sql_script = file.read()
    except Exception as e:
        Log.print_fail(f"Failed to read file \"{path}\"\n{e}\n")
        return False
    try:    # Execute SQL commands one-by-one
        engine = create_engine(connection_string())
        with engine.connect() as connection:
            sql_commands = sql_script.split(';')
            for command in sql_commands:
                # Ignore invalid queries
                if len(command.strip()) < 3: continue
                # Print query results
                result = connection.execute(text(command))
                # Print tables via pandas
                if result.returns_rows and result.keys():
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    print(df)
            connection.commit()
            Log.print_pass(f"Finished executing \"{path}\"\n")
        return True
    except Exception as e:
        Log.print_fail(f"Error executing SQL commands: {e}")
        return False

# Connect to the main database and return the table as a Pandas DataFrame
def dataframe_from_table(name):
    try:
        engine = create_engine(connection_string())
        table = Table(name, MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM EntityName;"))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        Log.print_fail(f"Failed to fetch table \"{name}\"\n{e}\n")
        return pd.DataFrame()


print("\n")
# Test connection to default database "mysql" on the MySQL engine
test_connection(connection_string("mysql"))
# Test connection to working database ".env/DB_NAME" on the MySQL engine
already_exists = test_connection(connection_string())
# Ensures the working database was created
if not already_exists:
    create_database()
    test_connection(connection_string())

execute_sql_file("./src/reset.sql")
execute_sql_file("./src/example1.sql")
print(dataframe_from_table("EntityName"))