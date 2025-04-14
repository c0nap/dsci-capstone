import os
from dotenv import load_dotenv
from database_connectors import (
    RelationalConnector
)

## Read environment variables at compile time
load_dotenv(".env")
print()

## Create database connectors.
db_connector = RelationalConnector(verbose=True)
## Test connection to default database "mysql" on the MySQL engine
db_connector.change_database("mysql")
db_connector.test_connection()
print()
## Test connection to working database ".env/DB_NAME" on the MySQL engine
database_name = os.getenv("DB_NAME")
db_connector.change_database(database_name)
already_exists = db_connector.test_connection(print_results=True)
## Ensures the working database was created
if not already_exists:
    db_connector.change_database("mysql")
    db_connector.create_database(database_name)
    db_connector.change_database(database_name)
    db_connector.test_connection(print_results=True)
print()

db_connector.execute_file("./tests/reset.sql")
db_connector.execute_file("./tests/example1.sql")
db_connector.execute_file("./tests/example2.sql")
df = db_connector.get_dataframe("EntityName")
print(f"EntityName table:\n{df}\n")
df = db_connector.get_dataframe("ExampleEAV")
print(f"ExampleEAV table:\n{df}\n")
