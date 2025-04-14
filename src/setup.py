from database_connectors import (
    RelationalConnector
)


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