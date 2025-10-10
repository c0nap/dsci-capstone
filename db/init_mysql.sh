#!/bin/sh
# Exit immediately if any command fails.
# This ensures that if creating the database or granting privileges fails,
# the container will stop instead of continuing with a partially initialized database.
set -e

# Connect to MySQL as root using the root password
# Run the following SQL statements
mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<-EOSQL

-- Create the working database if it doesn't already exist.
-- Using IF NOT EXISTS ensures this is idempotent, so restarting the container
-- does not cause an error.
CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\`;

-- Grant all privileges on the working database to the application user.
-- This gives the user full permissions (CREATE, DROP, SELECT, INSERT, UPDATE, DELETE, etc.)
-- within this database only. Using '%' allows the user to connect from any host.
GRANT ALL PRIVILEGES ON \`$MYSQL_DATABASE\`.* TO '$MYSQL_USER'@'%';

-- Apply privilege changes immediately so they take effect without restarting MySQL.
FLUSH PRIVILEGES;

EOSQL
