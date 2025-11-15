#!/bin/sh
# Mongo strips all color codes - VERY MESSY LOGS
ECHO_PREFIX="***** (init_monog.sh) *****   "
echo "$ECHO_PREFIX === INIT SCRIPT STARTING ==="

# Exit immediately if any command fails.
# Ensures that if any step fails, the container stops instead of continuing
# with a partially initialized database.
set -e

# Connect to MongoDB as root user.
# This assumes MONGO_INITDB_ROOT_USERNAME and MONGO_INITDB_ROOT_PASSWORD are set
# in the environment. Scripts in /docker-entrypoint-initdb.d/ run as root.
mongosh -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase "admin" <<-EOSQL

// -----------------------------------------
// Create the initial working database if it doesn't exist.
// MongoDB creates a database lazily when the first collection is created.
// We'll create a dummy collection to ensure it exists.
use $MONGO_INITDB_DATABASE;
db.createCollection("init");

use admin;
db.getUser("$MONGO_INITDB_ROOT_USERNAME");

use conan_capstone;
db.listCollections().toArray();

EOSQL


# // -----------------------------------------
# // Skip the rest of this file, and continue to login as admin.
# // This is unsafe, but necessary to create and drop databases.
# // (a secondary user could create a db, but could not elevate as dbOwner)
# // Keep the legacy code below for if we ever decide to change this.
# 
# 
# // -----------------------------------------
# // Create the secondary user if it doesn't exist.
# // This user will have full control over the initial working database
# // but will not have permissions on other databases.
# //if (db.getSiblingDB("admin").getUser("$MONGO_USER") == null) {
# //    db.getSiblingDB("admin").createUser({
# //        user: "$MONGO_USER",
# //        pwd: "$MONGO_PASSWORD",
# //        roles: [
# //            // Full read/write access to the working database
# //            { role: "readWrite", db: "$MONGO_INITDB_DATABASE" },
# //            // Administrative privileges on the working database (create/drop collections, indexes)
# //            { role: "dbAdmin", db: "$MONGO_INITDB_DATABASE" }
# //        ]
# //    });
# //}
# 
# // -----------------------------------------
# // Instructions for the secondary user:
# // - To create a new database:
# //     1. Connect as $MONGO_USER
# //     2. use new_db_name
# //     3. db.createCollection("init")
# //     4. db.grantRolesToUser("$MONGO_USER", [{ role: "dbOwner", db: "new_db_name" }])
# // - They now have full control over that database only
