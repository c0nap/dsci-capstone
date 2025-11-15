#!/bin/sh
YELLOW='\033[33m'
WHITE='\033[0m'
ECHO_PREFIX="*** ${YELLOW}(init_neo4j.sh)${WHITE} ***  "
echo "$ECHO_PREFIX === INIT SCRIPT STARTING ==="

# Exit immediately if any command fails.
# This ensures that if any step fails, the container stops instead of continuing
# with a partially initialized database.
set -e

NEO4J_USERNAME=$1
NEO4J_PASSWORD=$2
NEO4J_PORT=$3
NEO4J_HTTP_PORT=$4
MAX_RETRIES=4
RETRY_DELAY=5

# --- SET INITIAL SETTINGS ---
echo "$ECHO_PREFIX Setting Neo4j initial password..."
neo4j-admin dbms set-initial-password "$NEO4J_PASSWORD"
# Docker Compose settings will not propagate since Neo4j is no longer the primary process
echo "server.bolt.listen_address=0.0.0.0:$NEO4J_PORT" >> /var/lib/neo4j/conf/neo4j.conf
echo "server.http.listen_address=0.0.0.0:$NEO4J_HTTP_PORT" >> /var/lib/neo4j/conf/neo4j.conf


# --- INSTALL PLUGINS ---
echo "$ECHO_PREFIX Installing Neo4j plugins..."
PLUGIN_DIR=/var/lib/neo4j/plugins
echo "$ECHO_PREFIX      Downloading GDS..."
wget -q -O "$PLUGIN_DIR/neo4j-graph-data-science-2.22.0.jar" \
  "https://github.com/neo4j/graph-data-science/releases/download/2.22.0/neo4j-graph-data-science-2.22.0.jar"
echo "$ECHO_PREFIX      Downloading n10s..."
wget -q -O "$PLUGIN_DIR/neosemantics-5.26.0.jar" \
  "https://github.com/neo4j-labs/neosemantics/releases/download/5.26.0/neosemantics-5.26.0.jar"
echo "$ECHO_PREFIX Plugin installation complete."


# --- START NEO4J SERVER IN BACKGROUND ---
# Start the main Neo4j server process in the background so cypher-shell has a server to connect to.
echo "$ECHO_PREFIX Starting Neo4j server in background..."
neo4j console &
NEO4J_PID=$!

# --- WAIT FOR NEO4J TO BE READY ---
# Even though Neo4j logs "Started", the auth system may not be fully initialized yet.
# This causes "unauthorized due to authentication failure" errors with correct credentials.
# We actively test the connection until it succeeds instead of using a fixed delay.
echo "$ECHO_PREFIX Waiting for Neo4j to be ready..."
retry_count=0
while [ $retry_count -lt $MAX_RETRIES ]; do
    if cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "RETURN 1;" >/dev/null 2>/dev/null; then
        echo "$ECHO_PREFIX Neo4j is ready!"
        break
    fi
    retry_count=`expr "$retry_count" + 1`
    if [ $retry_count -lt $MAX_RETRIES ]; then
        echo "$ECHO_PREFIX Neo4j not ready yet, retrying in ${RETRY_DELAY}s... (attempt $retry_count/$MAX_RETRIES)"
        sleep $RETRY_DELAY
    fi
done

if [ $retry_count -eq $MAX_RETRIES ]; then
    echo "$ECHO_PREFIX ERROR: Neo4j failed to become ready after `expr "$MAX_RETRIES" \* "$RETRY_DELAY"` seconds."
fi

# --- CREATE SECONDARY USER ---
# Connect as the default admin user and create a secondary user (elevating to admin role is impossible - not supported in Neo4j Community Edition).
echo "$ECHO_PREFIX Creating secondary user '$NEO4J_USERNAME' with default role..."
# Try to create the user, but ignore the error if it already exists
if cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "SHOW USERS;" 2>/dev/null | grep -q "^$NEO4J_USERNAME[[:space:]]"; then
    echo "$ECHO_PREFIX User '$NEO4J_USERNAME' already exists. Skipping creation."
else
    if cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "CREATE USER \`$NEO4J_USERNAME\` SET PASSWORD '$NEO4J_PASSWORD' CHANGE NOT REQUIRED;" >/dev/null 2>&1; then
        echo "$ECHO_PREFIX Created user '$NEO4J_USERNAME'."
    else
        echo "$ECHO_PREFIX WARNING: Failed to create user '$NEO4J_USERNAME' (already exists). This is intended behavior since volumes retain users. Continuing..."
    fi
fi


# --- ATTACH TO NEO4J ---
# Trap SIGINT and SIGTERM to stop Neo4j gracefully
# This ensures that if Docker sends a stop signal (or user presses CTRL+C),
# the background Neo4j process receives it and shuts down cleanly.
trap 'echo "$ECHO_PREFIX Received exit signal. Stopping Neo4j..."; kill -TERM "$NEO4J_PID"; wait "$NEO4J_PID"; exit 0' INT TERM

echo "$ECHO_PREFIX Completed successfully. Attaching to Neo4j..."
# Wait for Neo4j to exit so the script doesn't terminate early
wait $NEO4J_PID