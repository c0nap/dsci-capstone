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



# --- INSTALL PLUGINS FROM /startup/neo4j-plugins.json ---
echo "$ECHO_PREFIX Installing Neo4j plugins..."
mkdir -p /plugins

PLUGIN_MANIFEST="/startup/neo4j-plugins.json"
if [ ! -f "$PLUGIN_MANIFEST" ]; then
    echo "$ECHO_PREFIX No plugin manifest found — skipping plugin install."
else
    # Extract plugin names (skip property keys)
    grep -oE '"[A-Za-z0-9._-]+": *\{' "$PLUGIN_MANIFEST" | \
    cut -d'"' -f2 | while IFS= read -r plugin; do
        [ "$plugin" = "properties" ] && continue
        echo "$ECHO_PREFIX Processing plugin: $plugin"

        versions_url=$(grep -A1 "\"$plugin\"" "$PLUGIN_MANIFEST" | grep '"versions"' | sed 's/.*"versions": *"//; s/".*//')
        location=$(grep -A1 "\"$plugin\"" "$PLUGIN_MANIFEST" | grep '"location"' | sed 's/.*"location": *"//; s/".*//')

        download_url=""
        if [ -n "$versions_url" ]; then
            echo "$ECHO_PREFIX Fetching versions from $versions_url"
            download_url=$(wget -q -O - "$versions_url" | grep -m1 '"url"' | sed 's/.*"url": *"//; s/".*//')
        fi

        if [ -n "$download_url" ]; then
            echo "$ECHO_PREFIX Downloading $plugin from $download_url"
            fname=$(basename "$download_url")
            wget -q "$download_url" -O "/plugins/$fname"
            case "$fname" in
                *.zip) unzip -qo "/plugins/$fname" -d /plugins ;;
            esac
        elif [ -n "$location" ]; then
            echo "$ECHO_PREFIX Plugin $plugin specifies location only ($location) — skipping download."
        else
            echo "$ECHO_PREFIX WARNING: No download info for $plugin — skipping."
            continue
        fi

        # Append plugin-specific config lines
        grep '"properties"' -A10 "$PLUGIN_MANIFEST" | awk -v p="$plugin" '
            $0 ~ "\""p"\"" {inblock=1; next}
            inblock && /\}/ {inblock=0}
            inblock && /": *"/ {
                gsub(/[",]/,"")
                split($0,a,":")
                print a[1]"="a[2]
            }' | while IFS= read -r prop; do
                key=$(printf '%s' "$prop" | cut -d'=' -f1)

                # Filter for supported config keys (Community Edition safe)
                case "$key" in
                    dbms.security.*|server.unmanaged_extension_classes|dbms.security.procedures.*|dbms.security.procedures.allowlist)
                        if [ -n "$key" ] && ! grep -q "^$key=" /var/lib/neo4j/conf/neo4j.conf 2>/dev/null; then
                            echo "$prop" >> /var/lib/neo4j/conf/neo4j.conf
                            echo "$ECHO_PREFIX Added config: $prop"
                        else
                            echo "$ECHO_PREFIX Skipping duplicate or empty key: $key"
                        fi
                        ;;
                    *)
                        echo "$ECHO_PREFIX Skipping unsupported config key: $key"
                        ;;
                esac
            done
    done
fi

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