.PHONY: up down logs python logs-python db shell-blazor
up:
	docker compose up -d

down:
	docker compose down

db:
	docker compose up -d db

python:
	docker compose up python

logs:
	docker compose logs -f

logs-python:
	docker compose logs -f python

shell-blazor:
	docker compose exec blazor sh

prune:
	docker system prune -f


DATABASES = MYSQL POSTGRES MONGO NEO4J
DB_SERVICES = mysql postgresql mongod neo4j
BLAZOR_DB_KEYS = MySql PostgreSql MongoDb Neo4j PythonApi

###############################################################################
# Start only the 'localhost' databases specified in .env
###############################################################################
.PHONY: db-start-local
db-start-local:
	@echo "=== Checking local databases... ==="
	@i=0; \
	for db in $(DATABASES); do \
		host=$$(grep "^$${db}_HOST=" .env | cut -d= -f2 | tr -d '\r\n'); \
		service=$$(echo $(DB_SERVICES) | cut -d' ' -f$$((i+1))); \
		if [ "$$host" = "localhost" ]; then \
			echo "=== Starting $$db ($$service)... ==="; \
			sudo service $$service start; \
		fi; \
		i=$$((i+1)); \
	done




# Helper functions used by the Dockerfiles
# 	- Generates .env.docker and appsettings.Docker.json for containerized deployments
# 	- Uses values from .env to swap hostnames inside Docker containers

# Run each recipe (function) in a single shell so helper functions and variables persist
.ONESHELL:

# Keep output clean - dont echo commands
.SILENT:

# File path variables (smaller footprint)
ENV_FILE := .env
ENV_DOCKER := .env.docker
APPSET := web-app/BlazorApp/appsettings.json
APPSET_DOCKER := web-app/BlazorApp/appsettings.Docker.json

# VERBOSE=1 prints detailed mapping info for debugging
VERBOSE ?= 0

###############################################################################
# Detect whether Make is running on Windows (OS) or WSL
###############################################################################
define detect_system_fn
detect_system() {
	if [ -n "$$WSL_DISTRO_NAME" ] || ( [ -f /proc/version ] && grep -qi microsoft /proc/version ); then
		echo "WSL"
	elif [ "$$OS" = "Windows_NT" ] || [ -d "/c/Windows" ] || ( command -v uname >/dev/null 2>&1 && uname -o 2>/dev/null | grep -qi msys ); then
		echo "OS"
	else
		echo "UNKNOWN"
	fi
}
endef

###############################################################################
# Classify a hostname into a table row category; see map_service()
###############################################################################
define classify_value_fn
classify_value() {
	val="$$1"
	os_ip="$$2"
	wsl_ip="$$3"
	if [ "$$val" = "$$os_ip" ]; then
		echo "Native OS"
	elif [ "$$val" = "$$wsl_ip" ]; then
		echo "Native WSL"
	elif [ "$$val" = "localhost" ] || [ "$$val" = "127.0.0.1" ]; then
		echo "Same Container"
	elif echo "$$val" | grep -Eq '^[A-Za-z0-9_-]+$$'; then
		echo "Parallel Container"
	else
		echo "External Container"
	fi
}
endef

###############################################################################
# Determine which hostname column (desktop | ce) to use
# Compares runtime location to PYTHON_SIDE
###############################################################################
define choose_mode_fn
choose_mode() {
	runtime="$$1"
	python_side="$$2"
	if [ "$$runtime" = "$$python_side" ]; then
		if [ "$$runtime" = "WSL" ]; then
			echo "ce"
		else
			echo "desktop"
		fi
	else
		# runtime and python_side differ: choose column according to mapping rules
		if [ "$$runtime" = "WSL" ] && [ "$$python_side" = "OS" ]; then
			echo "desktop"
		elif [ "$$runtime" = "OS" ] && [ "$$python_side" = "WSL" ]; then
			echo "ce"
		else
			echo "desktop"
		fi
	fi
}
endef

###############################################################################
# Map a classification to the appropriate hostname based on column
# Matches reference mapping table:
# | Service Location    | Docker Desktop (desktop) | docker-ce (ce) |
# | Native OS           | host.docker.internal     | OS_LOCAL_IP     |
# | Native WSL          | WSL_LOCAL_IP             | localhost       |
# | Parallel Container  | service_name             | service_name    |
# | External Container  | WSL_LOCAL_IP             | OS_LOCAL_IP     |
# | Same Container      | localhost                | localhost       |
###############################################################################
###############################################################################
# Map a classification to the appropriate hostname based on column
###############################################################################
define map_service_fn
map_service() {
	val_original="$$1"
	class="$$2"
	mode="$$3"
	os_ip="$$4"
	wsl_ip="$$5"
	case "$$class" in
		"Native OS")
			if [ "$$mode" = "desktop" ]; then
				echo "host.docker.internal"
			else
				echo "$$os_ip"
			fi
			;;
		"Native WSL")
			if [ "$$mode" = "desktop" ]; then
				echo "$$wsl_ip"
			else
				echo "localhost"
			fi
			;;
		"Parallel Container")
			# Always use the original service name for parallel containers (e.g., 'python', 'db')
			echo "$$val_original"
			;;
		"External Container")
			# External Container mapping from table: used for cross-Docker communication on the same machine
			# If the original value isn't a known IP, we assume it's a truly external/public IP and preserve it.
			if [ "$$val_original" = "$$os_ip" ] || [ "$$val_original" = "$$wsl_ip" ]; then
				if [ "$$mode" = "desktop" ]; then
					echo "$$wsl_ip"
				else
					echo "$$os_ip"
				fi
			else
				# Preserve the IP (public IP, external DNS, etc.)
				echo "$$val_original"
			fi
			;;
		"Same Container")
			echo "localhost"
			;;
		*)
			echo "**UNKNOWN**"
			;;
	esac
}
endef

###############################################################################
# Modify a connection string to swap out the hostname only
# Portable sed-escaping and in-place-safe replacements (no -i, no .bak)
###############################################################################
define replace_host_fn
replace_host_in_connstring() {
	file="$$1"
	orig="$$2"
	new="$$3"
	# escape for use in sed (delimiter '@'): backslashes, @, and &
	sed_escape() {
		printf '%s' "$$1" | sed -e 's/\\/\\\\/g' -e 's/@/\\@/g' -e 's/&/\\&/g'
	}

	orig_escaped=$$(sed_escape "$$orig")
	new_escaped=$$(sed_escape "$$new")

	# Use a safe temp file for in-place updates (portable)
	tmp=$$(mktemp 2>/dev/null || printf '/tmp/.env.tmp.%s' "$$RANDOM")
	
	# Apply targeted replacement: bolt://host:port -> bolt://new:port
	# Use '@' as the sed delimiter to avoid conflicts with '/' or ':' in hostnames
	sed "s@bolt://$$orig_escaped\\(:[0-9][0-9]*\\)@bolt://$$new_escaped\\1@g" "$$file" > "$$tmp" || true

	# Replace original file with tmp (preserve mode where possible)
	if mv "$$tmp" "$$file" 2>/dev/null; then
		:
	else
		# fallback: attempt to copy
		cat "$$tmp" > "$$file" || true
		rm -f "$$tmp" || true
	fi
}
endef

###############################################################################
# docker-detect: Verify settings used for hostname mapping (diagnostic only)
###############################################################################
.PHONY: docker-detect
docker-detect:
	$(detect_system_fn)
	$(classify_value_fn)
	$(choose_mode_fn)
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "ERROR: $(ENV_FILE) missing"; \
		exit 1; \
	fi
	@OS_IP=$$(awk -F= '/^OS_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	WSL_IP=$$(awk -F= '/^WSL_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	PY_SIDE=$$(awk -F= '/^PYTHON_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	BLAZ_SIDE=$$(awk -F= '/^BLAZOR_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	RUNTIME=$$(detect_system); \
	if [ "$$PY_SIDE" = "OS" ] || [ "$$PY_SIDE" = "WSL" ]; then \
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE"); \
	else \
		PY_SIDE=UNKNOWN; \
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE"); \
	fi; \
	echo "Runtime: $$RUNTIME | PYTHON_SIDE: $$PY_SIDE | Column: $$MODE"; \
	if [ "$(VERBOSE)" = "1" ]; then \
		echo "	OS_LOCAL_IP=$$OS_IP, WSL_LOCAL_IP=$$WSL_IP"; \
		echo "	detect_system says this container is running on $$RUNTIME"; \
		if [ "$$MODE" = "desktop" ]; then \
			echo "NOTE: Python container should be deployed to Docker Desktop"; \
		elif [ "$$MODE" = "ce" ]; then \
			echo "NOTE: Python container should be deployed to docker-ce"; \
		fi; \
		echo ".env-var PYTHON_SIDE: hostnames in '.env' are relative to $$PY_SIDE"; \
		echo ".env-var BLAZOR_SIDE: connection strings in 'appsettings.json' are relative to $$BLAZ_SIDE"; \
		if [ "$$BLAZ_SIDE" = "$$PY_SIDE" ]; then \
			echo "	docker-appsettings will copy directly from '.env'"; \
		else \
			echo "	docker-appsettings must map '.env' host IPs from $$PY_SIDE to $$BLAZ_SIDE"; \
		fi; \
	fi

###############################################################################
# docker-env: Generate .env.docker with Docker-appropriate hostnames
###############################################################################
.PHONY: docker-env
docker-env:
	$(detect_system_fn)
	$(classify_value_fn)
	$(choose_mode_fn)
	$(map_service_fn)
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "ERROR: $(ENV_FILE) not found"; \
		exit 1; \
	fi
	@OS_IP=$$(awk -F= '/^OS_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	WSL_IP=$$(awk -F= '/^WSL_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	if [ -z "$$OS_IP" ] || [ -z "$$WSL_IP" ]; then \
		echo "ERROR: OS_LOCAL_IP and WSL_LOCAL_IP must be set in $(ENV_FILE)"; \
		exit 1; \
	fi; \
	PY_SIDE=$$(awk -F= '/^PYTHON_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	RUNTIME=$$(detect_system); \
	if [ "$$PY_SIDE" = "OS" ] || [ "$$PY_SIDE" = "WSL" ]; then \
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE"); \
	else \
		PY_SIDE=UNKNOWN; \
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE"); \
	fi; \
	if [ "$(VERBOSE)" = "1" ]; then \
		echo "Generating $(ENV_DOCKER) using column: $$MODE"; \
	fi; \
	\
	# Copy original file to preserve order. This also keeps PYTHON_SIDE untouched. \
	cp $(ENV_FILE) $(ENV_DOCKER); \
	\
	# Loop through all host variables (excluding PYTHON_SIDE) and perform in-place replacement. \
	for VAR in MYSQL_HOST POSTGRES_HOST MONGO_HOST NEO4J_HOST BLAZOR_HOST PYTHON_HOST; do \
		VAL_RAW=$$(awk -F= -v v="$$VAR" '$$1==v{print $$2}' $(ENV_FILE) | tr -d '\r'); \
		[ -z "$$VAL_RAW" ] && continue; \
		\
		# Resolve IP variables (e.g., ${OS_LOCAL_IP}) in the value \
		VAL=$$(printf '%s' "$$VAL_RAW" | sed "s/\$${OS_LOCAL_IP}/$$OS_IP/g; s/\$${WSL_LOCAL_IP}/$$WSL_IP/g; s/\$$OS_LOCAL_IP/$$OS_IP/g; s/\$$WSL_LOCAL_IP/$$WSL_IP/g"); \
		\
		CLASS=$$(classify_value "$$VAL" "$$OS_IP" "$$WSL_IP"); \
		MAPPED=$$(map_service "$$VAL" "$$CLASS" "$$MODE" "$$OS_IP" "$$WSL_IP"); \
		\
		if [ "$$MAPPED" = "**UNKNOWN**" ]; then \
			MAPPED="$$VAL"; \
		fi; \
		\
		# In-place replacement logic (portable): Target the line starting with VAR= \
		MAPPED_ESCAPED=$$(echo "$$MAPPED" | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\&/g'); \
		tmp=$$(mktemp 2>/dev/null || printf '/tmp/.env.tmp.%s' "$$RANDOM"); \
		\
		# sed command: Find line starting with VAR= and replace everything after the '=' with MAPPED_ESCAPED \
		sed "s:^$$VAR=.*:$$VAR=$$MAPPED_ESCAPED:g" "$(ENV_DOCKER)" > "$$tmp" || true; \
		\
		# Move the temp file back to the destination (portable safe move) \
		if mv "$$tmp" "$(ENV_DOCKER)" 2>/dev/null; then \
			:; \
		else \
			cat "$$tmp" > "$(ENV_DOCKER)" || true; \
			rm -f "$$tmp" || true; \
		fi; \
		\
		if [ "$(VERBOSE)" = "1" ]; then \
			echo "  $$VAR: $$VAL -> $$MAPPED ($$CLASS)"; \
		fi; \
	done; \
	echo "✓ Generated $(ENV_DOCKER)"


###############################################################################
# docker-appsettings: Generate appsettings.Docker.json with Docker-appropriate hostnames
###############################################################################
.PHONY: docker-appsettings
docker-appsettings:
	$(detect_system_fn)
	$(classify_value_fn)
	$(choose_mode_fn)
	$(map_service_fn)
	$(replace_host_fn)
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "ERROR: $(ENV_FILE) missing"; \
		exit 1; \
	fi
	@if [ ! -f "$(APPSET)" ]; then \
		echo "ERROR: $(APPSET) missing"; \
		exit 1; \
	fi
	@OS_IP=$$(awk -F= '/^OS_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	WSL_IP=$$(awk -F= '/^WSL_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	if [ -z "$$OS_IP" ] || [ -z "$$WSL_IP" ]; then \
		echo "ERROR: OS_LOCAL_IP and WSL_LOCAL_IP must be set in $(ENV_FILE)"; \
		exit 1; \
	fi; \
	PY_SIDE=$$(awk -F= '/^PYTHON_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	BLAZ_SIDE=$$(awk -F= '/^BLAZOR_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r'); \
	RUNTIME=$$(detect_system); \
	\
	if [ "$$BLAZ_SIDE" != "OS" ] && [ "$$BLAZ_SIDE" != "WSL" ]; then \
		echo "ERROR: BLAZOR_SIDE must be 'WSL' or 'OS' (got $$BLAZ_SIDE)"; \
		exit 1; \
	fi; \
	# Determine the mapping mode (MODE) based on BLAZOR_SIDE and PYTHON_SIDE \
	MODE=$$(choose_mode "$$PY_SIDE" "$$BLAZ_SIDE"); \
	echo "Mode = $$MODE, from runtime = $$PY_SIDE and python_side = $$BLAZ_SIDE"; \
	\
	if [ "$(VERBOSE)" = "1" ]; then \
		echo "Generating $(APPSET_DOCKER) using column: $$MODE"; \
		if [ "$$BLAZ_SIDE" != "$$PY_SIDE" ]; then \
			echo "	mapping '.env' hostnames from PYTHON_SIDE ($$PY_SIDE) to BLAZOR_SIDE ($$BLAZ_SIDE)"; \
		else \
			echo "	copying '.env' hostnames since PYTHON_SIDE == BLAZOR_SIDE"; \
		fi; \
	fi; \
	cp $(APPSET) $(APPSET_DOCKER); \
	\
	# Services to check in appsettings.json, corresponding to BLAZOR_DB_KEYS \
	for SERVICE_KEY in $(BLAZOR_DB_KEYS); do \
		# 1. Determine the corresponding .env variable name (e.g., Neo4j -> NEO4J_HOST) \
		DB_VAR=$$(echo "$$SERVICE_KEY" | tr '[:lower:]' '[:upper:]' | sed 's/API/_\0/'); \
		DB_VAR=$$(echo "$$DB_VAR" | sed 's/\(SQL\|DB\)\(.\)/\1_\2/'); \
		ENV_HOST_VAR=$$(echo "$$DB_VAR" | sed 's/MYSQL/MY_SQL/' | sed 's/POSTGRESQL/POSTGRES_QL/' | sed 's/MONGODB/MONGO_DB/')_HOST; \
		ENV_HOST_VAR=$$(echo "$$ENV_HOST_VAR" | sed 's/_QL//' | sed 's/_DB//' | sed 's/MY_SQL/MYSQL/'); \
		\
		# 2. Extract the current host from appsettings.json (JSON_HOST) \
		CONN_STRING=$$(grep -E "\"$$SERVICE_KEY\"[[:space:]]*:[[:space:]]*\"" "$(APPSET_DOCKER)" | head -n 1); \
		[ -z "$$CONN_STRING" ] && continue; \
		JSON_HOST=$$(echo "$$CONN_STRING" | sed -E 's/.*bolt:\/\/(.*):[0-9]+".*/\1/'); \
		\
		# Check if extraction was successful \
		if [ -z "$$JSON_HOST" ] || [ "$$JSON_HOST" = "$$CONN_STRING" ]; then \
			if [ "$(VERBOSE)" = "1" ]; then \
				echo "  WARNING: Could not reliably extract host for $$SERVICE_KEY. Skipping."; \
			fi; \
			continue; \
		fi; \
		\
		# 3. Extract and resolve the ground truth host from .env (ENV_HOST_VAL) \
		ENV_HOST_RAW=$$(awk -F= -v v="$$ENV_HOST_VAR" '$$1==v{print $$2}' $(ENV_FILE) | tr -d '\r'); \
		[ -z "$$ENV_HOST_RAW" ] && continue; \
		ENV_HOST_VAL=$$(printf '%s' "$$ENV_HOST_RAW" | sed "s/\$${OS_LOCAL_IP}/$$OS_IP/g; s/\$${WSL_LOCAL_IP}/$$WSL_IP/g; s/\$$OS_LOCAL_IP/$$OS_IP/g; s/\$$WSL_LOCAL_IP/$$WSL_IP/g"); \
		\
		# 4. Resolve the JSON host to its concrete IP/Name (JSON_HOST_VAL) \
		JSON_HOST_VAL=$$(printf "%s" "$$JSON_HOST" | sed "s/\$${OS_LOCAL_IP}/$$OS_IP/g; s/\$${WSL_LOCAL_IP}/$$WSL_IP/g; s/\$$OS_LOCAL_IP/$$OS_IP/g; s/\$$WSL_LOCAL_IP/$$WSL_IP/g"); \
		\
		# 5. Choose the correct row in the hostname conversion table
		ENV_CLASS=$$(classify_value "$$ENV_HOST_VAL" "$$OS_IP" "$$WSL_IP"); \
		if [ "$$BLAZ_SIDE" != "$$PY_SIDE" ] && [ "$$ENV_CLASS" = "Same Container" ]; then \
			# Manually override 
			ENV_CLASS="Native $$PY_SIDE"; \
		fi; \
		echo "ConnectionStrings key: $$SERVICE_KEY"
		echo "	Column in table (DOCKER TYPE) = $$MODE"
		echo "	Row in table (DESTINATION) = $$ENV_CLASS"
		\
		# 6. Map the .env host to the FINAL Docker context (MODE) \
		FINAL_MAPPED=$$(map_service "$$ENV_HOST_VAL" "$$ENV_CLASS" "$$MODE" "$$OS_IP" "$$WSL_IP"); \
		\
		if [ "$$FINAL_MAPPED" = "**UNKNOWN**" ]; then \
			echo "  WARNING: Mapping value $$ENV_HOST_VAL from .env failed."; \
			continue; \
		fi; \
		\
		# 7. Validation: Compare the normalized hosts \
		if [ "$$FINAL_MAPPED" != "$$JSON_HOST_VAL" ]; then \
			echo "  WARNING: Host mismatch for $$SERVICE_KEY. .env ('$$ENV_HOST_VAL' -> '$$FINAL_MAPPED') does not match appsettings.json ('$$JSON_HOST_VAL'). Proceeding with .env as ground truth."; \
		fi; \
		\
		if [ "$(VERBOSE)" = "1" ]; then \
			echo "  Mapping $$ENV_HOST_VAL (from .env) -> $$FINAL_MAPPED (for Docker). Replacing host: $$JSON_HOST -> $$FINAL_MAPPED"; \
		fi; \
		\
		# 8. Replace the host found in the JSON ($$JSON_HOST) with the final mapped value ($$FINAL_MAPPED) \
		replace_host_in_connstring "$(APPSET_DOCKER)" "$$JSON_HOST" "$$FINAL_MAPPED"; \
	done; \
	echo "✓ Generated $(APPSET_DOCKER)"
