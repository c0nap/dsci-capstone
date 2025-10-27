# Keep output clean - dont echo commands in recipes
.SILENT:

# Keep output clean - dont notify for nested makes
MAKEFLAGS += --no-print-directory

# Run each recipe in a single shell so helper functions persist and local variables remain in scope
.ONESHELL:

# TODO: apply these variables to all make recipes
#    --verbose is reserved flag for pytest
#    VERBOSE in this file was created first (for env-docker)
VERBOSE ?= 0
VERBY ?=
COLOR ?=

###############################################################################
.PHONY: db-start-local db-start-docker db-stop-local
DATABASES = MYSQL POSTGRES MONGO NEO4J
DB_SERVICES = mysql postgresql mongod neo4j
BLAZOR_DB_KEYS = MySql PostgreSql MongoDb Neo4j PythonApi

# Start only the 'localhost' databases in .env  (on native WSL)
db-start-local:
	echo "Checking .env for localhost databases..."
	i=0
	for db in $(DATABASES); do
		# Grab host from .env ignoring comments and empty lines
		host=$$(grep -E "^[^#]*^$${db}_HOST=" .env | cut -d= -f2 | tr -d '\r\n')
		service=$$(echo $(DB_SERVICES) | cut -d' ' -f$$((i+1)))
		# Only start if host is exactly localhost
		if [ "$$host" = "localhost" ]; then
			echo "=== Starting $$db ($$service)... ==="
			sudo service $$service start
		else
			echo "[Skipping line $$db ($$host)]"
		fi
		i=$$((i+1))
	done

# Start only the '_service' databases in .env  (on docker-ce)
db-start-docker:
	echo "Checking .env for Docker databases..."
	i=0
	for db in $(DATABASES); do 
		# Grab host from .env ignoring comments and empty lines
		host=$$(grep -E "^[^#]*^$${db}_HOST=" .env | cut -d= -f2 | tr -d '\r\n')
		service=$$(echo $(DB_SERVICES) | cut -d' ' -f$$((i+1)))
		# Only start if host ends with _service
		if echo "$$host" | grep -q '_service$$'; then
			echo "=== Starting $$db via Docker ($$service)... ==="
			make docker-$$service
		else
			echo "[Skipping line $$db ($$host)]"
		fi
		i=$$((i+1))
	done

# Stop database services running locally on native WSL
db-stop-local:
	i=0
	for db_service in $(DB_SERVICES); do 
		sudo service $$db_service stop
		i=$$((i+1))
	done





###############################################################################
# Re-builds the container and runs it in the current shell.
# Accepts optional entry-point override.   make docker-python DETACHED=0 CMD="pytest ."
###############################################################################
.PHONY: docker-start
docker-start:
	# remove the container if it exists; silence errors if it doesn’t
	docker rm -f $(NAME) 2>/dev/null || true
	# create container with optional args and entrypoint
	docker create --name $(NAME) -p $(PORT) $(IMG) $(CMD)
	# add a secondary network to container (docker compose compatible) and apply service_name as alias
	make docker-network   # if does not exist
	docker network connect --alias $(HOST) $(NETWORK) $(NAME)
	# use interactive shell by default; attaches to container logs
	docker start $(if $(DETACHED),, -a -i) $(NAME)

.PHONY: docker-python docker-blazor
docker-python:
	make docker-start NAME="container-python" IMG="dsci-cap-img-python-dev:latest" \
		HOST="python_service" NETWORK="capstone_default" PORT="5054:5054" \
		DETACHED=$(DETACHED) CMD="$(CMD)"
docker-blazor:
	make docker-start NAME="container-blazor" IMG="dsci-cap-img-blazor-dev:latest" \
		HOST="blazor_service" NETWORK="capstone_default" PORT="5055:5055" \
		DETACHED=$(DETACHED) CMD="$(CMD)"
docker-questeval:
	make docker-start NAME="container-qeval" IMG="dsci-cap-img-qeval-dev:latest" \
		HOST="qeval_worker" NETWORK="capstone_default" PORT="5001:5001" \
		DETACHED=$(DETACHED) CMD="$(CMD)"
docker-bookscore:
	make docker-start NAME="container-bscore" IMG="dsci-cap-img-bscore-dev:latest" \
		HOST="bscore_worker" NETWORK="capstone_default" PORT="5002:5002" \
		DETACHED=$(DETACHED) CMD="$(CMD)"




###############################################################################
# Starts container detached (no output) so we can continue using shell
###############################################################################
docker-blazor-silent:
	make docker-blazor DETACHED=1 CMD="$(CMD)"
docker-python-silent:
	make docker-python DETACHED=1 CMD="$(CMD)"
docker-workers-silent:
	make docker-questeval DETACHED=1
	make docker-bookscore DETACHED=1

###############################################################################
# Recompile and launch containers so any source code changes will apply
###############################################################################
.PHONY: docker-python-dev docker-blazor-dev
docker-python-dev:
	make docker-build-dev-python || exit 1  # Stop if build fails
	make docker-python CMD="$(CMD)"
docker-blazor-dev:
	make docker-build-dev-blazor || exit 1  # Stop if build fails
	make docker-blazor-silent
docker-workers-dev:
	make docker-build-dev-workers || exit 1  # Stop if build fails
	make docker-workers-silent

###############################################################################
# Bypass the original pipeline and run pytests instead.
###############################################################################
.PHONY: docker-test docker-test-dev docker-test-raw docker-all-tests docker-all-main

# Run pytests using existing container images.
# Default to VERBY=0 and COLOR=1.
docker-test:
	make docker-python CMD="pytest \
		$(if $(filter 1,$(VERBY)),--log-success) \
		$(if $(filter 1,$(COLOR)),,--no-log-colors)  ."

# Recompiles docker images to test the latest source code
# Pytest will capture all console output - see non-capturing targets below.
docker-test-dev:
	make docker-build-dev-python
	make docker-test

# Default to NOT verbose, and NO colors in messages from the Log class.
docker-test-raw:
	make docker-build-dev-python
	make docker-python CMD="python -m pytest -s \
		$(if $(filter 1,$(VERBY)),--log-success) \
		$(if $(filter 1,$(COLOR)),,--no-log-colors) ."

# Shows Python print statements, but pytest output is messy.
# Default to verbose and colorful.
docker-test-fancy:
	make docker-build-dev-python
	make docker-python CMD="python -m pytest -s \
		$(if $(filter 0,$(VERBY)),,--log-success) \
		$(if $(filter 0,$(COLOR)),--no-log-colors) ."
	
# Deploy everything to docker, but only run pytests
docker-all-tests:
	make docker-all-dbs
	make docker-blazor-silent
	sleep 15   # extra time needed for neo4j
	make docker-test

# Deploy everything to docker and run the full pipeline
docker-all-main:
	make docker-all-dbs
	make docker-blazor-silent
	sleep 5   # extra time needed for neo4j, but pipeline setup takes time too
	make docker-python



###############################################################################
# Start worker services in their own containers.
###############################################################################
.PHONY: docker-all-workers docker-bscore docker-qeval
docker-all-workers:
	docker compose up -d bscore_worker
	docker compose up -d qeval_worker

###############################################################################
# Starts a relational DB, a document DB, and a graph DB in their own Docker containers
###############################################################################
.PHONY: docker-all-dbs
docker-all-dbs:
	make docker-mongo
	make docker-neo4j
	MAIN_DB=$$(awk -F= '/^DB_ENGINE=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	if [ "$$MAIN_DB" = "MYSQL" ]; then
		make docker-mysql
	elif [ "$$MAIN_DB" = "POSTGRES" ]; then
		make docker-postgres
	else
		echo "ERROR: Could not start relational DB_ENGINE; expected MYSQL or POSTGRES, but received $$MAIN_DB."
		exit 1
	fi

###############################################################################
# Create containers for individual databases
###############################################################################
.PHONY: docker-mysql docker-postgres docker-mongo docker-neo4j
docker-mysql:
	docker compose up -d mysql_service
docker-postgres:
	docker compose up -d postgres_service
docker-mongo:
	docker compose up -d mongo_service
docker-neo4j:
	docker compose up -d neo4j_service
	
	


###############################################################################
# Pulls the latest container images from GHCR, and gives them identical names to locally-generated images
###############################################################################
.PHONY: docker-pull docker-pull-python docker-pull-blazor
docker-pull:
	make docker-pull-python
	make docker-pull-blazor
docker-pull-python:
	# Python: pull, rename to local, and delete old names
	docker pull ghcr.io/c0nap/dsci-capstone/dsci-cap-img-python-prod:latest
	docker tag ghcr.io/c0nap/dsci-capstone/dsci-cap-img-python-prod:latest dsci-cap-img-python-dev:latest
	# Remove the GHCR tagged alias (keeps local image)
	docker rmi ghcr.io/c0nap/dsci-capstone/dsci-cap-img-python-prod:latest
docker-pull-blazor:
	# Blazor: pull, rename to local, and delete old names
	docker pull ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest
	docker tag ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest dsci-cap-img-blazor-dev:latest
	# Remove the GHCR tagged alias (keeps local image)
	docker rmi ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest
docker-retag-prod-dev:
	docker tag dsci-cap-img-python-prod:latest dsci-cap-img-python-dev:latest
	docker tag dsci-cap-img-blazor-prod:latest dsci-cap-img-blazor-dev:latest


###############################################################################
# Pulls images from our private namespace and gives them identical names to locally-generated images
###############################################################################
# DEVELOPMENT USE ONLY
DEVTAG ?= latest
.PHONY: docker-pull-dev docker-pull-dev-python docker-pull-dev-blazor
docker-pull-dev:
	make docker-pull-dev-python DEVTAG=$(DEVTAG)
	make docker-pull-dev-blazor DEVTAG=$(DEVTAG)
docker-pull-dev-python:
	docker pull ghcr.io/c0nap/dsci-cap-img-python-dev:$(DEVTAG)
docker-pull-dev-blazor:
	docker pull ghcr.io/c0nap/dsci-cap-img-blazor-dev:$(DEVTAG)
docker-retag-dev-local:
	docker tag ghcr.io/c0nap/dsci-cap-img-python-dev:$(DEVTAG) dsci-cap-img-python-dev:latest
	docker rmi ghcr.io/c0nap/dsci-cap-img-python-dev:$(DEVTAG)
	docker tag ghcr.io/c0nap/dsci-cap-img-blazor-dev:$(DEVTAG) dsci-cap-img-blazor-dev:latest
	docker rmi ghcr.io/c0nap/dsci-cap-img-blazor-dev:$(DEVTAG)

###############################################################################
# Creates the images locally using the Dockerfiles (for development)
###############################################################################
DOCKER_BUILD := docker buildx build --load
CACHE_ARGS ?=
.PHONY: docker-build-dev docker-build-dev-python docker-build-dev-blazor
docker-build-dev:
	make docker-build-dev-python
	make docker-build-dev-blazor
docker-build-dev-python:
	$(DOCKER_BUILD) $(CACHE_ARGS) -f docker/Dockerfile.python \
		--build-arg ENV_FILE=".env" \
		-t dsci-cap-img-python-dev:latest .
docker-build-dev-blazor:
	$(DOCKER_BUILD) $(CACHE_ARGS) -f docker/Dockerfile.blazor \
		--build-arg ENV_FILE=".env" \
		--build-arg APPSET_FILE=web-app/BlazorApp/appsettings.json \
		-t dsci-cap-img-blazor-dev:latest .

docker-build-dev-workers:
	make docker-build-dev-bscore
	make docker-build-dev-qeval
docker-build-dev-bscore:
	$(DOCKER_BUILD) $(CACHE_ARGS) -f docker/Dockerfile.bookscore \
		--build-arg ENV_FILE=".env" \
		--build-arg TASK="bookscore" \
		-t dsci-cap-img-bscore-dev:latest .
docker-build-dev-qeval:
	$(DOCKER_BUILD) $(CACHE_ARGS) -f docker/Dockerfile.questeval \
		--build-arg ENV_FILE=".env" \
		--build-arg TASK="questeval" \
		-t dsci-cap-img-qeval-dev:latest .


###############################################################################
# Generates fake credentials for the production Docker images
# Risks sharing secrets if used improperly - these are for automatic CI/CD only
###############################################################################
# DO NOT USE
.PHONY: docker-build-prod docker-build-prod-python docker-build-prod-blazor docker-retag-to-dev
docker-build-prod:
	make docker-build-prod-python
	make docker-build-prod-blazor
docker-build-prod-python:
	make env-dummy  # Generates .env.dummy from .env.example
	$(DOCKER_BUILD) $(CACHE_ARGS) -f docker/Dockerfile.python \
		--build-arg ENV_FILE=".env.dummy" \
		-t dsci-cap-img-python-prod:latest .
docker-build-prod-blazor:
	make env-dummy  # Generates .env.dummy from .env.example
	make appsettings-dummy  # Generates appsettings.Dummy.json from .env.dummy and appsettings.Example.json
	$(DOCKER_BUILD) $(CACHE_ARGS) -f docker/Dockerfile.blazor \
		--build-arg ENV_FILE=".env.dummy" \
		--build-arg APPSET_FILE=web-app/BlazorApp/appsettings.Dummy.json \
		-t dsci-cap-img-blazor-prod:latest .

###############################################################################
# Builds the latest container images, and attempt to push to this repository
# wont work until authenticated using 'docker login'
###############################################################################
# DO NOT USE
.PHONY: docker-publish docker-publish-python docker-publish-blazor
docker-publish:
	make docker-publish-python
	make docker-publish-blazor
docker-publish-python:
	# Use a dummy environment, and stop if build fails
	make docker-build-prod-python || exit 1
	# Python: tag for GHCR & push PUBLIC
	docker tag dsci-cap-img-python-prod:latest ghcr.io/c0nap/dsci-capstone/dsci-cap-img-python-prod:latest
	docker push ghcr.io/c0nap/dsci-capstone/dsci-cap-img-python-prod:latest
	# Remove the GHCR tagged alias (keeps local image)
	docker rmi ghcr.io/c0nap/dsci-capstone/dsci-cap-img-python-prod:latest
docker-publish-blazor:
	# Use a dummy environment, and stop if build fails
	make docker-build-prod-blazor || exit 1
	# Blazor: tag for GHCR & push PUBLIC
	docker tag dsci-cap-img-blazor-prod:latest ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest
	docker push ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest
	# Remove the GHCR tagged alias (keeps local image)
	docker rmi ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest


###############################################################################
# Push existing images to a private GHCR namespace.
# wont work until authenticated using 'docker login'
###############################################################################
# DEVELOPMENT USE ONLY
.PHONY: docker-push-dev docker-push-dev-python docker-push-dev-blazor
docker-push-dev:
	make docker-push-dev-python DEVTAG=$(DEVTAG)
	make docker-push-dev-blazor DEVTAG=$(DEVTAG)
docker-push-dev-python:
	# Python: tag for GHCR & push
	docker tag dsci-cap-img-python-dev:latest ghcr.io/c0nap/dsci-cap-img-python-dev:$(DEVTAG)
	docker push ghcr.io/c0nap/dsci-cap-img-python-dev:$(DEVTAG)
	# Remove the GHCR tagged alias (keeps local image)
	docker rmi ghcr.io/c0nap/dsci-cap-img-python-dev:$(DEVTAG)
docker-push-dev-blazor:
	# Blazor: tag for GHCR & push
	docker tag dsci-cap-img-blazor-dev:latest ghcr.io/c0nap/dsci-cap-img-blazor-dev:$(DEVTAG)
	docker push ghcr.io/c0nap/dsci-cap-img-blazor-dev:$(DEVTAG)
	# Remove the GHCR tagged alias (keeps local image)
	docker rmi ghcr.io/c0nap/dsci-cap-img-blazor-dev:$(DEVTAG)



###############################################################################
# Create a new docker network if it doesn't exist yet
# Add tags to ensure docker compose will recognize it
###############################################################################
.PHONY: docker-network
docker-network:
	if ! docker network inspect capstone_default >/dev/null 2>&1; then
	    docker network create \
	        --label com.docker.compose.network=default \
	        --label com.docker.compose.project=capstone \
	        capstone_default
	    echo "Created new network 'capstone_default'"
	else
		echo "Network 'capstone_default' already exists; continue..."
	fi


###############################################################################
.PHONY: docker-clean docker-delete-images docker-delete-volumes docker-delete docker-full-refresh
# Stops and removes all docker containers and networks
docker-clean:
	echo "Stopping and removing Docker Compose services..."
	docker compose down || true
	echo "Stopping all running containers..."
	docker ps -q | xargs -r docker stop || true
	echo "Removing all containers..."
	docker ps -aq | xargs -r docker rm || true
	echo "Removing all custom networks..."
	docker network ls --filter type=custom -q | xargs -r docker network rm || true
	echo "=== Docker cleanup complete! ==="
# Deletes all docker images and volumes
docker-delete:
	make docker-delete-images
	make docker-delete-volumes
	echo "=== Deleted persistent data from Docker. ==="
docker-delete-images:
	echo "Removing all images..."
	docker images -q | xargs -r docker rmi -f || true
	docker image prune -af
	docker builder prune -af
docker-delete-volumes:
	echo "Removing all volumes..."
	docker volume ls -q | xargs -r docker volume rm || true
# Deletes all docker data using the 2 above recipes
docker-full-reset:
	make docker-clean
	make docker-delete
	sudo service docker restart
	echo "=== Full Docker cleanup complete - all containers, images, volumes, and networks removed! ==="
###############################################################################
docker-python-size:
	 docker run --rm dsci-cap-img-python-dev:latest sh -c "
	  	echo '=== Largest packages ===' && \
	  	du -sh /usr/local/lib/python3.12/site-packages/* 2>/dev/null | sort -hr | head -15 && \
	  	echo '=== Torch version ===' && \
	  	python -c \"import torch; print(f'Torch: {torch.__version__}'); print(f'CUDA available: {torch.cuda.is_available()}')\""




###############################################################################
# Creates a fake secrets file (appsettings.json) for safe docker image distribution.
###############################################################################
.PHONY: appsettings-dummy appsettings-credentials appsettings-hosts-default appsettings-hosts-docker
appsettings-dummy:
	OUT_FILE=$${OUT-web-app/BlazorApp/appsettings.Dummy.json}
	ENV_FILE=$${ENVF-.env.dummy}
	echo "Generating dummy appsettings from appsettings.Example.json -> $$OUT_FILE..."
	if [ -f "$$OUT_FILE" ] && [ "$$OUT_FILE" != "web-app/BlazorApp/appsettings.Dummy.json" ]; then
		echo "$$OUT_FILE already exists, aborting..."
		exit 1
	else
		echo "$$OUT_FILE does not exist, copying..."
		cp web-app/BlazorApp/appsettings.Example.json "$$OUT_FILE"
	fi
	make appsettings-hosts-docker CONFIRMED="y" OUT="$$OUT_FILE" ENVF="$$ENV_FILE"
	make appsettings-credentials CONFIRMED="y" OUT="$$OUT_FILE" ENVF="$$ENV_FILE"
	echo "✓ Generated $$OUT_FILE"

# Replaces hostnames in appsettings.json with program defaults.
appsettings-hosts-default:
	$(replace_host_fn)
	$(replace_json_value_fn)
	OUT_FILE=$${OUT-web-app/BlazorApp/appsettings.Dummy.json}
	ENV_FILE=$${ENVF-.env.dummy}
	echo "Your hostnames in $$OUT_FILE will be overwritten with our default values."
	if [ "$$CONFIRMED" = "y" ] || [ "$$CONFIRMED" = "Y" ]; then
		response="y"
	else
		read -p "Continue? [y/N] " response
	fi
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then
		WSL_IP=$$(awk -F= '/^WSL_LOCAL_IP=/{print $$2}' $$ENV_FILE | tr -d '\r')
		replace_host_in_connstring "web-app/BlazorApp/appsettings.json" "ConnectionStrings.Neo4j" "$$WSL_IP"
		echo "Replaced hostnames in appsettings.json (default)."
	else
		echo "Aborted."
		exit 1
	fi

# Replaces hostnames in appsettings.json with Docker-CE equivalents.
appsettings-hosts-docker:
	$(replace_host_fn)
	$(replace_json_value_fn)
	OUT_FILE=$${OUT-web-app/BlazorApp/appsettings.Dummy.json}
	ENV_FILE=$${ENVF-.env.dummy}
	echo "Your hostnames in $$OUT_FILE will be overwritten with Docker service names."
	if [ "$$CONFIRMED" = "y" ] || [ "$$CONFIRMED" = "Y" ]; then
		response="y"
	else
		read -p "Continue? [y/N] " response
	fi
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then
		replace_host_in_connstring "web-app/BlazorApp/appsettings.json" "ConnectionStrings.Neo4j" "neo4j_service"
		echo "Replaced hostnames in $$OUT_FILE (full docker)."
	else
		echo "Aborted."
		exit 1
	fi

# Replaces usernames and passwords in appsettings.json with placeholder values.
appsettings-credentials:
	$(replace_json_value_fn)
	OUT_FILE=$${OUT-web-app/BlazorApp/appsettings.Dummy.json}
	ENV_FILE=$${ENVF-.env.dummy}
	echo "Your usernames and passwords in $$OUT_FILE will be overwritten with values from $$ENV_FILE file."
	if [ "$$CONFIRMED" = "y" ] || [ "$$CONFIRMED" = "Y" ]; then
		response="y"
	else
		read -p "Continue? [y/N] " response
	fi
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then
		neo4j_user=$$(awk -F= '/^NEO4J_USERNAME=/{print $$2}' $$ENV_FILE | tr -d '\r')
		replace_json_value "web-app/BlazorApp/appsettings.json" "Neo4j.Username" "$$neo4j_user"
		neo4j_password=$$(awk -F= '/^NEO4J_PASSWORD=/{print $$2}' $$ENV_FILE | tr -d '\r')
		replace_json_value "web-app/BlazorApp/appsettings.json" "Neo4j.Password" "$$neo4j_password"
		echo "Replaced credentials in $$OUT_FILE with values from $$ENV_FILE"
	else
		echo "Aborted."
		exit 1
	fi



###############################################################################
# Creates a fake secrets file (.env) for safe docker image distribution.
###############################################################################
.PHONY: env-dummy env-dummy-credentials env-hosts-default env-hosts-dev env-hosts-docker
env-dummy:
	OUT_FILE=$${OUT-.env.dummy}
	echo "Generating dummy .env from .env.example -> $$OUT_FILE..."
	if [ -f "$$OUT_FILE" ] && [ "$$OUT_FILE" != ".env.dummy" ]; then
		echo "$$OUT_FILE already exists, aborting..."
		exit 1
	else
		echo "$$OUT_FILE does not exist, copying..."
		cp .env.example "$$OUT_FILE"
	fi
	make env-hosts-docker CONFIRMED="y" OUT="$$OUT_FILE"
	make env-dummy-credentials CONFIRMED="y" OUT="$$OUT_FILE"
	echo "✓ Generated $$OUT_FILE"

# Replaces hostnames in .env with program defaults.
env-hosts-default:
	OUT_FILE=$${OUT-.env.dummy}
	echo "Your $$OUT_FILE hostnames will be overwritten with our default values."
	if [ "$$CONFIRMED" = "y" ] || [ "$$CONFIRMED" = "Y" ]; then
		response="y"
	else
		read -p "Continue? [y/N] " response
	fi
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then
		sed -i '/^[^#]*PYTHON_SIDE=/s/=.*/=WSL/' $$OUT_FILE
		sed -i '/^[^#]*BLAZOR_SIDE=/s/=.*/=OS/' $$OUT_FILE
		sed -i '/^[^#]*MYSQL_HOST=/s/=.*/=localhost/' $$OUT_FILE
		sed -i '/^[^#]*POSTGRES_HOST=/s/=.*/=localhost/' $$OUT_FILE
		sed -i '/^[^#]*MONGO_HOST=/s/=.*/=localhost/' $$OUT_FILE
		sed -i '/^[^#]*NEO4J_HOST=/s/=.*/=localhost/' $$OUT_FILE
		sed -i '/^[^#]*BLAZOR_HOST=/s|=.*|=\$$\{OS_LOCAL_IP\}|' $$OUT_FILE
		echo "Replaced hostnames (default) in $$OUT_FILE."
	else
		echo "Aborted."
		exit 1
	fi

# Replaces hostnames in .env with dev values.
env-hosts-dev:
	OUT_FILE=$${OUT-.env.dummy}
	echo "Your $$OUT_FILE hostnames will be overwritten with our development values."
	if [ "$$CONFIRMED" = "y" ] || [ "$$CONFIRMED" = "Y" ]; then
		response="y"
	else
		read -p "Continue? [y/N] " response
	fi
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then
		sed -i '/^[^#]*PYTHON_SIDE=/s/=.*/=WSL/' $$OUT_FILE
		sed -i '/^[^#]*BLAZOR_SIDE=/s/=.*/=OS/' $$OUT_FILE
		sed -i '/^[^#]*MYSQL_HOST=/s|=.*|=\$$\{OS_LOCAL_IP\}|' $$OUT_FILE
		sed -i '/^[^#]*POSTGRES_HOST=/s/=.*/=localhost/' $$OUT_FILE
		sed -i '/^[^#]*MONGO_HOST=/s|=.*|=\$$\{OS_LOCAL_IP\}|' $$OUT_FILE
		sed -i '/^[^#]*NEO4J_HOST=/s/=.*/=localhost/' $$OUT_FILE
		sed -i '/^[^#]*BLAZOR_HOST=/s|=.*|=\$$\{OS_LOCAL_IP\}|' $$OUT_FILE
		sed -i '/^[^#]*OS_LOCAL_IP=/s/=.*/=172.30.48.1/' $$OUT_FILE
		sed -i '/^[^#]*WSL_LOCAL_IP=/s/=.*/=172.30.63.202/' $$OUT_FILE
		echo "Replaced hostnames and local IPs (dev) in $$OUT_FILE."
	else
		echo "Aborted."
		exit 1
	fi

# Replaces hostnames in .env with Docker-CE equivalents.
env-hosts-docker:
	OUT_FILE=$${OUT-.env.dummy}
	echo "Your $$OUT_FILE hostnames will be overwritten with Docker service names."
	if [ "$$CONFIRMED" = "y" ] || [ "$$CONFIRMED" = "Y" ]; then
		response="y"
	else
		read -p "Continue? [y/N] " response
	fi
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then
		sed -i '/^[^#]*PYTHON_SIDE=/s/=.*/=WSL/' $$OUT_FILE
		sed -i '/^[^#]*BLAZOR_SIDE=/s/=.*/=WSL/' $$OUT_FILE
		sed -i '/^[^#]*MYSQL_HOST=/s/=.*/=mysql_service/' $$OUT_FILE
		sed -i '/^[^#]*POSTGRES_HOST=/s/=.*/=postgres_service/' $$OUT_FILE
		sed -i '/^[^#]*MONGO_HOST=/s/=.*/=mongo_service/' $$OUT_FILE
		sed -i '/^[^#]*NEO4J_HOST=/s/=.*/=neo4j_service/' $$OUT_FILE
		sed -i '/^[^#]*BLAZOR_HOST=/s/=.*/=blazor_service/' $$OUT_FILE
		echo "Replaced hostnames (full docker) in $$OUT_FILE."
	else
		echo "Aborted."
		exit 1
	fi

# Replaces usernames and passwords in .env with placeholder values.
env-dummy-credentials:
	OUT_FILE=$${OUT-.env.dummy}
	echo "Your $$OUT_FILE usernames and passwords will be overwritten with placeholders."
	if [ "$$CONFIRMED" = "y" ] || [ "$$CONFIRMED" = "Y" ]; then
		response="y"
	else
		read -p "Continue? [y/N] " response
	fi
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then
		sed -i '/^[^#]*MYSQL_USERNAME=/s/=.*/=Conan_Capstone_User_TMP/' $$OUT_FILE
		sed -i '/^[^#]*MYSQL_PASSWORD=/s/=.*/=Conan_Capstone_PASSWORD_TMP/' $$OUT_FILE
		sed -i '/^[^#]*POSTGRES_USERNAME=/s/=.*/=Conan_Capstone_User_TMP/' $$OUT_FILE
		sed -i '/^[^#]*POSTGRES_PASSWORD=/s/=.*/=Conan_Capstone_PASSWORD_TMP/' $$OUT_FILE
		sed -i '/^[^#]*MONGO_USERNAME=/s/=.*/=Conan_Capstone_User_TMP/' $$OUT_FILE
		sed -i '/^[^#]*MONGO_PASSWORD=/s/=.*/=Conan_Capstone_PASSWORD_TMP/' $$OUT_FILE
		sed -i '/^[^#]*NEO4J_USERNAME=/s/=.*/=Conan_Capstone_User_TMP/' $$OUT_FILE
		sed -i '/^[^#]*NEO4J_PASSWORD=/s/=.*/=Conan_Capstone_PASSWORD_TMP/' $$OUT_FILE
		echo "Replaced credentials (placeholder values) in $$OUT_FILE"
	else
		echo "Aborted."
		exit 1
	fi




###############################################################################
# Deletes temporary env and appsettings files (to avoid clutter)
make rm-env-appsettings:
	rm -f -v .env.dummy
	rm -f -v .env.docker
	rm -f -v web-app/BlazorApp/appsettings.Dummy.json
	rm -f -v web-app/BlazorApp/appsettings.Docker.json
###############################################################################










###############################################################################
# Helper functions used by the Dockerfiles (make env-docker, env-appsettings)
# 	- Generates .env.docker and appsettings.Docker.json for containerized deployments
# 	- Uses values from .env to swap hostnames inside Docker containers
###############################################################################

# File path variables (smaller footprint)
ENV_FILE := .env
ENV_DOCKER := .env.docker
APPSET := web-app/BlazorApp/appsettings.json
APPSET_DOCKER := web-app/BlazorApp/appsettings.Docker.json

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
# Detect whether Make is running in a Docker container
###############################################################################
define detect_container_fn
detect_container() {
	if [ -f /.dockerenv ] || grep -qa docker /proc/1/cgroup 2>/dev/null || grep -qa /docker/ /proc/self/mountinfo 2>/dev/null; then
		echo "true"
	else
		echo "false"
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
	in_container="$$4"
	runtime="$$5"  # WSL or OS
	
	if [ "$$val" = "$$os_ip" ]; then
		echo "Native OS"
	elif [ "$$val" = "$$wsl_ip" ]; then
		echo "Native WSL"
	elif [ "$$val" = "localhost" ] || [ "$$val" = "127.0.0.1" ]; then
		# localhost means different things depending on where WE are running
		if [ "$$in_container" = "false" ]; then
			echo "Same Container"
		elif [ "$$runtime" = "WSL" ]; then
			echo "Native WSL"
		else
			echo "Native OS"
		fi
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
# Map a hostname based on a given value and row / column labels
# Matches reference mapping table:
# | Service Location    | Docker Desktop (desktop) | docker-ce (ce)  |
# | Native OS           | host.docker.internal     | OS_LOCAL_IP     |
# | Native WSL          | WSL_LOCAL_IP             | WSL_LOCAL_IP    |
# | Parallel Container  | service_name             | service_name    |
# | External Container  | WSL_LOCAL_IP             | OS_LOCAL_IP     |
# | Same Container      | localhost                | localhost       |
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
				echo "$$wsl_ip"
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
# Replace a value in a JSON file using a dotted path key
# Usage: replace_json_value "file.json" "ConnectionStrings.Neo4j" "bolt://newhost:7687"
# Supports nested keys with dot notation (e.g., "Neo4j.Username")
###############################################################################
define replace_json_value_fn
replace_json_value() {
	file="$$1"
	key_path="$$2"
	new_value="$$3"
	sed_escape() {
		printf '%s' "$$1" | sed -e 's/\\/\\\\/g' -e 's/@/\\@/g' -e 's/&/\\&/g'
	}
	new_value_escaped=$$(sed_escape "$$new_value")
	last_key=$$(printf '%s' "$$key_path" | sed 's/.*\.//')
	last_key_escaped=$$(sed_escape "$$last_key")
	sed "s@\"$$last_key_escaped\"[[:space:]]*:[[:space:]]*\"[^\"]*\"@\"$$last_key_escaped\": \"$$new_value_escaped\"@g" "$$file" > "$$file.tmp" && mv "$$file.tmp" "$$file"
}
endef

###############################################################################
# Modify a connection string to swap out the hostname only
# 
# Pattern: s@bolt://[^:]*\(:[0-9][0-9]*\)@bolt://NEW_HOST\1@g
#     Matches bolt:// + any non-colon chars (the host) + captures port (:7687)
# Replaces with bolt://NEW_HOST + original port
###############################################################################
define replace_host_fn
replace_host_in_connstring() {
	file="$$1"
	key_path="$$2"
	new_host="$$3"
	last_key=$$(printf '%s' "$$key_path" | sed 's/.*\.//')
	current_value=$$(grep "\"$$last_key\"" "$$file" | sed 's/.*:[[:space:]]*"\([^"]*\)".*/\1/' | head -n 1)
	port=$$(printf '%s' "$$current_value" | sed 's/.*\(:[0-9][0-9]*\).*/\1/')
	new_connstring="bolt://$$new_host$$port"
	replace_json_value "$$file" "$$key_path" "$$new_connstring"
}
endef


###############################################################################
# Verify settings used for hostname mapping (diagnostic only)
###############################################################################
.PHONY: docker-detect
docker-detect:
	$(detect_system_fn)
	$(detect_container_fn)
	$(classify_value_fn)
	$(choose_mode_fn)
	if [ ! -f "$(ENV_FILE)" ]; then
		echo "ERROR: $(ENV_FILE) missing"
		exit 1
	fi
	OS_IP=$$(awk -F= '/^OS_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	WSL_IP=$$(awk -F= '/^WSL_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	PY_SIDE=$$(awk -F= '/^PYTHON_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	BLAZ_SIDE=$$(awk -F= '/^BLAZOR_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	RUNTIME=$$(detect_system)
	IN_CONTAINER=$$(detect_container)
	if [ "$$PY_SIDE" = "OS" ] || [ "$$PY_SIDE" = "WSL" ]; then
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE")
	else
		PY_SIDE=UNKNOWN
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE")
	fi
	echo "Runtime: $$RUNTIME  |  PYTHON_SIDE: $$PY_SIDE  |  Column: $$MODE  |  In Container: $$IN_CONTAINER"
	if [ "$(VERBOSE)" = "1" ]; then
		echo "	OS_LOCAL_IP=$$OS_IP, WSL_LOCAL_IP=$$WSL_IP"
		echo "	detect_system says this container is running on $$RUNTIME"
		if [ "$$MODE" = "desktop" ]; then
			echo "NOTE: Python container should be deployed to Docker Desktop"
		elif [ "$$MODE" = "ce" ]; then
			echo "NOTE: Python container should be deployed to docker-ce"
		fi
		echo ".env-var PYTHON_SIDE: hostnames in '.env' are relative to $$PY_SIDE"
		echo ".env-var BLAZOR_SIDE: connection strings in 'appsettings.json' are relative to $$BLAZ_SIDE"
		if [ "$$BLAZ_SIDE" = "$$PY_SIDE" ]; then
			echo "	appsettings-docker will copy directly from '.env'"
		else
			echo "	appsettings-docker must map '.env' host IPs from $$PY_SIDE to $$BLAZ_SIDE"
		fi
	fi

###############################################################################
# Generate .env.docker with Docker-appropriate hostnames
###############################################################################
.PHONY: env-docker
env-docker:
	$(detect_system_fn)
	$(detect_container_fn)
	$(classify_value_fn)
	$(choose_mode_fn)
	$(map_service_fn)
	if [ ! -f "$(ENV_FILE)" ]; then
		echo "ERROR: $(ENV_FILE) not found"
		exit 1
	fi
	OS_IP=$$(awk -F= '/^OS_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	WSL_IP=$$(awk -F= '/^WSL_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	if [ -z "$$OS_IP" ] || [ -z "$$WSL_IP" ]; then
		echo "ERROR: OS_LOCAL_IP and WSL_LOCAL_IP must be set in $(ENV_FILE)"
		exit 1
	fi
	PY_SIDE=$$(awk -F= '/^PYTHON_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	RUNTIME=$$(detect_system)
	IN_CONTAINER=$$(detect_container)
	if [ "$$PY_SIDE" = "OS" ] || [ "$$PY_SIDE" = "WSL" ]; then
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE")
	else
		PY_SIDE=UNKNOWN
		MODE=$$(choose_mode "$$RUNTIME" "$$PY_SIDE")
	fi
	if [ "$(VERBOSE)" = "1" ]; then
		echo "Generating $(ENV_DOCKER) using column: $$MODE"
	fi
	# Copy original file to preserve order. This also keeps PYTHON_SIDE untouched.
	cp $(ENV_FILE) $(ENV_DOCKER)
	# Loop through all host variables (excluding PYTHON_SIDE) and perform in-place replacement.
	for VAR in MYSQL_HOST POSTGRES_HOST MONGO_HOST NEO4J_HOST BLAZOR_HOST PYTHON_HOST; do
		VAL_RAW=$$(awk -F= -v v="$$VAR" '$$1==v{print $$2}' $(ENV_FILE) | tr -d '\r')
		[ -z "$$VAL_RAW" ] && continue

		# Resolve IP variables (e.g., ${OS_LOCAL_IP}) in the value
		VAL=$$(printf '%s' "$$VAL_RAW" | sed "s/\$${OS_LOCAL_IP}/$$OS_IP/g; s/\$${WSL_LOCAL_IP}/$$WSL_IP/g; s/\$$OS_LOCAL_IP/$$OS_IP/g; s/\$$WSL_LOCAL_IP/$$WSL_IP/g")
		CLASS=$$(classify_value "$$VAL" "$$OS_IP" "$$WSL_IP" "$$IN_CONTAINER" "$$RUNTIME")
		MAPPED=$$(map_service "$$VAL" "$$CLASS" "$$MODE" "$$OS_IP" "$$WSL_IP")
		if [ "$$MAPPED" = "**UNKNOWN**" ]; then
			MAPPED="$$VAL"
		fi

		# In-place replacement logic (portable): Target the line starting with VAR=
		MAPPED_ESCAPED=$$(echo "$$MAPPED" | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\&/g')
		tmp=$$(mktemp 2>/dev/null || printf '/tmp/.env.tmp.%s' "$$RANDOM")
		# sed command: Find line starting with VAR= and replace everything after the '=' with MAPPED_ESCAPED
		sed "s:^$$VAR=.*:$$VAR=$$MAPPED_ESCAPED:g" "$(ENV_DOCKER)" > "$$tmp" || true
		# Move the temp file back to the destination (portable safe move)
		if ! mv "$$tmp" "$(ENV_DOCKER)" 2>/dev/null; then
			cat "$$tmp" > "$(ENV_DOCKER)" || true
			rm -f "$$tmp" || true
		fi
		if [ "$(VERBOSE)" = "1" ]; then
			echo "  $$VAR: $$VAL -> $$MAPPED ($$CLASS)"
		fi
	done
	echo "✓ Generated $(ENV_DOCKER)"


###############################################################################
# Generate appsettings.Docker.json with Docker-appropriate hostnames
###############################################################################
.PHONY: appsettings-docker
appsettings-docker:
	$(detect_system_fn)
	$(detect_container_fn)
	$(classify_value_fn)
	$(choose_mode_fn)
	$(map_service_fn)
	$(replace_host_fn)
	$(replace_json_value_fn)
	if [ ! -f "$(ENV_FILE)" ]; then
		echo "ERROR: $(ENV_FILE) missing"
		exit 1
	fi
	if [ ! -f "$(APPSET)" ]; then
		echo "ERROR: $(APPSET) missing"
		exit 1
	fi
	OS_IP=$$(awk -F= '/^OS_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	WSL_IP=$$(awk -F= '/^WSL_LOCAL_IP=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	if [ -z "$$OS_IP" ] || [ -z "$$WSL_IP" ]; then
		echo "ERROR: OS_LOCAL_IP and WSL_LOCAL_IP must be set in $(ENV_FILE)"
		exit 1
	fi
	PY_SIDE=$$(awk -F= '/^PYTHON_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	BLAZ_SIDE=$$(awk -F= '/^BLAZOR_SIDE=/{print $$2}' $(ENV_FILE) | tr -d '\r')
	RUNTIME=$$(detect_system)
	IN_CONTAINER=$$(detect_container)

	if [ "$$BLAZ_SIDE" != "OS" ] && [ "$$BLAZ_SIDE" != "WSL" ]; then
		echo "ERROR: BLAZOR_SIDE must be 'WSL' or 'OS' (got $$BLAZ_SIDE)"
		exit 1
	fi
	# Determine the mapping mode (MODE) based on BLAZOR_SIDE and PYTHON_SIDE
	MODE=$$(choose_mode "$$PY_SIDE" "$$BLAZ_SIDE")
	echo "Mode = $$MODE, from runtime = $$PY_SIDE and python_side = $$BLAZ_SIDE"

	if [ "$(VERBOSE)" = "1" ]; then
		echo "Generating $(APPSET_DOCKER) using column: $$MODE"
		if [ "$$BLAZ_SIDE" != "$$PY_SIDE" ]; then
			echo "	mapping '.env' hostnames from PYTHON_SIDE ($$PY_SIDE) to BLAZOR_SIDE ($$BLAZ_SIDE)"
		else
			echo "	copying '.env' hostnames since PYTHON_SIDE == BLAZOR_SIDE"
		fi
	fi
	cp $(APPSET) $(APPSET_DOCKER)

	# Services to check in appsettings.json, corresponding to BLAZOR_DB_KEYS
	for SERVICE_KEY in $(BLAZOR_DB_KEYS); do
		# 1. Determine the corresponding .env variable name (e.g., Neo4j -> NEO4J_HOST)
		DB_VAR=$$(echo "$$SERVICE_KEY" | tr '[:lower:]' '[:upper:]' | sed 's/API/_\0/')
		DB_VAR=$$(echo "$$DB_VAR" | sed 's/\(SQL\|DB\)\(.\)/\1_\2/')
		ENV_HOST_VAR=$$(echo "$$DB_VAR" | sed 's/MYSQL/MY_SQL/' | sed 's/POSTGRESQL/POSTGRES_QL/' | sed 's/MONGODB/MONGO_DB/')_HOST
		ENV_HOST_VAR=$$(echo "$$ENV_HOST_VAR" | sed 's/_QL//' | sed 's/_DB//' | sed 's/MY_SQL/MYSQL/')
		
		# 2. Extract the current host from appsettings.json (JSON_HOST)
		CONN_STRING=$$(grep -E "\"$$SERVICE_KEY\"[[:space:]]*:[[:space:]]*\"" "$(APPSET_DOCKER)" | head -n 1)
		[ -z "$$CONN_STRING" ] && continue
		JSON_HOST=$$(echo "$$CONN_STRING" | sed -E 's/.*bolt:\/\/(.*):[0-9]+".*/\1/')
		
		# Check if extraction was successful
		if [ -z "$$JSON_HOST" ] || [ "$$JSON_HOST" = "$$CONN_STRING" ]; then
			if [ "$(VERBOSE)" = "1" ]; then
				echo "  WARNING: Could not reliably extract host for $$SERVICE_KEY. Skipping."
			fi
			continue
		fi
		
		# 3. Extract and resolve the ground truth host from .env (ENV_HOST_VAL)
		ENV_HOST_RAW=$$(awk -F= -v v="$$ENV_HOST_VAR" '$$1==v{print $$2}' $(ENV_FILE) | tr -d '\r')
		[ -z "$$ENV_HOST_RAW" ] && continue
		ENV_HOST_VAL=$$(printf '%s' "$$ENV_HOST_RAW" | sed "s/\$${OS_LOCAL_IP}/$$OS_IP/g; s/\$${WSL_LOCAL_IP}/$$WSL_IP/g; s/\$$OS_LOCAL_IP/$$OS_IP/g; s/\$$WSL_LOCAL_IP/$$WSL_IP/g")
		
		# 4. Resolve the JSON host to its concrete IP/Name (JSON_HOST_VAL)
		JSON_HOST_VAL=$$(printf "%s" "$$JSON_HOST" | sed "s/\$${OS_LOCAL_IP}/$$OS_IP/g; s/\$${WSL_LOCAL_IP}/$$WSL_IP/g; s/\$$OS_LOCAL_IP/$$OS_IP/g; s/\$$WSL_LOCAL_IP/$$WSL_IP/g")
		
		# 5. Choose the correct row in the hostname conversion table
		ENV_CLASS=$$(classify_value "$$ENV_HOST_VAL" "$$OS_IP" "$$WSL_IP" "$$IN_CONTAINER" "$$RUNTIME")
		if [ "$$BLAZ_SIDE" != "$$PY_SIDE" ] && [ "$$ENV_CLASS" = "Same Container" ]; then
			# Manually override 
			ENV_CLASS="Native $$PY_SIDE"
		fi
		echo "ConnectionStrings key: $$SERVICE_KEY"
		echo "	Column in table (DOCKER TYPE) = $$MODE"
		echo "	Row in table (DESTINATION) = $$ENV_CLASS"
		
		# 6. Map the .env host to the FINAL Docker context (MODE)
		FINAL_MAPPED=$$(map_service "$$ENV_HOST_VAL" "$$ENV_CLASS" "$$MODE" "$$OS_IP" "$$WSL_IP")
		
		if [ "$$FINAL_MAPPED" = "**UNKNOWN**" ]; then
			echo "  WARNING: Mapping value $$ENV_HOST_VAL from .env failed."
			continue
		fi
		
		# 7. Validation: Compare the normalized hosts
		if [ "$$FINAL_MAPPED" != "$$JSON_HOST_VAL" ]; then
			echo "  WARNING: Host mismatch for $$SERVICE_KEY. .env ('$$ENV_HOST_VAL' -> '$$FINAL_MAPPED') does not match appsettings.json ('$$JSON_HOST_VAL'). Proceeding with .env as ground truth."
		fi

		if [ "$(VERBOSE)" = "1" ]; then
			echo "  Mapping $$ENV_HOST_VAL (from .env) -> $$FINAL_MAPPED (for Docker). Replacing host: $$JSON_HOST -> $$FINAL_MAPPED"
		fi
		
		# 8. Replace the host found in the JSON ($$JSON_HOST) with the final mapped value ($$FINAL_MAPPED)
		replace_host_in_connstring "web-app/BlazorApp/appsettings.Docker.json" "ConnectionStrings.$(APPSET_DOCKER)" "$$FINAL_MAPPED"
	done
	echo "✓ Generated $(APPSET_DOCKER)"
