
Data Science Capstone - Patrick Conan
---

## Introduction

Docker can be used to run both the Python pipeline and Blazor Server simultaneously without juggling multiple entry points. The recommended setup installs Docker into WSL so the existing connection strings remain valid. We also provide instructions for Docker Desktop; this is the standard Docker GUI, but it makes hostname resolution more complex.

### Installing Docker in WSL (Ubuntu)

1. Install prerequisites `apt-transport-https`, `curl`, `gnupg`, and `lsb-release`.

Without these, you can’t fetch and trust Docker’s signing key or add an HTTPS repo.

```bash
sudo apt install -y apt-transport-https ca-certificates curl gnupg lsb-release
```

2. Add the GPG key

Apt refuses to install packages from unknown sources. The GPG key tells apt to trust packages signed by Docker.

```bash
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
```

3. Add the Docker Community Edition repository

By default, Ubuntu only knows about its own repos. Adding the Docker repo is like specifying which warehouse has the Docker software. Without it, apt won’t find `docker-ce` and related packages.

```bash
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list
```

This uses `lsb_release` to automatically fill in the URL with your Ubuntu version.

4. Run `apt update` after adding the repo

This refreshes apt’s index. Until you do, apt has no idea that Docker packages exist.
```bash
sudo apt update
```

5. Install Docker CE

Now apt knows where to find the package and can verify its signature.
```bash
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

Includes `docker-ce-cli` (enables usage of the `docker` command), `containerd.io` (to run containers), and `docker-compose-plugin` (enables Docker Compose for running multiple containers together).

6. Optional: allow running Docker without `sudo`

Adding your user to the `docker` group lets you run Docker commands without `sudo`. This is convenient, but it grants root-level access to Docker (low security risk for local development).

```bash
sudo usermod -aG docker $USER
newgrp docker
```

7. Start the Docker service.
```bash
sudo service docker start
```

8. Verify installation

```bash
docker --version
docker compose version
```

### Installing Docker Desktop (Windows)

1. Download the Docker GUI app from the official (downloads page)[https://docs.docker.com/desktop/setup/install/windows-install/] or from Microsoft Store.

2. Installation settings: Use WSL instead of Hyper-V, and disable legacy Windows containers (all containers are Linux-based in this project; even the Blazor app can be compiled this way).

3. (Optional) Create a Docker account on their (website)[https://docs.docker.com/accounts/create-account/], open Docker Desktop, and sign in.

(TBD)


---

# Quick Start

### 1. Pull Docker images
```bash
make docker-pull
```

### 2. Rebuild images with local secrets
```bash
make docker-build-dev
```

### 3. Set up databases

#### Full-Docker Setup
Switch hostnames in `.env` to Docker service names.
```bash
make env-hosts-docker OUT=".env"
```
Start databases in Docker containers.
```bash
make docker-all-dbs
```

#### Mixed Setup with Existing Databases
Instructions are in Advanced Usage section below.

### 4. Start Blazor server
```bash
make docker-blazor-silent
```
You can connect from Chrome using the LAN IP of WSL:
```powershell
(wsl hostname -I).Trim()
```

### 5. Start Python pipeline
```bash
make docker-python
```

### All-in-One:
```bash
make docker-all-main
```


---

## Advanced Usage Guide

### Dockerfiles & Images

When Docker builds a container, the corresponding Dockerfile will copy necessary files from the original system, and prepare the environment to run your source code by installing dependencies.

We recommend using the 2 provided containers:
- **container-python** in `Dockerfile.python`
- **container-blazor** in `Dockerfile.blazor`

#### 1. Create image from Dockerfile
Images contain everything Docker will need to run the container, and Docker will build the image by executing the Dockerfile. Note: `ENTRYPOINT` and `CMD` are not executed at this stage.
```bash
docker build -f Dockerfile.python -t dsci-cap-img-python-dev:latest .
```
- `-f Dockerfile.python` → tells Docker which Dockerfile to use.
- `-t dsci-cap-img-python-dev:latest` → tags the image with a name and optional tag. Some organizations will use a `test` or `release` tag.
- `.` → build context to make files visible to the container.

#### 2. Run container from image
This will immediately start running your source code by executing the `CMD` or `ENTRYPOINT` line in your Dockerfile.
```bash
docker run --name container-python -it dsci-cap-img-python-dev:latest
```
- `--name container-python` → gives the container an alias for convenience.
- `-it` → interactive terminal (python output will print to this console).

To run tests instead of the full pipeline, append your command to the end to override the entry point. Use `pytest .` to run pytests instead of the full pipeline, or `/bin/sh` to enter the container and run your own commands.
```bash
docker run --name container-python -it dsci-cap-img-python-dev:latest pytest .
```
- `-p 5055:5055` → must be used if the container wants to listen on a specific port.
- `-d` → detach the container from the current shell (no logs).

Our makefile simplifies this process by hard-coding helpful commands:
```bash
make docker-python CMD="optional_command"
```
```bash
make docker-test     # Runs pytests instead of full pipeline
```
```bash
make docker-blazor   # Starts the Blazor Server
```

#### 3. Diagnostics for containers and images

List all images:
```bash
docker images
```
```bash
docker images -f "dangling=true"
```

List all containers:
```bash
docker ps -a
```
```bash
docker compose ps -a
```

Print a snapshot of the console inside a container:
```bash
docker logs <container_name>
```

`docker exec` lets us run commands inside a container as if we were localhost. This can be useful when debugging database login issues.
```bash
docker exec container-mysql mysql -uroot -ppassword -e "SELECT 1;"
```

List root users & corresponding hostnames:
```bash
docker exec container-mysql mysql -uroot -ppassword -e "SELECT user,
 host FROM mysql.user WHERE user='root';"
```


#### 4. Stopping and removing containers
```bash
docker stop container-python
docker rm container-python
```

To delete all containers at once:
```bash
make docker-clean
```

#### 5. Deleting images
No real risk since your source code is unaffected. It may take a few minutes to rebuild everything.

```bash
make docker-delete-images
```
To delete a specific image:
```bash
docker rmi <img_name>
```

To delete dangling images after a build fails:
```bash
docker image prune
```

To delete all images manually:
```bash
docker rmi -f $(docker images -aq)
```
```bash
docker images -q | xargs -r docker rmi -f
```

#### 6. Deleting volumes
Volumes are data references inside a container to files on your host machine. They are not used at this stage of our project, and can sometimes cause unexpected behavior like persisting your database credentials.
```bash
docker volume rm $(docker volume ls -q | grep mysql)
```

#### 7. Deleting EVERYTHING from Docker
Sometimes Docker will stall indefinitely when building from a Dockerfile. The best solution is to delete all images, prune their data from disk, and restart your computer.

Our make target purges all containers & networks `make docker-clean`, and all volumes & images `make docker-delete`, but usually you must manually restart WSL via PowerShell `wsl --shutdown` to fix it.
```bash
make docker-full-reset
```

### 8. Cleaning up after Docker

(Optional) Inspect WSL for unnecessary files:
```bash
sudo apt install ncdu
sudo ncdu /var/lib
```

Delete any unused containers or images from Docker:
```bash
docker system prune
```

Downsize WSL once the space is freed (Windows 11 may have `wsl --compact` instead):
```powershell
wsl --shutdown
Optimize-VHD -Path "$env:LOCALAPPDATA\Packages\CanonicalGroupLimited.UbuntuonWindows_*\LocalState\ext4.vhdx" -Mode Full
```


# Docker Compose

Instead of manually compiling & running individual Docker images, we can use Docker Compose to start several containers from pre-downloaded images.

The default Python and Blazor container images are listed here so you can load our pre-configured environments into Docker via `ghcr.io/c0nap/dsci-capstone/image_name`.
- `dsci-cap-img-python-prod:latest`
- `dsci-cap-img-blazor-prod:latest`

These images can be downloaded from our GitHub Container Registry. Additional information about GHCR setup can be found on the [official website](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

<details>
  <summary><h4>Uploading a container image to GHCR</h4></summary>

1. Generate a GitHub token (classic) by visiting https://github.com/settings/tokens

2. Authenticate to GHCR (once). When prompted, enter your PAT key as the password.
```bash
docker login ghcr.io -u c0nap
```

3. Images are not linked to this repository by default, so add the following line to Dockerfiles. Does not change publishing behavior; just specifies where this image belongs.
```
LABEL org.opencontainers.image.source="https://github.com/c0nap/dsci-capstone"
```

4. Re-build your local image after changing the Dockerfile:
```bash
docker push ghcr.io/c0nap/image-name:latest
```

<details>
  <summary><h5>Push to our private GHCR namespace</h5></summary>

Images in the `ghcr.io/c0nap/` namespace will be private by default, but you should still avoid pushing images with baked-in secrets. Use the provided `make docker-push-dev`, or alternatively follow the remaining steps for each image.
- `dsci-cap-img-python-dev:latest`
- `dsci-cap-img-blazor-dev:latest`

Images used for automatic CI/CD tests are regenerated frequently:
- `dsci-cap-img-python-dev:RUN_ID`
- `dsci-cap-img-blazor-dev:RUN_ID`

Rename your container image so docker knows where to upload it. We use the `dev` suffix for private images meant to stay on your local machine.
```bash
docker tag <img_name> ghcr.io/c0nap/<img_name>
```

Send your image to our `c0nap` namespace:
```bash
docker push ghcr.io/c0nap/<img_name>
```

</details>

<details>
  <summary><h5>Publish as a PUBLIC repository package</h5></summary>

Images in the `ghcr.io/c0nap/dsci-capstone/` repository will be PUBLIC by default. Double check everything before manually pushing images here. We have make targets to do this safely.

A baked-in secrets file is required by our code, so building for production entails copying `.env.example` and using placeholder credentials.
```bash
make docker-build-prod
```

To verify the contents of dummy secrets files:
```bash
make env-dummy
make appsettings-dummy
```

A single command will generate credentials, build / tag with `prod`, and push to the PUBLIC repository.
```bash
make docker-publish
```

The safest approach is to rely on our CD script in Github Actions. This will always publish production-ready images using dummy credentials.

</details>

</details>


### Saving Time with Docker Images

#### 1. Download the Docker container images
```bash
make docker-pull
```

These images are public which means a login key is not required.
```bash
# docker pull ghcr.io/c0nap/dsci-capstone/<img_name>
docker pull ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest
docker pull ghcr.io/c0nap/dsci-capstone/dsci-cap-img-python-prod:latest
```

#### 2. Re-tag the images
This is done to avoid mixing up `dev` images which likely contain `.env` values baked-in, and `prod` images which are generated safely by coping `.env.example`.
```bash
docker tag ghcr.io/c0nap/dsci-capstone/dsci-cap-img-blazor-prod:latest dsci-cap-img-python-dev:latest
```
Our public Docker images will be tagged incorrectly when manually pulled. Use `make docker-pull` to avoid this, or fix the aliases using `make docker-retag-prod-dev`.


### 3. Run multiple containers together

Docker Compose lets us fully automate this process. Simply run `docker compose up` to launch Python, Blazor, and all database types inside Docker containers.

In contrast to normal Docker commands, `compose up` will attempt to automatically pull the images if not yet downloaded. This is the cleanest setup option for databases (_e.g._ MySQL, etc), and can 

To deploy only a specific container _e.g._ Blazor:
```bash
make docker-docker-silent
```
```bash
docker compose up -d blazor_service
```
- `-d` - Detach the container from the current shell. This is useful for the Blazor server since it will stay running forever.
- `_service` - Docker Compose requires service names `blazor_service` not container names like normal Docker commands `container-blazor`.
- Using existing Docker images will significantly speed up the build process. Blazor already has the compiled source code, and Python can skip dependency installation.

To stop all containers using Docker Compose:
```bash
docker compose down -v
```
- `-v` - Reset all shared data, _i.e._ volumes. Docker volumes will persist your database credentials. If you later change `.env` with a new user / password, you MUST delete the existing volume (which will delete all data from that database) or manually change the root password in container. Alternatively: `make docker-delete-volumes`
- No conditional logic can be performed inside your `docker-compose.yml`. In this project, we provide a facade container to launch all databases with `docker compose up -d databases_service` but this will launch BOTH MySQL and Postgres, irrespective of your selection in the `.env` file. No real downside, but use `make docker-all-dbs` to avoid this.

### 4. Docker Networks

In order for containers to communicate with one another using hostname = `service_name`, they must be running on the same Docker Network. Since our code uses a mix of normal Docker and Compose commands, this poses a signicant issue and is addressed by our Make targets.

#### Case 1) Docker Compose creates the network

In our `docker-compose.yml`, all containers are created on the same network `default`. Without renaming this network manually to `capstone_default`, Docker Compose will auto-complete the network name as `parent-folder-name_default`.
```yml
services:
  python_service:
    networks:
      - default

  mysql_service:
    networks:
      default:
        aliases:
          - databases_service

# Declare our docker network globally
networks:
  default:
    name: capstone_default
```
Note that we additionally grant database containers the network alias `databases_service` which does not override the `mysql_service` name. Applications can also connect using `databases_service` as long as a port is specified.

Once the network is created, the Make targets `docker-python` and `docker-blazor` will connect to an existing network called `capstone_default`.
```bash
docker network connect --alias python_service capstone_default container-python
```

This will fail if the container is already running, so we use `docker create` and `docker start` instead of the default `docker run`.

#### Case 2) Makefile creates the Docker Network

Our Make targets check if the `capstone_default` network exists, and then automatically create the network if it doesn't by calling `make docker-network`.

This entails adding appropriate labels so Docker Compose will recognize it as a valid network and connect its own containers to our existing network.
```bash
docker network create \
    --label com.docker.compose.network=default \
    --label com.docker.compose.project=capstone \
    capstone_default
```

#### Other Useful Commands
List all Docker Networks:
```bash
docker network ls
```
Print the name of the network a container is running on:
```bash
docker inspect container-neo4j --format='{{range $net,$v := .NetworkSettings.Networks}}{{$net}} {{end}}'
```


---

# Understanding the Makefile

### Terminology

- `Target` - name of the make command, _e.g._ in `make do-stuff`, the make target is `do-stuff`
- `Recipe` - body of a make target
- `Shell function` - clearly defines parameters, can return an exit code `return 0` or a string `echo`, wrapped in define / endef so make targets can recognize it, and must explicitly include in a recipe with `$(function_name_fn)`

- `POSIX` - a standard requiring systems to have basic shell functionality
- `bin/sh` - the lowest-level shell available in all environments, expected to be universal and fully POSIX-compliant
- `bin/bash` - uses a slightly different syntax which might not work on all systems
- `ONESHELL` - a Makefile parameter to run each recipe in a single shell so helper functions persist and local variables remain in scope
- `SILENT` - a Makefile parameter to disable command echo
- `/dev/null` - will delete anything fed into it
- `1>/dev/null` - will hide `stdout` (normal output of a function)
- `2>/dev/null` - will hide `stderr` (errors resulting from a function)

### Clean Syntax Tips

- The Makefile could potentially use `bin/bash` syntax, but we stick to `bin/sh` for universal compatibility with externally-sourced container images like `mysql`. 
- When checking a condition using an `if` statement, single square brackets are required for comparisons `if [ "$$VAR" = "YES" ]; then`, but not if the command returns an exit code `if ! my-function; then`, this works for `echo`, `mv`, etc).
- The `SILENT` flag means we never need to put `@` in front of lines to silence them.
- The `ONESHELL` flag changes the syntax inside recipes, and semicolons are only required as part of control statements like `if` and `for`. 
- Backslashes are required to split 1 command across multiple lines. Without `ONESHELL` they can also be used to feed a variable directly into a function: `VAR1=val \ VAR2=val \ command`.
- `$(VAR)` is for Make variables defined usually in global scope with `VAR = value1 value2`. `$$VAR` is for Shell variables usually defined with no spaces `VAR=value1` within a recipe.
- Named args can be passed with `make my-function ARG1="val-1" ARG2=2`. ARG1 and ARG2 are make variables so we use `$()` inside the recipe to let `my-function` use the passed values.
- Values can be passed to shell functions with `my-function val1 val2`. Their values can be accessed using `$$1` or `$$2` depending on the argument order, but ususally the shell function should reassign them with names `VERBOSE=$$1` and `$$VERBOSE`.
- We can also prepend args - but this creates environment variables only set for that command: `ARG1=1 ARG2=2 my-function`





---

## Environment Setup

Here we describe how to configure the `.env` file for advanced Docker setups. For example, running all components inside Docker containers, communicating with externally-hosted containers, and using a combination of Docker Desktop and docker-ce.

### Overview

Docker enables deployment of our individual pipeline components to various locations across your system. By default, Python runs on WSL and Blazor runs on Windows - this means `.env` usually contains WSL-centric hostnames, and `/web-app/BlazorApp/appsettings.json` contains Windows-centric connection strings.

When these processes are deployed to different system types, we need to change the hostnames in `.env` file. For example, if Neo4j is started from WSL, Python can access it using `NEO4J_HOST=localhost`. But if we deploy Python to a Docker container in Docker Desktop, this hostname must be changed to the local IP of WSL as seen from Windows, _e.g._ `NEO4J_HOST=172.30.63.202`.

When communicating with another container in the same Docker network, we use the service names (the headers in `docker-compose.yml`, for example `container-python` could post data to `container-blazor` using `BLAZOR_HOST=blazor_service` and `http://blazor_service:5055/api/metrics`). The container names just are used to shorten docker commands.

Refer to the conversion table below for specific network targets. Please note that `.env` is manually completed by referencing this table, and our current Makefile simply converts the hostnames to their Docker equivalents.


### Network Hostnames Table

This table displays the valid hostname (for any `*_HOST` in `.env`) when the current container is deployed to Windows (Docker Desktop application) or WSL (docker-ce binary). Rows indicate different targets that this container might want to communicate with.

| **Target Location**     | Docker Desktop (Windows) | docker-ce (WSL) |
|-------------------------|--------------------------|-----------------|
| **Native Windows**      | `host.docker.internal`   | `OS_LOCAL_IP`   |
| **Native WSL**          | `WSL_LOCAL_IP`           | `localhost`     |
| **Parallel Container**  | `service_name`           | `service_name`  |
| **External Container**  | `WSL_LOCAL_IP`           | `OS_LOCAL_IP`   |
| **Same Container**      | `localhost`              | `localhost`     |



<details>
  <summary><h2>Hostname Mapping Details</h2></summary>

When swapping from a WSL setup to a `docker-ce` setup, most hard-coded LAN IPs will still work since the CLI version of Docker runs on the same WSL instance.

Blazor is different since it expects IPs relative to Windows by default, so `appsettings.json` must be reconfigured for WSL deployment. We match up the `.env` hostnames depending on `PYTHON_SIDE` and `BLAZOR_SIDE`. If their values are not the same, we know the hostnames in Python are invalid for Blazor.

The typical approach to secret files would be setting the environment variables directly in `docker-compose.yml` as shown below. But doing this would definitely break `load_dotenv(".env")` in Python, requiring extra handling logic.

```yml
services:
  python:
    ...
    env_file:
      - .env          # primary (used for manual runs)
      - .env.docker   # overrides for container runs (last wins)
```

Instead, this process is automated by `make env-docker` and `make appsettings-docker` in the provided Makefile (see below). These create local copies of our secrets files for each container with mapped hostnames depending on 1) the intended deployment and 2) the observed operating system.

As such, the local copies of `.env` and `appsettings.json` are never modified by the program, and are kept as ground-truth for manual runs of the pipeline. Specifically, `.env` is mapped, and then used to regenerate `appsettings.json` as necessary.

</details>


### Makefile Commands for Docker

You can safely run these commands even if your project doesn't use Docker.

#### docker-detect
Verify settings used for hostname mapping (diagnostic only).
```bash
VERBOSE=1 make docker-detect
```

#### env-docker
Generate `.env.docker` with Docker-appropriate hostnames
```bash
VERBOSE=1 make env-docker
```

#### appsettings-docker
Generate `appsettings.Docker.json` with Docker-appropriate hostnames.
```bash
VERBOSE=1 make appsettings-docker
```

### Makefile Helper Functions

- `map_service()` - Map a hostname based on a given value and row / column labels.
- `classify_value()` - Determine the row in hostname table by comparing the value to `WSL_LOCAL_IP` and `OS_LOCAL_IP`.
- `choose_mode()` - Determine which hostname column (desktop | ce) to use by comparing `PYTHON_SIDE` to auto-detected runtime.
- `detect_system()` - Detect whether Make is running on Windows (OS) or WSL.
- `detect_container()` - Detect whether Make is running in a Docker container.
- `replace_host_in_connstring()` - Modify a connection string to swap out the hostname only.




## Dockerfile Optimization

Image size may not matter much for local development, but our CI/CD pipeline performs automatic testing and deployment. A large Docker image can significantly add to workflow execution time.

### Minimizing Image Size (Dockerfile)

- Avoid saving massive images inside a runner. Each GitHub Actions process has around 14 GB storage, and if we exceed that restriction, the workflow will fail.

- Revisit your `requirements.txt` and see if you can downgrade to a 'slim' version of some packages.

- For example, `torch` runs on GPU by default and has `cuda` dependencies. This adds ~6 GB to images. We can use the CPU version instead by specifying `--extra-index-url`, but as a result the `requirements.txt` is more messy.

- Execution order matters; the builder can reuse earlier layers if the only difference is something minor.

- For example, generate `.env` and read ARGS as late as possible.

- In a multi-stage dockerfile _e.g._ Blazor, LABEL must be defined after the last FROM.

### Faster Workflows

- For maximum speed, each workflow would be a single massive job. But if reusable workflows are desired for improved Separation of Concern, we need a way to transfer data between runners.

- Artifacts can be used to make compressed files available for all runners. For images, this is much faster than downloading from and uploading to GCHR.

- We still use GHCR as a checkpoint for Docker images. Successful PRs upload their tagged images to our private namespace to speed up future runs.



# Deployments

## GHCR (GitHub Container Repository)

Our latest stable images are always available on the repository [packages page](https://github.com/c0nap?tab=packages&repo_name=dsci-capstone).

Docker images are uploaded automatically via GitHub Actions. Our CD workflow runs PyTests, and then re-builds with dummy credentials.

Since they are public on GHCR, other image deployment platforms like AWS / ECS or Azure can simply pull the images directly with no login required. Publishing to Docker Hub would work the same way.

Otherwise, we would need to follow the same procedure for GHCR upload: re-tag local copies, and uplaod to platform-specific image registries.



## AWS (Amazon Web Services)

### Introduction

GHCR only stores container images. To deploy them to the cloud, we need a container orchestration service like Amazon's **Elastic Container Service (ECS)**.

Structure of ECS: Cluster → Service → Task → Containers

- **Task** - A single "run" of the multi-container system.

- **Task Definition** - Similar to `docker-compose.yml`, this is a blueprint describing one or more containers, their images, environment variables, CPU/memory limits, and networking.

- **Cluster** - A managed pool of compute resources where tasks are scheduled and run.

- **Service** - Manages task execution and system load by keeping the desired number of tasks running, restarting failed ones, and optionally routing traffic through a load balancer.

- **IAM Role** - Grants permission for containers, tasks, or services to access other parts of AWS (_e.g._ Secrets Manager).

- **Service Connect** - Provides internal DNS for containers so they can reach each other using service names instead of IP addresses.

- **Virtual Private Cloud (VPC)** - A private network that defines how tasks and services communicate within AWS and with the internet.

- **Fargate** - A serverless compute engine for containers. AWS hides the low-level EC2 instances behind the cluster.

### Initial Setup Guide

1. Create an account (Free Tier) on the [AWS website](https://aws.amazon.com/free/).

2. Pick your container service `ECS` to run your Docker images on EC2 or Fargate. `App Runner` is easier setup, and `EKS` is production-ready Kubernetes.

3. Start from the AWS management console: https://aws.amazon.com/console

4. Navigate to `ECS` page. For first-time users this page is very cluttered; click `Get Started` or just visit the [Clusters Page](https://console.aws.amazon.com/ecs/v2/clusters).

5. Create a new cluster called `dsci-cap-cluster` with blank namespace and `Fargate only`

6. **Create a task** and add your containers in the [Task Definition](https://.console.aws.amazon.com/ecs/v2/task-definitions).
- Port aliases are arbitrary, and do not correspond to Docker container names or service names.
- Stick to small resource options → `0.25 vCPU` and `0.5 GB memory`, and divide these values between your containers.
- Note: In testing, the main Python container required `0.75 vCPU` and `3 GB memory` to get past initial deployment.

7. **Create a service** to enable `Service Connect` - this lets us assign the correct Docker service names (like `blazor_service`).
- [Create new service](https://console.aws.amazon.com/ecs/v2/clusters/dsci-cap-cluster/create-service) or UI navigation: `ECS` > `Clusters` > `dsci-cap-cluster` > `Services tab` > `Create`.
- Service name `dsci-cap-service`
- In the `Service Connect` section, choose `Client and server`, select your namespace, and add port mappings (`Discovery` and `DNS` should both be `blazor_service`)
- If you haven't already, [create a namespace](https://console.aws.amazon.com/cloudmap/home/namespaces/create): Name `default`, select `API calls and DNS`, create or select `VPC`, and set `TTL = 600`.

8. Optional: Set up a **zero-spend budget** to prevent any bills: `AWS Billing` > `Budgets` > `Create budget`.


### Debug Container Logs

[Run task manually](https://console.aws.amazon.com/ecs/v2/clusters/dsci-cap-cluster/run-task) or UI navigation: `ECS` > `Clusters` > `dsci-cap-cluster` > `Tasks tab` > `Run New Task button`.

[View log streams](https://console.aws.amazon.com/cloudwatch/home#logsV2:log-groups) or UI navigation: `CloudWatch` > `Log groups` > `/ecs/dsci-capstone-task`. Do not use `/ecs/dsci-cap-service`, this contains only generic sidecar logs.

[View resource use](https://console.aws.amazon.com/ecs/v2/clusters/dsci-cap-cluster/services/dsci-cap-service/health)

### Task Definitions

[Change task settings](https://console.aws.amazon.com/ecs/v2/task-definitions/dsci-capstone-task/create-revision) or UI navigation: `ECS` > `Task Definitions` > `dsci-capstone-task` >  `Create New Revision button`.

[Restart the service scheduler with new task settings](https://console.aws.amazon.com/ecs/v2/clusters/dsci-cap-cluster/services/dsci-cap-service/update) or UI navigation: `ECS` > `Clusters` > `dsci-cap-cluster` > `Services tab` > `dsci-cap-service` > `Update Service button`.

`Update Service` will automatically deploy and start the task if `Force new deployment` is enabled.

Make sure you select your new `Task definition revision: LATEST`, or your updates will not apply to the launched service.

### Services (Task Scheduler)

An AWS service provides useful overhead to maintain persistent deployments, _i.e._ run your task forever. It doesnt make sense to use a service otherwise - just run the task manually instead.
- **Desired count** maintenance - Always keep N tasks running, or adjust based on metrics
- **Health checks** - Restart on error
- **Load balancing** - Distribute traffic across tasks
- **Rolling deployments** - Update without downtime

To stop a service from constantly redeploying your containers, you can:
- Update service with `Desired tasks = 0`
- Delete the service
- Auto-stop using a rule / schedule

[Create an EventBridge schedule](https://console.aws.amazon.com/scheduler/home#create-schedule) or UI navigation: `EventBridge` > `Schedules` > `Create Schedule button`.

<details>
  <summary><h4>Schedule creation details</h4></summary>

In our case, we named it `stop-dsci-cap-service-nightly`, and chose a `Recurring Cron-based` schedule. Type the values `0 2 * * ? *` to stop the service at 2am (UTC-4) daily. Another option is every `12 hours`.

To target our service, choose `ECS UpdateService` task, and paste the rule:
```json
{
  "Cluster": "dsci-cap-cluster",
  "Service": "dsci-cap-service",
  "DesiredCount": 0
}
```

The schedule can only `Use existing role`, so we grant permissions by creating `IAM role` > `Custom trust policy` > `Paste JSON` > `Add Permission: AmazonECS_FullAccess` > `Name: EventBridgeScheduler-ECS-Role`
```json
{
  "Version": "2012-10-01",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "scheduler.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

</details>

### Secrets

1. Store API keys in [AWS Secrets Manager](https://console.aws.amazon.com/secretsmanager/listsecrets).

2. Update your [Task Definition](https://console.aws.amazon.com/ecs/v2/task-definitions/dsci-capstone-task/create-revision) by navigating to `Container 1 (Python)` > `Environment variables`. Use the key name expected by Python, select `ValueFrom`, and paste the ARN from Secrets Manager for the value.

3. Update your [ECS execution role](https://console.aws.amazon.com/iam/home#/roles/details/ecsTaskExecutionRole) with permission policy `SecretsManagerReadWrite`.

Note: Blazor's builder will auto-detect environment variables like `Syncfusion:LicenseKey` when set in environment as `Syncfusion__LicenseKey`.

### Databases

Since we normally rely on `docker-compose.yml` to orchestrate our 3 database engines with initialization scripts and credentials, an AWS deployment would be tricky and require mirroring the Compose behavior manually.

The most reliable option is to expose databases to AWS, but this is unsafe and would break for simulaneous runs. May revisit later.





## Azure (Microsoft)

1. Create an [Azure Free or Pay-As-You-Go](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account) account.

2. Pick your container service