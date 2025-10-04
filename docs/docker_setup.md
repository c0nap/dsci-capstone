
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

7. Verify installation

```bash
docker --version
docker compose version
```

### Installing Docker Desktop (Windows)

1. Download the Docker GUI app from the official (downloads page)[https://docs.docker.com/desktop/setup/install/windows-install/] or from Microsoft Store.

2. Installation settings: Use WSL instead of Hyper-V, and disable legacy Windows containers (all containers are Linux-based in this project; even the Blazor app can be compiled this way).

3. (Optional) Create a Docker account on their (website)[https://docs.docker.com/accounts/create-account/], open Docker Desktop, and sign in.



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

### Makefile Commands for Docker

You can safely run these commands even if your project doesn't use Docker.

#### docker-detect
Verify settings used for hostname mapping (diagnostic only).
```bash
VERBOSE=1 make docker-detect
```

#### docker-env
Generate `.env.docker` with Docker-appropriate hostnames
```bash
VERBOSE=1 make docker-env
```

#### docker-appsettings
Generate `appsettings.Docker.json` with Docker-appropriate hostnames.
```bash
VERBOSE=1 make docker-appsettings
```

### Makefile Helper Functions

- `map_service()` - Map a hostname based on a given value and row / column labels.
- `classify_value()` - Determine the row in hostname table by comparing the value to `WSL_LOCAL_IP` and `OS_LOCAL_IP`.
- `choose_mode()` - Determine which hostname column (desktop | ce) to use by comparing `PYTHON_SIDE` to auto-detected runtime.
- `detect_system()` - Detect whether Make is running on Windows (OS) or WSL.
- `detect_container()` - Detect whether Make is running in a Docker container.
- `replace_host_in_connstring()` - Modify a connection string to swap out the hostname only.



---

## Usage Guide

### Dockerfiles & Images

When Docker builds a container, the corresponding Dockerfile will copy necessary files from the original system, and prepare the environment to run your source code by installing dependencies.

We recommend using the 2 provided containers:
- **container-python** in `Dockerfile.python`
- **container-blazor** in `Dockerfile.blazor`

#### 1. Create image from Dockerfile
Images contain everything Docker will need to run the container, and Docker will build the image by executing the Dockerfile. Note: `ENTRYPOINT` and `CMD` are not executed at this stage.
```bash
docker build -f Dockerfile.python -t dsci-cap-img-python:latest .
```
- `-f Dockerfile.python` → tells Docker which Dockerfile to use.
- `-t dsci-cap-img-python:latest` → tags the image with a name and optional tag. Some organizations will use a `test` or `release` tag.
- `.` → build context to make files visible to the container.

#### 2. Run container from image
This will immediately start running your source code by executing the `CMD` or `ENTRYPOINT` line in your Dockerfile.
```bash
docker run --name container-python -it dsci-cap-img-python:latest
```
- `--name container-python` → gives the container an alias for convenience.
- `-it` → interactive terminal (python output will print to this console).

To run tests instead of the full pipeline, append your command to the end to override the entry point. Use `pytest .` to run pytests instead of the full pipeline, or `/bin/sh` to enter the container and run your own commands.
```bash
docker run --name container-python -it dsci-cap-img-python:latest pytest .
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

#### 3. List containers or images
```bash
docker ps -a
docker images
```

#### 4. Stop containers & delete images
```bash
docker stop container-python
docker rm container-python
docker rmi dsci-cap-img-python:latest
```


### Docker Compose

Instead of manually compiling & running individual Docker images, we can use Docker Compose to start several containers from pre-downloaded images.

The default Python and Blazor container images are listed here so you can load our pre-configured environments into Docker via `ghcr.io/c0nap/image_name`.
- `dsci-cap-img-python:latest`
- `dsci-cap-img-blazor:latest`

These images can be downloaded from our GitHub Container Registry. Additional information about GHCR can be found on the [official website](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

<details>
  <summary><h4>Uploading a container image to GHCR</h4></summary>

```bash
echo $GITHUB_TOKEN | docker login ghcr.io -u yourusername --password-stdin
```

Send my image to the `c0nap` namespace:
```bash
docker push ghcr.io/c0nap/dsci-cap-img-blazor:latest
```

This is not linked to my repository by default.

</details>


#### 1. Download the Docker container images
```bash
docker pull ghcr.io/c0nap/dsci-cap-img-python:latest
```
```bash
docker pull ghcr.io/c0nap/dsci-cap-img-blazor:latest
```

#### 3. Run multiple containers together

#### 

```bash
make docker-all
```
```bash
make docker-all-dbs
```










The CLI version of Docker runs on WSL, so the normal hostnames and IPs specified in `.env` should still work. The Blazor app expects IPs relative to Windows by default, so `appsettings.json` is reconfigured for WSL deployment. Similarly, the containers from Docker Desktop run from Windows. This is fine for the Blazor app, but hostnames in `.env` must be fixed. This process is automated by `make docker-env` and `make docker-appsettings` in the provided Makefile.

The typical approach would be setting the environment variables directly in `docker-compose.yml` as shown below. But doing this would definitely break `load_dotenv(".env")` in Python, requiring extra handling logic.

```yml
services:

  python:
    ...
    env_file:
      - .env        # primary (used for manual runs)
      - .env.docker # overrides for container runs (last wins)

  blazor:
    ...
    volumes:
      - ./appsettings.Docker.json:/web-app/BlazorApp/appsettings.Docker.json
```

As such, `.env` and `appsettings.json` are never modified by the program, and are just kept as ground-truth for manual runs of the pipeline. When deployed to Docker, these files are sent to their respective Docker containers, which automatically apply the necessary changes to convert between hostnames depending on the intended deployment and the observed operating system.