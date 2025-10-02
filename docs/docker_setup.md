
Data Science Capstone - Patrick Conan
---

### Introduction

Docker can be used to run both the Python pipeline and Blazor Server simultaneously without juggling multiple entry points. The recommended setup installs Docker into WSL so our existing connection strings don't change. We also provide instructions for Docker Desktop, which is the typical solution but makes networking slightly more complex (must configure `.env.docker`).

### Installing Docker in WSL (Ubuntu)

See also: Neo4j installation

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

By default, Ubuntu only knows about its own repos. Adding the Docker repo is like specifying which warehouse has the Docker software. Without it, apt won’t find the `docker-ce` package.

This uses `lsb_release` to automatically fill in the URL with your Ubuntu version.

```bash
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list
```

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

### Installing Docker Desktop

### Local Environment Settings

This project uses `.env` (in conjunction with `.env.example` for WSL-centric connection strings, and `/web-app/BlazorApp/appsettings.json` (in conjunction with `appsettings.example.json`) for Windows-facing networking.

The CLI version of Docker
