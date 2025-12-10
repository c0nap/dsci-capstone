#!/bin/bash
set -e

# Start Docker without needing a password
sudo service docker start

# Move into your project directory
cd /mnt/c/dsci-cap/capstone

# Activate the Python environment
source ../env-cap/bin/activate

# Run your build/start commands
make docker-workers-dev
make docker-all-dbs
make docker-blazor-silent
make docker-python-dev

# Extract results from container
make docker-copy-logs
