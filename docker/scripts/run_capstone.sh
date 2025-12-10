#!/bin/bash
set -e

# Start Docker without needing a password
sudo service docker start

# Clear old results (TMP)
sleep 3
docker rm container-python

# Move into your project directory
cd /mnt/c/dsci-cap/capstone

# Activate the Python environment
source ../env-cap/bin/activate

# Run your build/start commands
make docker-workers-dev
make docker-all-dbs
make docker-blazor-silent
make docker-python-dev 2>&1 | tee ./logs/output.txt

# Extract results from container
make docker-copy-logs
