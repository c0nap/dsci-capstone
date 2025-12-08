# Use a small python base, include the minor version for reproducibility
FROM python:3.12-slim

# Connect the generated Docker image to this repository
LABEL org.opencontainers.image.source="https://github.com/c0nap/dsci-capstone"

# Enable relative paths - helpful name for container's root folder
WORKDIR /pipeline

# Make Python stdout/stderr unbuffered so logs show immediately
ENV PYTHONUNBUFFERED=1

# Install system dependencies in one layer
# Java installation is optional - necessary for 'stanza' / OpenIE relation extraction
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    make \
    pandoc \
    default-jre-headless \
 && pip install --upgrade pip setuptools wheel build \
 && rm -rf /var/lib/apt/lists/*

COPY deps/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_sm
RUN python -m nltk.downloader punkt punkt_tab stopwords


# Copy source code into the container (optional .dockerignore)
COPY src/ src/
COPY tests/ tests/
COPY smoke/ smoke/
COPY datasets/ datasets/

# Declare build args - whether to include .env or .env.dummy
ARG ENV_FILE

# Create .env file
COPY ${ENV_FILE} .env
COPY Makefile .
# Generate .env.docker with mapped hostnames
RUN make env-docker
RUN mv .env.docker .env

COPY pyproject.toml pytest.ini conftest.py .
# Used to merge with time-elapsed data with existing
COPY logs/elapsed_time.csv logs/

# default command
CMD ["python", "-m", "src.main"]