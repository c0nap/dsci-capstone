# Use a small python base, include the minor version for reproducibility
FROM python:3.12-slim

# Connect the generated Docker image to this repository
LABEL org.opencontainers.image.source="https://github.com/c0nap/dsci-capstone"

# Enable relative paths - helpful name for container's root folder
WORKDIR /flask

# Make Python stdout/stderr unbuffered so logs show immediately
ENV PYTHONUNBUFFERED=1

# Install system dependencies in one layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    make \
 && pip install --upgrade pip setuptools wheel build \
 && rm -rf /var/lib/apt/lists/*

COPY deps/metrics.txt .
RUN pip install --no-cache-dir -r metrics.txt
RUN python -m spacy download en_core_web_sm
RUN python -m nltk.downloader punkt punkt_tab stopwords


# Copy source code into the container (optional .dockerignore)
COPY src/ src/
COPY tests/ tests/
COPY smoke/ smoke/
COPY Makefile ./

# Declare build args - whether to include .env or .env.dummy
ARG ENV_FILE

# Create .env file
COPY ${ENV_FILE} .env
# Generate .env.docker with mapped hostnames
RUN make env-docker
RUN mv .env.docker .env

COPY pyproject.toml pytest.ini .

# Supply task as command line flag to set worker behavior
CMD ["python", "-m", "src.core.worker", "--task", "metricscore"]