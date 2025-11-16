# Database Example Datasets

This directory contains small, deterministic example files used by the
database-related tests. Each subfolder corresponds to a specific connector
type implemented in the project:

- **relational/** – SQL queries or fixture datasets for MySQL/PostgreSQL.
- **document/** – JSON or BSON-like structures for MongoDB tests.
- **graph/** – Cypher queries for Neo4j graph tests.

These examples are intentionally minimal so tests:
- run quickly,
- avoid external dependencies,
- stay stable across environments,
- and clearly demonstrate the expected input format for each connector.

All files here are for testing only and do not represent production data.