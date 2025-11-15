# Test Suite Overview

This project uses pytest with `pytest-order` and `pytest-dependency` to ensure
deterministic test sequencing.

Test groups:
- **test_db_basic / test_db_files** — relational, document, and graph connector tests.
- **test_kg_triples** — KnowledgeGraph parsing, ID to name mapping, and graph helpers.
- **examples-db**, **examples-llm** — minimal deterministic datasets used by the tests.

Run the suite with:

```bash
pytest .
```

Or (if Docker is set up):
```bash
make docker-all-dbs
make docker-test
```
