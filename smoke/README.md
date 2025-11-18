# Smoke Test Overview

This project isolates expensive tests into the `smoke` folder to keep normal PyTests quick and reusable. PyTest is still used for 

Test files:
- **test_models** — relation extraction and LLM tests.

Run the suite with:
```bash
pytest -m smoke smoke/
```

Or (if Docker is set up):
```bash
make docker-all-dbs
make docker-smoke
```
