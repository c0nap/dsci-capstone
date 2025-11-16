# Source Directory Overview

All application code lives inside this directory, organized into clear
responsibility-based modules:

- **core/** — orchestration logic (Session, Boss, Worker)
- **connectors/** — adapters for relational, document, and graph databases
- **components/** — pipeline logic (NLP, metrics, corpus processing, book parsing)
- **main.py / util.py** — entry points and shared logging / error-handling utilities

This layout separates low-level adapters, high-level pipeline operations,
and global orchestrators, improving maintainability and readability.
