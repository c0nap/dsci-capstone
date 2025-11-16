# Connectors Module Overview

This module provides lightweight adapters for each supported database:

- **relational.py** — MySQL/PostgreSQL connector (Factory Method via from_env)
- **document.py** — MongoDB connector
- **graph.py** — Neo4j connector
- **llm.py** — LLM connector via LangChain
- **base.py** — shared abstract base classes

All connectors expose a consistent operational interface but differ in
their underlying database engines. They are instantiated once inside the
global Session, ensuring stable configuration and preventing duplicate
connections.
