# Core Module Overview

This module contains the application's orchestration layer:
- the global **Session** (singleton)
- the **Boss** and **Worker** coordination scripts

## Pattern
- **Separation of Concerns** — Session manages dependency wiring so
  connectors and components remain focused on their own tasks

## Usage
```python
from src.core.context import session

session.relational_db.execute_query("SELECT 1")
session.main_graph.add_triple("Alice", "interactsWith", "Bob")
