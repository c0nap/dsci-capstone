# Components Module Overview

This module contains all high-level pipeline functionality:

- **Corpus** — text dataset ingestion (BookSum, NarrativeQA)
- **Relation Extractor** — NLP-based relation extraction
- **Metrics** — evaluation metrics (QuestEval, BooookScore, etc.)
- **Fact Storage** — Knowledge graph operations and semantic triple sanitization
- **Book Conversion** — Gutenberg preprocessing helpers (EPUB, HTML, TEI)

These components operate on data retrieved via the connectors and support
the full text-processing and knowledge-graph pipeline. Each class remains
independent, with dependencies injected through the global Session.
