# PyPitch

## Architecture Overview

```mermaid
flowchart LR
    A[Cricsheet JSON] --> B[Ingestion]
    B --> C[DuckDB Cache]
    C --> D[PyArrow Table]
    D --> E[Pandas]
```

The "Squash & Rename" Operation:

Do this: Run an interactive rebase (git rebase -i). Squash all those "swear" commits into one clean commit: feat: Implement core DuckDB ingestion pipeline.

Why: It erases the panic from history and makes you look like a pro who got it right the first time.

### Why Architecture Diagram

Add a diagram in your README showing: Cricsheet JSON -> Ingestion -> DuckDB (Cache) -> PyArrow Table -> Pandas.

* This visual proves you understand the *system*, not just the syntax.

### Modularization

Modularize the "God Object":

Break that 181-file change into distinct modules: pypitch.ingest, pypitch.storage, pypitch.metrics.

Senior Engineer Take: "I would fork this for the DuckDB logic, but I would be terrified to contribute to it because the Git history suggests the maintainer codes in a frenzy."