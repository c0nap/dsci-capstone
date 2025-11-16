
Data Science Capstone - Patrick Conan
---

# Design Patterns

## Session

### Singleton Design



### Factory Method

A naive instantiation approach would look like:
```python
if __name__ == "__main__":
    session = Session()
```

We could refine this 

### Property Access



### Why is reinitialization forbidden?

Since the singleton design is implmented in two parts (block duplicate calls to both `__new__` and `__init__`), a natural question is to consider if this second half is truly necessary.

This flawed approach could allow reinitialization of Session with new values at runtime - for example if the database connection details change unexpectedly - and could be theoretically used with dependency injection for a zero-downtime approach for critical applications.
```python
session = Session(verbose=True, graph_db=local_neo4j)
session = Session(verbose=False, graph_db=remote_neo4j)
```

We avoid this because it is non-standard and confounds the intended usage. If connection strings change, the user should restart the pipeline instead of attempting a recovery.

However, Python by design still allows assigning new values to fields at runtime. This enables:
```python
from src.core.context import session
session.graph_db = GraphConnector(...)
```

This type of manual reassignment is not intended, but provides a fallback if the object MUST be reconfigured for some reason.

