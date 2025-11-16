
Data Science Capstone - Patrick Conan
---

# Design Patterns

## Session

The session contains references to all pipeline components and databases. We can avoid reconstructing these objects by always using Session to access them. This also avoids reimplementing designs for similar components.

For example, GraphConnector and RelationalConnector both act like singletons (only one instance is permitted). We can keep their code focused instead on their respective tasks by creating one Session instance containing references to exactly one of each.

In this way the Session class handles singleton and dependency logic, creating **Separation of Concern** and contributing to cleaner, more understandable code.

It can be used as follows:
```python
from src.core.context import session
session.relational_db.execute_query("SELECT 1")
session.main_graph.add_triple("Alice", "ownsItem", "Alice's glasses")
```

### Singleton Design



### Factory Method

A naive instantiation approach would look like:
```python
if __name__ == "__main__":
    session = Session()
```

We refine this by exposing a factory method `get_session()` to always fetch the single instance, or create it lazily on first access.
```python
_session = None

def get_session():
    global _session
    if _session is None:
        _session = Session()
    return _session
```

In this example, `global` exposes the module-scoped variable to the local function scope.

**Advantages:** 

### Property Access

When using a factory method, 

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

