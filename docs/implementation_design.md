
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

Our standard implementation involves a static `_created` property on the Session class which gets set to True inside `__init__()`. This prevents future calls to `Session()` from assigning new values to local fields.

The class also needs to override the `__new__()` static method to prevent returning a different instance

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

In this example, `global` elevates the local function-scoped variable to module scope. The internal `_instance` variable of the Session class is assigned to the global `_session` variable.

**Advantage:** Loading occurs at runtime, not at import / collection time.

This is especially useful if the class is heavy (takes a long time to initialize) or error-prone.

### Convenient Access

When using a factory method, any code wishing to use the instance must call a method and assign the result to a variable. This is cumbersome, and Python offers a workaround by overriding the module-level `__getattr__` function.

```python
def __getattr__(name):
    # Only called as a fallback if module properties do not resolve
    if name == "session":
        return get_session()
    raise AttributeError(...)
```

This treats the import as a property similar to `session = Session()`, but without the compile-time construction. It can be used directly as a variable.

```python
from src.core.context import session, get_session
session.docs_db.execute_query('{"ping": 1}')

# Also valid, but awkward to use
session = get_session()
```

### Preventing Reconfiguration

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


## Relational Database Connector

TODO - from src.connectors.relational import RelationalConnector

### Factory Method

TODO - from_env


## Relation Extraction

TODO - Implement

### Chain of Responsibility

TODO - Implement


## Pipeline Architecture

TODO - Boss & Workers

Write directly to MongoDB

TODO - Metrics: run_bookscore, run_questeval, etc

