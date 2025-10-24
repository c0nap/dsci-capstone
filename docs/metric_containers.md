# Metrics Microservices Architecture

Docker-based microservices for running QuestEval and BooookScore metrics with different Python versions and isolated dependencies.

## Architecture

```
        ┌──────────────┐
        │  PostgreSQL  │
        │  Book Table  │─────────┐
        └───────┬──────┘         │
                │                │
                ▼                ▲
        ┌───────┴─────┐    ┌─────┴─────┐
        │  Blazor UI  │    │    Boss   │
        │    :5055    │    │  Pipeline │──────┐
        │             │    │   :5054   │      │
        │             │    │Python 3.12│      │
        └───────┬─────┘    └─────┬─────┘      │
                │                │            │
                ▲                ▼            │
        ┌───────┴────────────────┴─┐          │
        │           MongoDB        │          │
        │           (Shared)       │          │
        └─────┬───────────────┬────┘          │
              ▲               ▲               │
     ┌────────┴────┐   ┌──────┴───────┐       │
     │  QuestEval  │   │  BooookScore │       │
     │   Worker    │   │    Worker    │       │
     │    :5001    │   │    :5002     │       │
     │ Python 3.8  │   │ Python 3.10  │       │
     └────────┬────┘   └──────┬───────┘       │
              ▼               ▼               │
              └───────┬───────┘               │
                      │                       │
              HTTP POST /callback             │
                      │                       │
                      └───────────────────────┘
```

## 📦 Project Structure

```
.                                                                     
├── src/
│   ├── main.py                           # Boss orchestration service
│   ├── flasks.py                         # Reusable worker Flask code
│   └── session.py                        # MongoDB connection manager
│                                                                     
├── components/
│   └── metrics.py
│       ├── run_questeval()               # QuestEval task handler
│       └── run_bookscore()               # BooookScore task handler
│                                                                     
├── docker/
│   ├── Dockerfile.boss                   # Boss service container
│   ├── Dockerfile.questeval              # QuestEval worker container
│   └── Dockerfile.bookscore              # BooookScore worker container
│                                                                     
├── req/
│   ├── requirements.txt                  # Boss service dependencies
│   ├── flask-worker.txt                  # Worker service base
│   ├── questeval.txt                     # QuestEval dependencies
│   └── bookscore.txt                     # BooookScore dependencies
│                                                                     
├── docker-compose.yml                    # Service orchestration
├── Dockerfile.python                     # Boss build instructions
├── Dockerfile.questeval                  # Worker build instructions
├── Dockerfile.bookscore                  # Worker build instructions
│                                                                     
├── .env                                  # Shared configuration
└── Makefile                              # Container management
```

## 🏗️ Design Decisions

### Distributed Computing with Worker Containers

**Challenge:** Our essential summary metrics are mutually exclusive. They use different Python versions and have conflicting dependencies.

**Solution:** Each Dockerfile installs only its required dependencies, preventing conflicts.

**Why separate containers:**
- QuestEval requires Python 3.8 (legacy transformers compatibility)
- BooookScore requires Python 3.10 (newer spaCy/torch)
- Boss runs Python 3.12 (modern features)

**Implementation:** Single `flasks.py` file used by all workers, with task-specific logic imported dynamically via command-line arguments.

- Same Flask codebase = consistent error handling, logging, and API contract
- Workers import task handlers lazily: `from src.metrics import run_questeval`
- Minimal base imports (flask, requests, pymongo, os, dotenv, argparse) to avoid further conflicts
- Add new workers by updating .env + docker-compose.yml
- No code changes required for new task types

### Boss Task Distribution (Async with Order Tracking)

**Challenge:** Boss distributes tasks asynchronously (may arrive/complete out of order), but needs to track original sequence.

**Implementation:**
```python
# main.py - Flask entrypoint
# Boss stores expected completion order
task_tracker[story_id]["expected_order"] = [chunk1, chunk2, chunk3, ...]
task_tracker[story_id]["completed"] = set()

# Workers complete in any order, boss tracks gaps
# On callback, find next incomplete chunk to identify progress
```

- Tasks posted by boss simultaneously via HTTP (non-blocking)
- Workers complete independently, POST callbacks when done
- Boss tracks which chunks remain incomplete vs original order
- Enables parallel processing while maintaining sequence awareness

### PostgreSQL as State Manager

**Challenge:** Workers finish tasks asynchonously, boss needs to notify Blazor UI with pipeline progress.

**Solution:**
- Boss hides Postgres from workers to minimize dependencies
- Blazor has full completion picture available to render multiple progress bars

### MongoDB as Shared Storage

**Challenge:** Workers and boss need persistent storage, both need to prevent data overwrites.

**Implementation:**
- **Boss clears data before assignment:** `$unset` existing task data to prevent stale results
- **Workers validate before processing:** Raise error if data already exists
- **Workers mark in-progress:** `$set {task.status: "in_progress"}` before computation
- **Workers save results:** `$set {task.status: "completed", task.result: {...}}`

**MongoDB Schema:**
```javascript
{
  story_id: "story_123",
  chunk_id: "chunk_5",
  text: "...",  // Original content
  questeval: {
    status: "completed",
    result: { score: 0.75, ... }
  },
  bookscore: {
    status: "in_progress"  // Another worker processing
  }
}
```

### Unmanaged MongoDB Connections

**Challenge:** Boss and workers need long-lived database connections, but Session class uses MongoEngine's context managers designed for short operations.

**Solution:** Added `get_unmanaged_handle()` to Session class.

**Trade-offs:**
- ✅ Connection persists for Flask app lifetime
- ✅ Unique alias never conflicts with DocumentConnector's managed connections
- ✅ Automatic cleanup on process termination
- ⚠️ No explicit disconnect (acceptable for microservices)
- ⚠️ Relies on process death for cleanup (standard pattern)

This is only used by the boss container `main.py`, and worker dependencies are kept minimal by using only `pymongo.MongoClient` in `flask.py`.



## 🚀 Container Management

```bash
# Build and start all services
make docker-all-tasks

# Individual services
docker-compose up qeval_worker
docker-compose up bscore_worker
```

## 📊 Performance Notes

- **QuestEval**: ~10-30 seconds per chunk (GPU recommended)
- **BooookScore**: ~30-60 seconds per chunk (GPU recommended)  
- Both services support CUDA if available
- Memory: ~2-4GB per service with models loaded
- Parallel processing: Boss can distribute 10+ tasks simultaneously

## 🔐 Security Notes

- Services communicate on private Docker network
- MongoDB credentials in .env (gitignored)
- No external exposure by default (only boss port accessible)
- For production: Add authentication, TLS, rate limiting

## 📚 References

- [QuestEval Repository](https://github.com/ThomasScialom/QuestEval)
- [BooookScore Repository](https://github.com/lilakk/BooookScore)

## 📄 License

See individual metric libraries for their licenses.