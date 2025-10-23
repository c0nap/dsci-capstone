# Metrics Microservices Architecture

Docker-based microservices for running QuestEval and BooookScore metrics with different Python versions and isolated dependencies.

## Architecture

```
┌─────────────────┐
│   Main Python   │  (Your application)
│     Pipeline    │  - Makes HTTP requests
│   Python 3.12   │  - Receives callbacks
└────────┬────────┘  
         │
         ├─────────────────┬───────────────────┐
         │                 │                   │
    ┌────▼──────┐     ┌────▼───────┐     ┌─────▼─────┐
    │ QuestEval │     │BooookScore │     │  Blazor   │
    │  Service  │     │  Service   │     │    UI     │
    │  :5001    │     │   :5002    │     │ (Future)  │
    │ Python 3.8│     │ Python 3.10│     │           │
    └───────────┘     └────────────┘     └───────────┘
```

## 📦 Project Structure

```
.
├── flask_app.py                          # Main Flask application
├── metrics_client.py                     # Client library for main app
├── websocket_client.py                   # WebSocket client (optional)
├── Makefile                      # Container management
│
├── src/
│   └── flask.py                     # Workers reuse the same Flask code
│    ── main.py                      # Boss assigns work via HTTP POST
│    
│
├── docker/
│   ├── Dockerfile.questeval              # QuestEval container
│   └── Dockerfile.bookscore              # BooookScore container
│
├── requirements/
│   ├── questeval-requirements.txt        # QuestEval dependencies
│   └── booookscore-requirements.txt      # BooookScore dependencies
│
└── data/                                 # Shared data directory
```

## 🔌 API Usage

### QuestEval Service

**Endpoint:** `POST http://localhost:5001/calculate`

**Request:**
```json
{
  "job_id": "my-questeval-job-1",
  "callback_url": "http://main-app:8080/metrics/callback",
  "data": {
    "text": "Source document text here...",
    "summary": "Generated summary here...",
    "gold_summary": "Reference summary (optional)"
  }
}
```

**Response (202 Accepted):**
```json
{
  "status": "accepted",
  "job_id": "my-questeval-job-1",
  "metric": "questeval",
  "message": "Calculation started. Results will be POSTed to callback_url."
}
```

**Callback (to your app):**
```json
{
  "job_id": "my-questeval-job-1",
  "metric": "questeval",
  "status": "success",
  "result": {
    "questeval_score": 0.75,
    "detailed_scores": {...},
    "has_reference": true
  }
}
```

### BooookScore Service

**Endpoint:** `POST http://localhost:5002/calculate`

**Request:**
```json
{
  "job_id": "my-booookscore-job-1",
  "callback_url": "http://main-app:8080/metrics/callback",
  "data": {
    "file_data": "base64-encoded-pickled-gzipped-data"
  }
}
```

**Callback:**
```json
{
  "job_id": "my-booookscore-job-1",
  "metric": "booookscore",
  "status": "success",
  "result": {
    "booookscore": 0.82,
    "detailed_metrics": {...},
    "input_stats": {...}
  }
}
```

## 🐍 Python Client Usage

## 🛠️ Make Commands

```bash
make docker-all-workers
```


## 📊 Performance Notes

- **QuestEval**: ~10-30 seconds per summary (GPU recommended)
- **BooookScore**: ~30-60 seconds for full book (GPU recommended)
- Both services support CUDA if available
- Memory: ~2-4GB per service with models loaded

## 🔒 Security Notes

- Services are exposed on localhost by default
- For production, add authentication
- Consider using reverse proxy (nginx)
- Rate limit endpoints if exposing publicly

## 📚 References

- [QuestEval Repository](https://github.com/ThomasScialom/QuestEval)
- [BooookScore Repository](https://github.com/lilakk/BooookScore)

## 📄 License

See individual metric libraries for their licenses.