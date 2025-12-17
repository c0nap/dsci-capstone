"""Generic Flask worker microservice for distributed task processing.
Supports multiple task types via command-line arguments and dynamic imports."""

import argparse
from dotenv import load_dotenv
from flask import Flask, jsonify, request, Response
import os
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from queue import Queue
import requests
import threading
import time
from typing import Any, Callable, Dict, Generator, Optional, Tuple


MongoHandle = Generator["Database[Any]", None, None]


# Create a global session for communication with boss
# This keeps the TCP connection open, preventing "Read timed out" on high loads.
boss_session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
boss_session.mount('http://', adapter)

# Prevents creating a new DB connection for every single task assignment.
mongo_client_cache = {}

# ================= GENERATION CONTROL =================
# Tracks the "generation" of tasks. When the Boss resets (DELETE), 
# we increment this. Any running task with an old generation ID is discarded.
WORKER_GENERATION = 0
generation_lock = threading.Lock()
# ======================================================

def get_cached_client(uri: str) -> MongoClient:
    if uri not in mongo_client_cache:
        mongo_client_cache[uri] = MongoClient(uri)
    return mongo_client_cache[uri]

######################################################################################
# Background threading system for non-blocking task handling.
# Allows Flask to immediately respond to the boss service (202: accepted)
# while processing continues asynchronously in a separate thread.
######################################################################################
def task_worker() -> None:
    """Continuously process tasks from the global queue in the background.
    @details  Each task runs sequentially (or with limited concurrency if multiple workers are started).
    @note Uses queue.get() which blocks; exception handling ensures thread survival."""
    while True:
        # queue.get() blocks by default, so explicit sleep is not required
        func, args = task_queue.get()
        try:
            func(*args)
        except Exception as e:
            # Do not raise here; it kills the thread.
            print(f"Worker thread caught fatal error: {e}")
        finally:
            task_queue.task_done()


def process_task(
    mongo_db: MongoHandle,
    collection_name: str,
    chunk_id: str,
    task_name: str,
    chunk_doc: Dict[str, Any],
    boss_url: str,
    task_handler: Callable[[Dict[str, Any]], Dict[str, Any]],
    task_kwargs: Optional[Dict[str, Any]] = None,
    task_gen: int = 0
) -> None:
    """Perform the assigned task in a background thread.
    This includes updating task status, running the handler, saving results,
    and notifying the boss service when complete.
    @param mongo_db MongoDB database instance.
    @param collection_name The name of the target MongoDB collection.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task being executed.
    @param chunk_doc Document data for the current chunk.
    @param boss_url Callback URL for the boss service.
    @param task_handler Function that performs the actual task computation.
    @param task_kwargs Dict of configuration settings for each task.
    @throws Exception Logs and reports failures to the boss service."""
    task_kwargs = task_kwargs or {}
    try:
        notify_boss(boss_url, chunk_id, task_name, "started")
        mark_task_in_progress(mongo_db, collection_name, chunk_id, task_name)
        
        # 1. Run the heavy computation
        result = task_handler(chunk_doc, **task_kwargs)
        
        # 2. CHECK GENERATION: If the queue was purged while we were working, discard this.
        with generation_lock:
            if task_gen != WORKER_GENERATION:
                print(f"Discarding result for chunk {chunk_id} (Task Gen {task_gen} < Current {WORKER_GENERATION})")
                return 

        # 3. Save and Notify
        save_task_result(mongo_db, collection_name, chunk_id, task_name, result)
        notify_boss(boss_url, chunk_id, task_name, "completed")

    except Exception as e:
        # We also check generation here to avoid sending "failed" for a ghost task
        with generation_lock:
            if task_gen == WORKER_GENERATION:
                notify_boss(boss_url, chunk_id, task_name, "failed")
        
        print(f"Error while running {task_handler.__name__} with args {task_kwargs}: {e}")
        # Do not raise since this is handled by task_worker try/catch
######################################################################################


def load_mongo_config(database: str) -> str:
    """Load MongoDB configuration from environment variables.
    @param database Name of the MongoDB database to connect to.
    @return MongoDB connection string.
    @throws KeyError If required environment variables are missing."""
    load_dotenv(".env")

    db_prefix = "MONGO"
    engine = os.environ[f"{db_prefix}_ENGINE"]
    username = os.environ[f"{db_prefix}_USERNAME"]
    password = os.environ[f"{db_prefix}_PASSWORD"]
    host = os.environ[f"{db_prefix}_HOST"]
    port = os.environ[f"{db_prefix}_PORT"]

    auth_suffix = "?authSource=admin&uuidRepresentation=standard"
    mongo_uri = f"{engine}://{username}:{password}@{host}:{port}/{database}{auth_suffix}"

    return mongo_uri


def load_boss_config() -> str:
    """Load boss service callback URL from environment variables.
    @return Full callback URL for the boss service.
    @throws KeyError If PYTHON_HOST environment variable is missing."""
    load_dotenv(".env")
    BOSS_HOST = os.environ["PYTHON_HOST"]
    BOSS_PORT = os.environ["PYTHON_PORT"]
    BOSS_URL = f"http://{BOSS_HOST}:{BOSS_PORT}"
    return f"{BOSS_URL}/callback"


def get_task_info(task_name: str) -> Tuple[Callable[[Dict[str, Any]], Dict[str, Any]], Dict[str, Any]]:
    """Dynamically import and return the appropriate task handler function.
    @param task_name Name of the task type to execute.
    @return Callable that processes the task data and returns results.
    @throws ImportError If the task module cannot be imported.
    @throws AttributeError If the task function is not found in the module."""
    if task_name == "bookscore":
        from src.components.metrics import run_bookscore

        return run_bookscore, {
            "model": "gpt-4o",
            "use_v2": False,  # single-pass mode
        }
    elif task_name == "questeval":
        from src.components.metrics import run_questeval

        return run_questeval, {
            "qeval_task": "summarization",
            "use_cuda": False,
            "use_question_weighter": True,
        }
    else:
        raise ValueError(f"Unknown task type: {task_name}")


def load_imports(func):
    """Pre-warm the task by importing requirements.
    @param func  The function to perform a dummy call on."""
    try:
        func({})
    except Exception:
        pass


def mark_task_in_progress(mongo_db: MongoHandle, collection_name: str, chunk_id: str, task_name: str) -> None:
    """Mark a task as in-progress in MongoDB before processing begins.
    @param mongo_db MongoDB database instance.
    @param collection_name The name of our primary chunk storage collection in Mongo.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task being executed.
    @throws RuntimeError If task data already exists (preventing overwrites)."""
    collection = getattr(mongo_db, collection_name)

    # Check if task result already exists
    existing = collection.find_one({"_id": chunk_id}, {task_name: 1})

    if existing and task_name in existing:
        raise RuntimeError(f"Task {task_name} already has data for chunk_id={chunk_id}. " "Boss should have cleared this before assignment.")

    # Mark as in-progress
    collection.update_one({"_id": chunk_id}, {"$set": {f"{task_name}.status": "started"}}, upsert=True)


def save_task_result(mongo_db: MongoHandle, collection_name: str, chunk_id: str, task_name: str, result: Dict[str, Any]) -> None:
    """Save completed task results to MongoDB.
    @param mongo_db MongoDB database instance.
    @param collection_name The name of our primary chunk storage collection in Mongo.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task that was executed.
    @param result Dictionary containing task results to be stored."""
    collection = getattr(mongo_db, collection_name)
    update_data = {f"{task_name}.status": "completed", f"{task_name}.result": result}
    collection.update_one({"_id": chunk_id}, {"$set": update_data})
    print(f"FINISHED: chunk ID: {chunk_id}, result:\n{result}")


def notify_boss(boss_url: str, chunk_id: str, task_name: str, status: str) -> None:
    """Send completion notification to boss service.
    @details Spawns a thread so the worker doesn't block waiting for the Boss.
    Uses the global worker_session for connection reuse.
    @param boss_url Callback URL for the boss service.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the completed task.
    @param status Task completion status ('completed' or 'failed')."""
    payload = {"chunk_id": chunk_id, "task": task_name, "status": status}
    def _send_async():
        try:
            # Use global session + short timeout
            worker_session.post(boss_url, json=payload, timeout=2) 
        except Exception as e:
            # We print but don't raise, because we don't want to kill the worker process
            print(f"Failed to notify boss ({status}): {e}")
    # Daemon thread ensures this doesn't block program exit
    threading.Thread(target=_send_async, daemon=True).start()


def create_app(task_name: str, boss_url: str) -> Flask:
    """Create and configure Flask application for task processing.
    @param task_name Type of task this worker will process.
    @param boss_url Callback URL for the boss service.
    @return Configured Flask application instance."""
    app = Flask(__name__)

    # Load task handler on startup
    task_handler, task_args = get_task_info(task_name)
    load_imports(task_handler)
    if task_name == "questeval":
        print("\n" * 6)

    
    @app.route("/tasks/queue", methods=["POST", "DELETE"])
    def manage_queue() -> Tuple[Response, int]:
        """Handle incoming task assignments from boss service.
        @details
        POST: Enqueue a new task from the Boss.
        DELETE: Clear all pending tasks (used when resetting a story).
        @return JSON response with status code."""
        global WORKER_GENERATION
        
        if request.method == "POST":
            data = request.json
            chunk_id = data.get("chunk_id")
            database_name = data.get("database_name")
            collection_name = data.get("collection_name")
            
            if not database_name or not collection_name or not chunk_id:
                return jsonify({"error": "Missing required fields"}), 400

            # Reconnect to the database since DB_NAME or COLLECTION may have changed
            mongo_uri = load_mongo_config(database_name)
            mongo_client: MongoClient[Any] = get_cached_client(mongo_uri)  # Use cached client to optimize
            mongo_db = mongo_client[database_name]
    
            # Retrieve chunk data from MongoDB
            collection = getattr(mongo_db, collection_name)
            chunk_doc = collection.find_one({"_id": chunk_id})
            if not chunk_doc:
                return jsonify({"error": "Chunk not found"}), 404
            
            # Pass current generation to task
            current_gen = WORKER_GENERATION
            task_queue.put((
                process_task, 
                (mongo_db, collection_name, chunk_id, task_name, chunk_doc, boss_url, task_handler, task_args, current_gen)
            ))
            return jsonify({"status": "accepted"}), 202
    
        # Delete logic is necessary to remove ghost tasks when the boss restarts but workers do not
        elif request.method == "DELETE":
            # 1. Clear the Queue (removes pending)
            with task_queue.mutex:
                q_size = len(task_queue.queue)
                task_queue.queue.clear()
            
            # 2. Increment Generation (invalidates running)
            with generation_lock:
                WORKER_GENERATION += 1
                
            print(f"Purged {q_size} pending tasks. Bumped Gen ID to {WORKER_GENERATION} (invalidating active threads).")
            return jsonify({"status": "cleared", "purged_count": q_size}), 200

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flask worker microservice")
    parser.add_argument("--task", required=True, help="Task type to execute")
    args = parser.parse_args()

    # Create the task queue - boss will leave assignments here
    task_queue: "Queue[Tuple[Any, Any]]" = Queue()

    # Start one background worker thread (can increase to 2–4 for limited concurrency)
    for _ in range(2):
        threading.Thread(target=task_worker, daemon=True).start()

    # Flask prep: Boss URL never changes, but MongoDB connection might
    load_dotenv(".env")
    boss_url = load_boss_config()
    PORT = int(os.environ[f"{args.task.upper()}_PORT"])

    # Create and run app - disable hot-reaload on files changed
    app = create_app(args.task, boss_url)
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)
