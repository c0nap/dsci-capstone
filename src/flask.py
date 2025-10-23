"""Generic Flask worker microservice for distributed task processing.
Supports multiple task types via command-line arguments and dynamic imports."""

from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import argparse
import requests
import os
from dotenv import load_dotenv
from typing import Dict, Any, Callable, Optional
import queue, threading


######################################################################################
# Background threading system for non-blocking task handling.
# Allows Flask to immediately respond to the boss service (202: accepted)
# while processing continues asynchronously in a separate thread.
######################################################################################
task_queue = queue.Queue()
def task_worker():
    """Continuously process tasks from the global queue in the background.
    Each task runs sequentially (or with limited concurrency if multiple workers are started).
    @throws Exception Logs any runtime errors that occur during task execution."""
    while True:
        func, args = task_queue.get()
        try:
            func(*args)
        except Exception as e:
            print(f"Worker thread error: {e}")
        finally:
            task_queue.task_done()

# Start one background worker thread (can increase to 2–4 for limited concurrency)
threading.Thread(target=task_worker, daemon=True).start()


def process_task(mongo_db, collection_name, story_id, chunk_id, task_name, chunk_doc, boss_url, task_handler):
    """Perform the assigned task in a background thread.
    This includes updating task status, running the handler, saving results,
    and notifying the boss service when complete.
    @param mongo_db MongoDB database instance.
    @param collection_name The name of the target MongoDB collection.
    @param story_id Unique identifier for the story being processed.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task being executed.
    @param chunk_doc Document data for the current chunk.
    @param boss_url Callback URL for the boss service.
    @param task_handler Function that performs the actual task computation.
    @throws Exception Logs and reports failures to the boss service."""
    try:
        mark_task_in_progress(mongo_db, collection_name, story_id, chunk_id, task_name)
        result = task_handler(chunk_doc)
        save_task_result(mongo_db, collection_name, story_id, chunk_id, task_name, result)
        notify_boss(boss_url, story_id, chunk_id, task_name, "completed")
    except Exception as e:
        notify_boss(boss_url, story_id, chunk_id, task_name, "failed")
        print(f"Error in background task: {e}")
######################################################################################




def load_mongo_config(database: str) -> str:
    """Load MongoDB configuration from environment variables.
    @param database Name of the MongoDB database to connect to.
    @return MongoDB connection string.
    @throws KeyError If required environment variables are missing."""
    load_dotenv(".env")
    
    db_prefix = "MONGO"
    engine = os.getenv(f"{db_prefix}_ENGINE")
    username = os.getenv(f"{db_prefix}_USERNAME")
    password = os.getenv(f"{db_prefix}_PASSWORD")
    host = os.getenv(f"{db_prefix}_HOST")
    port = os.getenv(f"{db_prefix}_PORT")
    
    auth_suffix = "?authSource=admin&uuidRepresentation=standard"
    mongo_uri = f"{engine}://{username}:{password}@{host}:{port}/{database}{auth_suffix}"
    
    return mongo_uri


def load_boss_config() -> str:
    """Load boss service callback URL from environment variables.
    @return Full callback URL for the boss service.
    @throws KeyError If PYTHON_HOST environment variable is missing."""
    BOSS_HOST = os.getenv("PYTHON_HOST")
    BOSS_PORT = os.getenv("PYTHON_PORT")
    return f"http://{BOSS_HOST}:{BOSS_PORT}/callback"


def get_task_handler(task_name: str) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Dynamically import and return the appropriate task handler function.
    @param task_name Name of the task type to execute.
    @return Callable that processes the task data and returns results.
    @throws ImportError If the task module cannot be imported.
    @throws AttributeError If the task function is not found in the module."""
    if task_name == "bookscore":
        from src.metrics import run_bookscore
        return run_bookscore
    elif task_name == "questeval":
        from src.metrics import run_questeval
        return run_questeval
    else:
        raise ValueError(f"Unknown task type: {task_name}")


def mark_task_in_progress(mongo_db: Any, collection_name: str, story_id: str, chunk_id: str, task_name: str) -> None:
    """Mark a task as in-progress in MongoDB before processing begins.
    @param mongo_db MongoDB database instance.
    @param collection_name The name of our primary chunk storage collection in Mongo.
    @param story_id Unique identifier for the story.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task being executed.
    @throws RuntimeError If task data already exists (preventing overwrites)."""
    collection = getattr(mongo_db, collection_name)
    
    # Check if task result already exists
    existing = collection.find_one(
        {"story_id": story_id, "chunk_id": chunk_id},
        {task_name: 1}
    )
    
    if existing and task_name in existing:
        raise RuntimeError(
            f"Task {task_name} already has data for story_id={story_id}, chunk_id={chunk_id}. "
            "Boss should have cleared this before assignment."
        )
    
    # Mark as in-progress
    collection.update_one(
        {"story_id": story_id, "chunk_id": chunk_id},
        {"$set": {f"{task_name}.status": "in_progress"}},
        upsert=True
    )


def save_task_result(mongo_db: Any, collection_name: str, story_id: str, chunk_id: str, task_name: str, result: Dict[str, Any]) -> None:
    """Save completed task results to MongoDB.
    @param mongo_db MongoDB database instance.
    @param collection_name The name of our primary chunk storage collection in Mongo.
    @param story_id Unique identifier for the story.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task that was executed.
    @param result Dictionary containing task results to be stored."""
    collection = getattr(mongo_db, collection_name)
    
    update_data = {
        f"{task_name}.status": "completed",
        f"{task_name}.result": result
    }
    
    collection.update_one(
        {"story_id": story_id, "chunk_id": chunk_id},
        {"$set": update_data}
    )


def notify_boss(boss_url: str, story_id: str, chunk_id: str, task_name: str, status: str) -> None:
    """Send completion notification to boss service.
    @param boss_url Callback URL for the boss service.
    @param story_id Unique identifier for the story.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the completed task.
    @param status Task completion status ('completed' or 'failed')."""
    payload = {
        "story_id": story_id,
        "chunk_id": chunk_id,
        "task": task_name,
        "status": status
    }
    
    try:
        requests.post(boss_url, json=payload, timeout=5)
    except requests.RequestException as e:
        print(f"Failed to notify boss: {e}")


def create_app(task_name: str, boss_url: str) -> Flask:
    """Create and configure Flask application for task processing.
    @param task_name Type of task this worker will process.
    @param boss_url Callback URL for the boss service.
    @return Configured Flask application instance."""
    app = Flask(__name__)
    
    # Load task handler on startup
    task_handler = get_task_handler(task_name)
    
    @app.route("/start", methods=["POST"])
    def start():
        """Handle incoming task assignments from boss service.
        @return JSON response with status code."""
        data = request.json
        story_id = data.get("story_id")
        chunk_id = data.get("chunk_id")
        database_name = data.get("database_name")
        collection_name = data.get("collection_name")
        if not database_name or not collection_name:
            return jsonify({"error": "Missing database_name or collection_name"}), 400
        if not story_id or not chunk_id:
            return jsonify({"error": "Missing story_id or chunk_id"}), 400
        
        # Reconnect to the database since DB_NAME or COLLECTION may have changed
        mongo_uri = load_mongo_config(database_name)
        mongo_client = MongoClient(mongo_uri)
        mongo_db = mongo_client[database_name]

        # Retrieve chunk data from MongoDB
        collection = getattr(mongo_db, collection_name)
        chunk_doc = collection.find_one({
            "story_id": story_id,
            "chunk_id": chunk_id
        })
        if not chunk_doc:
            return jsonify({"error": "Chunk not found"}), 404
        
        # Enqueue the background task
        task_queue.put((process_task, (mongo_db, collection_name, story_id, chunk_id, task_name, chunk_doc, boss_url, task_handler)))
        return jsonify({"status": "accepted"}), 202
    
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flask worker microservice")
    parser.add_argument("--task", required=True, help="Task type to execute")
    parser.add_argument("--port", type=int, help="Port to run on")
    args = parser.parse_args()
    
    # Boss URL never changes, but MongoDB connection might
    boss_url = load_boss_config()
    
    # Create and run app
    app = create_app(args.task, boss_url)
    app.run(host="0.0.0.0", port=args.port)