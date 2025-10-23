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


def load_mongo_config() -> tuple[str, str]:
    """Load MongoDB configuration from environment variables.
    @return Tuple of (mongo_uri, database_name)
    @throws KeyError If required environment variables are missing."""
    load_dotenv(".env")
    
    db_prefix = "MONGO"
    engine = os.getenv(f"{db_prefix}_ENGINE")
    username = os.getenv(f"{db_prefix}_USERNAME")
    password = os.getenv(f"{db_prefix}_PASSWORD")
    host = os.getenv(f"{db_prefix}_HOST")
    port = os.getenv(f"{db_prefix}_PORT")
    database = os.getenv("DB_NAME")
    
    auth_suffix = "?authSource=admin&uuidRepresentation=standard"
    mongo_uri = f"{engine}://{username}:{password}@{host}:{port}/{database}{auth_suffix}"
    
    return mongo_uri, database


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


def mark_task_in_progress(mongo_db: Any, story_id: str, chunk_id: str, task_name: str) -> None:
    """Mark a task as in-progress in MongoDB before processing begins.
    @param mongo_db MongoDB database instance.
    @param story_id Unique identifier for the story.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task being executed.
    @throws RuntimeError If task data already exists (preventing overwrites)."""
    collection = mongo_db.chunks
    
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


def save_task_result(mongo_db: Any, story_id: str, chunk_id: str, task_name: str, result: Dict[str, Any]) -> None:
    """Save completed task results to MongoDB.
    @param mongo_db MongoDB database instance.
    @param story_id Unique identifier for the story.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task that was executed.
    @param result Dictionary containing task results to be stored."""
    collection = mongo_db.chunks
    
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


def create_app(task_name: str, mongo_uri: str, database_name: str, boss_url: str) -> Flask:
    """Create and configure Flask application for task processing.
    @param task_name Type of task this worker will process.
    @param mongo_uri MongoDB connection URI.
    @param database_name Name of the MongoDB database to use.
    @param boss_url Callback URL for the boss service.
    @return Configured Flask application instance."""
    app = Flask(__name__)
    
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[database_name]
    
    # Load task handler on startup
    task_handler = get_task_handler(task_name)
    
    @app.route("/start", methods=["POST"])
    def start():
        """Handle incoming task assignments from boss service.
        @return JSON response with status code."""
        data = request.json
        story_id = data.get("story_id")
        chunk_id = data.get("chunk_id")
        
        if not story_id or not chunk_id:
            return jsonify({"error": "Missing story_id or chunk_id"}), 400
        
        # Retrieve chunk data from MongoDB
        chunk_doc = mongo_db.chunks.find_one({
            "story_id": story_id,
            "chunk_id": chunk_id
        })
        
        if not chunk_doc:
            return jsonify({"error": "Chunk not found"}), 404
        
        try:
            # Mark task as in-progress
            mark_task_in_progress(mongo_db, story_id, chunk_id, task_name)
            
            # Execute task
            result = task_handler(chunk_doc)
            
            # Save results
            save_task_result(mongo_db, story_id, chunk_id, task_name, result)
            
            # Notify boss
            notify_boss(boss_url, story_id, chunk_id, task_name, "completed")
            
            return jsonify({"status": "accepted"}), 202
            
        except RuntimeError as e:
            return jsonify({"error": str(e)}), 409
        except Exception as e:
            notify_boss(boss_url, story_id, chunk_id, task_name, "failed")
            return jsonify({"error": str(e)}), 500
    
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flask worker microservice")
    parser.add_argument("--task", required=True, help="Task type to execute")
    parser.add_argument("--port", type=int, default=5000, help="Port to run on")
    args = parser.parse_args()
    
    # Load configuration
    mongo_uri, database_name = load_mongo_config()
    boss_url = load_boss_config()
    
    # Create and run app
    app = create_app(args.task, mongo_uri, database_name, boss_url)
    app.run(host="0.0.0.0", port=args.port)