"""Boss microservice for orchestrating distributed task processing.
Manages task distribution to workers and tracks completion order."""

from collections import defaultdict
from dotenv import load_dotenv
from flask import Flask, jsonify, request, Response
import os
import pandas as pd
from pymongo.database import Database
import requests
from src.connectors.document import DocumentConnector
from src.core.context import session
import threading
import time
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple
from src.util import Log


MongoHandle = Generator["Database[Any]", None, None]


def load_worker_config(task_types: List[str]) -> Dict[str, str]:
    """Load worker service URLs from environment variables.
    @param task_types  List of valid task keys to use when searching the .env
    @return  Dictionary mapping task names to worker URLs."""
    load_dotenv(".env")

    # Expected environment variables: BOOKSCORE_PORT, QUESTEVAL_HOST, etc.
    workers = {}

    for task in task_types:
        host_key = f"{task.upper()}_HOST"
        port_key = f"{task.upper()}_PORT"
        load_dotenv(".env")
        HOST = os.environ[host_key]
        PORT = os.environ[port_key]
        if HOST and PORT:
            workers[task] = f"http://{HOST}:{PORT}/tasks/queue"

    return workers


def clear_task_data(mongo_db: MongoHandle, collection_name: str, chunk_id: str, task_name: str) -> None:
    """Clear any existing task data before assigning new task to worker.
    @param mongo_db MongoDB database handle.
    @param collection_name The name of our primary chunk storage collection in Mongo.
    @param chunk_id Unique identifier for the chunk within the story.
    @param task_name Name of the task to clear."""
    collection = getattr(mongo_db, collection_name)
    collection.update_one({"_id": chunk_id}, {"$unset": {task_name: ""}})


def assign_task_to_worker(worker_url: str, database_name: str, collection_name: str, chunk_id: str) -> bool:
    """Assign a task to a worker microservice.
    @param worker_url Full URL of the worker's /start endpoint.
    @param database_name Name of the MongoDB database to use.
    @param collection_name The name of our primary chunk storage collection in Mongo.
    @param chunk_id Unique identifier for the chunk within the story.
    @return True if task was successfully assigned, False otherwise."""
    payload = {"database_name": database_name, "collection_name": collection_name, "chunk_id": chunk_id}

    try:
        response = requests.post(worker_url, json=payload, timeout=5)
        return response.status_code == 202
    except requests.RequestException as e:
        print(f"Failed to assign task to {worker_url}: {e}")
        return False


def create_app(docs_db: DocumentConnector, database_name: str, collection_name: str, worker_urls: Dict[str, str]) -> Flask:
    """Create and configure Flask application for boss service.
    @param docs_db MongoDB connector class.
    @param database_name Name of the MongoDB database to use.
    @param collection_name The name of our primary chunk storage collection in Mongo.
    @param worker_urls Dictionary mapping task names to worker URLs.
    @return Configured Flask application instance."""
    app = Flask(__name__)
    docs_db.change_database(database_name)
    mongo_db = docs_db.get_unmanaged_handle()

    # Track task completion with two DataFrames
    # Story-level tracking
    story_tracker = pd.DataFrame(columns=['story_id', 'preprocessing', 'chunking', 'summarization', 'metrics'])

    # Chunk-level tracking
    chunk_tracker = pd.DataFrame(
        columns=[
            'chunk_id',
            'story_id',
            'load_to_mongo',
            'relation_extraction',
            'llm_inference',
            'load_triples_to_neo4j',
            'graph_verbalization',
            'summarization',
            'metric_questeval',
            'metric_bookscore',
            'metrics_basic',
        ]
    )

    # Lock for thread-safe DataFrame operations
    import threading

    tracker_lock = threading.Lock()

    def update_story_status(story_id: int, task: str, status: str) -> None:
        """Update story-level task status. Auto-initializes with pending if not exists.
        @param story_id Unique identifier for the story.
        @param task Task name (preprocessing, chunking, summarization, metrics).
        @param status Status (pending, assigned, in-progress, completed)."""
        nonlocal story_tracker
        with tracker_lock:
            if story_id not in story_tracker['story_id'].values:
                # Initialize new story row with all tasks as pending
                new_row = pd.DataFrame(
                    [{'story_id': story_id, 'preprocessing': 'pending', 'chunking': 'pending', 'summarization': 'pending', 'metrics': 'pending'}]
                )
                story_tracker = pd.concat([story_tracker, new_row], ignore_index=True)

            # Update specific task status
            story_tracker.loc[story_tracker['story_id'] == story_id, task] = status
        # print(f"{" " * 16}Stories Status:\n{story_tracker}\n")

    def update_chunk_status(chunk_id: str, story_id: int, task: str, status: str) -> None:
        """Update chunk-level task status. Auto-initializes with pending if not exists.
        @param chunk_id Unique identifier for the chunk.
        @param story_id Unique identifier for the story.
        @param task Task name (extraction, load_to_mongo, etc.).
        @param status Status (pending, assigned, in-progress, completed, failed)."""
        nonlocal chunk_tracker
        with tracker_lock:
            if chunk_id not in chunk_tracker['chunk_id'].values:
                # Initialize new chunk row with all tasks as pending
                new_row = pd.DataFrame(
                    [
                        {
                            'chunk_id': chunk_id,
                            'story_id': story_id,
                            'extraction': 'pending',
                            'load_to_mongo': 'pending',
                            'relation_extraction': 'pending',
                            'llm_inference': 'pending',
                            'load_triples_to_neo4j': 'pending',
                            'graph_verbalization': 'pending',
                            'summarization': 'pending',
                            'metric_questeval': 'pending',
                            'metric_bookscore': 'pending',
                            'metrics_basic': 'pending',
                        }
                    ]
                )
                chunk_tracker = pd.concat([chunk_tracker, new_row], ignore_index=True)

            # If starting a new task, append timestamp
            if Log.RECORD_TIME and status == 'started':
                status = f"started, {time.time()}"

            # Update specific task status
            chunk_tracker.loc[chunk_tracker['chunk_id'] == chunk_id, task] = status
        # print(f"{" " * 16}Chunks Status:\n{chunk_tracker}\n")

    def check_story_completion(story_id: int, task_type: str) -> bool:
        """Check if all chunks for a story have completed a specific task.
        @param story_id Unique identifier for the story.
        @param task_type Task to check (e.g., 'metric_questeval', 'metric_bookscore').
        @return True if all chunks completed, False otherwise."""
        with tracker_lock:
            story_chunks = chunk_tracker[chunk_tracker['story_id'] == story_id]
            if story_chunks.empty:
                return False
            return all(story_chunks[task_type] == 'completed')

    def check_story_failure(story_id: int, task_type: str) -> bool:
        """Check if any chunks for a story have failed a specific task.
        @param story_id Unique identifier for the story.
        @param task_type Task to check (e.g., 'metric_questeval').
        @return True if any chunk failed, False otherwise."""
        with tracker_lock:
            story_chunks = chunk_tracker[chunk_tracker['story_id'] == story_id]
            if story_chunks.empty:
                return False
            return any(story_chunks[task_type] == 'failed')

    def record_elapsed_time(chunk_id: str, task: str) -> Optional[float]:
        if not Log.RECORD_TIME:
            return None
        start = get_task_start_time(chunk_id, task)
        if start is None:
            return None
        elapsed = time.time() - start
        Log.elapsed_time(f"worker_{task}", elapsed, chunk_id)
        return elapsed

    def get_task_start_time(chunk_id: str, task: str) -> Optional[float]:
        """Get the start timestamp for a given chunk and started task.
        @note  Expects chunk_tracker['task'] to have format: "started, <datetime>"
        @param chunk_id Unique identifier for the chunk.
        @param task Task name (extraction, load_to_mongo, etc.).
        @return: Timestamp converted to epoch seconds (float), or None if not started or not found."""
        with tracker_lock:
            chunk_status = chunk_tracker.loc[chunk_tracker['chunk_id'] == chunk_id, f'{task}']
            if chunk_status.empty:
                return None

            # Extract timestamp
            status = chunk_status.iloc[0]  # e.g., "started, 2025-12-01T18:50:00"
            if 'started,' not in status:
                return None
            _, timestamp = status.split(', ', 1)

            # Get epoch-seconds to compare with time.time()
            dt = datetime.fromisoformat(timestamp)
            return dt.timestamp()

    def get_elapsed_time(chunk_id: str, story_id: int, task: str) -> Optional[float]:
        """Get elapsed time in seconds for a completed task of a given chunk.
        @note  Expects chunk_tracker['task'] to have format: "completed, <float seconds>"
        @param chunk_id: Unique identifier for the chunk.
        @param task: Task name (e.g., 'extraction', 'load_to_mongo').
        @return: Elapsed seconds as float, or None if not completed or not found.
        """
        with tracker_lock:
            chunk_status = chunk_tracker.loc[chunk_tracker['chunk_id'] == chunk_id, f'{task}']
            if chunk_status.empty:
                return None
    
            status = chunk_status.iloc[0]  # e.g., "completed, 0.23495"
            if 'completed,' not in status and 'failed,' not in status:
                return None
    
            # Extract float seconds
            _, seconds = status.split(', ', 1)
            return float(seconds)

    def set_elapsed_time(chunk_id: str, task: str, seconds: float, status: str) -> None:
        """Record elapsed time for a chunk task, preserving status.
        @param chunk_id: Unique identifier for the chunk.
        @param task: Task name (e.g., 'extraction', 'load_to_mongo').
        @param seconds: Float seconds to record.
        @param status: Either 'completed' or 'failed'.
        """
        with tracker_lock:
            # Filter row
            mask = chunk_tracker['chunk_id'] == chunk_id
            if not mask.any():
                return
            # Write status
            chunk_tracker.loc[mask, task] = f"{status}, {seconds}"




    @app.route("/process_story", methods=["POST"])
    def process_story() -> Tuple[Response, int]:
        """Initiate processing for a story by distributing tasks to workers.
        @return JSON response indicating success or failure."""
        data = request.json
        story_id = data.get("story_id")
        task_type = data.get("task_type")

        if not story_id:
            return jsonify({"error": "Missing story_id"}), 400
        if not task_type or task_type not in worker_urls:
            return jsonify({"error": f"Unknown task type: {task_type}"}), 400

        # Get all chunks for this story
        collection = getattr(mongo_db, collection_name)
        chunks = list(collection.find({"story_id": story_id}))
        if not chunks:
            return jsonify({"error": f"Cannot distribute tasks: No chunks found for story {story_id}"}), 404

        # Map task_type to chunk-level task name
        task_mapping = {'questeval': 'metric_questeval', 'bookscore': 'metric_bookscore'}
        chunk_task = task_mapping.get(task_type, task_type)

        # Update story-level status to assigned
        update_story_status(story_id, 'metrics', 'assigned')

        # Distribute tasks to workers (async)
        worker_url = worker_urls[task_type]
        assigned = 0

        for chunk in chunks:
            chunk_id = chunk["_id"]

            # Initialize chunk tracker entry
            update_chunk_status(chunk_id, story_id, chunk_task, 'assigned')

            # Clear any existing task data
            clear_task_data(mongo_db, collection_name, chunk_id, task_type)

            # Assign task to worker - verify 202 accepted
            if assign_task_to_worker(worker_url, database_name, collection_name, chunk_id):
                update_chunk_status(chunk_id, story_id, chunk_task, 'assigned')
                assigned += 1
                print(f"[ASSIGNED] chunk '{chunk_id}' to worker {task_type}: using database '{database_name}' and collection '{collection_name}'")
            else:
                # If assignment failed, set status to failed
                print(f"WARNING: Failed to assign chunk {chunk_id} to worker")
                update_chunk_status(chunk_id, story_id, chunk_task, 'failed')

        return (
            jsonify({"status": "tasks_assigned", "story_id": story_id, "task_type": task_type, "total_chunks": len(chunks), "assigned": assigned}),
            200,
        )

    @app.route("/callback", methods=["POST"])
    def callback() -> Tuple[Response, int]:
        """Receive status notifications from worker services.
        Handles started, completed, and failed statuses.
        @return Simple acknowledgment response."""
        from src.main import pipeline_E

        data = request.json

        chunk_id = data.get("chunk_id")
        task = data.get("task")
        status = data.get("status")  # Expected: "started", "completed", or "failed"

        if not chunk_id or not task or not status:
            return jsonify({"error": "Missing required fields: chunk_id, task, status"}), 400

        print(f"[CALLBACK] chunk_id={chunk_id}, task={task}, status={status}")

        # Get specific chunk by chunk_id
        collection = getattr(mongo_db, collection_name)
        chunk = collection.find_one({"_id": chunk_id})

        if not chunk:
            # Cannot update tracker without story_id from chunk document
            # This indicates a more serious issue (chunk never existed or was deleted)
            print(f"[ERROR] Could not find chunk {chunk_id} in MongoDB - cannot update tracker")
            return jsonify({"error": f"Could not find chunk {chunk_id} in MongoDB."}), 404

        story_id = chunk["story_id"]

        # Map task to chunk-level task name
        task_mapping = {'questeval': 'metric_questeval', 'bookscore': 'metric_bookscore'}
        chunk_task = task_mapping[task]

        # Handle different status types
        if status == "started":
            # Update chunk status to in-progress
            update_chunk_status(chunk_id, story_id, chunk_task, 'in-progress')

            # Update story status to in-progress if not already
            update_story_status(story_id, 'metrics', 'in-progress')

        elif status == "completed":
            # Update chunk status to completed
            seconds = record_elapsed_time(chunk_id, chunk_task)
            update_chunk_status(chunk_id, story_id, chunk_task, 'completed')
            if seconds:
                set_elapsed_time(chunk_id, chunk_task, seconds)

            # Check if all chunks for this story completed this task
            if check_story_completion(story_id, chunk_task):
                print(f"[STORY COMPLETE] All chunks completed {chunk_task} for story {story_id}")

                # Check if all metric tasks are complete for the story
                all_metrics_complete = all(
                    [check_story_completion(story_id, 'metric_questeval'), check_story_completion(story_id, 'metric_bookscore')]
                )

                if all_metrics_complete:
                    # Update story-level metrics to completed
                    update_story_status(story_id, 'metrics', 'completed')

                    # FINALIZE PIPELINE - all workers finished for this story
                    # Access fields directly from the MongoDB document
                    book_id = chunk["book_id"]
                    book_title = chunk["book_title"]
                    text = chunk["text"]
                    summary = chunk["summary"]
                    gold_summary = chunk.get("gold_summary", text[: len(text) // 2])
                    bookscore = float(chunk["bookscore"]["result"]["value"])
                    questeval = float(chunk["questeval"]["result"]["value"])
                    pipeline_E(summary, book_title, book_id, text, gold_summary, bookscore, questeval)

                    print(f"[PIPELINE FINALIZED] Story {story_id} fully processed")

        elif status == "failed":
            # Update chunk status to failed
            print(f"[WARNING] Task {task} failed for chunk {chunk_id}")
            seconds = record_elapsed_time(chunk_id, chunk_task)
            update_chunk_status(chunk_id, story_id, chunk_task, 'failed')
            if seconds:
                set_elapsed_time(chunk_id, chunk_task, seconds)

            # Check if we should mark the story-level task as failed
            if check_story_failure(story_id, chunk_task):
                update_story_status(story_id, 'metrics', 'failed')
                print(f"[STORY FAILED] Story {story_id} has failed chunks for {chunk_task}")

        else:
            return jsonify({"error": f"Unknown status: {status}"}), 400

        return jsonify({"status": "received"}), 200

    @app.route("/status/<status_type>/<identifier>", methods=["GET"])
    def get_status(status_type: str, identifier: str) -> Tuple[Response, int]:
        """Get processing status for a story or chunk.
        @param status_type Either 'story' or 'chunk'.
        @param identifier Story ID or chunk ID.
        @return JSON response with status."""

        if status_type == "story":
            try:
                story_id = int(identifier)
            except ValueError:
                return jsonify({"error": "Invalid story_id"}), 400

            with tracker_lock:
                story_status = story_tracker[story_tracker['story_id'] == story_id]
                if story_status.empty:
                    return jsonify({"error": "Story not found"}), 404

                story_data = story_status.to_dict('records')[0]

                task_columns = [col for col in story_data.keys() if col not in ['story_id']]
                completed_tasks = sum(story_data[col] == 'completed' for col in task_columns)

                return (
                    jsonify(
                        {
                            "story_id": story_id,
                            "tasks": story_data,
                            "completed_tasks": completed_tasks,
                            "total_tasks": len(task_columns),
                            "completion_percentage": (completed_tasks / len(task_columns) * 100) if task_columns else 0,
                        }
                    ),
                    200,
                )

        elif status_type == "chunk":
            chunk_id = identifier

            with tracker_lock:
                chunk_status = chunk_tracker[chunk_tracker['chunk_id'] == chunk_id]
                if chunk_status.empty:
                    return jsonify({"error": "Chunk not found"}), 404

                chunk_data = chunk_status.to_dict('records')[0]
                story_id = chunk_data['story_id']

                task_columns = [col for col in chunk_data.keys() if col not in ['chunk_id', 'story_id']]
                completed_tasks = sum(chunk_data[col] == 'completed' for col in task_columns)

                return (
                    jsonify(
                        {
                            "chunk_id": chunk_id,
                            "story_id": story_id,
                            "tasks": chunk_data,
                            "completed_tasks": completed_tasks,
                            "total_tasks": len(task_columns),
                            "completion_percentage": (completed_tasks / len(task_columns) * 100) if task_columns else 0,
                        }
                    ),
                    200,
                )

        else:
            return jsonify({"error": f"Invalid status_type: {status_type}. Use 'story' or 'chunk'"}), 400

        return jsonify({"error": "Unknown error"}), 500

    @app.route("/status/<status_type>", methods=["POST"])
    def update_status(status_type: str) -> Tuple[Response, int]:
        """Update story or chunk task status. Auto-initializes if not exists.
        @param status_type Either 'story' or 'chunk'.
        Payload: {
            "story_id": int|str (required regardless of status_type)
            "chunk_id": int (required if status_type='chunk')
            "task": str (required) - task column name
            "status": str (required) - new status value
        }
        @return JSON response with acknowledgment."""
        data = request.json

        story_id = data.get("story_id")
        chunk_id = data.get("chunk_id")
        task = data.get("task")
        status = data.get("status")

        if status_type == "story" and not all([story_id, task, status]):
            return jsonify({"error": "Missing required fields: story_id, task, status"}), 400
        if status_type == "chunk" and not all([story_id, chunk_id, task, status]):
            return jsonify({"error": "Missing required fields: story_id, chunk_id, task, status"}), 400

        if status_type == "story":
            try:
                story_id = int(story_id)
            except (ValueError, TypeError):
                return jsonify({"error": "Invalid story_id, must be integer"}), 400

            update_story_status(story_id, task, status)
            print(f"[STATUS] Story {story_id}: {task} -> {status}")

        elif status_type == "chunk":
            update_chunk_status(chunk_id, story_id, task, status)
            print(f"[STATUS] Chunk {chunk_id}: {task} -> {status}")

        else:
            return jsonify({"error": f"Invalid status_type: {status_type}. Use 'story' or 'chunk'"}), 400

        return jsonify({"status": "updated"}), 200

    @app.route("/tracker/story", methods=["GET"])
    def get_story_tracker() -> Tuple[Response, int]:
        """Get complete story tracker DataFrame.
        @return JSON response with story tracker data."""
        with tracker_lock:
            return jsonify(story_tracker.to_dict('records')), 200

    @app.route("/tracker/chunk", methods=["GET"])
    def get_chunk_tracker() -> Tuple[Response, int]:
        """Get complete chunk tracker DataFrame.
        @return JSON response with chunk tracker data."""
        with tracker_lock:
            return jsonify(chunk_tracker.to_dict('records')), 200

    return app


def create_boss_thread(DB_NAME: str, BOSS_PORT: int, COLLECTION: str) -> None:
    # Drop old chunks
    mongo_db = session.docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, COLLECTION)
    collection.drop()
    print("Deleted old chunks...")

    # Load configuration
    task_types = ["questeval", "bookscore"]
    worker_urls = load_worker_config(task_types)
    if not worker_urls:
        print("Warning: No worker URLs configured. Set WORKER_<TASKNAME> environment variables.")

    # Create and run app
    app = create_app(session.docs_db, DB_NAME, COLLECTION, worker_urls)

    # Start the Flask server in the background - disable hot-reaload on files changed
    run_app = lambda: app.run(host="0.0.0.0", port=BOSS_PORT, use_reloader=False)
    threading.Thread(target=run_app, daemon=True).start()

    # Wait for boss to be ready
    time.sleep(1)


##############################################################################################
# Helpers to interact with the Flask boss thread.
# Used to process our set of example books on pipeline start.
##############################################################################################
def post_story_status(boss_port: int, story_id: int, task: str, status: str) -> requests.models.Response:
    """Send a story-level update to the boss Flask app.
    @param boss_port Port the boss microservice is running on.
    @param story_id Unique identifier for the story.
    @param task Task name (extraction, load_to_mongo, etc.).
    @param status Status (pending, assigned, in-progress, completed, failed).
    @return JSON response indicating success or failure."""
    return requests.post(f'http://localhost:{boss_port}/status/story', json={'story_id': story_id, 'task': task, 'status': status})


def post_chunk_status(boss_port: int, chunk_id: str, story_id: int, task: str, status: str) -> requests.models.Response:
    """Send a chunk-level update to the boss Flask app.
    @param boss_port Port the boss microservice is running on.
    @param chunk_id Unique identifier for the chunk.
    @param story_id Unique identifier for the story.
    @param task Task name (extraction, load_to_mongo, etc.).
    @param status Status (pending, assigned, in-progress, completed, failed).
    @return JSON response indicating success or failure."""
    return requests.post(
        f'http://localhost:{boss_port}/status/chunk', json={'story_id': story_id, "chunk_id": chunk_id, 'task': task, 'status': status}
    )


def post_process_full_story(boss_port: int, story_id: int, task_type: str) -> requests.models.Response:
    """Process all chunks in MongoDB matching the provided story ID.
    @param boss_port Port the boss microservice is running on.
    @param story_id Unique identifier for the story.
    @param task_type Worker name (questeval, bookscore).
    @return JSON response indicating success or failure."""
    return requests.post(f'http://localhost:{boss_port}/process_story', json={'story_id': story_id, 'task_type': task_type})
