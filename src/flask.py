from flask import Flask, request, jsonify
from pymongo import MongoClient
import argparse, requests
from dotenv import load_dotenv
import os

def create_app(task, mongo_uri, boss_url):
    app = Flask(__name__)
    mongo = MongoClient(mongo_uri).get_database()
    if task == "bookscore":
    	from src.metrics import run_questeval
    elif task == "questeval":
    	from src.metrics import run_bookscore

    @app.route("/start", methods=["POST"])
    def start():
        #folder = request.json.get("folder")
        #doc = mongo.books.find_one({"_id": folder})
        if not doc:
            return jsonify({"error": "not found"}), 404

        if task == "bookscore":
            result = {"bookscore": {"score": len(doc.get("text", "")) % 100}}
        elif task == "questeval":
            result = {"questeval": {"score": len(doc.get("text", "")) // 10}}
        else:
            result = {"note": f"Unknown task {task}"}

        #mongo.books.update_one({"_id": folder}, {"$set": result})
        requests.post(boss_url, json={...})
        return "Accepted", 202

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    args = parser.parse_args()

	load_dotenv(".env")
	DB = "MONGO"
	mongo_engine = os.getenv(f"{DB}_ENGINE")
    mongo_username = os.getenv(f"{DB}_USERNAME")
    mongo_password = os.getenv(f"{DB}_PASSWORD")
    mongo_host = os.getenv(f"{DB}_HOST")
    mongo_port = os.getenv(f"{DB}_PORT")
    mongo_database = os.getenv("DB_NAME")
	_auth_suffix = "?authSource=admin" + "&uuidRepresentation=standard"
    mongo_uri = f"{mongo_engine}://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_database}{_auth_suffix}"

    boss_host = os.getenv(f"PYTHON_HOST")
    boss_url = f"http://{boss_host}:5054/callback"


    

    app = create_app(args.task, mongo_uri, boss_url)
    app.run(host="0.0.0.0", port=args.port)
