from components.book_conversion import Book, Chunk, EPUBToTEI, ParagraphStreamTEI, Story
from components.metrics import Metrics
import json
import os
import pandas as pd
import random
import traceback


def convert_single():
    """Converts one EPUB file to TEI format."""
    print("\n\nCHAPTERS for book 1: FAIRY TALES")
    epub_file_1 = "./datasets/examples/nested-fairy-tales.epub"
    converter = EPUBToTEI(epub_file_1, save_pandoc=True, save_tei=True)
    converter.convert_to_tei()
    converter.clean_tei()
    # converter.print_chapters(200)

    print("\n\nCHAPTERS for book 2: MYTHS")
    epub_file_2 = "./datasets/examples/nested-myths.epub"
    converter = EPUBToTEI(epub_file_2, save_pandoc=True, save_tei=True)
    converter.convert_to_tei()
    converter.clean_tei()
    # converter.print_chapters(200)


def convert_from_csv():
    """Converts several EPUB files to TEI format.
    @note  Files are specified as rows in a CSV which contains parsing instructions."""
    try:
        df = pd.read_csv("datasets/books.csv")
    except FileNotFoundError:
        print("Error: datasets/books.csv not found")
        return
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    for _, row in df.iterrows():
        try:
            print(f"\n{'='*50}")
            print(f"Processing: {row.get('epub_path', 'Unknown path')}")

            # Handle NaN values for start/end strings - convert to None
            start_str = row.get("start_string")
            end_str = row.get("end_string")
            if pd.isna(start_str):
                start_str = None
            if pd.isna(end_str):
                end_str = None

            converter = EPUBToTEI(
                row.get("epub_path"),
                save_pandoc=False,
                save_tei=True,
            )
            converter.convert_to_tei()
            converter.clean_tei()

        except Exception as e:
            print(f"Error processing {row.get('epub_path', 'unknown')}: {e}")
            traceback.print_exc()


### Will revisit later - Book classes need refactoring ###

# from components.text_processing import BookFactoryTEI, Story

# def main2():
#     try:
#         df = pd.read_csv("datasets/books.csv")
#     except FileNotFoundError:
#         print("Error: datasets/books.csv not found")
#         return
#     except Exception as e:
#         print(f"Error reading CSV: {e}")
#         return

#     for _, row in df.iterrows():
#         try:
#             print(f"\n{'='*50}")
#             print(f"Processing: {row.get('epub_path', 'Unknown path')}")

#             # Handle NaN values for start/end strings
#             start_str = row.get("start_string")
#             end_str = row.get("end_string_exclusive")

#             # Convert NaN to None
#             if pd.isna(start_str):
#                 start_str = None
#             if pd.isna(end_str):
#                 end_str = None

#             factory = BookFactoryTEI(
#                 title_key=row.get("title_key", "Title"),
#                 author_key=row.get("author_key", "Author"),
#                 language_key=row.get("language_key", "Language"),
#                 date_key=row.get("date_key", "Release date"),
#                 start_string=start_str,
#                 end_string_exclusive=end_str
#             )

#             # Pass book_id to create_book method
#             book = factory.create_book(
#                 row["epub_path"],
#                 row["book_id"]
#             )

#             print("\n=== BOOK METADATA ===")
#             print(f"Book ID: {book.book_id}")
#             print(f"Story ID: {row.get('story_id', 'Not specified')}")
#             print(f"Title: {book.title}")
#             print(f"Author: {book.author}")
#             print(f"Language: {book.language}")
#             print(f"Release date: {book.release_date}")


#             print(f"\n=== CHAPTER EXTRACTION ===")
#             print(f"Start string: {start_str}")
#             print(f"End string: {end_str}")

#             chapters = list(book.stream_chapters())
#             print(f"Extracted chapters: {len(chapters)}")

#             for idx, (num, head, paras, chapter_text, lstart, lend) in enumerate(chapters[:5], start=1):
#                 snippet = (chapter_text[:100] + "...") if len(chapter_text) > 100 else chapter_text
#                 print(f"  Chapter {num}: {head[:50]}...")
#                 print(f"    Paragraphs: {len(paras)}")
#                 print(f"    Lines: {lstart}-{lend}")
#                 print(f"    Text: {snippet}")

#             # Create story and chunks
#             story = Story([book], story_id=row.get("story_id"))
#             max_chunk_length = 500
#             story.pre_split_chunks(max_chunk_length)
#             chunks = list(story.stream_chunks(max_chunk_length))

#             print("\n=== STORY SUMMARY ===")
#             print(f"Total chunks: {len(chunks)}")
#             print("Chunk previews (first 10):")
#             for i in range(min(10, len(chunks))):
#                 c = chunks[i]
#                 snippet = (c.text[:80] + "...") if len(c.text) > 80 else c.text
#                 print(f"  [{i}] Story:{c.story_percent:.1f}% Chapter:{c.chapter_percent:.1f}% - {snippet}")

#         except Exception as e:
#             print(f"Error processing {row.get('epub_path', 'unknown')}: {e}")
#             traceback.print_exc()


tei = "./datasets/examples/trilogy-wishes-1.tei"
chapters = """
CHAPTER 1 BEAUTIFUL AS THE DAY\n
CHAPTER 2 GOLDEN GUINEAS\n
CHAPTER 3 BEING WANTED\n
CHAPTER 4 WINGS\n
CHAPTER 5 NO WINGS\n
CHAPTER 6 A CASTLE AND NO DINNER\n
CHAPTER 7 A SIEGE AND BED\n
CHAPTER 8 BIGGER THAN THE BAKER'S BOY\n
CHAPTER 9 GROWN UP\n
CHAPTER 10 SCALPS\n
CHAPTER 11 THE LAST WISH\n
"""  # Corresponds to text within <head> in TEI file.
start = ""
end = "But I must say no more."


def chunk_single():
    """Creates a Story and many Chunks from a TEI file.
    @details
        Requires hard-coded specificaitons
            - List of all chapter names.
            - Optional start / end strings."""
    chaps = [line.strip() for line in chapters.splitlines() if line.strip()]
    reader = ParagraphStreamTEI(
        tei,
        book_id=1,
        story_id=1,
        allowed_chapters=chaps,
        start_inclusive=start,
        end_inclusive=end,
    )
    story = Story(reader)
    story.pre_split_chunks(max_chunk_length=1500)
    chunks = list(story.stream_chunks())

    print("\n=== STORY SUMMARY ===")
    print(f"Total chunks: {len(chunks)}")
    print("Chunk previews (first 10):")
    for i in range(min(10, len(chunks))):
        c = chunks[i]
        snippet = (c.text[:80] + "...") if len(c.text) > 80 else c.text
        print(f"  [{i}] Story:{c.story_percent:.1f}% Chapter:{c.chapter_percent:.1f}% - {snippet}")

    print("\n\nFull chunks (last 3):")
    for i in range(len(chunks) - 3, len(chunks)):
        c = chunks[i]
        print(f"  [{i}] {c}")
        print(c.text)
        print()


def test_relation_extraction():
    """Runs REBEL on a basic example; used for debugging."""
    from components.text_processing import RelationExtractor

    sample_text = "Alice met Bob in the forest. Bob then went to the village."
    extractor = RelationExtractor(model_name="Babelscape/rebel-large")
    print(extractor.extract(sample_text))


def process_single():
    """Uses NLP and LLM to process an existing TEI file."""
    from components.text_processing import LLMConnector, RelationExtractor

    try:
        df = pd.read_csv("datasets/books.csv")
    except FileNotFoundError:
        print("Error: datasets/books.csv not found")
        return
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    row = df.iloc[0]

    print(f"\n{'='*50}")
    print(f"Processing: {row.get('tei_path', 'Unknown path')}")

    # Handle NaN values for start/end strings - convert to None
    start_str = row.get("start_string")
    end_str = row.get("end_string")
    if pd.isna(start_str):
        start_str = None
    if pd.isna(end_str):
        end_str = None

    chapters = row.get("chapters")
    chaps = [line.strip() for line in chapters.splitlines() if line.strip()]
    reader = ParagraphStreamTEI(
        tei,
        book_id=1,
        story_id=1,
        allowed_chapters=chaps,
        start_inclusive=start,
        end_inclusive=end,
    )
    story = Story(reader)
    story.pre_split_chunks(max_chunk_length=1500)
    chunks = list(story.stream_chunks())

    print("\n=== STORY SUMMARY ===")
    print(f"Total chunks: {len(chunks)}")

    print("\n=== NLP EXTRACTION SAMPLE ===")
    re_rebel = "Babelscape/rebel-large"
    re_rst = "GAIR/rst-information-extraction-11b"
    ner_renard = "compnet-renard/bert-base-cased-literary-NER"
    nlp = RelationExtractor(model_name=re_rebel, max_tokens=1024)
    llm = LLMConnector(
        temperature=0,
        system_prompt="You are a helpful assistant that converts semantic triples into structured JSON.",
    )

    unique_numbers = random.sample(range(len(chunks)), 2)
    for i in unique_numbers:
        c = chunks[i]
        print("\nChunk details:")
        print(f"  [{i}] {c}\n")
        print(c.text)

        extracted = nlp.extract(c.text, parse_tuples=True)
        print(f"\nNLP output:")
        for triple in extracted:
            print(triple)
        print()

        triples_string = ""
        for triple in extracted:
            triples_string += str(triple) + "\n"
        prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
        prompt += f"And here is the original text:\n{c.text}\n\n"
        prompt += "Output JSON with keys: s (subject), r (relation), o (object).\n"
        prompt += "Remove nonsensical triples but otherwise retain all relevant entries, and add new ones to encapsulate events, dialogue, and core meaning where applicable."
        llm_output = llm.execute_query(prompt)

        print("\n    LLM prompt:")
        print(llm.system_prompt)
        print(prompt)

        print("\n    LLM output:")
        print(llm_output)

        try:
            data = json.loads(llm_output)
            print("\nValid JSON")
        except json.JSONDecodeError as e:
            print("\nInvalid JSON:", e)
            continue

        json_path = f"./datasets/triples/chunk-{i}_story-{c.story_id}.json"
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        with open(json_path, "w") as f:
            f.write(llm_output)
        print(f"JSON saved to '{json_path}'")

        print("\n" + "=" * 50 + "\n")


triple_files = [
    "./datasets/triples/chunk-160_story-1.json",
    "./datasets/triples/chunk-70_story-1.json",
]


def graph_triple_files(session):
    """Loads JSON into Neo4j to test the Blazor graph page."""
    for json_path in triple_files:
        print(f"\n{'='*50}")
        print(f"Processing: {json_path}")

        # Load existing triples to save NLP time / LLM tokens during MVP stage
        with open(json_path, "r") as f:
            triples = json.load(f)

        for triple in triples:
            subj = triple["s"]
            rel = triple["r"]
            obj = triple["o"]

            print(subj, rel, obj)
            session.graph_db.add_triple(subj, rel, obj)

    print(f"\n{'='*50}")
    session.graph_db.print_triples(max_col_width=20)


response_files = ["./datasets/triples/chunk-160_story-1.txt"]


def output_single(session):
    """Generates a summary from triples stored in JSON, and posts data to Blazor."""
    from components.text_processing import LLMConnector

    json_path = triple_files[0]
    response_path = response_files[0]

    print(f"\n{'='*50}")
    print(f"Processing: {json_path}")

    # Load existing triples to save NLP time / LLM tokens during MVP stage
    with open(json_path, "r") as f:
        triples = json.load(f)

    for triple in triples:
        subj = triple["s"]
        rel = triple["r"]
        obj = triple["o"]
        print(subj, rel, obj)
        session.graph_db.add_triple(subj, rel, obj)

    # basic linear verbalization of triples (concatenate)
    edge_count_df = session.graph_db.get_edge_counts(top_n=3)
    print("\nMost relevant nodes:")
    print(edge_count_df)

    triples_df = session.graph_db.get_all_triples()
    triples_string = ""
    for _, row in edge_count_df.iterrows():
        node_name = row.get("node_name")
        node_triples_df = triples_df[triples_df["subject"] == node_name]

        for _, triple in node_triples_df.iterrows():
            subj = triple.get("subject")
            rel = triple.get("relation")
            obj = triple.get("object")
            triples_string += f"{subj} {rel} {obj}\n"
    print("\nTriples which best represent the graph:")
    print(triples_string)

    if response_path == "":
        # Prompt LLM to generate summary
        llm = LLMConnector(
            temperature=0,
            system_prompt="You are a helpful assistant that processes semantic triples.",
        )
        prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
        prompt += "Transform this data into a coherent, factual, and concise summary. Some relations may be irrelevant, so don't force yourself to include every single one.\n"
        prompt += "Output your generated summary and nothing else."
        response = llm.execute_query(prompt)

        # Write response to file for next time
        response_path = json_path.replace(".json", ".txt")
        with open(response_path, "w") as f:
            f.write(response)
        print(f"LLM output was saved to '{response_path}'")
    else:
        # Load existing prompt to save LLM tokens during MVP stage
        with open(response_path, "r") as f:
            response = f.read()

    print("\nGenerated summary:")
    print(response)

    m = Metrics()
    m.post_basic_output(book_id="1", book_title="Five Children and It", summary=response)
    print("\nOutput sent to web app.")

























def full_pipeline(session, collection_name, epub_path, book_chapters, start_str, end_str, book_id, story_id, book_title):
    chunks = pipeline_1(epub_path, book_chapters, start_str, end_str, book_id, story_id)
    triples, chunk = pipeline_2(session, collection_name, chunks, book_title)
    triples_string = pipeline_3(session, triples)
    summary = pipeline_4(session, collection_name, triples_string, chunk.get_chunk_id())
    pipeline_5a(summary, book_title, book_id)


def old_main(session, collection_name):
    # convert_from_csv()
    # chunk_single()
    # process_single()
    # graph_triple_files(session)
    # (Metrics()).post_example_results()
    # output_single(session)

    full_pipeline(
        session,
        collection_name,
        epub_path="./datasets/examples/trilogy-wishes-2.epub",
        book_chapters="""
CHAPTER 1. THE EGG\n
CHAPTER 2. THE TOPLESS TOWER\n
CHAPTER 3. THE QUEEN COOK\n
CHAPTER 4. TWO BAZAARS\n
CHAPTER 5. THE TEMPLE\n
CHAPTER 6. DOING GOOD\n
CHAPTER 7. MEWS FROM PERSIA\n
CHAPTER 8. THE CATS, THE COW, AND THE BURGLAR\n
CHAPTER 9. THE BURGLAR’S BRIDE\n
CHAPTER 10. THE HOLE IN THE CARPET\n
CHAPTER 11. THE BEGINNING OF THE END\n
CHAPTER 12. THE END OF THE END\n
""",
        start_str="",
        end_str="end of the Phoenix and the Carpet.",
        book_id=2,
        story_id=1,
        book_title="The Phoenix and the Carpet",
    )
















def pipeline_1(epub_path, book_chapters, start_str, end_str, book_id, story_id):
    """Connects all components to convert an EPUB file to a book summary.
    @details  Data conversions:
        - EPUB file
        - XML (TEI)
    """

    # convert EPUB file
    print(f"\n{'='*50}")
    print(f"Processing: {epub_path}")

    converter = EPUBToTEI(epub_path, save_pandoc=False, save_tei=True)
    converter.convert_to_tei()
    converter.clean_tei()
    tei_path = converter.tei_path

    chaps = [line.strip() for line in book_chapters.splitlines() if line.strip()]
    reader = ParagraphStreamTEI(
        tei_path,
        book_id,
        story_id,
        allowed_chapters=chaps,
        start_inclusive=start_str,
        end_inclusive=end_str,
    )
    story = Story(reader)
    story.pre_split_chunks(max_chunk_length=1500)
    chunks = list(story.stream_chunks())

    print("\n=== STORY SUMMARY ===")
    print(f"Total chunks: {len(chunks)}")
    return chunks


def pipeline_2(session, collection_name, chunks, book_title):
    """Extracts triples from a random chunk.
    @details
        - JSON triples (NLP & LLM)"""
    from components.text_processing import LLMConnector, RelationExtractor
    import json

    re_rebel = "Babelscape/rebel-large"
    re_rst = "GAIR/rst-information-extraction-11b"
    ner_renard = "compnet-renard/bert-base-cased-literary-NER"
    nlp = RelationExtractor(model_name=re_rebel, max_tokens=1024)
    llm = LLMConnector(
        temperature=0,
        system_prompt="You are a helpful assistant that converts semantic triples into structured JSON.",
    )

    unique_number = random.sample(range(len(chunks)), 1)[0]
    c = chunks[unique_number]
    print("\nChunk details:")
    print(f"  index: {c}\n")
    print(c.text)

    mongo_db = session.docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    collection.insert_one(c.to_mongo_dict())
    collection.update_one({"_id": c.get_chunk_id()}, {"$set": {"book_title": book_title}})
    print(f"    [Inserted chunk into Mongo with chunk_id: {c.get_chunk_id()}]")

    extracted = nlp.extract(c.text, parse_tuples=True)
    print(f"\nNLP output:")
    triples_string = ""
    for triple in extracted:
        print(triple)
        triples_string += str(triple) + "\n"
    print()

    prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
    prompt += f"And here is the original text:\n{c.text}\n\n"
    prompt += "Output JSON with keys: s (subject), r (relation), o (object).\n"
    prompt += "Remove nonsensical triples but otherwise retain all relevant entries, and add new ones to encapsulate events, dialogue, and core meaning where applicable."
    llm_output = llm.execute_query(prompt)

    print("\n    LLM prompt:")
    print(llm.system_prompt)
    print(prompt)
    print("\n    LLM output:")
    print(llm_output)
    print("\n" + "=" * 50 + "\n")

    try:
        triples = json.loads(llm_output)
        print("\nValid JSON")
    except json.JSONDecodeError as e:
        print("\nInvalid JSON:", e)
        return

    return triples, c


def pipeline_3(session, triples):
    """Generates a LLM summary using Neo4j triples.
    @details
        - Neo4j graph database
        - Blazor graph page"""
    for triple in triples:
        subj = triple["s"]
        rel = triple["r"]
        obj = triple["o"]
        print(subj, rel, obj)
        session.graph_db.add_triple(subj, rel, obj)

    # basic linear verbalization of triples (concatenate)
    edge_count_df = session.graph_db.get_edge_counts(top_n=3)
    print("\nMost relevant nodes:")
    print(edge_count_df)

    triples_df = session.graph_db.get_all_triples()
    triples_string = ""
    for _, row in edge_count_df.iterrows():
        node_name = row.get("node_name")
        node_triples_df = triples_df[triples_df["subject"] == node_name]

        for _, triple in node_triples_df.iterrows():
            subj = triple.get("subject")
            rel = triple.get("relation")
            obj = triple.get("object")
            triples_string += f"{subj} {rel} {obj}\n"
    print("\nTriples which best represent the graph:")
    print(triples_string)
    return triples_string


def pipeline_4(session, collection_name, triples_string, chunk_id):
    """Generate chunk summary"""
    from components.text_processing import LLMConnector

    # Prompt LLM to generate summary
    llm = LLMConnector(
        temperature=0,
        system_prompt="You are a helpful assistant that processes semantic triples.",
    )
    prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
    prompt += "Transform this data into a coherent, factual, and concise summary. Some relations may be irrelevant, so don't force yourself to include every single one.\n"
    prompt += "Output your generated summary and nothing else."
    summary = llm.execute_query(prompt)

    print("\nGenerated summary:")
    print(summary)

    mongo_db = session.docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    collection.update_one({"_id": chunk_id}, {"$set": {"summary": summary}})
    print(f"    [Wrote summary to Mongo with chunk_id: {chunk_id}]")

    return summary


def pipeline_5a(summary, book_title, book_id):
    """Send book info to Blazor
    - Post to Blazor metrics page"""
    from components.metrics import Metrics

    m = Metrics()
    m.post_basic_output(book_id, book_title, summary)
    print("\nOutput sent to web app.")


def pipeline_5b(summary, book_title, book_id, chunk, gold_summary="", bookscore=None, questeval=None):
    """Send metrics to Blazor
    - Compute basic metrics (ROUGE, BERTScore)
    - Wait for advanced metrics (QuestEval, BooookScore)
    - Post to Blazor metrics page"""
    from components.metrics import Metrics

    m = Metrics()
    m.post_basic_metrics(book_id, book_title, summary, gold_summary, chunk, booook_score=bookscore, questeval_score=questeval)
    print("\nOutput sent to web app.")























"""Boss microservice for orchestrating distributed task processing.
Manages task distribution to workers and tracks completion order."""

from collections import defaultdict
from components.document_storage import DocumentConnector
from dotenv import load_dotenv
from flask import Flask, jsonify, request, Response
import os
import pandas as pd
from pymongo.database import Database
import requests
from src.setup import Session
import threading
import time
from typing import Any, Dict, Generator, List, Optional, Tuple


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
        chunk_task = task_mapping.get(task, task)

        # Handle different status types
        if status == "started":
            # Update chunk status to in-progress
            update_chunk_status(chunk_id, story_id, chunk_task, 'in-progress')

            # Update story status to in-progress if not already
            update_story_status(story_id, 'metrics', 'in-progress')

        elif status == "completed":
            # Update chunk status to completed
            update_chunk_status(chunk_id, story_id, chunk_task, 'completed')

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
                    gold_summary = chunk.get("gold_summary", "")
                    bookscore = chunk["bookscore"]
                    questeval = chunk["questeval"]
                    pipeline_5b(summary, book_title, book_id, text, gold_summary, bookscore, questeval)

                    print(f"[PIPELINE FINALIZED] Story {story_id} fully processed")

        elif status == "failed":
            # Update chunk status to failed
            print(f"[WARNING] Task {task} failed for chunk {chunk_id}")
            update_chunk_status(chunk_id, story_id, chunk_task, 'failed')

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
    return requests.post(f'http://localhost:{BOSS_PORT}/process_story', json={'story_id': story_id, 'task_type': task_type})


##############################################################################################


load_dotenv(".env")
if __name__ == "__main__":
    session = Session(verbose=False)
    load_dotenv(".env")
    DB_NAME = os.environ["DB_NAME"]
    BOSS_PORT = int(os.environ["PYTHON_PORT"])
    COLLECTION = os.environ["COLLECTION_NAME"]

    # Drop old chunks
    mongo_db = session.docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, COLLECTION)
    collection.drop()
    print("Deleted old chunks...")

    # old_main(session, COLLECTION)

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



    # TODO - PIPELINE HERE
    story_id = 1
    book_id = 2
    book_title = "The Phoenix and the Carpet"
    post_story_status(BOSS_PORT, story_id, 'preprocessing', 'in-progress')
    post_story_status(BOSS_PORT, story_id, 'chunking', 'in-progress')
    chunks = pipeline_1(
        epub_path="./datasets/examples/trilogy-wishes-2.epub",
        book_chapters="""
CHAPTER 1. THE EGG\n
CHAPTER 2. THE TOPLESS TOWER\n
CHAPTER 3. THE QUEEN COOK\n
CHAPTER 4. TWO BAZAARS\n
CHAPTER 5. THE TEMPLE\n
CHAPTER 6. DOING GOOD\n
CHAPTER 7. MEWS FROM PERSIA\n
CHAPTER 8. THE CATS, THE COW, AND THE BURGLAR\n
CHAPTER 9. THE BURGLAR’S BRIDE\n
CHAPTER 10. THE HOLE IN THE CARPET\n
CHAPTER 11. THE BEGINNING OF THE END\n
CHAPTER 12. THE END OF THE END\n
""",
        start_str="",
        end_str="end of the Phoenix and the Carpet.",
        book_id=book_id,
        story_id=story_id,
    )
    post_story_status(BOSS_PORT, story_id, 'preprocessing', 'completed')
    post_story_status(BOSS_PORT, story_id, 'chunking', 'completed')

    triples, chunk = pipeline_2(session, COLLECTION, chunks, book_title)
    chunk_id = chunk.get_chunk_id()
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'completed')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'completed')

    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'in-progress')
    triples_string = pipeline_3(session, triples)
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'completed')

    post_story_status(BOSS_PORT, story_id, 'summarization', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'summarization', 'in-progress')
    summary = pipeline_4(session, COLLECTION, triples_string, chunk.get_chunk_id())
    post_story_status(BOSS_PORT, story_id, 'summarization', 'completed')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'summarization', 'completed')
    # pipeline_5 is moved to callback() to finalize asynchronously
    # pipeline_5a(summary, book_title, book_id)

    # Post chunk - this will enqueue worker processing
    for task_type in ["questeval", "bookscore"]:
        response = post_process_full_story(BOSS_PORT, story_id, task_type)
        print(f"Triggered {task_type}: {response.json()}")


    

    # Hand off to Flask - keep main thread alive so daemon thread continues
    print("Initial processing complete. Server listening for additional requests from Blazor...")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
