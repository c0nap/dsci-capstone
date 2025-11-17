from dotenv import load_dotenv
import json
import os
import pandas as pd
import random
from src.components.book_conversion import Book, Chunk, EPUBToTEI, ParagraphStreamTEI, Story
from src.components.metrics import Metrics
from src.core.boss import (
    create_boss_thread,
    post_chunk_status,
    post_process_full_story,
    post_story_status
)
from src.core.context import session
import time
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
    from src.components.relation_extraction import RelationExtractor
    from src.connectors.llm import LLMConnector

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


def graph_triple_files():
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
            session.main_graph.add_triple(subj, rel, obj)

    print(f"\n{'='*50}")
    session.main_graph.print_triples(max_col_width=20)


response_files = ["./datasets/triples/chunk-160_story-1.txt"]


def output_single():
    """Generates a summary from triples stored in JSON, and posts data to Blazor."""
    from src.connectors.llm import LLMConnector

    json_path = triple_files[0]
    response_path = response_files[0]

    print(f"\n{'='*50}")
    print(f"Processing: {json_path}")

    # Load existing triples to save NLP time / LLM tokens during MVP stage
    with open(json_path, "r") as f:
        triples = json.load(f)

    for triple in triples:
        print(triple["s"], triple["r"], triple["o"])
    # TODO: normalize
    session.main_graph.add_triples_json(triples)

    # basic linear verbalization of triples (concatenate)
    edge_count_df = session.main_graph.get_edge_counts(top_n=3)
    edge_count_df = session.main_graph.find_element_names(edge_count_df, ["node_name"], ["node_id"], "node", "name", drop_ids=True)
    print("\nMost relevant nodes:")
    print(edge_count_df)

    triples_df = session.main_graph.get_by_ranked_degree(min_rank=3, id_columns=["subject_id"])
    triples_string = session.main_graph.to_triples_string(triples_df, format="triple")
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

























def full_pipeline(collection_name, epub_path, book_chapters, start_str, end_str, book_id, story_id, book_title):
    chunks = pipeline_1(epub_path, book_chapters, start_str, end_str, book_id, story_id)
    triples, chunk = pipeline_2(collection_name, chunks, book_title)
    triples_string = pipeline_3(triples)
    summary = pipeline_4(collection_name, triples_string, chunk.get_chunk_id())
    pipeline_5a(summary, book_title, book_id)


def old_main(collection_name):
    # convert_from_csv()
    # chunk_single()
    # process_single()
    # graph_triple_files()
    # (Metrics()).post_example_results()
    # output_single()

    full_pipeline(
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


def pipeline_2(collection_name, chunks, book_title):
    """Extracts triples from a random chunk.
    @details
        - JSON triples (NLP & LLM)"""
    import json
    from src.components.relation_extraction import RelationExtractor
    from src.connectors.llm import LLMConnector

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


def pipeline_3(triples):
    """Generates a LLM summary using Neo4j triples.
    @details
        - Neo4j graph database
        - Blazor graph page"""
    for triple in triples:
        subj = triple["s"]
        rel = triple["r"]
        obj = triple["o"]
        print(subj, rel, obj)
        session.main_graph.add_triple(subj, rel, obj)

    # basic linear verbalization of triples (concatenate)
    edge_count_df = session.main_graph.get_edge_counts(top_n=3)
    edge_count_df = session.main_graph.find_element_names(edge_count_df, ["node_name"], ["node_id"], "node", "name", drop_ids=True)
    print("\nMost relevant nodes:")
    print(edge_count_df)

    triples_df = session.main_graph.get_all_triples()
    triples_df = session.main_graph.triples_to_names(triples_df, drop_ids=True)
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


def pipeline_4(collection_name, triples_string, chunk_id):
    """Generate chunk summary"""
    from src.connectors.llm import LLMConnector

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
    from src.components.metrics import Metrics

    m = Metrics()
    m.post_basic_output(book_id, book_title, summary)
    print("\nOutput sent to web app.")


# TODO: reconcile duplicate with boss.py
from src.core.boss import pipeline_5b

























##############################################################################################

if __name__ == "__main__":
    # TODO: handle this better - half env parsing is here, half is in boss.py
    load_dotenv(".env")
    DB_NAME = os.environ["DB_NAME"]
    BOSS_PORT = int(os.environ["PYTHON_PORT"])
    COLLECTION = os.environ["COLLECTION_NAME"]
    create_boss_thread(DB_NAME, BOSS_PORT, COLLECTION)

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

    triples, chunk = pipeline_2(COLLECTION, chunks, book_title)
    chunk_id = chunk.get_chunk_id()
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'completed')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'completed')

    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'in-progress')
    triples_string = pipeline_3(triples)
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'completed')

    post_story_status(BOSS_PORT, story_id, 'summarization', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'summarization', 'in-progress')
    summary = pipeline_4(COLLECTION, triples_string, chunk.get_chunk_id())
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
