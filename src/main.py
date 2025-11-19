from dotenv import load_dotenv
import os
import time
from src.core import stages
# move to stages
from src.core.boss import (
    create_boss_thread,
    post_chunk_status,
    post_process_full_story,
    post_story_status
)
from src.util import Log


@Log.time
def pipeline_A(epub_path, book_chapters, start_str, end_str, book_id, story_id):
    """Connects all components to convert an EPUB file to a book summary.
    @details  Data conversions:
        - EPUB file
        - XML (TEI)
    """
    print(f"\n{'='*50}")
    print(f"Processing: {epub_path}")

    tei_path = stages.task_01_convert_epub(epub_path)
    story = stages.task_02_parse_chapters(tei_path, book_chapters, book_id, story_id, start_str, end_str)
    chunks = stages.task_03_chunk_story(story)

    print("\n=== STORY SUMMARY ===")
    print(f"Total chunks: {len(chunks)}")
    return chunks


@Log.time
def pipeline_B(collection_name, chunks, book_title):
    """Extracts triples from a random chunk.
    @details
        - JSON triples (NLP & LLM)"""
    ci, c = stages.task_10_random_chunk(chunks)
    print("\nChunk details:")
    print(f"  index: {ci}\n")
    print(c.text)

    stages.task_11_send_chunk(c, collection_name, book_title)
    print(f"    [Inserted chunk into Mongo with chunk_id: {c.get_chunk_id()}]")

    extracted = stages.task_12_relation_extraction_rebel(c.text)
    print(f"\nNLP output:")
    for triple in extracted:
        print(triple)
    print()
    triples_string = stages.task_13_concatenate_triples(extracted)
    
    prompt, llm_output = stages.task_14_relation_extraction_llm(triples_string, c.text)
    print("\n    LLM prompt:")
    print(prompt)
    print("\n    LLM output:")
    print(llm_output)
    print("\n" + "=" * 50 + "\n")

    triples = stages.task_15_sanitize_triples_llm(llm_output)
    print("\nValid JSON")
    return triples, c



@Log.time
def pipeline_C(json_triples):
    """Generates a LLM summary using Neo4j triples.
    @details
        - Neo4j graph database
        - Blazor graph page"""
    for triple in json_triples:
        print(triple["s"], triple["r"], triple["o"])
    stages.task_20_send_triples(json_triples)

    # basic linear verbalization of triples (concatenate)
    edge_count_df = stages.task_21_graph_summary()
    print("\nMost relevant nodes:")
    print(edge_count_df)

    triples_string = stages.task_22_verbalize_triples()
    print("\nTriples which best represent the graph:")
    print(triples_string)
    return triples_string


@Log.time
def full_pipeline(collection_name, epub_path, book_chapters, start_str, end_str, book_id, story_id, book_title):
    chunks = pipeline_A(epub_path, book_chapters, start_str, end_str, book_id, story_id)
    triples, chunk = pipeline_B(collection_name, chunks, book_title)
    triples_string = pipeline_C(triples)
    summary = stages.pipeline_4(collection_name, triples_string, chunk.get_chunk_id())
    stages.pipeline_5a(summary, book_title, book_id)


def old_main(collection_name):
    # convert_from_csv()
    # chunk_single()
    # process_single()
    # graph_triple_files()
    # session.metrics.post_example_results()
    # output_single()

    full_pipeline(
        collection_name,
        epub_path="./tests/examples-pipeline/epub/trilogy-wishes-2.epub",
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
    chunks = pipeline_A(
        epub_path="./tests/examples-pipeline/epub/trilogy-wishes-2.epub",
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

    triples, chunk = pipeline_B(COLLECTION, chunks, book_title)
    chunk_id = chunk.get_chunk_id()
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'completed')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'completed')

    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'in-progress')
    triples_string = stages.pipeline_3(triples)
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'completed')

    post_story_status(BOSS_PORT, story_id, 'summarization', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'summarization', 'in-progress')
    summary = stages.pipeline_4(COLLECTION, triples_string, chunk.get_chunk_id())
    post_story_status(BOSS_PORT, story_id, 'summarization', 'completed')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'summarization', 'completed')
    # stages.pipeline_5 is moved to callback() to finalize asynchronously
    # stages.pipeline_5a(summary, book_title, book_id)

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
