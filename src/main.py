from dotenv import load_dotenv
import os
import time
from src.core import stages
# move to stages



def full_pipeline(collection_name, epub_path, book_chapters, start_str, end_str, book_id, story_id, book_title):
    chunks = stages.pipeline_1(epub_path, book_chapters, start_str, end_str, book_id, story_id)
    triples, chunk = stages.pipeline_2(collection_name, chunks, book_title)
    triples_string = stages.pipeline_3(triples)
    summary = stages.pipeline_4(collection_name, triples_string, chunk.get_chunk_id())
    stages.pipeline_5a(summary, book_title, book_id)


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
    chunks = stages.pipeline_1(
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

    triples, chunk = stages.pipeline_2(COLLECTION, chunks, book_title)
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
