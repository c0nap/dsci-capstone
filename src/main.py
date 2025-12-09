from dotenv import load_dotenv
import os
import pickle
from src.charts import Plot
from src.core import stages
from src.core.boss import (
    create_boss_thread,
    post_chunk_status,
    post_process_full_story,
    post_story_status
)
from src.util import Log
import time
from typing import Dict


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

    extracted = stages.task_12_relation_extraction(c.text)
    print(f"\nNLP output:")
    for triple in extracted:
        print(triple)
    print()
    triples_string = stages.task_13_concatenate_triples(extracted)

    prompt, llm_output = stages.task_14_validate_llm(triples_string, c.text)
    print("\n    LLM prompt:")
    print(prompt)
    print("\n    LLM output:")
    print(llm_output)
    print("\n" + "=" * 50 + "\n")

    triples = stages.task_15_sanitize_triples_llm(llm_output)
    triples = stages.task_16_moderate_triples_llm(triples)
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
    edge_count_df = stages.task_21_1_describe_graph()
    print("\nMost relevant nodes:")
    print(edge_count_df)

    triples_df = task_22_fetch_subgraph()
    triples_string = stages.task_23_verbalize_triples(triples_df)
    print("\nTriples which best represent the graph:")
    print(triples_string)
    return triples_string


@Log.time
def pipeline_D(collection_name, triples_string, chunk_id):
    """Generate chunk summary"""
    _, summary = stages.task_30_summarize_llm(triples_string)
    print("\nGenerated summary:")
    print(summary)

    stages.task_31_send_summary(summary, collection_name, chunk_id)
    print(f"    [Wrote summary to Mongo with chunk_id: {chunk_id}]")

    return summary


@Log.time
def pipeline_E(
    summary: str, book_title: str, book_id: str, chunk: str = "", gold_summary: str = "", bookscore: float = None, questeval: float = None
) -> Dict[str, float]:
    """Compute metrics and send available data to Blazor"""
    from src.core.stages import (
        task_45_eval_rouge,
        task_45_eval_bertscore,
        task_45_eval_ngrams,
        task_45_eval_jsd,
        task_45_eval_coverage,
        task_45_eval_ncd,
        task_45_eval_salience,
        task_45_eval_faithfulness,
        task_45_eval_readability,
        task_45_eval_sentence_coherence,
        task_45_eval_entity_grid,
        task_45_eval_diversity,
        task_45_eval_stopwords,
    )
    if chunk != "":
        _entity_coverage = task_45_eval_coverage(summary, chunk)
        CORE_METRICS: Dict[str, float] = {
            "rougeL_recall" : task_45_eval_rouge(summary, chunk)["rougeL_recall"],
            "bertscore" : task_45_eval_bertscore(summary, chunk)["bertscore_f1"],
            "novel_ngrams" : task_45_eval_ngrams(summary, chunk)["novel_ngram_pct"],
            "jsd_stats" : task_45_eval_jsd(summary, chunk)["jsd"],
            "entity_coverage" : _entity_coverage["entity_coverage"],
            "entity_hallucination" : _entity_coverage["entity_hallucination"],
            "ncd_overlap" : task_45_eval_ncd(summary, chunk)["ncd"],
            "salience_recall" : task_45_eval_salience(summary, chunk)["salience_recall"],
            "nli_faithfulness" : task_45_eval_faithfulness(summary, chunk)["nli_faithfulness"],
            "readability_delta" : task_45_eval_readability(summary, chunk)["readability_delta"],
            "sentence_coherence" : task_45_eval_sentence_coherence(summary)["sentence_coherence"],
            "entity_grid_coherence" : task_45_eval_entity_grid(summary)["entity_grid_coherence"],
            "lexical_diversity" : task_45_eval_diversity(summary)["lexical_diversity"],
            "stopword_ratio" : task_45_eval_stopwords(summary)["stopword_ratio"],
            "bookscore" : bookscore,
            "questeval" : questeval,
        }

    if chunk == "":
        stages.task_40_post_summary(book_id, book_title, summary)
    else:
        stages.task_40_post_payload(book_id, book_title, summary, gold_summary, chunk, bookscore, questeval)
    print("\nOutput sent to web app.")
    return CORE_METRICS


@Log.time
def full_pipeline(collection_name, epub_path, book_chapters, start_str, end_str, book_id, story_id, book_title):
    chunks = pipeline_A(epub_path, book_chapters, start_str, end_str, book_id, story_id)
    triples, chunk = pipeline_B(collection_name, chunks, book_title)
    triples_string = pipeline_C(triples)
    summary = pipeline_D(collection_name, triples_string, chunk.get_chunk_id())
    pipeline_E(summary, book_title, book_id)


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
    from src.core.context import session

    session.setup()
    # TODO: handle this better - half env parsing is here, half is in boss.py
    load_dotenv(".env")
    DB_NAME = os.environ["DB_NAME"]
    BOSS_PORT = int(os.environ["PYTHON_PORT"])
    COLLECTION = os.environ["COLLECTION_NAME"]
    create_boss_thread(DB_NAME, BOSS_PORT, COLLECTION)

    # TODO - PIPELINE HERE
    load_from_checkpoint = False
    compute_worker_metrics = True
    checkpoint_path = "./datasets/checkpoint.pkl"
    os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)

    story_id = 1
    book_id = 2
    book_title = "The Phoenix and the Carpet"

    if load_from_checkpoint:
        with open(checkpoint_path, "rb") as f_read:
            data = pickle.load(f_read)
        triples = data["triples"]
        chunk = data["chunk"]
        print(f"Checkpoint loaded from {checkpoint_path}")
    else:
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

        with open(checkpoint_path, "wb") as f_write:
            pickle.dump({"triples": triples, "chunk": chunk}, f_write)
        print(f"Checkpoint saved to {checkpoint_path}")

    chunk_id = chunk.get_chunk_id()
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'relation_extraction', 'completed')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'llm_inference', 'completed')

    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'in-progress')
    triples_string = pipeline_C(triples)
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'graph_verbalization', 'completed')

    post_story_status(BOSS_PORT, story_id, 'summarization', 'in-progress')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'summarization', 'in-progress')
    summary = pipeline_D(COLLECTION, triples_string, chunk.get_chunk_id())
    post_story_status(BOSS_PORT, story_id, 'summarization', 'completed')
    post_chunk_status(BOSS_PORT, chunk_id, story_id, 'summarization', 'completed')

    # Post chunk - this will enqueue worker processing
    if compute_worker_metrics:
        for task_type in ["questeval", "bookscore"]:
            response = post_process_full_story(BOSS_PORT, story_id, task_type)
            print(f"Triggered {task_type}: {response.json()}")
            # pipeline_E is moved to callback() to finalize asynchronously
    else:
        pipeline_E(summary, book_title, book_id)

    # Write core function timing - Keyboard interrupt doesnt work
    Log.print_timing_summary()
    Log.dump_timing_csv()  # TODO: Eventually updated by callback()
    Plot.time_elapsed_by_names()

    # Hand off to Flask - keep main thread alive so boss thread continues
    print("Initial processing complete. Server listening for additional requests from Blazor...")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
