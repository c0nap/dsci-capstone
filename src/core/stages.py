import json
import random
from src.components.book_conversion import Book, Chunk, EPUBToTEI, ParagraphStreamTEI, Story
from src.core.context import session
from src.util import Log

# unused?
import traceback
from typing import Optional


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

    triples_df = session.main_graph.get_by_ranked_degree(worst_rank=3, enforce_count=True, id_columns=["subject_id"])
    triples_df = session.main_graph.triples_to_names(triples_df, drop_ids=True)
    triples_string = session.main_graph.to_triples_string(triples_df, mode="triple")
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

    session.metrics.post_basic_output(book_id="1", book_title="Five Children and It", summary=response)
    print("\nOutput sent to web app.")






























##########################################################################


# PIPELINE STAGE A - PREPROCESS / BOOKS -> CHUNKS
def task_01_convert_epub(epub_path, converter: Optional[EPUBToTEI] = None):
    with Log.timer():
        # TODO: refactor converter, move to session.tei_converter?
        if converter is None:
            converter = EPUBToTEI(epub_path, save_pandoc=False, save_tei=True)
        converter.epub_path = epub_path
        converter.convert_to_tei()
        converter.clean_tei()
        # TODO: converter.print_chapters(200)
        return converter.tei_path


def task_02_parse_chapters(tei_path, book_chapters, book_id, story_id, start_str, end_str):
    with Log.timer():
        # TODO: refactor Story creation to make tests modular - still not independent yet
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
        return story


def task_03_chunk_story(story, max_chunk_length=1500):
    with Log.timer():
        story.pre_split_chunks(max_chunk_length=max_chunk_length)
        chunks = list(story.stream_chunks())
        return chunks


# PIPELINE STAGE B - RELATION EXTRACTION / CHUNKS -> TRIPLES
def task_10_random_chunk(chunks):
    with Log.timer():
        unique_numbers, sample = task_10_sample_chunks(chunks, n_sample=1)
        return (unique_numbers[0], sample[0])


def task_10_sample_chunks(chunks, n_sample):
    with Log.timer():
        unique_numbers = random.sample(range(len(chunks)), n_sample)
        sample = []
        for i in unique_numbers:
            c = chunks[i]
            sample.append(c)
        return (unique_numbers, sample)


def task_11_send_chunk(c, collection_name, book_title):
    with Log.timer():
        # TODO: remove book_title from chunk schema?
        mongo_db = session.docs_db.get_unmanaged_handle()
        collection = getattr(mongo_db, collection_name)
        collection.insert_one(c.to_mongo_dict())
        collection.update_one({"_id": c.get_chunk_id()}, {"$set": {"book_title": book_title}})


# TODO: 11, 12, 13 fit better as preprocessing tasks
# tied to pipeline_B -> pipeline_A


def task_12_relation_extraction_rebel(text, max_tokens=1024, parse_tuples=True):
    with Log.timer():
        from src.components.relation_extraction import RelationExtractor

        # TODO: move to session.rel_extract
        re_rebel = "Babelscape/rebel-large"
        # TODO: different models
        # re_rst = "GAIR/rst-information-extraction-11b"
        # ner_renard = "compnet-renard/bert-base-cased-literary-NER"
        nlp = RelationExtractor(model_name=re_rebel, max_tokens=max_tokens)
        extracted = nlp.extract(text, parse_tuples=parse_tuples)
        return extracted


def task_13_concatenate_triples(extracted):
    with Log.timer():
        # TODO: to_triples_string in RelationExtractor?
        triples_string = ""
        for triple in extracted:
            triples_string += str(triple) + "\n"
        return triples_string


def task_14_relation_extraction_llm(triples_string, text):
    with Log.timer():
        from src.connectors.llm import LLMConnector

        # TODO: move to session.llm
        llm = LLMConnector(
            temperature=0,
            system_prompt="You are a helpful assistant that converts semantic triples into structured JSON.",
        )
        prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
        prompt += f"And here is the original text:\n{text}\n\n"
        prompt += "Output JSON with keys: s (subject), r (relation), o (object).\n"
        prompt += "Remove nonsensical triples but otherwise retain all relevant entries, and add new ones to encapsulate events, dialogue, and core meaning where applicable."
        llm_output = llm.execute_query(prompt)
        return (prompt, llm_output)


def task_15_sanitize_triples_llm(llm_output: str):
    with Log.timer():
        # TODO: call LLM.normalize_triples
        json_triples = json.loads(llm_output)
        return json_triples


# PIPELINE STAGE C - ENRICHMENT / TRIPLES -> GRAPH
def task_20_send_triples(triples):
    with Log.timer():
        session.main_graph.add_triples_json(triples)


# TODO: 20 -> B


def group_21_1_describe_graph(top_n=3):
    with Log.timer():
        edge_count_df = session.main_graph.get_edge_counts()
        edge_count_df = session.main_graph.find_element_names(edge_count_df, ["node_name"], ["node_id"], "node", "name", drop_ids=True)
        # TODO: other graph summary dataframes / consolidate
        return edge_count_df


def group_21_2_send_statistics():
    with Log.timer():
        # TODO: upload to mongo
        pass


def group_21_3_post_statistics():
    with Log.timer():
        # TODO: notify blazor
        pass


def task_22_verbalize_triples(mode="triple"):
    with Log.timer():
        triples_df = session.main_graph.get_by_ranked_degree(worst_rank=3, enforce_count=True, id_columns=["subject_id"])
        triples_df = session.main_graph.triples_to_names(triples_df, drop_ids=True)
        triples_string = session.main_graph.to_triples_string(triples_df, mode=mode)
        return triples_string


# PIPELINE STAGE D - CONSOLIDATE / GRAPH -> SUMMARY
def task_30_summarize_llm(triples_string):
    """Prompt LLM to generate summary"""
    with Log.timer():
        from src.connectors.llm import LLMConnector

        # TODO: move to session.llm
        llm = LLMConnector(
            temperature=0,
            system_prompt="You are a helpful assistant that processes semantic triples.",
        )
        prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
        prompt += "Transform this data into a coherent, factual, and concise summary. Some relations may be irrelevant, so don't force yourself to include every single one.\n"
        prompt += "Output your generated summary and nothing else."
        summary = llm.execute_query(prompt)
        return (prompt, summary)


def task_31_send_summary(summary, collection_name, chunk_id):
    with Log.timer():
        mongo_db = session.docs_db.get_unmanaged_handle()
        collection = getattr(mongo_db, collection_name)
        collection.update_one({"_id": chunk_id}, {"$set": {"summary": summary}})


# PIPELINE STAGE E - EVALUATE / SUMMARY -> METRICS
def task_40_post_summary(book_id, book_title, summary):
    """Send book info to Blazor
    - Post to Blazor metrics page"""
    # TODO: pytest
    with Log.timer():
        session.metrics.post_basic_output(book_id, book_title, summary)


def task_40_post_payload(book_id, book_title, summary, gold_summary, chunk, bookscore, questeval):
    """Send metrics to Blazor
    - Compute basic metrics (ROUGE, BERTScore)
    - Wait for advanced metrics (QuestEval, BooookScore)
    - Post to Blazor metrics page"""
    # TODO: pytest
    with Log.timer():
        session.metrics.post_basic_metrics(book_id, book_title, summary, gold_summary, chunk, booook_score=bookscore, questeval_score=questeval)


# TODO: move rouge / bertscore out of post function
# TODO: move post out of metrics

##########################################################################
