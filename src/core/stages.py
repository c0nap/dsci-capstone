from src.components.book_conversion import Book, Chunk, EPUBToTEI, ParagraphStreamTEI, Story
from src.connectors.llm import to_triples_string
from src.components.relation_extraction import RelationExtractor, Triple
from src.core.context import session
from src.util import Log

# unused?
import traceback
from typing import Dict, List, Optional, Tuple, Any
from src.components.metrics import (
    run_rouge_l,
    run_bertscore,
    run_novel_ngrams,
    run_jsd_stats,
    run_entity_coverage,
    run_ncd_overlap,
    run_salience_recall,
    run_nli_faithfulness,
    run_readability_delta,
    run_sentence_coherence,
    run_entity_grid_coherence,
    run_lexical_diversity,
    run_stopword_ratio,
)

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







##########################################################################


# PIPELINE STAGE A - PREPROCESS / BOOKS -> CHUNKS
def task_01_convert_epub(epub_path: str, converter: Optional[EPUBToTEI] = None) -> str:
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
def task_10_sample_chunks(chunks):
    # TODO: trivial time elapsed, consider combining with another task
    return session.config.get_chunks(session.config.chunk_selection_method, chunks)

def task_11_send_chunks(chunks, collection_name, book_title):
    with Log.timer(label=f"[{len(chunks)}]"):
        for c in chunks:
            # TODO: remove book_title from chunk schema?
            mongo_db = session.docs_db.get_unmanaged_handle()
            collection = getattr(mongo_db, collection_name)
            collection.insert_one(c.to_mongo_dict())
            collection.update_one({"_id": c.get_chunk_id()}, {"$set": {"book_title": book_title}})


# TODO: 11, 12, 13 fit better as preprocessing tasks
# tied to pipeline_B -> pipeline_A
    

def task_12_relation_extraction(text: str) -> List[Triple]:
    extractor_type = session.config.relation_extractor_type
    with Log.timer(label=f"[{extractor_type}]"):
        re = session.config.get_extractor(extractor_type)
        extracted = re.extract(text)
        return extracted


def task_14_validate_llm(triples: List[Triple], text: str) -> Tuple[str, List[Triple]]:
    llm_connector_type = session.config.validation_llm_engine
    with Log.timer(label=f"[{llm_connector_type}]"):
        triples_string = to_triples_string(triples)
        # TOOD: reasoning_effort, model_name, prompt_basic
        system_prompt = "You are a helpful assistant that converts semantic triples into structured JSON."
        llm = session.config.get_llm(llm_connector_type, system_prompt)
        prompt = f"Here are some semantic triples extracted from a story chunk:\n{triples_string}\n"
        prompt += f"And here is the original text:\n{text}\n\n"
        prompt += "Output JSON with keys: s (subject), r (relation), o (object).\n"
        prompt += "Remove nonsensical triples but otherwise retain all relevant entries, and add new ones to encapsulate events, dialogue, and core meaning where applicable."
        triples = llm.execute_to_triples(prompt)
        return (prompt, triples)


def task_16_moderate_triples_llm(triples: List[Triple], text: str) -> List[Triple]:
    """Filter offensive content from literary triples.
    @param triples  Normalized triples in JSON format.
    @param text  The original source text for context.
    @return Safe triples for knowledge graph insertion."""
    moderation_strategy = session.config.moderation_strategy
    with Log.timer(label=f"[{moderation_strategy}]"):
        from src.connectors.llm import flag_triples
        safe, bad = flag_triples(triples, session.config.get_moderation_thresholds())
        if moderation_strategy == "resolve":
            if not bad:  # Optimization: If nothing is bad, skip the expensive LLM call
                return safe
            fixed = _task_16_resolve_strategy(bad, text)
            return safe + fixed
        else:  # == "drop":
            return safe


def _task_16_resolve_strategy(
    bad_triples: List[Tuple[Triple, Dict[str, float]]],
    text: str,
) -> List[Triple]:
    """Attempt to fix flagged triples using context from original text.
    @details
    - Uses LLM to distinguish between malicious content and literary depictions
    - Drops triples that cannot be redeemed
    @param bad_triples  List of (triple, reasons) tuples
    @param text  The source text chunk
    @return List of corrected/sanitized triples
    """
    from src.connectors.llm import to_flagged_reasons
    llm_connector_type = session.config.moderation_llm_engine
    triples_string = to_flagged_reasons(bad_triples)

    system_prompt = "You are a helpful assistant that corrects harmful content in old fiction."
    llm = session.config.get_llm(llm_connector_type, system_prompt)
    prompt = f"Here are some flagged triples extracted from a story chunk:\n{triples_string}\n"
    prompt += f"And here is the original text:\n{text}\n\n"
    prompt += "Output JSON with keys: s (subject), r (relation), o (object).\n"
    prompt += "For each triple you must fix the harmful content by inspecting the intent of the original text."
    prompt += "If the original text has genuinely harmful content represented by this triple, then drop this triple."

    triples = llm.execute_to_triples(prompt)
    return triples




# PIPELINE STAGE C - ENRICHMENT / TRIPLES -> GRAPH
def task_20_send_triples(triples: List[Triple]) -> None:
    with Log.timer():
        session.main_graph.add_triples_json(triples)


# TODO: 20 -> B


def task_21_1_describe_graph(top_n=3):
    with Log.timer():
        edge_count_df = session.main_graph.get_edge_counts(top_n)
        edge_count_df = session.main_graph.find_element_names(edge_count_df, ["node_name"], ["node_id"], "node", "name", drop_ids=True)
        # TODO: other graph summary dataframes / consolidate
        return edge_count_df


def task_21_2_send_statistics():
    with Log.timer():
        # TODO: upload to mongo
        pass


def task_21_3_post_statistics():
    with Log.timer():
        # TODO: notify blazor
        pass


def task_22_fetch_subgraph():
    """Retrieve and convert subgraph to named triples."""
    lookup_mode = session.config.graph_lookup_mode
    with Log.timer(label=f"[{lookup_mode}]"):
        triples_df = session.config.get_subgraph(lookup_mode)
        triples_df = session.main_graph.triples_to_names(triples_df, drop_ids=True)
        return triples_df


def task_23_verbalize_triples(triples_df):
    """Convert triples to string format for LLM consumption."""
    verbal_mode = session.config.verbalize_triples_mode
    with Log.timer(label=f"[{verbal_mode}]"):
        triples_string = session.main_graph.to_triples_string(triples_df, verbal_mode)
        return triples_string


# PIPELINE STAGE D - CONSOLIDATE / GRAPH -> SUMMARY
def task_30_summarize_llm(triples_string: str = None, text: str = None) -> Tuple[str, str]:
    """Prompt LLM to generate summary"""
    use_triples = session.config.triples_visible
    use_text = session.config.source_text_visible
    llm_connector_type = session.config.summary_llm_engine
    # TODO: maybe make this a string config instead of 2 bools
    if use_triples and use_text:
        label = "all"
    else:
        label = "triples" if use_triples else "text"
    with Log.timer(label=f"[{label}]"):
        # TOOD: reasoning_effort, model_name, prompt_basic
        system_prompt = "You are a helpful assistant that summarizes text."
        llm = session.config.get_llm(llm_connector_type, system_prompt)
        prompt = session.config.get_final_prompt(use_triples, use_text, triples_string, text)
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
        return session.metrics.post_example(book_id, book_title, summary)


def task_40_post_payload(book_id, book_title, summary, gold_summary, chunk, bookscore, questeval):
    """Send metrics to Blazor
    - Compute basic metrics (ROUGE, BERTScore)
    - Wait for advanced metrics (QuestEval, BooookScore)
    - Post to Blazor metrics page"""
    # TODO: pytest
    with Log.timer():
        session.metrics.post_basic(book_id, book_title, summary, gold_summary, chunk, booook_score=bookscore, questeval_score=questeval)


def task_45_eval_rouge(summary, chunk):
    """Compute metric for ROUGE-L Recall (Coverage Score)"""
    with Log.timer():
        return run_rouge_l(summary, chunk)

def task_45_eval_bertscore(summary, chunk):
    """Compute metric for BERTScore embedding similarity"""
    with Log.timer():
        return run_bertscore(summary, chunk)

def task_45_eval_ngrams(summary, chunk):
    """Compute metric for Novel n-gram Percentage"""
    with Log.timer():
        return run_novel_ngrams(summary, chunk)

def task_45_eval_jsd(summary, chunk):
    """Compute metric for Jensen-Shannon Divergence (JSD)"""
    with Log.timer():
        return run_jsd_stats(summary, chunk)

def task_45_eval_coverage(summary, chunk):
    """Compute metric for Entity Coverage & Hallucination (spaCy)"""
    with Log.timer():
        return run_entity_coverage(summary, chunk)

def task_45_eval_ncd(summary, chunk):
    """Compute metric for Normalized Compression Distance (NCD)"""
    with Log.timer():
        return run_ncd_overlap(summary, chunk)

def task_45_eval_salience(summary, chunk):
    """Compute metric for TF-IDF Salience Recall"""
    with Log.timer():
        return run_salience_recall(summary, chunk)

def task_45_eval_faithfulness(summary, chunk):
    """Compute metric for NLI-based Faithfulness Score"""
    with Log.timer():
        return run_nli_faithfulness(summary, chunk)

def task_45_eval_readability(summary, chunk):
    """Compute metric for Readability Delta (textstats)"""
    with Log.timer():
        return run_readability_delta(summary, chunk)

def task_45_eval_sentence_coherence(summary):
    """Compute metric for Sentence Coherence (Adjacent Embedding Similarity)"""
    with Log.timer():
        return run_sentence_coherence(summary)

def task_45_eval_entity_grid(summary):
    """Compute metric for Entity Grid Coherence (Discourse Structure)"""
    with Log.timer():
        return run_entity_grid_coherence(summary)

def task_45_eval_diversity(summary):
    """Compute metric for Lexical Diversity (Type-Token Ratio)"""
    with Log.timer():
        return run_lexical_diversity(summary)

def task_45_eval_stopwords(summary):
    """Compute metric for Stopword Ratio (Content Density)"""
    with Log.timer():
        return run_stopword_ratio(summary)





# TODO: move rouge / bertscore out of post function
# TODO: move post out of metrics

##########################################################################
