import random
from src.components.book_conversion import Book, Chunk, EPUBToTEI, ParagraphStreamTEI, Story
from src.core.context import session
import json
from typing import Optional
# unused?
import traceback



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

# PIPELINE STAGE A
def linear_01_convert_epub(epub_path, converter: Optional[EPUBToTEI] = None):
    if converter is None:
        converter = EPUBToTEI(epub_path, save_pandoc=False, save_tei=True)
    converter.epub_path = epub_path
    converter.convert_to_tei()
    converter.clean_tei()
    # TODO: converter.print_chapters(200)
    return converter.tei_path

def linear_02_parse_chapters(tei_path, book_chapters, book_id, story_id, start_str, end_str):
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

def linear_03_chunk_story(story, max_chunk_length=1500):
    story.pre_split_chunks(max_chunk_length=max_chunk_length)
    chunks = list(story.stream_chunks())
    return chunks

# PIPELINE STAGE B
def linear_10_random_chunk(chunks):
    unique_numbers, sample = linear_10_sample_chunks(chunks, n_sample = 1)
    return (unique_numbers[0], sample[0])

def linear_10_sample_chunks(chunks, n_sample):
    unique_numbers = random.sample(range(len(chunks)), n_sample)[0]
    sample = []
    for i in unique_numbers:
        c = chunks[i]
        sample.append(c)
    return (unique_numbers, sample)

def linear_11_send_chunk(c):
    mongo_db = session.docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    collection.insert_one(c.to_mongo_dict())
    collection.update_one({"_id": c.get_chunk_id()}, {"$set": {"book_title": book_title}})

def linear_12_relation_extraction_rebel(text):
    from src.components.relation_extraction import RelationExtractor
    re_rebel = "Babelscape/rebel-large"
    # TODO: different models
    #re_rst = "GAIR/rst-information-extraction-11b"
    #ner_renard = "compnet-renard/bert-base-cased-literary-NER"

    nlp = RelationExtractor(model_name=re_rebel, max_tokens=1024)
    triples = nlp.extract(text, parse_tuples=True)
    return triples

##########################################################################


def pipeline_B(collection_name, chunks, book_title):
    """Extracts triples from a random chunk.
    @details
        - JSON triples (NLP & LLM)"""
    import json
    from src.connectors.llm import LLMConnector

    
    llm = LLMConnector(
        temperature=0,
        system_prompt="You are a helpful assistant that converts semantic triples into structured JSON.",
    )

    ci, c = linear_10_random_chunk(chunks)
    print("\nChunk details:")
    print(f"  index: {ci}\n")
    print(c.text)

    linear_11_send_chunk(c)
    print(f"    [Inserted chunk into Mongo with chunk_id: {c.get_chunk_id()}]")

    extracted = linear_12_relation_extraction_rebel(c.text)
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
        print(triple["s"], triple["r"], triple["o"])
    # TODO: normalize
    session.main_graph.add_triples_json(triples)

    # basic linear verbalization of triples (concatenate)
    edge_count_df = session.main_graph.get_edge_counts(top_n=3)
    edge_count_df = session.main_graph.find_element_names(edge_count_df, ["node_name"], ["node_id"], "node", "name", drop_ids=True)
    print("\nMost relevant nodes:")
    print(edge_count_df)

    triples_df = session.main_graph.get_by_ranked_degree(min_rank=3, id_columns=["subject_id"])
    triples_df = session.main_graph.triples_to_names(triples_df, drop_ids=True)
    triples_string = session.main_graph.to_triples_string(triples_df, mode="triple")
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
    session.metrics.post_basic_output(book_id, book_title, summary)
    print("\nOutput sent to web app.")


def pipeline_5b(
    summary: str, book_title: str, book_id: str, chunk: str, gold_summary: str = "", bookscore: float = None, questeval: float = None
) -> None:
    """Send metrics to Blazor
    - Compute basic metrics (ROUGE, BERTScore)
    - Wait for advanced metrics (QuestEval, BooookScore)
    - Post to Blazor metrics page"""
    session.metrics.post_basic_metrics(book_id, book_title, summary, gold_summary, chunk, booook_score=bookscore, questeval_score=questeval)
    print("\nOutput sent to web app.")

