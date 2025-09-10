#from src.setup import Session
from components.book_conversion import Book, Chunk, EPUBToTEI, Story, ParagraphStreamTEI
import pandas as pd
import random
import traceback
import json
import os

#session = Session()
#print()

def convert_single():
    print("\n\nCHAPTERS for book 1: FAIRY TALES")
    epub_file_1 = "./datasets/examples/nested-fairy-tales.epub"
    converter = EPUBToTEI(epub_file_1, chapter_div_type = "level3", save_pandoc=True, save_tei=True)
    converter.convert_to_tei()
    converter.clean_tei()
    converter.print_chapters(200)
    
    print("\n\nCHAPTERS for book 2: MYTHS")
    epub_file_2 = "./datasets/examples/nested-myths.epub"
    converter = EPUBToTEI(epub_file_2, chapter_div_type = "level2", save_pandoc=True, save_tei=True)
    converter.convert_to_tei()
    converter.clean_tei()
    converter.print_chapters(200)


def convert_from_csv():
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

            converter = EPUBToTEI(row.get("epub_path"), chapter_div_type = "level2", save_pandoc=False, save_tei=True)
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
    chaps = [line.strip() for line in chapters.splitlines() if line.strip()]
    reader = ParagraphStreamTEI(tei, book_id = 1, story_id = 1, allowed_chapters = chaps, start_inclusive = start, end_inclusive = end)
    story = Story(reader)
    story.pre_split_chunks(max_chunk_length = 1500)
    chunks = list(story.stream_chunks())

    print("\n=== STORY SUMMARY ===")
    print(f"Total chunks: {len(chunks)}")
    print("Chunk previews (first 10):")
    for i in range(min(10, len(chunks))):
        c = chunks[i]
        snippet = (c.text[:80] + "...") if len(c.text) > 80 else c.text
        print(f"  [{i}] Story:{c.story_percent:.1f}% Chapter:{c.chapter_percent:.1f}% - {snippet}")

    print("\n\nFull chunks (last 3):")
    for i in range(len(chunks)-3, len(chunks)):
        c = chunks[i]
        print(f"  [{i}] {c}")
        print(c.text)
        print()



def process_single():
    from components.text_processing import RelationExtractor, LLMConnector

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
    reader = ParagraphStreamTEI(tei, book_id = 1, story_id = 1, allowed_chapters = chaps, start_inclusive = start, end_inclusive = end)
    story = Story(reader)
    story.pre_split_chunks(max_chunk_length = 1500)
    chunks = list(story.stream_chunks())

    print("\n=== STORY SUMMARY ===")
    print(f"Total chunks: {len(chunks)}")

    print("\n=== NLP EXTRACTION SAMPLE ===")
    re_rebel = "Babelscape/rebel-large"
    re_rst = "GAIR/rst-information-extraction-11b"
    ner_renard = "compnet-renard/bert-base-cased-literary-NER"
    nlp = RelationExtractor(model_name=re_rebel, max_tokens=1024)
    llm = LLMConnector(temperature=0, system_prompt = "You are a helpful assistant that converts semantic triples into structured JSON.")

    unique_numbers = random.sample(range(len(chunks)), 2)
    for i in unique_numbers:
        c = chunks[i]
        print("\nChunk details:")
        print(f"  [{i}] {c}\n")
        print(c.text)

        extracted = nlp.extract(c.text, parse_tuples = True)
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

        print("\n" + "="*50 + "\n")


triple_files = [
    "./datasets/triples/chunk-160_story-1.json",
    "./datasets/triples/chunk-70_story-1.json"
]
def graph_triple_files():
    for json_path in triple_files:
        # Load existing triples to save NLP time / LLM tokens during MVP stage
        with open(json_path, "r") as f:
            triples = json.load(f)

        for triple in triples:
            subj = triple['s']
            rel = triple['r']
            obj = triple['o']
            print(subj, rel, obj)



if __name__ == "__main__":
    #convert_from_csv()
    #chunk_single()
    #process_single()
    graph_triple_files()