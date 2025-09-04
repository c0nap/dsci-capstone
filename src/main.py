#from src.setup import Session
from components.text_processing import Book, Chunk, EPUBToTEI, Story, ParagraphStreamTEI
import pandas as pd

#session = Session()
#print()

def main1():
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
#             import traceback
#             traceback.print_exc()


def main3():
    tei = "./datasets/examples/trilogy-wishes-1.tei"
    reader = ParagraphStreamTEI(tei, book_id = 1, story_id = 1)
    story = Story(reader)
    chunks = list(story.stream_chunks())

    print("\n=== STORY SUMMARY ===")
    print(f"Total chunks: {len(chunks)}")
    print("Chunk previews (first 10):")
    for i in range(min(10, len(chunks))):
        c = chunks[i]
        snippet = (c.text[:80] + "...") if len(c.text) > 80 else c.text
        print(f"  [{i}] Story:{c.story_percent:.1f}% Chapter:{c.chapter_percent:.1f}% - {snippet}")



if __name__ == "__main__":
    main3()