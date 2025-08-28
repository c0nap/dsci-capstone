#from src.setup import Session
from components.text_processing import Book, Chunk, EPUBToTEI

#session = Session()
#print()

# book = Book(filename = "./datasets/examples/Fairy_Tales.txt",
#     chapter_delimiter_count = 4, chapter_heading_end_count = 2, section_delimiter_count = 4,
#     book_start_line = 405,
#     book_end_line = 10320,
#     author_key = "Editor:")

# print("Start reading example book 1")
# book.debug_pre_scan()

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
