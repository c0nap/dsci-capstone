#from src.setup import Session
from components.text_processing import Book, Chunk

#session = Session()
#print()

book = Book(filename = "./datasets/examples/Fairy_Tales.txt",
    chapter_delimiter_count = 4, chapter_heading_end_count = 2, section_delimiter_count = 4,
    book_start_line = 405,
    book_end_line = 10320,
    author_key = "Editor:")

print("Start reading example book 1")
book.debug_pre_scan()


