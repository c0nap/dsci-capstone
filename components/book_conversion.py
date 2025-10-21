from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
from lxml import etree
import os
import pandas as pd
import pypandoc
import re
import spacy
from typing import Dict, Iterator, List, Tuple


nlp = spacy.blank("en")  # blank English model, no pipeline
sentencizer = nlp.add_pipe("sentencizer")


class Chunk:
    """Lightweight container for a span of story text.
    @details
        - Carries positional metadata so downstream consumers can reconstruct context.
        - Filter by story_id to fetch all chunks for a particular story.
        - Use story_percent and chapter_percent to quickly sort chunks by intended order.
        - Use book_id, chapter_number, line_start, and line_end to locate this chunk within source material.
    """

    def __init__(
        self,
        text: str,
        book_id: int,
        chapter_number: int,
        line_start: int,
        line_end: int,
        story_id: int,
        story_percent: float,
        chapter_percent: float,
        max_chunk_length: int = -1,
    ):
        """Construct a Chunk.
        @param text  The text content for this span.
        @param book_id  Corresponds to a single book file in the dataset.
        @param chapter_number  The chapter containing this chunk in the book file, 1-based.
        @param line_start  The starting line within the TEI file, 1-based.
        @param line_end  The inclusive ending line index within the TEI file (>= line_start).
        @param story_id  A stable id for the overall story. May be identical to book_id
        @param story_percent  Approximate progress through the whole story [0.0, 100.0].
        @param chapter_percent  Approximate progress through the current segment [0.0, 100.0].
        @param max_chunk_length  Max allowed characters (<= 0 means "no limit").
        @throws ValueError  if text exceeds max_chunk_length when max_chunk_length > 0.
        """
        self.text: str = text
        self.book_id: int = book_id
        self.chapter_number: int = chapter_number
        self.line_start: int = line_start
        self.line_end: int = line_end
        self.story_id: int = story_id
        self.story_percent: float = story_percent
        self.chapter_percent: float = chapter_percent
        self.max_chunk_length: int = max_chunk_length
        self.length: int = self.char_count(False)

        if max_chunk_length > 0 and self.length > max_chunk_length:
            raise ValueError(f"Invalid chunk: text has {self.length} chars; " f"expected ≤ {max_chunk_length}")

    def char_count(self, prune_newlines: bool = False) -> int:
        """Computes the character count.
        @param prune_newlines  Whether to remove newlines for the count.
        @return  The number of characters in the chunk text."""
        if prune_newlines:
            return len(self.text.replace("\n", ""))
        return len(self.text)

    def __repr__(self) -> str:
        return (
            f"Chunk(len={self.length}, book={self.book_id}, chapter={self.chapter_number}, "
            f"story={self.story_id}, lines=[{self.line_start},{self.line_end}], "
            f"story%={self.story_percent:.3f}, chapter%={self.chapter_percent:.3f})"
        )


class StoryStreamAdapter(ABC):
    @abstractmethod
    def stream_segments(self) -> Chunk:
        """Yields sanitized parts of a book.
        @details
            - Story segments usually correspond to chapters.
            - They serve as borders between chunking operations, ensuring chunks
            do not span multiple chapters. Implementation is handled by child classes BookStream, etc.
            - Segments should be pre-cleaned and must contain 1 paragraph per
            line with all other newlines removed."""
        pass

    def stream_paragraphs(self) -> Chunk:
        """Concrete helper method to split segments into paragraphs.
        @details  The Chunk class is repurposed here so we pass location info. Depending on the Story.pre_split_chunks implementation, this might be unnecessary.
        """
        pass

    def stream_sentences(self) -> str:
        """Concrete helper method to split paragraphs into sentences. Mostly for debugging."""
        pass


class Story:
    def __init__(self, reader: StoryStreamAdapter):
        self.reader = reader
        self.chunks: list[Chunk] = []

    def stream_chunks(self) -> Chunk:
        for chunk in self.chunks:
            yield chunk

    def pre_split_chunks(self, max_chunk_length: int):
        """Splits paragraphs into chunks.
        @details
            - Populates self.chunks with Chunk objects that obey max_chunk_length.
            - Combines adjacent paragraphs when possible.
            - Falls back to splitting by sentences if one paragraph is too long."""
        buffer = []  # stores candidates to consolidate into chunks

        for seg in self.reader.stream_segments():
            # Case 1: paragraph itself is too long
            if max_chunk_length > 0 and seg.char_count() > max_chunk_length:
                if buffer:  # clear anything left over - we need the entire buffer for this operation
                    self._merge_chunks(buffer, max_chunk_length)
                    buffer = []

                # if we can't split by paragraphs, sentences are the next best option
                doc = nlp(seg.text)
                sentences = [sent.text for sent in doc.sents]

                # combine sentences until adding another would surpass limit
                previous_sentences = ""
                for sentence in sentences:
                    # append the next sentence
                    candidate = (previous_sentences + " " + sentence).strip()
                    # judge the new string
                    if max_chunk_length > 0 and len(candidate) > max_chunk_length:
                        # failed - revert to previous iteration
                        if len(sentence) > max_chunk_length:
                            print(f"Uh oh! {len(sentence)} > {max_chunk_length}")
                            print(sentence)
                        if previous_sentences:
                            self.chunks.append(self._make_single(seg, previous_sentences.strip(), max_chunk_length))
                        # start new chunk with this sentence
                        previous_sentences = sentence
                    else:  # otherwise valid, and accept the candidate
                        previous_sentences = candidate

                # flush whatever is left
                if previous_sentences:
                    self.chunks.append(self._make_single(seg, previous_sentences.strip(), max_chunk_length))
                continue

            # Case 2: try combining paragraphs
            candidate = "\n".join([c.text for c in buffer] + [seg.text]) if buffer else seg.text
            if max_chunk_length > 0 and len(candidate) > max_chunk_length:
                self._merge_chunks(buffer, max_chunk_length)
                buffer = [seg]
            else:
                buffer.append(seg)

        # flush leftover
        if buffer:
            self._merge_chunks(buffer, max_chunk_length)

    def _merge_chunks(self, segs, max_len):
        start, end = segs[0], segs[-1]
        text = "\n".join(s.text for s in segs)
        self.chunks.append(self._make_single(end, text, max_len, start))

    def _make_single(self, seg, text, max_len, start=None):
        return Chunk(
            text=text,
            book_id=seg.book_id,
            chapter_number=seg.chapter_number,
            line_start=start.line_start if start else seg.line_start,
            line_end=seg.line_end,
            story_id=seg.story_id,
            story_percent=seg.story_percent,
            chapter_percent=start.chapter_percent if start else seg.chapter_percent,
            max_chunk_length=max_len,
        )


class ParagraphStreamTEI(StoryStreamAdapter):
    """Streams paragraphs from a TEI file as Chunk objects."""

    xml_namespace = {"tei": "http://www.tei-c.org/ns/1.0"}
    encoding = "utf-8"

    def __init__(
        self,
        tei_path: str,
        book_id: int,
        story_id: int,
        allowed_chapters: list[str] = None,
        start_inclusive: str = "",
        end_inclusive: str = "",
    ):
        """Create a ParagraphStreamTEI object.
        @param tei_path  Path to an existing TEI XML file.
        @param book_id  ID for this book.
        @param story_id  ID for this story (may be same as book_id).
        @param allowed_chapters  A list of valid chapter titles. Must exactly match the contents of head.
        @param start_inclusive  (Optional) Unique string representing the start of the book.
        @param end_inclusive  (Optional) Unique string representing the end of the book.
        """
        self.tei_path = tei_path
        self.book_id = book_id
        self.story_id = story_id
        self.allowed_chapters = allowed_chapters
        self.start_inclusive = start_inclusive
        self.end_inclusive = end_inclusive

        # Read lines for line-based position tracking
        with open(tei_path, encoding=self.encoding) as f:
            self.lines = f.readlines()

        # Parse TEI
        self.root = etree.parse(tei_path).getroot()

        # TMP: Necessary to fix chapter percentages
        self.chunks = list(self.pre_compute_segments())

    def stream_segments(self) -> Iterator[Chunk]:
        for chunk in self.chunks:
            yield chunk

    def pre_compute_segments(self) -> List[Chunk]:
        """Splits the target book into paragraphs.
        @details
            Yields Chunk objects for each paragraph (<p>) in the TEI file.
            Uses etree Element.sourceline to approximate start/end line in TEI.
            Supports optional start_inclusive / end_inclusive boundaries to slice text and stop iteration.
            Computes progress percentages using character counts:
                - story_percent: progress through the entire story
                - chapter_percent: progress through the current chapter
            Populates self.chunks so they can be streamed as requested by interface
        """
        book_chunks = []
        chapter_counter = 0
        start_found = not self.start_inclusive  # True if no start boundary specified
        end_reached = False  # Flag to stop iteration after end_inclusive

        for div in self.root.findall(".//tei:div", self.xml_namespace):
            chapter_counter += 1
            div_type = div.get("type", "unknown")
            head = div.find("tei:head", self.xml_namespace)
            chapter_name = (head.text or div_type).strip() if head is not None else div_type

            # Skip divs not in allowed_chapters
            if self.allowed_chapters and chapter_name not in self.allowed_chapters:
                continue

            # Gather paragraphs
            paragraphs = div.findall("tei:p", self.xml_namespace)
            total_paragraphs = len(paragraphs)

            chapter_chunks = []

            for idx, p in enumerate(paragraphs):
                paragraph_text = "".join(p.itertext()).strip()
                if not paragraph_text:
                    continue

                # --- Apply start boundary ---
                if not start_found:
                    start_pos = paragraph_text.find(self.start_inclusive)
                    if start_pos >= 0:
                        # Include the start boundary itself
                        paragraph_text = paragraph_text[start_pos:].strip()
                        start_found = True
                    else:
                        continue  # Skip paragraph until start_inclusive is found

                # --- Apply end boundary ---
                if self.end_inclusive:
                    end_pos = paragraph_text.find(self.end_inclusive)
                    if end_pos >= 0:
                        # Include the end boundary itself
                        paragraph_text = paragraph_text[: end_pos + len(self.end_inclusive)].strip()
                        end_reached = True

                # Line numbers from TEI file
                line_start = p.sourceline or 1  # etree gives first line of element
                paragraph_line_count = paragraph_text.count("\n") + 1
                line_end = line_start + paragraph_line_count - 1

                # Collapse line breaks within paragraphs
                paragraph_text = re.sub(r"\s*\n\s*", " ", paragraph_text)

                c = Chunk(
                    text=paragraph_text,
                    book_id=self.book_id,
                    chapter_number=chapter_counter,
                    line_start=line_start,
                    line_end=line_end,
                    story_id=self.story_id,
                    story_percent=-1,  # Manually computed later
                    chapter_percent=-1,  # ^
                    max_chunk_length=-1,  # No limit in MVP
                )
                chapter_chunks.append(c)

                # Stop iteration if end boundary reached
                if end_reached:
                    break

            # TMP: Fix percentages
            # foreach chapter in book:
            total_chapter_chars = sum(chunk.char_count() for chunk in chapter_chunks)
            cumulative_chars = 0
            for chunk in chapter_chunks:
                chunk.chapter_percent = 100.0 * cumulative_chars / max(total_chapter_chars, 1)
                cumulative_chars += chunk.char_count()
            # merge lists
            book_chunks += chapter_chunks

            if end_reached:
                break

        # for single book:
        total_book_chars = sum(chunk.char_count() for chunk in book_chunks)
        cumulative_chars = 0
        for chunk in book_chunks:
            chunk.story_percent = 100.0 * cumulative_chars / max(total_book_chars, 1)
            cumulative_chars += chunk.char_count()
        return book_chunks


class Book:
    def __init__(
        self,
        title_key: str = "Title:",
        author_key: str = "Author:",
        language_key: str = "Language:",
        date_key: str = "Release date:",
    ):
        self.title_key = title_key
        self.author_key = author_key
        self.language_key = language_key
        self.date_key = date_key
        self.title: str = None
        self.author: str = None
        self.language: str = None
        self.release_date: datetime.date = None


class BookStream(StoryStreamAdapter):
    def __init__(self, book: Book):
        self.book = book

    def stream_segments(self) -> Chunk:
        for text, metadata in book.stream_chapters():
            yield (text, metadata)


class BookFactory(ABC):
    @abstractmethod
    def create_book(self) -> Book:
        pass


class EPUBToTEI:
    """Converts EPUB files to XML format (TEI specification).
    @details  Takes an EPUB book file and converts it to TEI in order to represent its chapter hierarchy.
    """

    xml_namespace = {"tei": "http://www.tei-c.org/ns/1.0"}
    encoding = "utf-8"

    def __init__(self, epub_path, save_pandoc=False, save_tei=True):
        """Initialize the converter.
        @param epub_path  String containing the relative path to an EPUB file.
        @param save_pandoc  Flag to save the intermediate Pandoc output to .tei.xml
        @param save_tei  Flag to save the final TEI file as .tei"""
        self.epub_path = epub_path
        self.save_pandoc = save_pandoc
        self.pandoc_xml_path = epub_path.replace(".epub", "_pandoc.tei.xml")
        self.save_tei = save_tei
        self.tei_path = epub_path.replace(".epub", ".tei")
        self.raw_tei_content = None
        self.clean_tei_content = None

    def convert_to_tei(self):
        """Uses Pandoc to draft a TEI string from EPUB."""
        if self.save_pandoc:
            pypandoc.convert_file(self.epub_path, "tei", outputfile=self.pandoc_xml_path)
            with open(self.pandoc_xml_path, encoding=self.encoding) as f:
                self.raw_tei_content = f.read()
        else:
            self.raw_tei_content = pypandoc.convert_file(self.epub_path, "tei")

    def clean_tei(self):
        """Wrap root if missing, sanitize ids, and save cleaned TEI."""
        content = self.raw_tei_content or open(self.pandoc_xml_path, encoding=self.encoding).read()

        # Ensure root <TEI>
        if not content.lstrip().startswith("<TEI"):
            content = f"<TEI xmlns='{self.xml_namespace['tei']}'>\n{content}\n</TEI>"

        content = self._sanitize_ids(content)
        content = self._prune_bad_tags(content)
        self.clean_tei_content = content

        if self.save_tei:
            root = etree.fromstring(content.encode(self.encoding))
            etree.ElementTree(root).write(self.tei_path, encoding=self.encoding, xml_declaration=True)

    def _sanitize_ids(self, content: str) -> str:
        """Sanitize XML IDs in the TEI content to ensure they are valid and consistent.
        @details
            Pandoc sometimes generates invalid or non-unique `xml:id` attributes
            (e.g., containing spaces, punctuation, or mixed casing). Since we rely
            on these IDs as dictionary keys / anchors, we sanitize them using a
            regex to enforce alphanumeric/underscore/dash format.
        @param content  The raw TEI XML string possibly containing invalid xml:id attributes.
        @return  A TEI XML string with valid NCNames, prefixed with 'id_'."""

        def repl(m):
            val = re.sub(r"[^a-zA-Z0-9_-]", "_", m.group(1))
            return f'xml:id="id_{val}"'

        return re.sub(r'xml:id="([^"]+)"', repl, content)

    def _prune_bad_tags(self, content: str) -> str:
        """Replace all `lb` tags with newline characters in TEI."""
        return re.sub(r"<lb\s*/?>", " ", content)

    # Paragraph handling has been moved to ParagraphStreamTEI.
    # def _normalize_paragraphs(self, content: str) -> str:
    #   """Remove raw newlines inside <p> tags, collapsing them to spaces."""
    #   if self.save_tei:
    #       root = etree.parse(self.tei_path).getroot()
    #   else:
    #       root = etree.fromstring(self.clean_tei_content.encode(self.encoding))

    #   for p in root.findall(".//tei:p", namespaces=self.xml_namespace):
    #       # join all text pieces inside <p>, replacing newlines with spaces
    #       text = "".join(p.itertext())
    #       text = re.sub(r"\s*\n\s*", " ", text)  # collapse line breaks

    #   return etree.tostring(root, encoding=self.encoding).decode(self.encoding)

    # def print_chapters(self, limit: int = 100):
    #     """Old debug method: print chapter names with snippet."""
    #     if self.save_tei:
    #         root = etree.parse(self.tei_path).getroot()
    #     else:
    #         root = etree.fromstring(self.clean_tei_content.encode(self.encoding))

    #     print(f"{'-'*60}")
    #     for div in root.findall(".//tei:div"):
    #         head = div.find("tei:head", self.xml_namespace)
    #         chapter_name = (head.text or "Untitled").strip()
    #         paragraphs = [
    #             p.text.strip()
    #             for p in div.findall("tei:p", self.xml_namespace)
    #             if p.text
    #         ]
    #         chapter_text = " ".join(paragraphs)
    #         snippet = (
    #             (chapter_text[:limit] + "...")
    #             if len(chapter_text) > limit
    #             else chapter_text
    #         )
    #         print(f"Chapter: {chapter_name}\nText: {snippet}\n{'-'*60}")


### Outdated code ###


# Manual data annotation:
# Edit chapter start / end lines via CSV
# Move *** START key down to just before the first chapter
# class Book1:
#     def __init__(
#         self,
#         filename: str,
#         encoding: str = "utf-8",
#         chapter_delimiter_count: int = 4,
#         chapter_heading_end_count: int = 2,
#         section_delimiter_count: int = 4,
#         book_start_line: int = -1,
#         book_end_line: int = -1,
#         max_chunk_length: int = 500,
#         title_key: str = "Title:",
#         author_key: str = "Author:",
#         language_key: str = "Language:",
#         date_key: str = "Release date:",
#     ):
#         self.filename = filename
#         self.encoding = encoding
#         self.chapter_delimiter_count: str = chapter_delimiter_count
#         self.chapter_heading_end_count: str = chapter_heading_end_count
#         self.section_delimiter_count: str = section_delimiter_count
#         self.book_start_line: int = book_start_line
#         self.book_end_line: int = book_end_line
#         self.max_chunk_length: int = max_chunk_length

#         self.title_key = title_key
#         self.author_key = author_key
#         self.language_key = language_key
#         self.date_key = date_key
#         self.title: str = None
#         self.author: str = None
#         self.language: str = None
#         self.release_date: datetime.date = None
#         self.total_chars: int = 0

#         self.chapter_info_df = pd.DataFrame(
#             columns=["chapter_name", "num_chars", "line_start", "line_end"]
#         )
#         base_filename = os.path.splitext(self.filename)[0]
#         chapter_csv_filename = f"{base_filename}_chapters.csv"
#         found_chapter_file = os.path.exists(chapter_csv_filename)
#         if found_chapter_file:
#             print("Chapter info CSV found. Loading data.")
#             self.chapter_info_df = pd.read_csv(chapter_csv_filename)
#             self.pre_scan(
#                 extract_chapters=False
#             )  # Run pre_scan without extracting chapters
#         else:
#             print("Chapter info CSV not found. Performing full pre-scan.")
#             self.pre_scan(extract_chapters=True)

#     def pre_scan(self, extract_chapters: bool = True):
#         """
#         Performs a single, unified pass to capture all metadata and chapter info.
#         """
#         in_book_content = False
#         self.total_chars = 0
#         line_number = 0

#         chapter_data = []
#         current_chapter_name = "NULL"
#         current_chapter_line_start = -1
#         chapter_chars_count = 0
#         blank_lines_count = 0

#         with open(self.filename, "r", encoding=self.encoding) as f:
#             for line in f:

#                 # State control logic
#                 line_number += 1
#                 if line_number == self.book_start_line and not in_book_content:
#                     in_book_content = True
#                 if line_number == self.book_end_line and in_book_content:
#                     in_book_content = False
#                     break

#                 # Metadata extraction
#                 if not in_book_content:  # header
#                     if self.title_key in line:
#                         self.title = line.split(self.title_key, 1)[1].strip()
#                     elif self.author_key in line:
#                         self.author = line.split(self.author_key, 1)[1].strip()
#                     elif self.language_key in line:
#                         self.language = line.split(self.language_key, 1)[1].strip()
#                     elif self.date_key in line:
#                         self.release_date = line.split(self.date_key, 1)[1].strip()
#                         self.release_date = re.sub(
#                             r"\[.*?\]", "", self.release_date
#                         ).strip()
#                         try:
#                             parsed_date = datetime.strptime(
#                                 self.release_date, "%B %d, %Y"
#                             ).date()
#                             self.release_date = parsed_date
#                         except ValueError:
#                             self.release_date = None
#                             Log.warn_parse(
#                                 "Book.release_date", self.release_date, "Date"
#                             )

#                 # Chapter extraction
#                 if in_book_content:
#                     self.total_chars += len(line.rstrip("\n"))

#                     if extract_chapters:
#                         # Accumulate blank lines
#                         if not line.strip():
#                             blank_lines_count += 1
#                         else:
#                             # Reached a non-blank line
#                             if blank_lines_count >= self.chapter_delimiter_count:
#                                 if chapter_chars_count > 0:
#                                     max_name_length = 60
#                                     if len(current_chapter_name) > max_name_length:
#                                         current_chapter_name = (
#                                             current_chapter_name[:max_name_length]
#                                             + "..."
#                                         )
#                                     chapter_data.append(
#                                         {
#                                             "chapter_name": current_chapter_name,
#                                             "num_chars": chapter_chars_count,
#                                             "line_start": current_chapter_line_start,
#                                             "line_end": line_number
#                                             - blank_lines_count
#                                             - 1,
#                                         }
#                                     )

#                                 chapter_chars_count = 0
#                                 current_chapter_line_start = line_number
#                                 if "Chapter" in line or line.strip().isupper():
#                                     section_num = 1
#                                     current_chapter_name = (
#                                         line.strip() + f"_{section_num}"
#                                     )
#                                 else:
#                                     base_chapter_name = chapter_data[-1][
#                                         "chapter_name"
#                                     ].split("_")[0]
#                                     section_num += 1
#                                     current_chapter_name = (
#                                         base_chapter_name + f"_{section_num}"
#                                     )

#                             chapter_chars_count += len(line)
#                             blank_lines_count = 0

#         # Save the DataFrame if extraction was requested and data was found
#         if extract_chapters and chapter_data:
#             self.chapter_info_df = pd.DataFrame(chapter_data)
#             base_filename = os.path.splitext(self.filename)[0]
#             chapter_csv_filename = f"{base_filename}_chapters.csv"
#             self.chapter_info_df.to_csv(chapter_csv_filename, index=False)

#     def debug_pre_scan(self):
#         """
#         Prints the metadata and chapter information to help debug the pre-scan process.
#         """
#         print("--- Starting Pre-scan Debugging ---")
#         print(f"Book Filename: {self.filename}")
#         print("\n### Metadata ###")
#         print(f"Title: {self.title}")
#         print(f"Author: {self.author}")
#         print(f"Language: {self.language}")
#         print(f"Release Date: {self.release_date}")
#         print(f"Book Start Line: {self.book_start_line}")
#         print(f"Book End Line: {self.book_end_line}")
#         print(f"Total Characters in Book Content: {self.total_chars}")

#         print("\n### Chapter Information ###")
#         if not self.chapter_info_df.empty:
#             print(self.chapter_info_df)
#         else:
#             print("No chapter information found.")

#         print("\n--- Pre-scan Debugging Complete ---")

#     def read_chunks(self) -> Iterator[Chunk]:
#         """
#         A simplified generator that yields memory-efficient Chunks from the book file.
#         It processes the book line by line, maintaining state for chunking.
#         """
#         line_num = 0
#         cumulative_chars = 0
#         current_chapter_index = 0
#         current_chapter_chars = 0
#         section = 0
#         in_book_content = False

#         section_name, total_chapter_chars = self.chapter_info_df[current_chapter_index]

#         paragraph_buffer = []
#         current_paragraph_start_line = 0
#         blank_count = 0

#         with open(self.filename, "r", encoding=self.encoding) as f:
#             for line in f:
#                 line_num += 1

#                 # Skip to the start of the book content
#                 if not in_book_content:
#                     if self.book_start_key in line:
#                         in_book_content = True
#                     continue

#                 # Stop at the end of the book content
#                 if self.book_end_key in line:
#                     break

#                 line_stripped = line.strip()

#                 if not line_stripped:
#                     blank_count += 1
#                 else:
#                     # Process the previous paragraph if there were blank lines
#                     if blank_count > 0 and paragraph_buffer:
#                         # Form the paragraph text
#                         paragraph_text = " ".join(paragraph_buffer).strip()

#                         if len(paragraph_text) > 0:
#                             # Check for chapter break (4+ blank lines)
#                             if blank_count >= 3:
#                                 # Yield the last chunk of the previous chapter
#                                 if paragraph_text:
#                                     yield from self._process_and_yield_chunks(
#                                         paragraph_text,
#                                         current_paragraph_start_line,
#                                         line_num - blank_count,
#                                         cumulative_chars - len(line),
#                                         section_name,
#                                         current_chapter_chars - len(line),
#                                         total_chapter_chars,
#                                         section,
#                                     )

#                                 # Update to the next chapter's metadata
#                                 current_chapter_index += 1
#                                 if current_chapter_index < len(self.chapter_info_df):
#                                     section_name, total_chapter_chars = (
#                                         self.chapter_info_df[current_chapter_index]
#                                     )
#                                     current_chapter_chars = 0

#                                 section = 0

#                             # Check for section break (2 blank lines, typically)
#                             elif blank_count == 2:
#                                 # Yield the current chunk and increment section index
#                                 if paragraph_text:
#                                     yield from self._process_and_yield_chunks(
#                                         paragraph_text,
#                                         current_paragraph_start_line,
#                                         line_num - blank_count,
#                                         cumulative_chars - len(line),
#                                         section_name,
#                                         current_chapter_chars - len(line),
#                                         total_chapter_chars,
#                                         section,
#                                     )
#                                 section += 1

#                             else:  # Regular paragraph break (1 blank line)
#                                 yield from self._process_and_yield_chunks(
#                                     paragraph_text,
#                                     current_paragraph_start_line,
#                                     line_num - blank_count,
#                                     cumulative_chars - len(line),
#                                     section_name,
#                                     current_chapter_chars - len(line),
#                                     total_chapter_chars,
#                                     section,
#                                 )

#                     # Reset buffer and start a new paragraph
#                     paragraph_buffer = [line_stripped]
#                     current_paragraph_start_line = line_num
#                     blank_count = 0

#                 cumulative_chars += len(line)
#                 current_chapter_chars += len(line)

#             # Yield any remaining content after the loop ends
#             if paragraph_buffer:
#                 paragraph_text = " ".join(paragraph_buffer).strip()
#                 if len(paragraph_text) > 0:
#                     yield from self._process_and_yield_chunks(
#                         paragraph_text,
#                         current_paragraph_start_line,
#                         line_num,
#                         cumulative_chars,
#                         section_name,
#                         current_chapter_chars,
#                         total_chapter_chars,
#                         section,
#                     )

#     def _process_and_yield_chunks(
#         self,
#         text: str,
#         line_start: int,
#         line_end: int,
#         current_book_chars: int,
#         chapter_name: str,
#         current_chapter_chars: int,
#         total_chapter_chars: int,
#         section: int,
#     ) -> Iterator[Chunk]:
#         """Helper method to handle the logic of chunking and yielding."""
#         # Split a large paragraph into multiple chunks if needed
#         if len(text) > self.max_chunk_length:
#             sentences = re.split(r"(?<=[.!?])\s+", text)
#             current_chunk_text = ""
#             for sentence in sentences:
#                 if len(current_chunk_text) + len(sentence) + 1 > self.max_chunk_length:
#                     if current_chunk_text:
#                         yield self._create_chunk(
#                             current_chunk_text.strip(),
#                             line_start,
#                             line_end,
#                             current_book_chars,
#                             current_chapter_chars,
#                             chapter_name,
#                             total_chapter_chars,
#                             section,
#                         )
#                         section += 1
#                     current_chunk_text = sentence.strip() + " "
#                 else:
#                     current_chunk_text += sentence.strip() + " "

#             if current_chunk_text:
#                 yield self._create_chunk(
#                     current_chunk_text.strip(),
#                     line_start,
#                     line_end,
#                     current_book_chars,
#                     current_chapter_chars,
#                     chapter_name,
#                     total_chapter_chars,
#                     section,
#                 )
#         else:
#             # If it fits, merge with the next one. Otherwise, yield a single chunk.
#             yield self._create_chunk(
#                 text,
#                 line_start,
#                 line_end,
#                 current_book_chars,
#                 current_chapter_chars,
#                 chapter_name,
#                 total_chapter_chars,
#                 section,
#             )

#     def _create_chunk(
#         self,
#         text: str,
#         line_start: int,
#         line_end: int,
#         total_book_chars: int,
#         chapter_chars: int,
#         chapter_name: str,
#         total_chapter_chars: int,
#         section: int,
#     ) -> Chunk:
#         """Helper to create a single Chunk object with calculated percentages."""
#         return Chunk(
#             text=text,
#             chapter=chapter_name,
#             section=section,
#             line_start=line_start,
#             line_end=line_end,
#             story_percent=(
#                 (total_book_chars / self.total_book_chars) * 100
#                 if self.total_book_chars > 0
#                 else 0
#             ),
#             segment_percent=(
#                 (chapter_chars / total_chapter_chars) * 100
#                 if total_chapter_chars > 0
#                 else 0
#             ),
#         )
