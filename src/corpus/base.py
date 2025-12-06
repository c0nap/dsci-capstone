from abc import ABC, abstractmethod
from pandas import DataFrame, read_csv
from typing import Optional, Iterator
import requests
import time
import os
import shutil


class DatasetLoader(ABC):
    """Base loader for book datasets from various sources.
    @details
    Handles streaming for large datasets, normalizes output to a 
    common schema, and provides shared utilities for Gutenberg lookups.
    Manages a global index that tracks books across all datasets.
    """
    
    TEXTS_DIR = "./datasets/texts"
    INDEX_FILE = "./datasets/index.csv"
    NEXT_ID_FILE = "./datasets/next_id.txt"
    GUTENDEX_API = "https://gutendex.com/books"
    
    @abstractmethod
    def load(self, streaming: bool = False) -> DataFrame | Iterator[dict]:
        """Load dataset.
        @param streaming  If True, return an iterator; if False, return a DataFrame.
        @return  Either a pandas DataFrame or an iterator of dict records.
        """
        pass
    
    @abstractmethod
    def get_schema(self) -> list[str]:
        """Return list of available fields in normalized output.
        @return  List of column names that will be present in the output DataFrame.
        """
        pass

    
    def fetch_gutenberg_metadata(self, query: str = None, gutenberg_id: str | int = None) -> Optional[dict]:
        """Search or lookup metadata from Gutendex.
        @param query  General search string (title/author). Used if gutenberg_id is None.
        @param gutenberg_id  Specific Gutenberg ID. If provided, performs direct lookup.
        @return  Normalized dict with keys: title, author, language, gutenberg_id, text_url.
                 Returns None if not found or on error.
        """
        if not query and not gutenberg_id:
            return None

        try:
            # 1. Direct ID Lookup
            if gutenberg_id:
                url = f"{self.GUTENDEX_API}/{gutenberg_id}"
                params = {}
            # 2. Search Query
            else:
                url = self.GUTENDEX_API
                params = {"search": query}

            response = requests.get(url, params=params, timeout=10)
            
            # Rate limit politeness
            if response.status_code == 429:
                time.sleep(2)
                response = requests.get(url, params=params, timeout=10)

            response.raise_for_status()
            data = response.json()

            # Handle search results vs direct object return
            result = None
            if gutenberg_id:
                result = data  # Direct ID returns the object
            elif data.get("count", 0) > 0:
                result = data["results"][0]  # Search returns list, take top match

            if not result:
                return None

            # Normalize output
            authors = [a.get("name", "") for a in result.get("authors", [])]
            
            # Find text URL (prefer plain text)
            formats = result.get("formats", {})
            text_url = formats.get("text/plain; charset=utf-8") or formats.get("text/plain")

            return {
                "gutenberg_id": result.get("id"),
                "title": result.get("title", ""),
                "author": ", ".join(authors) if authors else "",
                "language": result.get("languages", [""])[0],
                "text_url": text_url
            }

        except Exception as e:
            print(f"Warning: Gutendex lookup failed for {gutenberg_id or query}: {e}")
            return None
    
    # --------------------------------------------------
    # Download Helpers
    # --------------------------------------------------

    def _calculate_subset_size(self, total: int, n: int = None, fraction: float = None) -> int:
        """Calculate number of items to download.
        @param total  Total number of items available.
        @param n  Number of items to download. If None, downloads all.
        @param fraction  Fraction of dataset to download (0.0-1.0). Overrides n if set.
        @return  Number of items to download.
        """
        if fraction is not None:
            return int(total * fraction)
        elif n is None:
            return total
        return min(n, total)
    
    # --------------------------------------------------
    # Global Index Management
    # --------------------------------------------------
    
    def _get_next_id(self) -> int:
        """Get next available book ID and increment counter.
        @return  Next book ID as integer.
        @details
        Atomically reads and increments the global ID counter in next_id.txt.
        Creates the file with ID 1 if it doesn't exist.
        """
        os.makedirs(os.path.dirname(self.NEXT_ID_FILE), exist_ok=True)
        
        if not os.path.exists(self.NEXT_ID_FILE):
            with open(self.NEXT_ID_FILE, "w") as f:
                f.write("1")
            return 1
        
        with open(self.NEXT_ID_FILE, "r") as f:
            current_id = int(f.read().strip())
        
        with open(self.NEXT_ID_FILE, "w") as f:
            f.write(str(current_id + 1))
        
        return current_id
    
    def _save_text(self, book_id: int, title: str, text: str) -> str:
        """Save text to global texts directory.
        @param book_id  Global book ID.
        @param title  Normalized book title.
        @param text  Full text content.
        @return  Path to saved text file.
        """
        os.makedirs(self.TEXTS_DIR, exist_ok=True)
        
        # Normalize title for filename
        filename = f"{book_id:05d}_{title.replace(' ', '_')}.txt"
        filepath = os.path.join(self.TEXTS_DIR, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(text)
        
        return filepath
    
    def _append_to_index(self, row: dict) -> None:
        """Append book entry to global index.
        @param row  Dict with keys: book_id, title, gutenberg_id, text_path, and dataset-specific fields.
        @details
        Creates index.csv if it doesn't exist. Appends row to existing index.
        """
        # Create index with headers if doesn't exist
        if not os.path.exists(self.INDEX_FILE):
            headers = ["book_id", "title", "gutenberg_id", "text_path", 
                       "booksum_id", "booksum_path", 
                       "nqa_id", "nqa_path", 
                       "litbank_id", "litbank_path"]
            df = DataFrame(columns=headers)
            df.to_csv(self.INDEX_FILE, index=False)
        
        # Read existing index
        index_df = read_csv(self.INDEX_FILE)
        
        # Append new row
        new_row = DataFrame([row])
        index_df = DataFrame(list(index_df.to_dict('records')) + list(new_row.to_dict('records')))
        
        # Save updated index
        index_df.to_csv(self.INDEX_FILE, index=False)

    @staticmethod
    def make_index_row(**kwargs) -> dict:
        """Create standardized index row with all fields.
        @param kwargs  Fields to populate (book_id, title, gutenberg_id, etc.)
        @return  Dict with all index fields, unpopulated fields set to None.
        @details
        Use this factory to ensure consistent schema across datasets.
        """
        row = {
            "book_id": None,
            "title": None,
            "text_path": None,
            "gutenberg_id": None,
            "booksum_id": None,
            "booksum_path": None,
            "nqa_id": None,
            "nqa_path": None,
            "litbank_id": None,
            "litbank_path": None,
        }
        row.update(kwargs)
        return row


# --------------------------------------------------
# Helper Functions - Index Management
# --------------------------------------------------

def get_book_count() -> int:
    """Get total number of books in index.
    @return  Number of books, or 0 if index doesn't exist.
    """
    if not os.path.exists(DatasetLoader.INDEX_FILE):
        return 0
    df = read_csv(DatasetLoader.INDEX_FILE)
    return len(df)


def load_text_from_path(text_path: str) -> str:
    """Load full text from saved file path.
    @param text_path  Path to text file.
    @return  Full text content as string.
    @details
    Use this helper to retrieve full text when needed for processing,
    without keeping it in memory in the DataFrame.
    """
    with open(text_path, "r", encoding="utf-8") as f:
        return f.read()


def prune_references() -> int:
    """Remove index entries where text files no longer exist.
    @return  Number of entries removed.
    @details
    File-system integrity check: removes ghost entries from previous runs.
    Does not handle deduplication or alignment logic.
    """
    index_file = DatasetLoader.INDEX_FILE
    if not os.path.exists(index_file):
        return 0

    df = read_csv(index_file)
    initial_count = len(df)

    def file_exists(path):
        return isinstance(path, str) and os.path.exists(path)

    df = df[df['text_path'].apply(file_exists)]
    
    removed = initial_count - len(df)
    if removed > 0:
        df.to_csv(index_file, index=False)
    
    return removed


def prune_keys() -> int:
    """Remove entries with no usable identifier for alignment.
    @return  Number of entries removed.
    @details
    An entry needs at least one identifier (gutenberg_id OR title) to be
    aligned across datasets. This removes rows that have neither.
    """
    index_file = DatasetLoader.INDEX_FILE
    if not os.path.exists(index_file):
        return 0
    
    df = read_csv(index_file)
    initial_count = len(df)
    
    def has_identifier(row):
        has_gid = isinstance(row.get('gutenberg_id'), (int, float, str)) and \
                  str(row['gutenberg_id']).strip() not in ['', 'nan', 'None']
        has_title = isinstance(row.get('title'), str) and row['title'].strip() != ''
        return has_gid or has_title
    
    df = df[df.apply(has_identifier, axis=1)]
    
    removed = initial_count - len(df)
    if removed > 0:
        df.to_csv(index_file, index=False)
    
    return removed


def hard_reset() -> int:
    """Renumber all existing datasets starting from ID 00001.
    @details
    1. Scans valid text files.
    2. Renames files sequentially (00001, 00002...).
    3. Updates global index and next_id counter.
    @return  The number of books in the index, now equivalent to the maximum book ID.
    """
    if not os.path.exists(DatasetLoader.INDEX_FILE):
        return 0
    df = read_csv(DatasetLoader.INDEX_FILE)
    if df.empty:
        return 0

    # Sort by current book_id to maintain relative order
    df = df.sort_values('book_id')
    
    id_map = {}  # Track mapping from old_id -> new_id for logging and debugging
    for new_id_int, (idx, row) in enumerate(df.iterrows(), start=1):
        old_path = str(row['text_path'])
        old_id = int(row['book_id'])
        title = str(row['title'])
        
        # Generate new path
        # Format: datasets/texts/00001_title.txt
        new_filename = f"{new_id_int:05d}_{title.replace(' ', '_')}.txt"
        new_path = os.path.join(DatasetLoader.TEXTS_DIR, new_filename)
        
        # Rename file on disk
        if old_path != new_path:
            shutil.move(old_path, new_path)
            
        # Update DataFrame
        df.at[idx, 'book_id'] = new_id_int
        df.at[idx, 'text_path'] = new_path
        id_map[old_id] = new_id_int

    # Save updated index
    df.to_csv(DatasetLoader.INDEX_FILE, index=False)
    
    # Update next_id file
    with open(DatasetLoader.NEXT_ID_FILE, "w") as f:
        f.write(str(len(df) + 1))
    return len(df)
