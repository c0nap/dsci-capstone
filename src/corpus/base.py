from abc import ABC, abstractmethod
from pandas import DataFrame, read_csv, concat
from typing import Optional, Iterator
import os
import shutil
import threading
from src.corpus import align


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

    @staticmethod
    def _make_index_row(**kwargs) -> dict:
        """Create standardized index row with core fields + any extras.
        @param kwargs  Any dataset-specific fields (e.g., booksum_id="123").
        @return  Dict with core fields initialized + kwargs merged in.
        """
        row = {
            "book_id": None,
            "title": None,
            "text_path": None,
            "gutenberg_id": None,
        }
        row.update(kwargs)
        return row

    def append_to_index(self, book_id: int, title: str, text_path: str, gutenberg_id: int | None, **kwargs) -> None:
        """Append a single book entry to the global index.
        @param book_id  Unique internal ID for the book.
        @param title  Normalized title of the book.
        @param text_path  Path to the local text file.
        @param gutenberg_id  Gutenberg ID if available (or None).
        @param kwargs  Additional dataset-specific columns (e.g., nqa_id).
        @details  If new keys are provided, pandas adds them and fills existing rows with N/A.
        This makes sense in ABC because only derived loaders should append to the index.
        """
        row = self._make_index_row(
            book_id=book_id, 
            title=title, 
            text_path=text_path, 
            gutenberg_id=gutenberg_id, 
            **kwargs
        )
        new_row = DataFrame([row])

        if not os.path.exists(self.INDEX_FILE):
            df = new_row  # Create new index file
        else:  # If exists, load and merge
            df = read_csv(self.INDEX_FILE)
            df = concat([df, new_row], ignore_index=True)
        save_as_index(df)
    
    def save_text(self, book_id: int, title: str, text: str) -> str:
        """Save text to global texts directory.
        @param book_id  Unique internal ID for the book.
        @param title  Normalized title of the book.
        @param text_path  Path to the local text file.
        @return  Path to saved text file.
        """
        os.makedirs(self.TEXTS_DIR, exist_ok=True)
        
        # Clean file name using convention.
        clean_title = title.replace(' ', '_')
        filename = get_text_name(book_id, clean_title, ".txt")
        filepath = os.path.join(self.TEXTS_DIR, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(text)
        
        return filepath

    # --------------------------------------------------
    # Global Index Management (Thread-Safe)
    # --------------------------------------------------
    _id_lock = threading.Lock()
    _current_global_id = None
    
    def _get_next_id(self) -> int:
        """Get next available book ID safely across multiple threads.
        @return  Next unique integer ID.
        @details
        CRITICAL: ATOMICITY REQUIRED FOR MULTI-THREADING.
        When running parallel downloads (e.g., ThreadPoolExecutor), multiple 
        threads request IDs simultaneously. Without a Mutex (Lock), 
        threads could read the same ID before incrementing, causing collisions.
        """
        with self._id_lock:
            # Lazy Initialization: Hydrate state on first access
            if self._current_global_id is None:
                self._current_global_id = self._initialize_id_counter()
            
            # Increment and return
            self._current_global_id += 1
            return self._current_global_id

    @classmethod
    def _set_next_id(cls, next_id: int) -> None:
        """Manually update the global counter.
        @param next_id  The integer value to set the counter to.
        @details
        Useful for maintenance tasks like 'reindex_rows' which compress the 
        ID sequence. Using this helper ensures the thread lock is respected 
        during manual updates.
        """
        with cls._id_lock:
            cls._current_global_id = next_id

    def _initialize_id_counter(self) -> int:
        """Hydrate in-memory counter from the filesystem.
        @return  The highest book ID currently in index.csv (or 0).
        @details
        JUSTIFICATION:
        Since we are using an in-memory counter (for speed/thread-safety), 
        we lose state when the program stops. This function scans the 
        single source of truth (index.csv) at startup to determine the 
        'High Water Mark', ensuring we continue numbering where we left off 
        rather than overwriting existing IDs starting at 1.
        """
        if not os.path.exists(self.INDEX_FILE):
            return 0
        
        try:
            # We only need the book_id column, which is fast to load
            df = read_csv(self.INDEX_FILE, usecols=['book_id'])
            if df.empty:
                return 0
            return int(df['book_id'].max())
        except (ValueError, KeyError):
            return 0
    
    

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
    @details  Can retrieve full text as needed without keeping it in DataFrame memory.
    """
    with open(text_path, "r", encoding="utf-8") as f:
        return f.read()


def get_index() -> DataFrame:
    """Read the index file into memory.
    @return  DataFrame containing book IDs and file paths to various datasets.
    """
    df = read_csv(DatasetLoader.INDEX_FILE)
    return df


def save_as_index(df: DataFrame) -> None:
    """Write the DataFrame to the global index file.
    @param  DataFrame containing book IDs and file paths to various datasets.
    """
    df.to_csv(DatasetLoader.INDEX_FILE, index=False)


def prune_bad_refs(df: DataFrame) -> DataFrame:
    """Remove index entries where text files no longer exist.
    @details
    - File-system integrity check: removes ghost entries from previous runs.
    - Does not handle deduplication or alignment logic.
    """
    df = df[df['text_path'].apply(os.path.exists)]
    return df


def reindex_rows() -> None:
    """Renumber books sequentially starting from ID 1.
    @note  Not a pure function because file names are altered.
    @details
    - Renames 'text_path' column only: datasets/texts/00001_title.txt
    - Each dataset maintains its own metadata.csv with internal IDs and file paths.
    - Use the dataset-specific ID returned by @ref src.corpus.base.DatasetLoader.get_schema to lookup specific books.
    """
    df = get_index()
    if df.empty:
        return

    for new_id, (idx, row) in enumerate(df.iterrows(), start=1):
        # Fix the ID prefix of full-text files: datasets/texts/00001_title.txt
        title = str(row['title']).replace(' ', '_')
        old_path = str(row['text_path'])
        _, ext = os.path.splitext(old_path)

        new_filename = align.get_text_name(new_id, title, ext)
        new_path = os.path.join(DatasetLoader.TEXTS_DIR, new_filename)
        
        # Only move if the path has actually changed and source exists
        if os.path.exists(old_path) and old_path != new_path:
            shutil.move(old_path, new_path)
        
        # Update index and save to file
        df.at[idx, 'book_id'] = new_id
        df.at[idx, 'text_path'] = new_path
    df = df.sort_values('book_id').reset_index(drop=True)
    save_as_index(df)
    DatasetLoader._increment_id(len(df))
