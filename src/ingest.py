"""Dataset loading and alignment for book corpora.

Loaders handle fetching and normalizing datasets from various sources.
Alignment functions handle fuzzy matching across datasets.
"""

from abc import ABC, abstractmethod
from typing import Iterator, Optional
from pandas import DataFrame, read_csv
from datasets import load_dataset, DatasetDict  # type: ignore
from rapidfuzz import fuzz, process
import shutil
import re
import os
import subprocess
import requests
import time
from urllib.parse import unquote


# --------------------------------------
# Loaders
# --------------------------------------

class DatasetLoader(ABC):
    """Base loader for book datasets from various sources.
    @details
    Handles streaming for large datasets, normalizes output to a 
    common schema, and provides shared utilities for Gutenberg lookups.
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


class BookSumLoader(DatasetLoader):
    """Loads BookSum dataset from HuggingFace.
    @details
    Provides book-level summaries with full text. Normalizes titles
    by removing punctuation and lowercasing for alignment.
    """
    
    def __init__(self, cache_dir: str = "./datasets/booksum", split: str = "train"):
        self.dataset_name = "ubaada/booksum-complete-cleaned"
        self.subset = "books"
        self.split = split
        self.cache_dir = cache_dir
        self.metadata_file = f"{cache_dir}/metadata.csv"
    
    def download(self, n: int = None, fraction: float = None) -> None:
        """Download dataset to local cache.
        @param n  Number of books to download. If None, downloads all.
        @param fraction  Fraction of dataset to download (0.0-1.0). Overrides n if set.
        @details
        Downloads HuggingFace dataset in streaming mode for efficiency,
        saves text to global texts dir, saves metadata as CSV, and appends to global index.
        """
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Stream dataset for efficiency
        ds = load_dataset(self.dataset_name, self.subset, split=self.split, streaming=True)
        
        # Debug: print available columns from first item
        first_item = next(iter(ds))
        print(f"BookSum columns: {list(first_item.keys())}")
        
        # Calculate how many to download
        # Note: with streaming, we don't know total size upfront unless we iterate once
        # So we just use n directly or default to all
        num_to_download = n if n is not None else float('inf')
        
        # Process each book
        rows = []
        count = 0
        for raw in ds:
            if count >= num_to_download:
                break
            
            book_id = self._get_next_id()
            
            # Normalize title
            title = raw.get("title", "")
            if isinstance(title, str):
                title = title.lower().strip()
                title = re.sub(r"[\W_]+", " ", title).strip()
            else:
                title = ""
            
            # Gutenberg Lookup (BookSum has no ID, so search by title)
            gutenberg_id = None
            if title:
                meta = self.fetch_gutenberg_metadata(query=title)
                if meta:
                    gutenberg_id = meta.get("gutenberg_id")

            # Save full text
            text = raw.get("text", "")
            text_path = self._save_text(book_id, title, text)
            
            # Extract original BookSum ID (if available, otherwise use count)
            booksum_id = raw.get("bid", count)
            
            # Metadata row
            metadata_row = {
                "book_id": book_id,
                "booksum_id": booksum_id,
                "title": title,
                "summary": raw.get("summary", "")
            }
            rows.append(metadata_row)
            
            # Append to global index
            index_row = {
                "book_id": book_id,
                "title": title,
                "gutenberg_id": gutenberg_id,
                "text_path": text_path,
                "booksum_id": booksum_id,
                "booksum_path": self.metadata_file,
                "nqa_id": None,
                "nqa_path": None,
                "litbank_id": None,
                "litbank_path": None
            }
            self._append_to_index(index_row)
            
            count += 1
            print(f"Downloaded {count}/{num_to_download if num_to_download != float('inf') else '?'} books", end='\r')
        
        print()  # New line after progress
        
        # Save metadata
        df = DataFrame(rows)
        df.to_csv(self.metadata_file, index=False)
    
    def load(self, streaming: bool = False) -> DataFrame:
        """Load dataset from cache.
        @param streaming  Ignored - always loads full DataFrame from cache.
        @return  DataFrame with metadata.
        @details
        Raises FileNotFoundError if cache doesn't exist. No fallback to download.
        """
        if not os.path.exists(self.metadata_file):
            raise FileNotFoundError(
                f"BookSum cache not found at {self.metadata_file}. "
                f"Run download() first."
            )
        
        return read_csv(self.metadata_file)
    
    def get_schema(self) -> list[str]:
        return ["book_id", "booksum_id", "title", "summary"]


class NarrativeQALoader(DatasetLoader):
    """Loads NarrativeQA dataset from HuggingFace.
    @details
    Provides book metadata (title, author, ID) with full text and summary.
    """
    
    def __init__(self, cache_dir: str = "./datasets/narrativeqa", split: str = "train"):
        self.dataset_name = "narrativeqa"
        self.split = split
        self.cache_dir = cache_dir
        self.metadata_file = f"{cache_dir}/metadata.csv"
    
    def download(self, n: int = None, fraction: float = None) -> None:
        """Download dataset to local cache.
        @param n  Number of books to download. If None, downloads all.
        @param fraction  Fraction of dataset to download (0.0-1.0). Overrides n if set.
        @details
        Downloads HuggingFace dataset in streaming mode for efficiency,
        saves text to global texts dir, saves metadata as CSV, and appends to global index.
        """
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Stream dataset for efficiency
        ds = load_dataset(self.dataset_name, split=self.split, streaming=True)
        
        # Debug: print available columns from first item
        first_item = next(iter(ds))
        print(f"NarrativeQA columns: {list(first_item.keys())}")
        if 'document' in first_item:
            print(f"Sample document keys: {list(first_item['document'].keys())}")
        
        # Calculate how many to download
        num_to_download = n if n is not None else float('inf')
        
        # Process each book
        rows = []
        count = 0
        seen_ids = set() # Track processed NQA IDs

        for raw in ds:
            if count >= num_to_download:
                break
            
            doc = raw.get("document", {})
            nqa_id = doc.get("id", "")
            
            # --- Fix: Deduplicate by NQA ID ---
            if not nqa_id or nqa_id in seen_ids:
                continue
            seen_ids.add(nqa_id)
            
            # --- Fix: Resolve Title & ID ---
            title = ""
            url = doc.get("url", "")
            author = doc.get("author", "").strip().lower()
            gutenberg_id = None

            # 1. Try regex from URL
            gid_match = re.search(r'gutenberg\.org/ebooks/(\d+)', url)
            if gid_match:
                gutenberg_id = gid_match.group(1)
            # Slow path: search by title
                meta = self.fetch_gutenberg_metadata(gutenberg_id=gutenberg_id)
                if meta:
                    title = meta.get("title", "").lower()
            
            # 2. Fallback: Extract title from URL slug
            if not title and url:
                slug = url.split('/')[-1].replace('.html', '').replace('.htm', '')
                try:
                    title = unquote(slug).replace('_', ' ').replace('-', ' ').lower()
                except:
                    title = slug.replace('_', ' ').replace('-', ' ').lower()
            
            # Clean title
            title = re.sub(r"[\W_]+", " ", title).strip()

            # Save full text
            book_id = self._get_next_id()
            text = doc.get("text", "")
            summary = doc.get("summary", {}).get("text", "")
            text_path = self._save_text(book_id, title, text)
            
            # Metadata row
            metadata_row = {
                "book_id": book_id,
                "nqa_id": nqa_id,
                "title": title,
                "author": author,
                "summary": summary
            }
            rows.append(metadata_row)
            
            # Append to global index
            index_row = {
                "book_id": book_id,
                "title": title,
                "gutenberg_id": gutenberg_id,
                "text_path": text_path,
                "booksum_id": None,
                "booksum_path": None,
                "nqa_id": nqa_id,
                "nqa_path": self.metadata_file,
                "litbank_id": None,
                "litbank_path": None
            }
            self._append_to_index(index_row)
            
            count += 1
            print(f"Downloaded {count}/{num_to_download if num_to_download != float('inf') else '?'} books", end='\r')
        
        print()  # New line after progress
        
        # Save metadata
        df = DataFrame(rows)
        df.to_csv(self.metadata_file, index=False)
    
    def load(self, streaming: bool = False) -> DataFrame:
        """Load dataset from cache.
        @param streaming  Ignored - always loads full DataFrame from cache.
        @return  DataFrame with metadata.
        @details
        Raises FileNotFoundError if cache doesn't exist. No fallback to download.
        """
        if not os.path.exists(self.metadata_file):
            raise FileNotFoundError(
                f"NarrativeQA cache not found at {self.metadata_file}. "
                f"Run download() first."
            )
        
        return read_csv(self.metadata_file)
    
    def get_schema(self) -> list[str]:
        return ["book_id", "nqa_id", "title", "author", "summary"]


class LitBankLoader(DatasetLoader):
    """Loads LitBank dataset from local GitHub repository.
    @details
    Processes LitBank's ~2,000 word excerpts from 100 works of fiction.
    Copies and renames all annotation files (entities, events, coref, quotations).
    """
    
    def __init__(self, repo_path: str = "./datasets/litbank", cache_dir: str = "./datasets/litbank"):
        """Initialize LitBank loader.
        @param repo_path  Path to cloned litbank repository.
        @param cache_dir  Output directory for renamed files and metadata.
        """
        self.repo_path = repo_path
        self.cache_dir = cache_dir
        self.metadata_file = f"{cache_dir}/metadata.csv"
    
    def download(self, n: int = None, fraction: float = None) -> None:
        """Process LitBank files and copy annotations.
        @param n  Number of books to process. If None, processes all.
        @param fraction  Fraction of books to process (0.0-1.0). Overrides n if set.
        @details
        Reads original LitBank files, fetches Gutenberg metadata,
        copies and renames all annotation files with book IDs,
        saves text to global texts directory, and updates global index.
        """
        # Check original repo exists
        entities_path = os.path.join(self.repo_path, "entities/brat")
        if not os.path.exists(entities_path):
            raise FileNotFoundError(
                f"LitBank repository not found at {self.repo_path}. "
                f"Clone it first: git clone https://github.com/dbamman/litbank {self.repo_path}"
            )
        
        # Get all books from entities/brat (has all books)
        all_files = [f for f in os.listdir(entities_path) if f.endswith("_brat.txt")]
        total = len(all_files)
        num_to_process = self._calculate_subset_size(total, n, fraction)
        
        files_to_process = all_files[:num_to_process]
        
        # Process each book
        rows = []
        for fname in files_to_process:
            row = self._process_book(fname)
            if row:
                rows.append(row)
        
        # Save metadata
        df = DataFrame(rows)
        df.to_csv(self.metadata_file, index=False)
    
    def _process_book(self, fname: str) -> dict:
        """Process a single LitBank book.
        @param fname  Original filename (e.g., "1342_pride_and_prejudice_brat.txt")
        @return  Metadata dict with all annotation paths.
        """
        # Parse original filename
        stem = fname.replace("_brat.txt", "")  # "1342_pride_and_prejudice"
        parts = stem.split("_", 1)
        if len(parts) < 2:
            return None
        
        gutenberg_id = parts[0]
        original_title = parts[1].replace("_", " ")
        
        # Get new book ID
        book_id = self._get_next_id()
        
        # Fetch metadata from Gutendex using base class Helper
        # This now uses the centralized logic
        metadata = self.fetch_gutenberg_metadata(gutenberg_id=gutenberg_id)
        
        # Fallback to file parsing if API fails
        if metadata:
            title = metadata.get("title", "").strip().lower()
            author = metadata.get("author", "").strip().lower()
        else:
            title = original_title.lower()
            author = ""
        
        if not title:
            title = original_title.lower()
        
        # Read and save main text
        entities_brat_path = os.path.join(self.repo_path, "entities/brat", fname)
        with open(entities_brat_path, encoding="utf-8") as f:
            text = f.read()
        text_path = self._save_text(book_id, title, text)
        
        # Copy and rename all annotation files
        new_stem = f"{book_id:05d}_{title.replace(' ', '_')}_brat"
        annotation_paths = self._copy_annotations(stem, new_stem)
        
        # Extract LitBank ID
        lb_id = int(stem.split("_")[0])

        # Build metadata row
        metadata_row = {
            "book_id": book_id,
            "litbank_id": lb_id,
            "title": title,
            "author": author,
            "gutenberg_id": gutenberg_id,
            **annotation_paths
        }
        
        # Append to global index
        index_row = {
            "book_id": book_id,
            "title": title,
            "gutenberg_id": gutenberg_id,
            "text_path": text_path,
            "booksum_id": None,
            "booksum_path": None,
            "nqa_id": None,
            "nqa_path": None,
            "litbank_id": lb_id,
            "litbank_path": self.metadata_file
        }
        self._append_to_index(index_row)
        
        return metadata_row
    
    def _copy_annotations(self, old_stem: str, new_stem: str) -> dict:
        """Copy and rename all LitBank annotation files.
        @param old_stem  Original filename stem (e.g., "1342_pride_and_prejudice")
        @param new_stem  New filename stem with book ID (e.g., "00001_pride_and_prejudice")
        @return  Dict mapping annotation types to new file paths.
        """
        paths = {}
        
        # Entities: brat (.txt, .ann) and tsv
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "entities/brat", f"{old_stem}_brat.txt"),
            os.path.join(self.cache_dir, "entities/brat", f"{new_stem}.txt"),
            "entities_brat_txt", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "entities/brat", f"{old_stem}_brat.ann"),
            os.path.join(self.cache_dir, "entities/brat", f"{new_stem}.ann"),
            "entities_brat_ann", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "entities/tsv", f"{old_stem}_brat.tsv"),
            os.path.join(self.cache_dir, "entities/tsv", f"{new_stem}.tsv"),
            "entities_tsv", paths
        )
        
        # Events: tsv only
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "events/tsv", f"{old_stem}_brat.tsv"),
            os.path.join(self.cache_dir, "events/tsv", f"{new_stem}.tsv"),
            "events_tsv", paths
        )
        
        # Coref: brat (.txt, .ann) and conll
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "coref/brat", f"{old_stem}_brat.txt"),
            os.path.join(self.cache_dir, "coref/brat", f"{new_stem}.txt"),
            "coref_brat_txt", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "coref/brat", f"{old_stem}_brat.ann"),
            os.path.join(self.cache_dir, "coref/brat", f"{new_stem}.ann"),
            "coref_brat_ann", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "coref/conll", f"{old_stem}_brat.conll"),
            os.path.join(self.cache_dir, "coref/conll", f"{new_stem}.conll"),
            "coref_conll", paths
        )
        
        # Quotations: brat (.txt, .ann) only
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "quotations/brat", f"{old_stem}_brat.txt"),
            os.path.join(self.cache_dir, "quotations/brat", f"{new_stem}.txt"),
            "quotations_brat_txt", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "quotations/brat", f"{old_stem}_brat.ann"),
            os.path.join(self.cache_dir, "quotations/brat", f"{new_stem}.ann"),
            "quotations_brat_ann", paths
        )
        
        return paths
    
    def _copy_file_if_exists(self, src: str, dst: str, key: str, paths_dict: dict) -> None:
        """Copy file if it exists and record path.
        @param src  Source file path.
        @param dst  Destination file path.
        @param key  Key to store in paths_dict.
        @param paths_dict  Dict to update with file path.
        """
        import shutil
        
        if os.path.exists(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            paths_dict[key] = dst
        else:
            paths_dict[key] = None
    
    def load(self, streaming: bool = False) -> DataFrame:
        """Load dataset from cache.
        @param streaming  Ignored - always loads full DataFrame from cache.
        @return  DataFrame with metadata.
        @details
        Raises FileNotFoundError if cache doesn't exist. No fallback to download.
        """
        if not os.path.exists(self.metadata_file):
            raise FileNotFoundError(
                f"LitBank cache not found at {self.metadata_file}. "
                f"Run download() first."
            )
        
        return read_csv(self.metadata_file)
    
    def get_schema(self) -> list[str]:
        return ["book_id", "litbank_id", "title", "author", "gutenberg_id",
                "entities_brat_txt", "entities_brat_ann", "entities_tsv",
                "events_tsv", "coref_brat_txt", "coref_brat_ann", "coref_conll",
                "quotations_brat_txt", "quotations_brat_ann"]




















# --------------------------------------
# Alignment Functions
# --------------------------------------

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


def normalize_title(t: str) -> str:
    """Remove punctuation and extra whitespace from title.
    @param t  The title string to normalize.
    @return  Normalized title with punctuation removed and whitespace collapsed.
    """
    t = t.lower()
    t = re.sub(r"[\W_]+", " ", t)
    return t.strip()


def exact_merge(df1: DataFrame, df2: DataFrame, 
                suffix1: str, suffix2: str, 
                key_columns: list[str]) -> DataFrame:
    """Merge dataframes on exact key matches.
    @details
    Normalizes keys before merging. Use for high-confidence matches
    where titles are expected to be identical after normalization.
    All columns (except keys) are suffixed for consistency with other merge functions.
    @param df1  The left-hand DataFrame.
    @param df2  The right-hand DataFrame.
    @param suffix1  Suffix for df1 columns in output.
    @param suffix2  Suffix for df2 columns in output.
    @param key_columns  List of column names to match on (will be normalized before merge).
    @return  A new pandas DataFrame containing only rows with exact matches on normalized keys.
    """
    from pandas import merge
    
    df1 = df1.copy()
    df2 = df2.copy()
    
    # Normalize keys
    for key in key_columns:
        df1[key] = df1[key].map(normalize_title)
        df2[key] = df2[key].map(normalize_title)
    
    # Merge on normalized keys
    # Use temporary suffixes since we'll rename everything anyway
    merged = merge(df1, df2, on=key_columns, how="inner", suffixes=("_tmp_left", "_tmp_right"))
    
    # Manually reconstruct with proper suffixes for ALL columns
    result_rows = []
    for _, row in merged.iterrows():
        new_row = {}
        
        # Keep key columns as-is (these don't get suffixed by pandas)
        for key in key_columns:
            new_row[key] = row[key]
        
        # Add suffixed columns from df1
        for col in df1.columns:
            if col not in key_columns:
                # Column might be suffixed or not depending on if it conflicted
                if col in merged.columns:
                    new_row[f"{col}{suffix1}"] = row[col]
                elif f"{col}_tmp_left" in merged.columns:
                    new_row[f"{col}{suffix1}"] = row[f"{col}_tmp_left"]
        
        # Add suffixed columns from df2
        for col in df2.columns:
            if col not in key_columns:
                # Column might be suffixed or not depending on if it conflicted
                if col in merged.columns:
                    new_row[f"{col}{suffix2}"] = row[col]
                elif f"{col}_tmp_right" in merged.columns:
                    new_row[f"{col}{suffix2}"] = row[f"{col}_tmp_right"]
        
        result_rows.append(new_row)
    
    return DataFrame(result_rows)


def fuzzy_merge(df1: DataFrame, df2: DataFrame, 
                suffix1: str, suffix2: str, 
                key: str = "title", 
                threshold: int = 90,
                scorer=fuzz.token_sort_ratio) -> DataFrame:
    """Perform a two-way fuzzy merge between two DataFrames on a text column (e.g., book titles).
    @details
    - For each row in the left DataFrame, the function searches the right DataFrame for the most
    similar string in the specified key column using RapidFuzz.
    - It returns a merged DataFrame containing the best matches above a similarity threshold.
    @param df1  The left-hand DataFrame containing a text column to match on.
    @param df2  The right-hand DataFrame containing a text column to match against.
    @param suffix1  Suffix for df1 columns in output (e.g., "_booksum").
    @param suffix2  Suffix for df2 columns in output (e.g., "_nqa").
    @param key  The name of the column containing the strings to compare (default: "title").
    @param threshold  Minimum similarity score (0–100) required to consider a match valid. Defaults to 90.
    @param scorer  A RapidFuzz scoring function such as `fuzz.token_sort_ratio` or `fuzz.token_set_ratio`.
    @return  A new pandas DataFrame containing the compared strings, score, and all other columns.
    @note
        This function performs a one-to-one best match per left row.
        To ensure only confident matches are kept, adjust the `threshold` parameter.
    """
    matches = []
    right_keys = df2[key].tolist()
    
    for _, row in df1.iterrows():
        left_key = row[key]
        best = process.extractOne(left_key, right_keys, scorer=scorer)
        
        if best and best[1] >= threshold:
            right_key, score, idx = best
            right_row = df2.iloc[idx]
            
            merged_row = {
                f"{key}{suffix1}": left_key,
                f"{key}{suffix2}": right_key,
                "score": score,
            }
            
            # Add all other columns with suffixes
            for col in df1.columns:
                if col != key:
                    merged_row[f"{col}{suffix1}"] = row[col]
            
            for col in df2.columns:
                if col != key:
                    merged_row[f"{col}{suffix2}"] = right_row[col]
            
            matches.append(merged_row)
    
    return DataFrame(matches)


def text_similarity_merge(df1: DataFrame, df2: DataFrame,
                          suffix1: str, suffix2: str,
                          text_col1: str = "text_path",
                          text_col2: str = "text_path",
                          threshold: float = 0.85,
                          method: str = "jaccard") -> DataFrame:
    """Merge dataframes by comparing full text similarity.
    @details
    For each row in df1, compares full text against all texts in df2.
    Uses set-based similarity metrics (Jaccard, overlap coefficient).
    Only includes matches above similarity threshold.
    @param df1  The left-hand DataFrame.
    @param df2  The right-hand DataFrame.
    @param suffix1  Suffix for df1 columns in output.
    @param suffix2  Suffix for df2 columns in output.
    @param text_col1  Column in df1 containing text or text_path.
    @param text_col2  Column in df2 containing text or text_path.
    @param threshold  Minimum similarity score (0.0-1.0). Defaults to 0.85.
    @param method  Similarity method: "jaccard" or "overlap".
    @return  DataFrame with matched rows and similarity scores.
    @note
        This is computationally expensive for large datasets.
        Consider using on already title-matched subsets.
    """
    matches = []
    
    for _, row1 in df1.iterrows():
        # Load text from path or use directly
        text1 = _load_text_if_path(row1[text_col1])
        tokens1 = set(text1.lower().split())
        
        best_match = None
        best_score = 0.0
        
        for idx2, row2 in df2.iterrows():
            text2 = _load_text_if_path(row2[text_col2])
            tokens2 = set(text2.lower().split())
            
            # Calculate similarity
            if method == "jaccard":
                score = len(tokens1 & tokens2) / len(tokens1 | tokens2) if tokens1 or tokens2 else 0
            elif method == "overlap":
                score = len(tokens1 & tokens2) / min(len(tokens1), len(tokens2)) if tokens1 and tokens2 else 0
            else:
                raise ValueError(f"Unknown method: {method}")
            
            if score > best_score:
                best_score = score
                best_match = (idx2, row2)
        
        # Only include if above threshold
        if best_match and best_score >= threshold:
            idx2, row2 = best_match
            
            merged_row = {
                "similarity_score": best_score,
            }
            
            # Add all columns with suffixes
            for col in df1.columns:
                merged_row[f"{col}{suffix1}"] = row1[col]
            
            for col in df2.columns:
                merged_row[f"{col}{suffix2}"] = row2[col]
            
            matches.append(merged_row)
    
    return DataFrame(matches)


def _load_text_if_path(value: str) -> str:
    """Helper to load text from file path or return value directly.
    @param value  Either text content or path to text file.
    @return  Text content as string.
    """
    if isinstance(value, str) and os.path.exists(value):
        return load_text_from_path(value)
    return str(value)


# --------------------------------------
# Main Helper Functions
# --------------------------------------

def download_and_index(n_booksum: int = None, 
                       n_nqa: int = None, 
                       n_litbank: int = None,
                       litbank_repo: str = "./datasets/litbank") -> None:
    """Download datasets and build global index.
    @details
    Downloads specified number of books from each dataset,
    saves texts to global directory, and constructs unified index.
    This helper can be swapped out for alternative indexing strategies.
    @param n_booksum  Number of BookSum books to download. None = all.
    @param n_nqa  Number of NarrativeQA books to download. None = all.
    @param n_litbank  Number of LitBank books to download. None = all.
    @param litbank_repo  Path to cloned LitBank repository.
    """
    print("=== Downloading Datasets ===\n")
    
    # Download BookSum
    if n_booksum is None or n_booksum == 0:
        print(f"Skipping BookSum (n=0)...\n")
    else:
        if n_booksum == -1:
            n_booksum = None
        print(f"Downloading BookSum (n={n_booksum or 'all'})...")
        loader = BookSumLoader()
        loader.download(n=n_booksum)
        print(f"✓ BookSum downloaded\n")

    # Download NarrativeQA
    if n_nqa is None or n_nqa == 0:
        print(f"Skipping NarrativeQA (n=0)...\n")
    else:
        if n_nqa == -1:
            n_nqa = None
        print(f"Downloading NarrativeQA (n={n_nqa or 'all'})...")
        loader = NarrativeQALoader()
        loader.download(n=n_nqa)
        print(f"✓ NarrativeQA downloaded\n")
    
    # Download LitBank
    if n_litbank is None or n_litbank == 0:
        print(f"Skipping LitBank (n=0)...\n")
    else:
        if n_litbank == -1:
            n_litbank = None
        # Auto-resolve repo path
        resolved_repo = ensure_github_repo(litbank_url, "litbank")
        print(f"Processing LitBank (n={n_litbank or 'all'})...")
        loader = LitBankLoader(repo_path=resolved_repo)
        loader.download(n=n_litbank)
        print(f"✓ LitBank processed\n")


def print_index() -> None:
    """Inspects the cross-dataset book registry, and prints a helpful summary."""
    print("\n=== Global Index Summary ===")
    if os.path.exists(DatasetLoader.INDEX_FILE):
        index = read_csv(DatasetLoader.INDEX_FILE)
        print(f"Total books indexed: {len(index)}")
        print(f"Index location: {DatasetLoader.INDEX_FILE}")
        print(f"Texts directory: {DatasetLoader.TEXTS_DIR}")
    else:
        print("No index file created (no data downloaded)")


def prune_index() -> None:
    """Remove invalid entries: missing files AND duplicates.
    @details
    1. Removes rows where 'text_path' file is missing.
    2. Deduplicates based on ['gutenberg_id', 'title'].
       - If GID matches, it's a duplicate.
       - If GID is missing (NaN), it relies on Title matching.
    """
    index_file = DatasetLoader.INDEX_FILE
    if not os.path.exists(index_file):
        return

    print("Verifying index integrity...")
    df = read_csv(index_file)
    initial_count = len(df)

    # --- Step 1: Remove rows with missing text files ---
    def file_exists(path):
        return isinstance(path, str) and os.path.exists(path)

    df = df[df['text_path'].apply(file_exists)]

    # --- Step 2: Deduplicate (Gutenberg ID > Title) ---
    # We use both columns. Pandas treats (NaN, Title) == (NaN, Title) as a duplicate,
    # which correctly handles datasets like BookSum that lack Gutenberg IDs.
    dedup_keys = ['title']
    if 'gutenberg_id' in df.columns:
        dedup_keys.append('gutenberg_id')
        
    # keep='first' ensures we keep the existing entry and drop the new duplicate
    df = df.drop_duplicates(subset=dedup_keys, keep='first')

    # Sort by book_id to keep things tidy
    df = df.sort_values('book_id')
    
    # Write back if changes needed
    final_count = len(df)
    if final_count < initial_count:
        df.to_csv(index_file, index=False)
        print(f"✓ Pruned index: {initial_count} -> {final_count} entries (removed duplicates/missing).\n")
    else:
        print("✓ Index integrity check passed.\n")


def hard_reset() -> None:
    """Renumber all existing datasets starting from ID 00001.
    @details
    1. Prunes invalid entries first.
    2. Scans valid text files.
    3. Renames files sequentially (00001, 00002...).
    4. Updates global index and next_id counter.
    """
    print("\n=== Hard Reset (Renumbering) ===")
    
    if not os.path.exists(DatasetLoader.INDEX_FILE):
        print("No index to reset.")
        return

    df = read_csv(DatasetLoader.INDEX_FILE)
    if df.empty:
        return

    # Sort by current book_id to maintain relative order
    df = df.sort_values('book_id')
    
    # Track mapping from old_id -> new_id for logging/debugging
    id_map = {}
    
    print("Renumbering files...")
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
        
    print(f"✓ Renumbered {len(df)} books (IDs 1 to {len(df)})")
    print(f"✓ Next ID set to {len(df) + 1}\n")



litbank_url = "https://github.com/dbamman/litbank.git"

def ensure_github_repo(repo_url: str, repo_name: str, user_path: str = None) -> str:
    """Find or clone a GitHub repository.
    @param repo_url   The .git URL to clone.
    @param repo_name  A short name/keyword for the repo (e.g., "litbank").
    @param user_path  Optional specific path provided by user.
    @return           Absolute path to the repository.
    @details
    1. Checks user_path if provided.
    2. If no path provided, searches ./datasets for folder matching repo_name.
    3. If not found, clones to ./datasets/{repo_name}.
    """
    base_dir = "./datasets"
    default_target = os.path.join(base_dir, repo_name)

    # --- Strategy 1: Check explicit user path ---
    if user_path:
        if os.path.exists(user_path):
            return user_path
        # If provided but missing, we clone THERE
        clone_target = user_path
        print(f"Path '{user_path}' not found. Will clone {repo_name} there.")

    # --- Strategy 2: Search / Default Path ---
    else:
        # Check exact default
        if os.path.exists(default_target):
            return default_target

        # Fuzzy search in ./datasets (e.g., finds "litbank-master" when looking for "litbank")
        if os.path.exists(base_dir):
            for item in os.listdir(base_dir):
                full_path = os.path.join(base_dir, item)
                if repo_name.lower() in item.lower() and os.path.isdir(full_path):
                    print(f"Found existing {repo_name} repo at: {full_path}")
                    return full_path

        clone_target = default_target

    # --- Strategy 3: Execute Clone ---
    print(f"Cloning {repo_name} from GitHub to {clone_target}...")
    
    try:
        os.makedirs(os.path.dirname(clone_target), exist_ok=True)
        subprocess.check_call(["git", "clone", repo_url, clone_target])
        print(f"✓ {repo_name} cloned successfully.\n")
        return clone_target
        
    except FileNotFoundError:
        raise RuntimeError("Error: 'git' command not found. Please install git.")
    except subprocess.CalledProcessError:
        raise RuntimeError(f"Error: Failed to clone {repo_url}. Check connection/permissions.")


# --------------------------------------
# Main
# --------------------------------------

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Download and index book datasets for text processing pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download 10 books from each dataset
  python ingest.py --n-booksum 10 --n-nqa 10 --n-litbank 10
  
  # Download all BookSum and NarrativeQA, skip LitBank
  python ingest.py --n-booksum 0 --n-nqa 0
  
  # Download only LitBank with custom repo path
  python ingest.py --n-litbank 5 --litbank-repo ./data/litbank
        """
    )
    
    parser.add_argument(
        "--n-booksum",
        type=int,
        default=-1,
        help="Number of BookSum books to download (default: all)"
    )
    
    parser.add_argument(
        "--n-nqa",
        type=int,
        default=-1,
        help="Number of NarrativeQA books to download (default: all)"
    )
    
    parser.add_argument(
        "--n-litbank",
        type=int,
        default=-1,
        help="Number of LitBank books to process (default: all)"
    )
    
    parser.add_argument(
        "--litbank-repo",
        type=str,
        default=None,
        help="Path to cloned LitBank repository (default: ./datasets/litbank)"
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Renumber all downloaded books starting from ID 1"
    )
    
    args = parser.parse_args()
    
    # Call the helper function with parsed arguments
    download_and_index(
        n_booksum=args.n_booksum,
        n_nqa=args.n_nqa,
        n_litbank=args.n_litbank,
        litbank_repo=args.litbank_repo
    )
    # Always prune index.csv to remove ghost entries
    prune_index()
    # Renumber if requested
    if args.reset:
        hard_reset()
    print_index()
