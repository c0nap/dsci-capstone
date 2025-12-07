from pandas import DataFrame, read_csv
from urllib.parse import unquote
from datasets import load_dataset, DatasetDict  # type: ignore
import os
import re
from src.corpus.base import DatasetLoader


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
    
    def download(self, n: int = None) -> None:
        """Download dataset to local cache.
        @param n  Number of books to download. If None, downloads all.
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
            index_row = DatasetLoader.make_index_row(
                book_id = book_id,
                title = title,
                text_path = text_path,
                gutenberg_id = gutenberg_id,
                booksum_id = booksum_id,
                booksum_path = self.metadata_file,
            )
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
    
    def download(self, n: int = None) -> None:
        """Download dataset to local cache.
        @param n  Number of books to download. If None, downloads all.
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
            
            ## --- Fix: Resolve Title & ID ---
            title = ""
            url = doc.get("url", "")
            author = doc.get("author", "").strip().lower()
            gutenberg_id = None
            
            # 1. Try regex from URL
            gid_match = re.search(r'gutenberg\.org/ebooks/(\d+)', url)
            if gid_match:
                gutenberg_id = gid_match.group(1)
                # Direct ID lookup
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
            index_row = DatasetLoader.make_index_row(
                book_id = book_id,
                title = title,
                text_path = text_path,
                gutenberg_id = gutenberg_id,
                nqa_id = nqa_id,
                nqa_path = self.metadata_file,
            )
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
