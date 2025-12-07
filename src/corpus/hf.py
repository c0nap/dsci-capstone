from pandas import DataFrame, read_csv
from datasets import load_dataset  # type: ignore
from typing import Optional, Any
import os
import re
from urllib.parse import unquote
from abc import abstractmethod
from src.corpus.base import DatasetLoader
from src.corpus import align


class HuggingFaceLoader(DatasetLoader):
    """Intermediate loader for streaming datasets from HuggingFace.
    @details
    Abstracts the common pattern of:
    1. Streaming a HF dataset.
    2. Extracting text/metadata per row via the '_extract_book_data' hook.
    3. Saving text to disk using the base class logic.
    4. Updating the global index safely.
    """
    
    def __init__(self, dataset_name: str, subset: Optional[str], split: str, cache_dir: str):
        self.dataset_name = dataset_name
        self.subset = subset
        self.split = split
        self.cache_dir = cache_dir
        self.metadata_file = os.path.join(cache_dir, "metadata.csv")

    @abstractmethod
    def _extract_book_data(self, raw_item: dict) -> Optional[dict]:
        """Hook: Transform a raw HF row into our standardized dict.
        @return Dict with keys: 'source_id', 'title', 'text', 'summary', 'gutenberg_id', 'extras'.
                Return None to skip the item.
        """
        pass

    @abstractmethod
    def _get_dataset_specific_index_cols(self, source_id: str | int) -> dict:
        """Hook: Map source_id to the specific index columns (e.g. {'booksum_id': 123})."""
        pass

    def download(self, n: int = None) -> None:
        """Download dataset to local cache.
        @param n  Number of books to download. If None, downloads all.
        """
        os.makedirs(self.cache_dir, exist_ok=True)
        
        print(f"Initializing stream for {self.dataset_name}...")
        try:
            ds = load_dataset(
                self.dataset_name, 
                self.subset, 
                split=self.split, 
                streaming=True, 
                trust_remote_code=True
            )
        except Exception as e:
            print(f"Error loading dataset {self.dataset_name}: {e}")
            return

        num_to_download = n if n is not None else float('inf')
        rows = []
        count = 0
        skipped = 0

        for raw in ds:
            if count >= num_to_download:
                break

            # 1. Extract (Subclass Logic)
            data = self._extract_book_data(raw)
            if not data:
                skipped += 1
                continue

            # 2. Assign ID (Thread-Safe Base Logic)
            book_id = self.consume_next_id()
            
            # 3. Save Text (Base Logic)
            title = data.get('title', 'untitled')
            text = data.get('text', '')
            text_path = self.save_text(book_id, title, text)
            
            # 4. Prepare Metadata Row (For local CSV)
            meta_row = {
                "book_id": book_id,
                "source_id": data.get("source_id"),
                "title": title,
                "summary": data.get("summary", ""),
            }
            meta_row.update(data.get("extras", {}))
            rows.append(meta_row)

            # 5. Update Global Index (Base Logic)
            specific_cols = self._get_dataset_specific_index_cols(data.get("source_id"))
            
            self.append_to_index(
                book_id=book_id,
                title=title,
                text_path=text_path,
                gutenberg_id=data.get("gutenberg_id"),
                **specific_cols
            )

            count += 1
            print(f"[{self.__class__.__name__}] Processed: {count} | Skipped: {skipped}", end='\r')

        print(f"\n[{self.__class__.__name__}] Download complete. Saving metadata...")
        
        if rows:
            df = DataFrame(rows)
            df.to_csv(self.metadata_file, index=False)
        else:
            print(f"Warning: No valid rows processed for {self.dataset_name}.")

    def load(self, streaming: bool = False) -> DataFrame:
        """Load local metadata from CSV."""
        if not os.path.exists(self.metadata_file):
            raise FileNotFoundError(f"Cache not found at {self.metadata_file}. Run download() first.")
        return read_csv(self.metadata_file)


# ---------------------------------------------------------------------
# Concrete Implementations
# ---------------------------------------------------------------------

class BookSumLoader(HuggingFaceLoader):
    """Loads BookSum dataset (ubaada/booksum-complete-cleaned)."""
    
    def __init__(self, cache_dir: str = "./datasets/booksum", split: str = "train"):
        super().__init__(
            dataset_name="ubaada/booksum-complete-cleaned", 
            subset="books", 
            split=split, 
            cache_dir=cache_dir
        )
        self._internal_count = 0

    def _extract_book_data(self, raw_item: dict) -> Optional[dict]:
        # 1. Normalize Title
        raw_title = raw_item.get("title", "")
        if not isinstance(raw_title, str):
            return None
        
        # align.normalize_title handles punctuation/spacing/lowercase
        title = align.normalize_title(raw_title)
        if not title:
            return None

        # 2. Gutenberg Lookup (BookSum has no ID, strict title search via align)
        gutenberg_id = None
        # Note: In production, consider caching this to reduce API hits
        meta = align.fetch_gutenberg_metadata(query=title)
        if meta:
            gutenberg_id = meta.get("gutenberg_id")

        self._internal_count += 1
        # Use 'bid' if present, else fallback to internal counter
        source_id = raw_item.get("bid", self._internal_count)

        return {
            "source_id": source_id,
            "title": title,
            "text": raw_item.get("text", ""),
            "summary": raw_item.get("summary", ""),
            "gutenberg_id": gutenberg_id,
            "extras": {} 
        }

    def _get_dataset_specific_index_cols(self, source_id: str | int) -> dict:
        return {
            "booksum_id": source_id,
            "booksum_path": self.metadata_file
        }

    def get_schema(self) -> list[str]:
        return ["book_id", "source_id", "title", "summary"]


class NarrativeQALoader(HuggingFaceLoader):
    """Loads NarrativeQA dataset."""

    def __init__(self, cache_dir: str = "./datasets/narrativeqa", split: str = "train"):
        super().__init__(
            dataset_name="narrativeqa",
            subset=None,
            split=split,
            cache_dir=cache_dir
        )
        self._seen_ids = set()

    def _extract_book_data(self, raw_item: dict) -> Optional[dict]:
        doc = raw_item.get("document", {})
        nqa_id = doc.get("id", "")

        # Deduplication
        if not nqa_id or nqa_id in self._seen_ids:
            return None
        self._seen_ids.add(nqa_id)

        url = doc.get("url", "")
        raw_title = ""

        # 1. Gutenberg ID Extraction (Delegated to align.py)
        gutenberg_id = align.extract_gutenberg_id(url)
        
        # 2. Title Extraction (Fallback to URL slug if metadata missing)
        # Note: This is extraction logic, not matching logic, so it stays here.
        if not raw_title and url:
            slug = url.split('/')[-1].replace('.html', '').replace('.htm', '')
            try:
                # Basic URL decoding
                raw_title = unquote(slug).replace('_', ' ').replace('-', ' ')
            except:
                raw_title = slug

        title = align.normalize_title(raw_title)

        return {
            "source_id": nqa_id,
            "title": title,
            "text": doc.get("text", ""),
            "summary": doc.get("summary", {}).get("text", ""),
            "gutenberg_id": gutenberg_id,
            "extras": {
                "author": doc.get("author", "").strip(),
                "url": url
            }
        }

    def _get_dataset_specific_index_cols(self, source_id: str | int) -> dict:
        return {
            "nqa_id": source_id,
            "nqa_path": self.metadata_file
        }

    def get_schema(self) -> list[str]:
        return ["book_id", "source_id", "title", "summary", "author", "url"]
