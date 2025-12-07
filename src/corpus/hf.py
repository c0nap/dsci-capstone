from pandas import DataFrame, read_csv
from datasets import load_dataset  # type: ignore
from typing import Optional, Any, List, Dict, Union
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
    def _extract_book_data(self, raw_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Hook: Transform a raw HF row into our standardized dict.
        @return Dict with KEYS MATCHING GET_SCHEMA + 'text' and 'gutenberg_id'.
                Example: {'nqa_id': '...', 'title': '...', 'text': '...', ...}
                Return None to skip the item.
        """
        pass

    @abstractmethod
    def make_index_cols(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate the dataset-specific columns for the global index, not the dataset schema.
        @param data  The dictionary returned by @ref src.corpus.hf.HuggingFaceLoader_extract_book_data.
        @return Dict with keys like {'nqa_id': ..., 'nqa_path': ...}
        """
        pass

    def download(self, n: Optional[int] = None) -> None:
        """Download dataset to local cache.
        @param n  Number of books to download. If None, downloads all.
        """
        os.makedirs(self.cache_dir, exist_ok=True)
        
        print(f"Initializing stream for {self.dataset_name}...")
        try:
            dataset = load_dataset(
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
        rows: List[Dict[str, Any]] = []
        count = 0
        skipped = 0

        for raw in dataset:
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
            # We pop 'text' so it doesn't get saved to the CSV metadata file
            text = data.pop("text", "")
            title = data.get("title", "untitled")
            text_path = self.save_text(book_id, title, text)
            
            # 4. Prepare Metadata Row (For local CSV)
            # Inject our internal book_id into the dataset specific record
            data["book_id"] = book_id
            
            # We treat 'gutenberg_id' as index-only data, typically not needed in local meta
            # but we can keep it if desired. For now, we use it for alignment.
            gutenberg_id = data.get("gutenberg_id")
            
            rows.append(data)

            # 5. Update Global Index (Base Logic)
            self.append_to_index(
                book_id=book_id,
                title=title,
                text_path=text_path,
                gutenberg_id=gutenberg_id,
                **self.make_index_cols(data)
            )

            count += 1
            print(f"[{self.__class__.__name__}] Processed: {count} | Skipped: {skipped}", end='\r')

        print(f"\n[{self.__class__.__name__}] Download complete. Saving metadata...")
        
        if rows:
            df = DataFrame(rows)
            # Ensure columns match get_schema logic if strictly enforcing
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

    def _extract_book_data(self, raw_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw_title = raw_item["title"]
        title = align.normalize_title(raw_title)
        gutenberg_id = align.fetch_gutenberg_metadata(query=title)["gutenberg_id"]
        booksum_id = raw_item["bid"]

        return {
            "booksum_id": booksum_id,
            "title": title,
            "text": raw_item["text"],
            "summary": raw_item["summary"],
            "gutenberg_id": gutenberg_id
        }

    def make_index_cols(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "booksum_id": data["booksum_id"],
            "booksum_path": self.metadata_file
        }

    def get_schema(self) -> List[str]:
        return ["book_id", "title", "booksum_id", "summary"]


class NarrativeQALoader(HuggingFaceLoader):
    """Loads NarrativeQA dataset."""

    def __init__(self, cache_dir: str = "./datasets/narrativeqa", split: str = "train"):
        super().__init__(
            dataset_name="narrativeqa",
            subset=None,
            split=split,
            cache_dir=cache_dir
        )

    def _extract_book_data(self, raw_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        doc = raw_item["document"]
        nqa_id = doc["nqa_id"]
        url = doc["url"]
        raw_title = doc["title"]
        gutenberg_id = align.extract_gutenberg_id(url)
        if not gutenberg_id:
            gutenberg_id = align.fetch_gutenberg_metadata(query=title)["gutenberg_id"]
        title = align.normalize_title(raw_title)
        
        return {
            "nqa_id": nqa_id,
            "title": title,
            "text": doc["text"],
            "summary": doc["summary"]["text"],
            "author": doc["author"],
            "url": url,
            "gutenberg_id": gutenberg_id
        }

    def make_index_cols(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "nqa_id": data["nqa_id"],
            "nqa_path": self.metadata_file
        }

    def get_schema(self) -> List[str]:
        return ["book_id", "title", "nqa_id", "summary", "author", "url"]
