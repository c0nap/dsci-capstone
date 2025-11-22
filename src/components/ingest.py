"""Dataset loading and alignment for book corpora.

Loaders handle fetching and normalizing datasets from various sources.
Alignment functions handle fuzzy matching across datasets.
"""

from abc import ABC, abstractmethod
from typing import Iterator
import pandas as pd
from datasets import load_dataset, DatasetDict  # type: ignore
from rapidfuzz import fuzz, process
import re


# --------------------------------------
# Loaders
# --------------------------------------

class DatasetLoader(ABC):
    """Base loader for book datasets from various sources.
    @details
    Handles streaming for large datasets and normalizes output to a 
    common schema (title, author, text, summary, etc.)
    """
    
    @abstractmethod
    def load(self, streaming: bool = False) -> pd.DataFrame | Iterator[dict]:
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
    
    def download(self) -> None:
        """Download dataset to local cache.
        @details
        Downloads HuggingFace dataset and saves as CSV to cache_dir.
        Subsequent loads will use the cached version.
        """
        import os
        os.makedirs(self.cache_dir, exist_ok=True)
        ds = load_dataset(self.dataset_name, self.subset, split=self.split)
        df = pd.DataFrame([self._normalize_row(row) for row in ds])
        df.to_csv(f"{self.cache_dir}/{self.split}.csv", index=False)
    
    def load(self, streaming: bool = False) -> pd.DataFrame | Iterator[dict]:
        import os
        cache_file = f"{self.cache_dir}/{self.split}.csv"
        
        # Load from cache if available
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        
        # Otherwise fetch from HuggingFace
        ds = load_dataset(self.dataset_name, self.subset, split=self.split, streaming=streaming)
        
        if streaming:
            return (self._normalize_row(row) for row in ds)
        
        return pd.DataFrame([self._normalize_row(row) for row in ds])
    
    def _normalize_row(self, row: dict) -> dict:
        """Map BookSum fields to common schema with cleaned title.
        @param row  Raw row from HuggingFace dataset.
        @return  Normalized dict with keys: title, text, summary.
        """
        title = row.get("title", "")
        if isinstance(title, str):
            title = title.lower().strip()
            title = re.sub(r"[\W_]+", " ", title).strip()
        else:
            title = ""
        
        return {
            "title": title,
            "text": row.get("text", ""),
            "summary": row.get("summary", ""),
        }
    
    def get_schema(self) -> list[str]:
        return ["title", "text", "summary"]


class NarrativeQALoader(DatasetLoader):
    """Loads NarrativeQA dataset from HuggingFace.
    @details
    Provides book metadata (title, author, ID) without full text.
    Useful for aligning with other sources that have full text.
    """
    
    def __init__(self, cache_dir: str = "./datasets/narrativeqa", split: str = "train"):
        self.dataset_name = "narrativeqa"
        self.split = split
        self.cache_dir = cache_dir
    
    def download(self) -> None:
        """Download dataset to local cache.
        @details
        Downloads HuggingFace dataset and saves as CSV to cache_dir.
        """
        import os
        os.makedirs(self.cache_dir, exist_ok=True)
        ds = load_dataset(self.dataset_name, split=self.split)
        df = pd.DataFrame([self._normalize_row(row) for row in ds])
        df.to_csv(f"{self.cache_dir}/{self.split}.csv", index=False)
    
    def load(self, streaming: bool = False) -> pd.DataFrame | Iterator[dict]:
        import os
        cache_file = f"{self.cache_dir}/{self.split}.csv"
        
        # Load from cache if available
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        
        # Otherwise fetch from HuggingFace
        ds = load_dataset(self.dataset_name, split=self.split, streaming=streaming)
        
        if streaming:
            return (self._normalize_row(row) for row in ds)
        
        return pd.DataFrame([self._normalize_row(row) for row in ds])
    
    def _normalize_row(self, row: dict) -> dict:
        """Extract document metadata from NarrativeQA format.
        @param row  Raw row from HuggingFace dataset containing nested 'document' dict.
        @return  Normalized dict with keys: title, author, nqa_id.
        """
        doc = row.get("document", {})
        return {
            "title": doc.get("title", "").strip().lower(),
            "author": doc.get("author", "").strip().lower(),
            "nqa_id": doc.get("id", ""),
        }
    
    def get_schema(self) -> list[str]:
        return ["title", "author", "nqa_id"]


class LitBankLoader(DatasetLoader):
    """Loads LitBank dataset from local GitHub repository.
    @details
    LitBank contains ~2,000 word excerpts from 100 works of fiction.
    Text files are in brat format within the repo. Full text is saved to
    a separate directory, and metadata is fetched from Gutendex API.
    
    This approach keeps memory overhead low by not storing full text in
    the DataFrame. Instead, we save text files and reference their paths.
    """
    
    def __init__(self, repo_path: str, text_output_dir: str = "./datasets/litbank_texts"):
        """Initialize LitBank loader.
        @param repo_path  Path to cloned litbank repository.
        @param text_output_dir  Directory to save full text files.
        """
        self.repo_path = repo_path
        self.text_dir = "entities/brat"
        self.text_output_dir = text_output_dir
        self.gutendex_url = "https://gutendex.com/books"
        self.cache_file = f"{text_output_dir}/metadata.csv"
    
    def download(self) -> None:
        """Download metadata and save text files.
        @details
        Processes all text files from repo, fetches metadata from Gutendex,
        saves text to output directory, and caches metadata as CSV.
        """
        import os
        from pathlib import Path
        
        text_path = Path(self.repo_path) / self.text_dir
        if not text_path.exists():
            raise FileNotFoundError(f"LitBank text directory not found: {text_path}")
        
        os.makedirs(self.text_output_dir, exist_ok=True)
        
        rows = list(self._iter_files(text_path))
        df = pd.DataFrame(rows)
        df.to_csv(self.cache_file, index=False)
    
    def load(self, streaming: bool = False) -> pd.DataFrame | Iterator[dict]:
        import os
        from pathlib import Path
        
        # Load from cache if available
        if os.path.exists(self.cache_file):
            return pd.read_csv(self.cache_file)
        
        # Otherwise process files
        text_path = Path(self.repo_path) / self.text_dir
        if not text_path.exists():
            raise FileNotFoundError(f"LitBank text directory not found: {text_path}")
        
        os.makedirs(self.text_output_dir, exist_ok=True)
        
        if streaming:
            return self._iter_files(text_path)
        
        rows = list(self._iter_files(text_path))
        return pd.DataFrame(rows)
    
    def _fetch_gutenberg_metadata(self, gutenberg_id: str) -> dict:
        """Fetch metadata from Gutendex API.
        @param gutenberg_id  Project Gutenberg ID number.
        @return  Dict with keys: title, authors, subjects, language, download_url
        @details
        Uses the public Gutendex API (https://gutendex.com) which provides
        Project Gutenberg catalog metadata in JSON format.
        """
        import requests
        
        try:
            url = f"{self.gutendex_url}/{gutenberg_id}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Extract relevant fields
            authors = [a.get("name", "") for a in data.get("authors", [])]
            
            return {
                "title": data.get("title", ""),
                "authors": authors,
                "author": ", ".join(authors) if authors else "",
                "subjects": data.get("subjects", []),
                "language": data.get("languages", [""])[0],
                "download_url": data.get("formats", {}).get("text/plain", ""),
            }
        except Exception as e:
            # Return empty metadata on error
            return {
                "title": "",
                "authors": [],
                "author": "",
                "subjects": [],
                "language": "",
                "download_url": "",
            }
    
    def load(self, streaming: bool = False) -> DataFrame | Iterator[dict]:
        import os
        from pathlib import Path
        
        text_path = Path(self.repo_path) / self.text_dir
        
        if not text_path.exists():
            raise FileNotFoundError(f"LitBank text directory not found: {text_path}")
        
        # Create output directory for text files
        os.makedirs(self.text_output_dir, exist_ok=True)
        
        if streaming:
            return self._iter_files(text_path)
        
        rows = list(self._iter_files(text_path))
        return DataFrame(rows)
    
    def _iter_files(self, text_path) -> Iterator[dict]:
        """Stream text files from disk, save full text separately.
        @param text_path  Path to directory containing .txt files.
        @return  Iterator of normalized dicts with file paths instead of text.
        @details
        Extracts gutenberg_id from filename, fetches metadata from Gutendex,
        saves full text to separate file, returns metadata + file path.
        """
        import os
        
        for fname in os.listdir(text_path):
            if not fname.endswith("_brat.txt"):
                continue
            
            # Parse filename: {gutenberg_id}_{title}_brat.txt
            stem = fname.replace("_brat.txt", "")
            parts = stem.split("_", 1)
            
            if len(parts) < 2:
                continue
            
            gutenberg_id = parts[0]
            filename_title = parts[1].replace("_", " ").strip().lower()
            
            # Read text from LitBank excerpt
            input_path = os.path.join(text_path, fname)
            with open(input_path, encoding="utf-8") as f:
                text = f.read()
            
            # Save text to separate file
            output_filename = f"{gutenberg_id}_{filename_title.replace(' ', '_')}.txt"
            output_path = os.path.join(self.text_output_dir, output_filename)
            
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
            
            # Fetch metadata from Gutendex
            metadata = self._fetch_gutenberg_metadata(gutenberg_id)
            
            # Use Gutendex title if available, otherwise use filename title
            title = metadata.get("title", "").strip().lower()
            if not title:
                title = filename_title
            
            yield {
                "gutenberg_id": gutenberg_id,
                "title": title,
                "author": metadata.get("author", "").strip().lower(),
                "language": metadata.get("language", ""),
                "text_path": output_path,
                "download_url": metadata.get("download_url", ""),
            }
    
    def get_schema(self) -> list[str]:
        return ["gutenberg_id", "title", "author", "language", "text_path", "download_url"]


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
    t = re.sub(r"[\W_]+", " ", t)
    return t.strip()


def exact_merge(df1: pd.DataFrame, df2: pd.DataFrame, 
                suffix1: str, suffix2: str, 
                key_columns: list[str]) -> pd.DataFrame:
    """Merge dataframes on exact key matches.
    @details
    Normalizes keys before merging. Use for high-confidence matches
    where titles are expected to be identical after normalization.
    @param df1  The left-hand DataFrame.
    @param df2  The right-hand DataFrame.
    @param suffix1  Suffix for df1 columns in output.
    @param suffix2  Suffix for df2 columns in output.
    @param key_columns  List of column names to match on (will be normalized before merge).
    @return  A new pandas DataFrame containing only rows with exact matches on normalized keys.
    """
    df1 = df1.copy()
    df2 = df2.copy()
    
    for key in key_columns:
        df1[key] = df1[key].map(normalize_title)
        df2[key] = df2[key].map(normalize_title)
    
    return pd.merge(df1, df2, on=key_columns, how="inner", 
                    suffixes=(suffix1, suffix2))


def fuzzy_merge(df1: pd.DataFrame, df2: pd.DataFrame, 
                suffix1: str, suffix2: str, 
                key: str = "title", 
                threshold: int = 90,
                scorer=fuzz.token_sort_ratio) -> pd.DataFrame:
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
    
    return pd.DataFrame(matches)


# --------------------------------------
# Main
# --------------------------------------

if __name__ == "__main__":
    from components.metrics import Metrics
    
    # Load datasets
    booksum = BookSumLoader().load()
    nqa = NarrativeQALoader().load()
    
    # Load LitBank - saves text to files, fetches metadata from Gutendex
    # litbank = LitBankLoader("./datasets/litbank", text_output_dir="./datasets/litbank_texts").load()
    # print(f"LitBank rows: {len(litbank)}")
    # print(f"Sample: {litbank.iloc[0]}")
    
    # To read full text later:
    # text = load_text_from_path(litbank.loc[0, 'text_path'])
    
    # Align using fuzzy matching
    merged = fuzzy_merge(booksum, nqa, "_booksum", "_nqa", 
                         key="title", threshold=70)
    
    # Example: align BookSum with LitBank on title
    # merged_litbank = fuzzy_merge(booksum, litbank, "_booksum", "_litbank",
    #                               key="title", threshold=75)
    
    # Save outputs
    booksum.to_csv("./datasets/metrics/booksum.csv", index=False)
    nqa.to_csv("./datasets/metrics/nqa.csv", index=False)
    merged.to_csv("./datasets/metrics/merged.csv", index=False)
    
    print(f"BookSum rows: {len(booksum)}")
    print(f"NarrativeQA rows: {len(nqa)}")
    print(f"Fuzzy matches: {len(merged)}")
    
    # Example metric computation
    m = Metrics()
    m.post_basic_metrics(
        "1", 
        merged.loc[0, 'title_nqa'], 
        merged.loc[0, 'summary_booksum'], 
        merged.loc[0, 'summary_booksum']
    )