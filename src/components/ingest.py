"""Dataset loading and alignment for book corpora.

Loaders handle fetching and normalizing datasets from various sources.
Alignment functions handle fuzzy matching across datasets.
"""

from abc import ABC, abstractmethod
from typing import Iterator, Any
from pandas import DataFrame, merge
from datasets import load_dataset, DatasetDict  # type: ignore
from rapidfuzz import fuzz, process
import re


# --------------------------------------
# Loaders
# --------------------------------------

class DatasetLoader(ABC):
    """Base loader for book datasets from various sources.
    @details
    Handles streaming for large datasets
    """
    
    @abstractmethod
    def load(self, streaming: bool = False) -> DataFrame | Iterator[Any]:
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
    Provides book-level summaries with full text.
    Normalizes titles by removing punctuation and lowercasing (for future alignment).
    """
    
    def __init__(self, split: str = "train"):
        self.dataset_name = "ubaada/booksum-complete-cleaned"
        self.subset = "books"
        self.split = split
    
    def load(self, streaming: bool = False) -> DataFrame | Iterator[dict]:
        ds = load_dataset(self.dataset_name, self.subset, split=self.split, streaming=streaming)
        
        if streaming:
            return (self._normalize_row(row) for row in ds)
        
        return DataFrame([self._normalize_row(row) for row in ds])
    
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
    
    def __init__(self, split: str = "train"):
        self.dataset_name = "narrativeqa"
        self.split = split
    
    def load(self, streaming: bool = False) -> DataFrame | Iterator[dict]:
        ds = load_dataset(self.dataset_name, split=self.split, streaming=streaming)
        
        if streaming:
            return (self._normalize_row(row) for row in ds)
        
        return DataFrame([self._normalize_row(row) for row in ds])
    
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


# --------------------------------------
# Alignment Functions
# --------------------------------------

def normalize_title(t: str) -> str:
    """Remove punctuation and extra whitespace from title.
    @param t  The title string to normalize.
    @return  Normalized title with punctuation removed and whitespace collapsed.
    """
    t = re.sub(r"[\W_]+", " ", t)
    return t.strip()


def exact_merge(df1: DataFrame, df2: DataFrame, 
                suffix1: str, suffix2: str, 
                key_columns: list[str]) -> DataFrame:
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
    
    return merge(df1, df2, on=key_columns, how="inner", 
                    suffixes=(suffix1, suffix2))


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


# --------------------------------------
# Main
# --------------------------------------

if __name__ == "__main__":
    from src.components.metrics import Metrics
    
    # Load datasets
    booksum = BookSumLoader().load()
    nqa = NarrativeQALoader().load()
    
    # Align using fuzzy matching
    merged = fuzzy_merge(booksum, nqa, "_booksum", "_nqa", 
                         key="title", threshold=70)
    
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