from pandas import DataFrame
from rapidfuzz import fuzz, process
import os
import re
from src.corpus.base import load_text_from_path, DatasetLoader
from typing import Tuple, Optional, Dict, List, Any
import requests
import time

GUTENDEX_API = "https://gutendex.com/books"

# --------------------------------------------------
# Helper Functions - Index Normalization
# --------------------------------------------------

def prune_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate books based on gutenberg_id and title.
    @return  Number of duplicate entries removed.
    @details
    Deduplicates based on ['gutenberg_id', 'title']:
    1. If GID matches, it's a duplicate.
    2. If GID is missing (NaN), relies on title matching.
    3. Keeps first occurrence, drops later duplicates.
    Examples:
    - (123, "moby dick") vs (123, "moby dick") → duplicate (same GID)
    - (123, "moby dick") vs (456, "moby dick") → NOT duplicate (different GIDs)
    - (NaN, "moby dick") vs (NaN, "moby dick") → duplicate (same title, no GID)
    - (NaN, "moby dick") vs (123, "moby dick") → NOT duplicate (one has GID)
    """
    keys = ['title', 'gutenberg_id']
    df = df.drop_duplicates(subset=keys, keep='first')
    return df

def normalize_title(title: str) -> str:
    """Remove punctuation, special chars, and standardize format."""
    if not title: return ""
    t = str(title).lower()
    t = re.sub(r"[\W_]+", " ", t).strip()
    return re.sub(r"\s+", "_", t)


# --------------------------------------------------
# Helper Functions - Gutenberg ID / Gutendex
# --------------------------------------------------

def extract_gutenberg_id(url: str) -> Optional[int]:
    """Extract the numeric ID from a Gutenberg URL.
    @param url  String provided by a dataset, e.g. 'https://www.gutenberg.org/ebooks/12345'
    @return  Gutenberg ID, e.g. 12345 or None"""
    match = re.search(r'gutenberg\.org/ebooks/(\d+)', url or "")
    return int(match.group(1)) if match else None

def gutendex_lookup_id(gutenberg_id: int) -> Optional[Dict[str, Any]]:
    """Fetch metadata for a specific Gutenberg ID.
    @param gutenberg_id  The numeric ID of the book.
    @return  Metadata dict with keys: gutenberg_id, title, author, language, text_url.
        Returns None if the ID does not exist or API fails.
    """
    url = f"{GUTENDEX_API}/{gutenberg_id}"
    results = _request_gutendex(url)
    return _unpack_gutendex(results.json())

def gutendex_search_title(query: str, author: Optional[str] = None, min_fuzz: int = 85) -> Optional[Dict[str, Any]]:
    """Search for a book by title (and optionally author) with fuzzy verification.
    @param query  Title string to search for (natural language preferred).
    @param author  Optional author name to filter/rank results.
    @param min_fuzz  Minimum fuzzy match score (0-100) required to accept a result.
    @return  Metadata dict with keys: gutenberg_id, title, author, language, text_url.
        Returns None if no confident match is found.
    """
    if not query:  # Not defensive: Prevent bad request if bad input
        return None
    candidates = _request_gutendex(url=GUTENDEX_API, params={"search": query, "languages": "en"})
    if not candidates:
        return None
    return _filter_candidates(candidates, query, author, min_fuzz)

def _request_gutendex(url: str, params: Dict[str, Any] = {}, timeout: int = 10, retry_delay: int = 2) -> d:
    """ """
    response = requests.get(url, params=params, timeout=timeout)
    if resp.status_code == 429:  # throttled: too many requests
        time.sleep(retry_delay)
        response = requests.get(url, params=params, timeout=timeout)
    if resp.status_code == 200:  # success
        results = response.json().get("results", [])
    return results

def _unpack_gutendex(data: dict) -> dict:
    """Normalize Gutendex JSON to our schema."""
    authors = [a.get("name", "") for a in data.get("authors", [])]
    formats = data.get("formats", {})
    
    # Priority: utf-8 -> ascii -> any plain text
    text_url = (formats.get("text/plain; charset=utf-8") or 
                formats.get("text/plain; charset=us-ascii") or 
                formats.get("text/plain"))

    return {
        "gutenberg_id": data.get("id"),
        "title": data.get("title", ""),
        "author": ", ".join(authors),
        "language": data.get("languages", [""])[0],
        "text_url": text_url
    }


def _filter_candidates(candidates: List[dict], 
                       query: str, 
                       author: str | None, 
                       min_score: int) -> Optional[dict]:
    """Select best match from candidates using fuzzy logic."""
    if not candidates:
        return None

    # Pre-process for fuzzy matching (spaces preferred over underscores)
    clean_query = query.replace("_", " ")
    clean_author = author.lower() if author else None
    
    best_match = None
    best_score = 0

    for cand in candidates:
        # 1. Author Check (if provided)
        if clean_author:
            cand_authors = [a.get("name", "").lower() for a in cand.get("authors", [])]
            # Return None if no author matches loosely (score < 70)
            if not process.extractOne(clean_author, cand_authors, scorer=fuzz.token_set_ratio, score_cutoff=70):
                continue

        # 2. Title Score
        cand_title = cand.get("title", "")
        score = fuzz.token_sort_ratio(clean_query, cand_title)
        
        if score > best_score:
            best_score = score
            best_match = cand

    if best_match and best_score >= min_score:
        return _unpack_gutendex(best_match)
    
    return None



# --------------------------------------------------
# Helper Functions - Text Similarity & Title Matching
# --------------------------------------------------

def get_text_name(book_id: int, title: str, extension: str) -> str:
    """Generate the standardized filename for a book.
    @param book_id  Internal ID (e.g., 1).
    @param title  Title string.
    @param extension  File extension including dot (e.g., '.txt').
    @return  Formatted string: '00001_title_of_book.txt'.
    """
    clean_title = normalize_title(title)
    return f"{book_id:05d}_{clean_title}{extension}"

def normalize_title(title: str) -> str:
    """Remove punctuation, special chars, and standardize title format.
    @param title  A raw title string.
    @details  Example: "Moby-Dick; or, The Whale!" -> "moby_dick_or_the_whale"
    @return  Normalized string: lowercase, alphanumeric, underscore-separated."""
    t = str(title).lower()
    
    # Replace all non-alphanumeric characters (punctuation, quotes, etc) with spaces
    # [\W_] matches anything that isn't [a-zA-Z0-9]. 
    t = re.sub(r"[\W_]+", " ", t)
    t = t.strip()
    
    # Collapse multiple spaces and replace with single underscore
    t = re.sub(r"\s+", "_", t)
    return t


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
        text1 = load_text_from_path(row1[text_col1])
        tokens1 = set(text1.lower().split())
        
        best_match = None
        best_score = 0.0
        
        for idx2, row2 in df2.iterrows():
            text2 = load_text_from_path(row2[text_col2])
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



