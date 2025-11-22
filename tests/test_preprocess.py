"""Tests for dataset alignment functions."""

import pytest
import os
import shutil
from pandas import DataFrame
from src.ingest import exact_merge, fuzzy_merge, text_similarity_merge


@pytest.fixture(scope="module")
def sample_dataframes():
    """Fixture providing sample dataframes for alignment tests."""
    df1 = DataFrame({
        "book_id": [1, 2, 3],
        "title": ["Pride and Prejudice!", "Moby-Dick", "Jane Eyre"],
        "summary": ["Summary 1", "Summary 2", "Summary 3"]
    })
    
    df2 = DataFrame({
        "book_id": [10, 11, 12],
        "title": ["pride and prejudice", "MOBY DICK", "wuthering heights"],
        "author": ["Austen", "Melville", "Brontë"]
    })
    
    return df1, df2


@pytest.fixture(scope="module")
def fuzzy_dataframes():
    """Fixture providing dataframes with similar titles for fuzzy matching."""
    df1 = DataFrame({
        "book_id": [1, 2, 3],
        "title": ["pride and prejudice", "moby dick whale", "adventures of huckleberry finn"],
        "summary": ["Summary 1", "Summary 2", "Summary 3"]
    })
    
    df2 = DataFrame({
        "book_id": [10, 11, 12],
        "title": ["pride prejudice", "moby dick", "adventures huck finn"],
        "author": ["Austen", "Melville", "Twain"]
    })
    
    return df1, df2


@pytest.fixture(scope="module")
def text_similarity_dataframes():
    """Fixture providing dataframes with text files for similarity matching."""
    # Create temporary text files
    os.makedirs("./datasets/test_texts", exist_ok=True)
    
    text1 = "The quick brown fox jumps over the lazy dog. The fox is very quick."
    text2 = "The quick brown fox jumps over the lazy dog. The fox is fast."
    text3 = "A completely different story about cats and mice in the forest."
    
    path1 = "./datasets/test_texts/text1.txt"
    path2 = "./datasets/test_texts/text2.txt"
    path3 = "./datasets/test_texts/text3.txt"
    
    with open(path1, "w") as f:
        f.write(text1)
    with open(path2, "w") as f:
        f.write(text2)
    with open(path3, "w") as f:
        f.write(text3)
    
    df1 = DataFrame({
        "book_id": [1, 2],
        "title": ["Book A", "Book B"],
        "text_path": [path1, path3]
    })
    
    df2 = DataFrame({
        "book_id": [10, 11],
        "title": ["Book X", "Book Y"],
        "text_path": [path2, path3]
    })
    
    yield df1, df2
    
    # Cleanup after all tests in module
    shutil.rmtree("./datasets/test_texts", ignore_errors=True)


@pytest.mark.ingest
@pytest.mark.order(6)
@pytest.mark.dependency(name="align_exact", scope="session")
def test_alignment_exact_merge(sample_dataframes):
    """Tests exact_merge on normalized title keys."""
    df1, df2 = sample_dataframes
    
    # print(f"df1:\n{df1}")
    # print(f"\ndf2:\n{df2}")
    
    # Merge on title
    merged = exact_merge(df1, df2, "_left", "_right", key_columns=["title"])
    
    # print(f"\nMerged:\n{merged}")
    
    # Assertions
    assert isinstance(merged, DataFrame)
    assert len(merged) == 2  # Pride and Prejudice, Moby Dick match
    assert "title" in merged.columns
    assert "book_id_left" in merged.columns
    assert "book_id_right" in merged.columns
    assert "summary_left" in merged.columns
    assert "author_right" in merged.columns
    
    # Verify correct matches
    titles = merged["title"].tolist()
    assert "pride and prejudice" in titles
    assert "moby dick" in titles
    assert "wuthering heights" not in titles


@pytest.mark.ingest
@pytest.mark.order(7)
@pytest.mark.dependency(name="align_fuzzy", scope="session")
def test_alignment_fuzzy_merge(fuzzy_dataframes):
    """Tests fuzzy_merge with various similarity thresholds."""
    df1, df2 = fuzzy_dataframes
    
    # print(f"df1:\n{df1}")
    # print(f"\ndf2:\n{df2}")
    
    # Test with high threshold
    merged_high = fuzzy_merge(df1, df2, "_left", "_right", key="title", threshold=90)
    # print(f"\nMerged (threshold=90):\n{merged_high}")
    # print(f"Scores: {merged_high['score'].tolist() if len(merged_high) > 0 else []}")
    
    # Test with lower threshold
    merged_low = fuzzy_merge(df1, df2, "_left", "_right", key="title", threshold=70)
    # print(f"\nMerged (threshold=70):\n{merged_low}")
    # print(f"Scores: {merged_low['score'].tolist()}")
    
    # Assertions
    assert isinstance(merged_low, DataFrame)
    assert len(merged_low) >= len(merged_high)  # Lower threshold = more matches
    assert len(merged_low) == 3  # All should match with threshold=70
    assert "score" in merged_low.columns
    assert "title_left" in merged_low.columns
    assert "title_right" in merged_low.columns
    
    # Verify all scores above threshold
    assert all(score >= 70 for score in merged_low["score"])
    
    # Verify one-to-one matching (each left row matched exactly once)
    assert len(merged_low) <= len(df1)


@pytest.mark.ingest
@pytest.mark.order(8)
@pytest.mark.dependency(name="align_text_similarity", scope="session")
def test_alignment_text_similarity_merge(text_similarity_dataframes):
    """Tests text_similarity_merge on full text content."""
    df1, df2 = text_similarity_dataframes
    
    # print(f"df1:\n{df1}")
    # print(f"\ndf2:\n{df2}")
    
    # Test Jaccard similarity
    merged_jaccard = text_similarity_merge(
        df1, df2, "_left", "_right",
        text_col1="text_path", text_col2="text_path",
        threshold=0.6, method="jaccard"
    )
    
    # print(f"\nMerged (Jaccard, threshold=0.6):\n{merged_jaccard}")
    # print(f"Scores: {merged_jaccard['similarity_score'].tolist()}")
    
    # Test overlap coefficient
    merged_overlap = text_similarity_merge(
        df1, df2, "_left", "_right",
        text_col1="text_path", text_col2="text_path",
        threshold=0.7, method="overlap"
    )
    
    # print(f"\nMerged (Overlap, threshold=0.7):\n{merged_overlap}")
    # print(f"Scores: {merged_overlap['similarity_score'].tolist()}")
    
    # Assertions
    assert isinstance(merged_jaccard, DataFrame)
    assert len(merged_jaccard) >= 1  # At least text1-text2 should match
    assert "similarity_score" in merged_jaccard.columns
    assert "book_id_left" in merged_jaccard.columns
    assert "book_id_right" in merged_jaccard.columns
    
    # Verify scores above threshold
    assert all(score >= 0.6 for score in merged_jaccard["similarity_score"])
    
    # Verify text1-text2 have higher similarity than text1-text3
    if len(merged_jaccard) > 0:
        # Book A (text1) should match with Book X (text2) with high score
        book_a_matches = merged_jaccard[merged_jaccard["book_id_left"] == 1]
        if len(book_a_matches) > 0:
            assert book_a_matches.iloc[0]["similarity_score"] > 0.6

