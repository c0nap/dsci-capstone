"""Smoke tests for dataset loaders."""

import pytest
import os
from pandas import DataFrame, read_csv
from src.ingest import BookSumLoader, NarrativeQALoader, LitBankLoader


@pytest.mark.smoke
@pytest.mark.ingest
@pytest.mark.order(1)
@pytest.mark.dependency(name="ingest_booksum_minimal", scope="session")
def test_ingest_booksum_minimal():
    """Downloads and loads 1 book from BookSum."""
    print("\n" + "=" * 60)
    print("Testing BookSum Loader")
    print("=" * 60)
    
    loader = BookSumLoader(cache_dir="./datasets/booksum_test")
    
    # Test download
    print("Downloading 1 book from BookSum...")
    loader.download(n=1)
    print("✓ Download complete")
    
    # Test load
    print("\nLoading from cache...")
    df = loader.load()
    print(f"✓ Loaded {len(df)} books")
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample row:\n{df.iloc[0]}")
    
    # Assertions
    assert isinstance(df, DataFrame)
    assert len(df) == 1
    assert "book_id" in df.columns
    assert "title" in df.columns
    assert "summary" in df.columns
    
    # Verify text file exists
    text_path = os.path.join("./datasets/texts", os.listdir("./datasets/texts")[0])
    assert os.path.exists(text_path)
    print(f"✓ Text file created: {text_path}")


@pytest.mark.smoke
@pytest.mark.ingest
@pytest.mark.order(2)
@pytest.mark.dependency(name="ingest_narrativeqa_minimal", scope="session")
def test_ingest_narrativeqa_minimal():
    """Downloads and loads 1 book from NarrativeQA."""
    print("\n" + "=" * 60)
    print("Testing NarrativeQA Loader")
    print("=" * 60)
    
    loader = NarrativeQALoader(cache_dir="./datasets/narrativeqa_test")
    
    # Test download
    print("Downloading 1 book from NarrativeQA...")
    loader.download(n=1)
    print("✓ Download complete")
    
    # Test load
    print("\nLoading from cache...")
    df = loader.load()
    print(f"✓ Loaded {len(df)} books")
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample row:\n{df.iloc[0]}")
    
    # Assertions
    assert isinstance(df, DataFrame)
    assert len(df) == 1
    assert "book_id" in df.columns
    assert "nqa_id" in df.columns
    assert "title" in df.columns
    assert "author" in df.columns
    assert "summary" in df.columns


@pytest.mark.smoke
@pytest.mark.ingest
@pytest.mark.order(3)
@pytest.mark.dependency(name="ingest_litbank_minimal", scope="session")
def test_ingest_litbank_minimal():
    """Processes and loads 1 book from LitBank."""
    print("\n" + "=" * 60)
    print("Testing LitBank Loader")
    print("=" * 60)
    
    # Check repo exists
    if not os.path.exists("./datasets/litbank"):
        pytest.skip("LitBank repo not found. Clone it first: git clone https://github.com/dbamman/litbank ./datasets/litbank")
    
    loader = LitBankLoader(cache_dir="./datasets/litbank_test")
    
    # Test download
    print("Processing 1 book from LitBank...")
    loader.download(n=1)
    print("✓ Download complete")
    
    # Test load
    print("\nLoading from cache...")
    df = loader.load()
    print(f"✓ Loaded {len(df)} books")
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample row:\n{df.iloc[0]}")
    
    # Assertions
    assert isinstance(df, DataFrame)
    assert len(df) == 1
    assert "book_id" in df.columns
    assert "litbank_id" in df.columns
    assert "title" in df.columns
    assert "author" in df.columns
    assert "gutenberg_id" in df.columns
    
    # Verify annotation files copied
    sample_row = df.iloc[0]
    entities_tsv = sample_row.get("entities_tsv")
    if entities_tsv:
        assert os.path.exists(entities_tsv)
        print(f"✓ Annotation file created: {entities_tsv}")


@pytest.mark.smoke
@pytest.mark.ingest
@pytest.mark.order(4)
@pytest.mark.dependency(depends=["ingest_booksum_minimal", "ingest_narrativeqa_minimal"], scope="session")
def test_ingest_global_index():
    """Verifies global index.csv is created and populated."""
    print("\n" + "=" * 60)
    print("Checking Global Index")
    print("=" * 60)
    
    assert os.path.exists("./datasets/index.csv"), "Global index not found"
    
    index = read_csv("./datasets/index.csv")
    print(f"✓ Global index exists with {len(index)} books")
    print(f"Columns: {list(index.columns)}")
    print(f"\nSample rows:\n{index.head()}")
    
    # Assertions
    assert isinstance(index, DataFrame)
    assert len(index) >= 2  # At least booksum + narrativeqa
    assert "book_id" in index.columns
    assert "title" in index.columns
    assert "text_path" in index.columns
    assert "booksum_id" in index.columns
    assert "nqa_id" in index.columns


@pytest.mark.smoke
@pytest.mark.ingest
@pytest.mark.order(5)
@pytest.mark.dependency(name="ingest_strict_separation", scope="session")
def test_ingest_strict_separation():
    """Verifies load() fails without cache (strict separation)."""
    print("\n" + "=" * 60)
    print("Testing Strict Load Separation")
    print("=" * 60)
    
    # Try loading without download
    loader = BookSumLoader(cache_dir="./datasets/booksum_nonexistent")
    
    with pytest.raises(FileNotFoundError) as exc_info:
        loader.load()
    
    print(f"✓ Load correctly failed: {exc_info.value}")
    assert "Run download() first" in str(exc_info.value)