"""Test script to verify dataset loaders match README spec."""

from ingest import BookSumLoader, NarrativeQALoader, LitBankLoader

print("=" * 60)
print("Testing BookSum Loader")
print("=" * 60)
try:
    loader = BookSumLoader()
    print("Downloading 1 book from BookSum...")
    loader.download(n=1)
    print("✓ Download complete")
    
    print("\nLoading from cache...")
    df = loader.load()
    print(f"✓ Loaded {len(df)} books")
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample row:\n{df.iloc[0]}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "=" * 60)
print("Testing NarrativeQA Loader")
print("=" * 60)
try:
    loader = NarrativeQALoader()
    print("Downloading 1 book from NarrativeQA...")
    loader.download(n=1)
    print("✓ Download complete")
    
    print("\nLoading from cache...")
    df = loader.load()
    print(f"✓ Loaded {len(df)} books")
    print(f"Columns: {list(df.columns)}")
    print(f"\nSample row:\n{df.iloc[0]}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "=" * 60)
print("Testing LitBank Loader")
print("=" * 60)
try:
    import os
    if not os.path.exists("./datasets/litbank"):
        print("✗ LitBank repo not found. Clone it first:")
        print("  git clone https://github.com/dbamman/litbank ./datasets/litbank")
    else:
        loader = LitBankLoader()
        print("Processing 1 book from LitBank...")
        loader.download(n=1)
        print("✓ Download complete")
        
        print("\nLoading from cache...")
        df = loader.load()
        print(f"✓ Loaded {len(df)} books")
        print(f"Columns: {list(df.columns)}")
        print(f"\nSample row:\n{df.iloc[0]}")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "=" * 60)
print("Checking Global Index")
print("=" * 60)
try:
    import os
    from pandas import read_csv
    
    if os.path.exists("./datasets/index.csv"):
        index = read_csv("./datasets/index.csv")
        print(f"✓ Global index exists with {len(index)} books")
        print(f"Columns: {list(index.columns)}")
        print(f"\nSample rows:\n{index.head()}")
    else:
        print("✗ Global index not found")
except Exception as e:
    print(f"✗ Error: {e}")

print("\n" + "=" * 60)
print("Testing Strict Load Separation")
print("=" * 60)
try:
    # Try loading without download
    loader = BookSumLoader(cache_dir="./datasets/booksum_test")
    df = loader.load()
    print("✗ Load should have failed without download!")
except FileNotFoundError as e:
    print(f"✓ Load correctly failed: {e}")
except Exception as e:
    print(f"✗ Unexpected error: {e}")