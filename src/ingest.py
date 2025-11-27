from pandas import read_csv
import os
from src.corpus.read import ensure_github_repo
from src.corpus.base import DatasetLoader
from src.corpus.hf import BookSumLoader, NarrativeQALoader
from src.corpus.read import LitBankLoader
from src.corpus.align import prune_index
import shutil

# --------------------------------------------------
# Helper Functions - Corpus Index Orchestration
# --------------------------------------------------

def download_by_counts(n_booksum: int = None, 
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
    
    print("Indexing text files...")
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



def clean_index(reindex: bool = False) -> None:
    """
    @details 
    Since the individual DatasetLoaders simply append new rows to the index file, a manual
    post-processing step is required for the following objectives:
    1. Ensure each path corresponds to a valid file (ghost rows).
    2. (Optional) Ensure a clean number sequence for book IDs, i.e. book 1 ... book N."""
    print("Verifying index integrity...")
    # Remove ghost entries from previous runs.
    final_count, initial_count = align.prune_index()
    if final_count < initial_count:
        print(f"✓ Pruned index: {initial_count} -> {final_count} entries (removed {initial_count - final_count} invalid/duplicate).")
    else:
        print("✓ Index integrity check passed.")
    # Renumber if requested
    if args.reset:
        hard_reset()


litbank_url = "https://github.com/dbamman/litbank.git"

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
    download_by_counts(
        n_booksum=args.n_booksum,
        n_nqa=args.n_nqa,
        n_litbank=args.n_litbank,
        litbank_repo=args.litbank_repo
    )
    clean_index(args.reset)
    print_index()
