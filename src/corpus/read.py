from pandas import DataFrame, read_csv
import subprocess
import os


# --------------------------------------
# Helper Functions - Dataset Download
# --------------------------------------

def ensure_github_repo(repo_url: str, repo_name: str, user_path: str = None) -> str:
    """Find or clone a GitHub repository.
    @param repo_url   The .git URL to clone.
    @param repo_name  A short name/keyword for the repo (e.g., "litbank").
    @param user_path  Optional specific path provided by user.
    @return           Absolute path to the repository.
    @details
    1. Checks user_path if provided.
    2. If no path provided, searches ./datasets for folder matching repo_name.
    3. If not found, clones to ./datasets/{repo_name}.
    """
    base_dir = "./datasets"
    default_target = os.path.join(base_dir, repo_name)

    # --- Strategy 1: Check explicit user path ---
    if user_path:
        if os.path.exists(user_path):
            return user_path
        # If provided but missing, we clone THERE
        clone_target = user_path
        print(f"Path '{user_path}' not found. Will clone {repo_name} there.")

    # --- Strategy 2: Search / Default Path ---
    else:
        # Check exact default
        if os.path.exists(default_target):
            return default_target

        # Fuzzy search in ./datasets (e.g., finds "litbank-master" when looking for "litbank")
        if os.path.exists(base_dir):
            for item in os.listdir(base_dir):
                full_path = os.path.join(base_dir, item)
                if repo_name.lower() in item.lower() and os.path.isdir(full_path):
                    print(f"Found existing {repo_name} repo at: {full_path}")
                    return full_path

        clone_target = default_target

    # --- Strategy 3: Execute Clone ---
    print(f"Cloning {repo_name} from GitHub to {clone_target}...")
    
    try:
        os.makedirs(os.path.dirname(clone_target), exist_ok=True)
        subprocess.check_call(["git", "clone", repo_url, clone_target])
        print(f"✓ {repo_name} cloned successfully.\n")
        return clone_target
        
    except FileNotFoundError:
        raise RuntimeError("Error: 'git' command not found. Please install git.")
    except subprocess.CalledProcessError:
        raise RuntimeError(f"Error: Failed to clone {repo_url}. Check connection/permissions.")


# --------------------------------------
# Data Loader Classes (1 per dataset)
# --------------------------------------

class LitBankLoader(DatasetLoader):
    """Loads LitBank dataset from local GitHub repository.
    @details
    Processes LitBank's ~2,000 word excerpts from 100 works of fiction.
    Copies and renames all annotation files (entities, events, coref, quotations).
    """
    
    def __init__(self, repo_path: str = "./datasets/litbank", cache_dir: str = "./datasets/litbank"):
        """Initialize LitBank loader.
        @param repo_path  Path to cloned litbank repository.
        @param cache_dir  Output directory for renamed files and metadata.
        """
        self.repo_path = repo_path
        self.cache_dir = cache_dir
        self.metadata_file = f"{cache_dir}/metadata.csv"
    
    def download(self, n: int = None, fraction: float = None) -> None:
        """Process LitBank files and copy annotations.
        @param n  Number of books to process. If None, processes all.
        @param fraction  Fraction of books to process (0.0-1.0). Overrides n if set.
        @details
        Reads original LitBank files, fetches Gutenberg metadata,
        copies and renames all annotation files with book IDs,
        saves text to global texts directory, and updates global index.
        """
        # Check original repo exists
        entities_path = os.path.join(self.repo_path, "entities/brat")
        if not os.path.exists(entities_path):
            raise FileNotFoundError(
                f"LitBank repository not found at {self.repo_path}. "
                f"Clone it first: git clone https://github.com/dbamman/litbank {self.repo_path}"
            )
        
        # Get all books from entities/brat (has all books)
        all_files = [f for f in os.listdir(entities_path) if f.endswith("_brat.txt")]
        total = len(all_files)
        num_to_process = self._calculate_subset_size(total, n, fraction)
        
        files_to_process = all_files[:num_to_process]
        
        # Process each book
        rows = []
        for fname in files_to_process:
            row = self._process_book(fname)
            if row:
                rows.append(row)
        
        # Save metadata
        df = DataFrame(rows)
        df.to_csv(self.metadata_file, index=False)
    
    def _process_book(self, fname: str) -> dict:
        """Process a single LitBank book.
        @param fname  Original filename (e.g., "1342_pride_and_prejudice_brat.txt")
        @return  Metadata dict with all annotation paths.
        """
        # Parse original filename
        stem = fname.replace("_brat.txt", "")  # "1342_pride_and_prejudice"
        parts = stem.split("_", 1)
        if len(parts) < 2:
            return None
        
        gutenberg_id = parts[0]
        original_title = parts[1].replace("_", " ")
        
        # Get new book ID
        book_id = self._get_next_id()
        
        # Fetch metadata from Gutendex using base class Helper
        # This now uses the centralized logic
        metadata = self.fetch_gutenberg_metadata(gutenberg_id=gutenberg_id)
        
        # Fallback to file parsing if API fails
        if metadata:
            title = metadata.get("title", "").strip().lower()
            author = metadata.get("author", "").strip().lower()
        else:
            title = original_title.lower()
            author = ""
        
        if not title:
            title = original_title.lower()
        
        # Read and save main text
        entities_brat_path = os.path.join(self.repo_path, "entities/brat", fname)
        with open(entities_brat_path, encoding="utf-8") as f:
            text = f.read()
        text_path = self._save_text(book_id, title, text)
        
        # Copy and rename all annotation files
        new_stem = f"{book_id:05d}_{title.replace(' ', '_')}_brat"
        annotation_paths = self._copy_annotations(stem, new_stem)
        
        # Extract LitBank ID
        lb_id = int(stem.split("_")[0])

        # Build metadata row
        metadata_row = {
            "book_id": book_id,
            "litbank_id": lb_id,
            "title": title,
            "author": author,
            "gutenberg_id": gutenberg_id,
            **annotation_paths
        }
        
        # Append to global index
        index_row = DatasetLoader.make_index_row(
            book_id = book_id,
            title = title,
            text_path = text_path,
            gutenberg_id = gutenberg_id,
            litbank_id = lb_id,
            litbank_path = self.metadata_file,
        )
        self._append_to_index(index_row)
        
        return metadata_row
    
    def _copy_annotations(self, old_stem: str, new_stem: str) -> dict:
        """Copy and rename all LitBank annotation files.
        @param old_stem  Original filename stem (e.g., "1342_pride_and_prejudice")
        @param new_stem  New filename stem with book ID (e.g., "00001_pride_and_prejudice")
        @return  Dict mapping annotation types to new file paths.
        """
        paths = {}
        
        # Entities: brat (.txt, .ann) and tsv
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "entities/brat", f"{old_stem}_brat.txt"),
            os.path.join(self.cache_dir, "entities/brat", f"{new_stem}.txt"),
            "entities_brat_txt", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "entities/brat", f"{old_stem}_brat.ann"),
            os.path.join(self.cache_dir, "entities/brat", f"{new_stem}.ann"),
            "entities_brat_ann", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "entities/tsv", f"{old_stem}_brat.tsv"),
            os.path.join(self.cache_dir, "entities/tsv", f"{new_stem}.tsv"),
            "entities_tsv", paths
        )
        
        # Events: tsv only
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "events/tsv", f"{old_stem}_brat.tsv"),
            os.path.join(self.cache_dir, "events/tsv", f"{new_stem}.tsv"),
            "events_tsv", paths
        )
        
        # Coref: brat (.txt, .ann) and conll
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "coref/brat", f"{old_stem}_brat.txt"),
            os.path.join(self.cache_dir, "coref/brat", f"{new_stem}.txt"),
            "coref_brat_txt", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "coref/brat", f"{old_stem}_brat.ann"),
            os.path.join(self.cache_dir, "coref/brat", f"{new_stem}.ann"),
            "coref_brat_ann", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "coref/conll", f"{old_stem}_brat.conll"),
            os.path.join(self.cache_dir, "coref/conll", f"{new_stem}.conll"),
            "coref_conll", paths
        )
        
        # Quotations: brat (.txt, .ann) only
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "quotations/brat", f"{old_stem}_brat.txt"),
            os.path.join(self.cache_dir, "quotations/brat", f"{new_stem}.txt"),
            "quotations_brat_txt", paths
        )
        self._copy_file_if_exists(
            os.path.join(self.repo_path, "quotations/brat", f"{old_stem}_brat.ann"),
            os.path.join(self.cache_dir, "quotations/brat", f"{new_stem}.ann"),
            "quotations_brat_ann", paths
        )
        
        return paths
    
    def _copy_file_if_exists(self, src: str, dst: str, key: str, paths_dict: dict) -> None:
        """Copy file if it exists and record path.
        @param src  Source file path.
        @param dst  Destination file path.
        @param key  Key to store in paths_dict.
        @param paths_dict  Dict to update with file path.
        """
        import shutil
        
        if os.path.exists(src):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            paths_dict[key] = dst
        else:
            paths_dict[key] = None
    
    def load(self, streaming: bool = False) -> DataFrame:
        """Load dataset from cache.
        @param streaming  Ignored - always loads full DataFrame from cache.
        @return  DataFrame with metadata.
        @details
        Raises FileNotFoundError if cache doesn't exist. No fallback to download.
        """
        if not os.path.exists(self.metadata_file):
            raise FileNotFoundError(
                f"LitBank cache not found at {self.metadata_file}. "
                f"Run download() first."
            )
        
        return read_csv(self.metadata_file)
    
    def get_schema(self) -> list[str]:
        return ["book_id", "litbank_id", "title", "author", "gutenberg_id",
                "entities_brat_txt", "entities_brat_ann", "entities_tsv",
                "events_tsv", "coref_brat_txt", "coref_brat_ann", "coref_conll",
                "quotations_brat_txt", "quotations_brat_ann"]

