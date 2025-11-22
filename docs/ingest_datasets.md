# Dataset Loading System

## Core Concept
Separate **downloading** (fetch and cache) from **loading** (read from cache).


## File Structure

```
ingest.py
├── DatasetLoader (ABC)               # Base class, ID manager, global index
├── BookSumLoader                     # HuggingFace dataset
├── NarrativeQALoader                 # HuggingFace dataset  
├── LitBankLoader                     # GitHub repository
└── Alignment functions               # fuzzy_merge, exact_merge

./datasets/
├── texts/
│   ├── 00001_moby_dick.txt
│   ├── 00002_pride_and_prejudice.txt
│   └── ...
├── index.csv                         # Global: book_id, title, gutenberg_id, text_path, {dataset}_metadata_path
├── booksum/
│   └── metadata.csv
├── narrativeqa/
│   └── metadata.csv
└── litbank/                          # Mirror the structure of LitBank repository
    ├── metadata.csv                  # Contains: entities_path, coref_path, etc.
    ├── entities/
    │   └── tsv/                      # Supplemental files: also reindexed by ingest.py
    │       └── 00003_bleak_house_brat.tsv
    ├── coref/
    │   └── conll/
    │       └── 00003_bleak_house_brat.conll
    ├── events/
    │   └── tsv/
    │       └── 00003_bleak_house_brat.tsv
    └── quotations/
        └── tsv/
            └── 00003_bleak_house_brat.tsv
```

## Book Indexing

**Global ID tracking:** `./datasets/next_id.txt` increments atomically. Each book gets unique ID during `download()`, saved as `./datasets/texts/{id}_{title}.txt`. 

**Global index:** ABC maintains `./datasets/index.csv` as a unified registry. When any loader downloads books, it appends rows with book_id, title, gutenberg_id (when available), text_path, and a column for that dataset's metadata path (booksum_metadata_path, narrativeqa_metadata_path, or litbank_metadata_path). This allows cross-dataset lookups - find a book by ID, see which datasets have metadata for it, and locate all associated files.

## Dataset Schemas

```
booksum/metadata.csv
├── book_id
├── title
└── summary

narrativeqa/metadata.csv
├── book_id
├── title
├── author
├── nqa_id
└── summary

litbank/metadata.csv
├── book_id
├── title
├── events_path           # → ./datasets/litbank/events/tsv/{id}_{title}.tsv
├── entities_path         # → ./datasets/litbank/entities/tsv/{id}_{title}.tsv
├── coref_path            # → ./datasets/litbank/coref/conll/{id}_{title}.conll
└── quotations_path       # → ./datasets/litbank/quotations/tsv/{id}_{title}.tsv

litbank/.../{id}_{title}.tsv
├── tokens                # One token per row
├── events                # Second column: O | EVENT
├── entities              # Multiple columns: O | B-FAC | I-LOC | ...

litbank/.../{id}_{title}_brat.ann
├── tokens                # 
├── coref                 # 
└── quotations            #

index.csv                 # Keeps track of global indexing and alignment
├── book_id
├── title
├── gutenberg_id
├── text_path             # → ./datasets/texts/{id}_{title}.txt
├── booksum_id
├── booksum_path          # → ./datasets/booksum/metadata.csv (if book in BookSum)
├── nqa_id
├── nqa_path              # → ./datasets/narrativeqa/metadata.csv (if book in NarrativeQA)
├── litbank_id
└── litbank_path          # → ./datasets/litbank/metadata.csv (if book in LitBank)
```


## Setup

LitBank is not available on HuggingFace, and must be manually cloned into the `datasets` folder.
```bash
git clone https://github.com/dbamman/litbank ./datasets/litbank
```

BookSum and NarrativeQA require no external setup.

Run the setup script to clean the `datasets` folder and download / allign datasets before the first run.
```bash
python -m src.components.ingest
```


## Usage

### 1. First Time (Download)
```python
loader = BookSumLoader(cache_dir="./datasets/booksum")
loader.download(n=10)  # Fetch 10 books from HuggingFace, save to cache
```

### 2. Subsequent Times (Load)
```python
loader = BookSumLoader(cache_dir="./datasets/booksum")
df = loader.load()  # Reads from cache, no download
```

### Full code example
```python
# Download once
BookSumLoader().download(n=1)          # Minimal
NarrativeQALoader().download(n=10)     # Specific count
LitBankLoader("./litbank").download(fraction=0.1)  # Percentage

# Load many times (from cache)
bs = BookSumLoader().load()
nqa = NarrativeQALoader().load()
lit = LitBankLoader("./litbank").load()

# Align
merged = fuzzy_merge(bs, nqa, "_bs", "_nqa", key="title", threshold=70)

# Read full text only when needed
text = load_text_from_path(lit.loc[0, 'text_path'])
```


## Alignment Functions

**Module-level functions (not methods):**
- `fuzzy_merge(df1, df2, ...)` - RapidFuzz string matching
- `exact_merge(df1, df2, ...)` - Exact key matching after normalization
- `normalize_title(t)` - Remove punctuation, lowercase
- `load_text_from_path(path)` - Read saved text file

## Why This Design?

**Separation of concerns:**
- `download()` = I/O and external APIs
- `load()` = Cache management
- `_normalize_row()` = Data transformation

**Cache-first:**
- Download once, load many times
- Faster iteration during development
- No repeated API calls

**Flexible subsets:**
- `download(n=1)` for quick testing
- `download()` for full dataset
