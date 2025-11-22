# Dataset Loading System

## Core Concepts

Separate **downloading** (fetch and cache) from **loading** (read from cache).

`load()` will fail if `ingest.py` has not yet been run, or if the datasets folder is empty.


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
    │   ├── brat/                     # Supplemental files: also reindexed by ingest.py
    │   │   ├── 00003_bleak_house_brat.txt
    │   │   └── 00003_bleak_house_brat.ann
    │   └── tsv/
    │       └── 00003_bleak_house_brat.tsv
    ├── events/
    │   └── tsv/
    │       └── 00003_bleak_house_brat.tsv
    ├── coref/
    │   ├── brat/
    │   │   ├── 00003_bleak_house_brat.txt
    │   │   └── 00003_bleak_house_brat.ann
    │   └── conll/
    │       └── 00003_bleak_house_brat.conll
    └── quotations/
        └── brat/
            ├── 00003_bleak_house_brat.txt
            └── 00003_bleak_house_brat.ann
```

## Book Indexing

**Global ID tracking:** `./datasets/next_id.txt` increments atomically. Each book gets unique ID during `download()`, saved as `./datasets/texts/{id}_{title}.txt`. 

**Global index:** ABC maintains `./datasets/index.csv` as a unified registry. When any loader downloads books, it appends rows with book_id, title, gutenberg_id (when available), text_path, and a column for that dataset's metadata path (booksum_path, nqa_path, or litbank_path). This allows cross-dataset lookups - find a book by ID, see which datasets have metadata for it, and locate all associated files.

## Dataset Schemas

```
booksum/metadata.csv
├── book_id               # Our global ID
├── booksum_id            # Original BookSum ID
├── title
└── summary

narrativeqa/metadata.csv
├── book_id               # Our global ID
├── nqa_id                # Original NarrativeQA document.id
├── title
├── author
└── summary

litbank/metadata.csv
├── book_id               # Our global ID
├── litbank_id            # Original filename stem (e.g., "1342_pride_and_prejudice")
├── title
├── author
├── gutenberg_id
├── entities_brat_txt     # → ./datasets/litbank/entities/brat/{id}_{title}_brat.txt
├── entities_brat_ann     # → ./datasets/litbank/entities/brat/{id}_{title}_brat.ann
├── entities_tsv          # → ./datasets/litbank/entities/tsv/{id}_{title}_brat.tsv
├── events_tsv            # → ./datasets/litbank/events/tsv/{id}_{title}_brat.tsv
├── coref_brat_txt        # → ./datasets/litbank/coref/brat/{id}_{title}_brat.txt
├── coref_brat_ann        # → ./datasets/litbank/coref/brat/{id}_{title}_brat.ann
├── coref_conll           # → ./datasets/litbank/coref/conll/{id}_{title}_brat.conll
├── quotations_brat_txt   # → ./datasets/litbank/quotations/brat/{id}_{title}_brat.txt
└── quotations_brat_ann   # → ./datasets/litbank/quotations/brat/{id}_{title}_brat.ann

litbank annotation formats:
├── entities/tsv          # Token per row, columns for entity types (O, B-PER, I-FAC, etc.)
├── events/tsv            # Token per row, second column for EVENT labels (O or EVENT)
├── coref/conll           # CoNLL format for coreference chains
└── *.brat.txt/.brat.ann  # Brat standoff format (entities, coref, quotations)

index.csv
├── book_id               # Our global ID
├── title
├── gutenberg_id
├── text_path             # → ./datasets/texts/{id}_{title}.txt
├── booksum_id            # Original BookSum ID (if in BookSum)
├── booksum_path          # → ./datasets/booksum/metadata.csv
├── nqa_id                # Original NarrativeQA document.id (if in NarrativeQA)
├── nqa_path              # → ./datasets/narrativeqa/metadata.csv
├── litbank_id            # Original LitBank filename stem (if in LitBank)
└── litbank_path          # → ./datasets/litbank/metadata.csv
```


## Setup

LitBank is not available on HuggingFace, and must be manually cloned into the `datasets` folder.
```bash
git clone https://github.com/dbamman/litbank ./datasets/litbank
```

BookSum and NarrativeQA require no external setup.

Run the setup script to clean the `datasets` folder and download / align datasets before the first run.
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

# Align by book title
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

**Cache-first:**
- Download once, load many times
- Faster iteration during development
- No repeated API calls

**Flexible subsets:**
- `download(n=1)` for quick testing
- `download()` for full dataset
