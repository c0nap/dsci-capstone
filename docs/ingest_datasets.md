# Dataset Loading System

## Core Concept
Separate **downloading** (fetch and cache) from **loading** (read from cache).

## File Structure

```
ingest.py
├── DatasetLoader (ABC)               # Base class and ID manager
├── BookSumLoader                     # HuggingFace dataset
├── NarrativeQALoader                 # HuggingFace dataset  
├── LitBankLoader                     # Local repo
└── Alignment functions               # fuzzy_merge, exact_merge, TODO

./datasets/
├── texts/
│   ├── 00001_moby_dick.txt          # Global incrementing IDs
│   ├── 00002_pride_and_prejudice.txt
│   ├── 00003_adventures_of_sherlock_holmes.txt
│   └── ...
├── booksum/
│   └── metadata.csv                     # book_id, title, summary, text_path
├── narrativeqa/
│   └── metadata.csv                     # book_id, title, author, nqa_id, text_path, summary_path
└── litbank/
    └── metadata.csv                  # book_id, title, author, text_path
```


TODO: 3 components in LitBank - event annotation, dialogue annotationm, coreference annotation
litbank/original/x.txt - full text of each book
litbank/events/tsv/x.tsv - each row is a token, and the 2nd column says O normally, or EVENT if event
litbank/entities/tsv/x.tsv - each row is a token, and several other columns saying the type of entity (B-PER I-FAC etc)
litbank/quotations/tsv/x.tsv - each row is a token, and several other columns saying the type of entity (B-PER I-FAC etc)

```
Quotation annotations
Annotations are contained in .tsv files, which have two components (quote annotations and speaker attribution annotations); each type has its own structure:

Label   Quote ID    Start sentence ID   Start token ID (within sentence)    End sentence ID End token ID (within sentence)  Quotation
QUOTE   Q342    54  0   54  13  “ Of course , we ’ll take over your furniture , mother , ”
Label   Quote ID    Speaker ID
ATTRIB  Q342    Winnie_Verloc-3
Sentence IDs correspond to the row of the sentence in the corresponding .txt file; token IDs correspond to index of the token within that sentence.
```


## Data Flow

### 1. First Time (Download)
```python
loader = BookSumLoader(cache_dir="./datasets/booksum")
loader.download(n=10)  # Fetch 10 books from HuggingFace, save to cache
```

**What happens:**
- Fetches from HuggingFace API
- Normalizes rows via `_normalize_row()`
- Saves as CSV to `./datasets/booksum/train.csv`

### 2. Subsequent Times (Load)
```python
loader = BookSumLoader(cache_dir="./datasets/booksum")
df = loader.load()  # Reads from cache, no download
```

**What happens:**
- Checks if `./datasets/booksum/train.csv` exists
- If yes: reads CSV with pandas `read_csv()`
- If no: falls back to download from HuggingFace

## Key Classes

### DatasetLoader (ABC)
```python
def _calculate_subset_size(total, n, fraction) -> int
    # Helper: converts n/fraction to actual number
    # Used by all loaders to handle download(n=10) or download(fraction=0.1)
```





### BookSumLoader / NarrativeQALoader
**HuggingFace datasets - same pattern:**
- `download()` - Fetches via `load_dataset()`, saves CSV
- `load()` - Reads CSV cache or fetches if missing
- `_normalize_row()` - Maps HF format to common schema

**Cache location:** `{cache_dir}/{split}.csv`

### LitBankLoader
**Local repo with API metadata - different pattern:**

1. **download()** - Processes local files:
   - Reads text from `{repo_path}/entities/brat/*.txt`
   - Saves text to `{text_output_dir}/{gutenberg_id}_{title}.txt`
   - Fetches metadata from Gutendex API
   - Saves metadata to `{text_output_dir}/metadata.csv`

2. **load()** - Reads metadata CSV cache

**Why separate text files?**
- DataFrame only stores paths, not full text (memory efficient)
- Use `load_text_from_path()` to read when needed

**Cache location:** `{text_output_dir}/metadata.csv`

## Alignment Functions

**Module-level functions (not methods):**
- `fuzzy_merge(df1, df2, ...)` - RapidFuzz string matching
- `exact_merge(df1, df2, ...)` - Exact key matching after normalization
- `normalize_title(t)` - Remove punctuation, lowercase
- `load_text_from_path(path)` - Read saved text file

## Usage Pattern

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
- Helper in ABC avoids code duplication