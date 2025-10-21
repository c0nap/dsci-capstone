from datasets import load_dataset
import re
import pandas as pd

# --------------------------------------
# BookSum
# --------------------------------------
def load_booksum():
	# full dataset with all subsets
	#booksum = load_dataset("kmfoda/booksum")
	# booksum = load_dataset(
	#     "json",
	#     data_files="./datasets/booksum/booksum-data/book-level/book_summaries.json"
	# )
	booksum = load_dataset("ubaada/booksum-complete-cleaned", "books")
	#chapters_ds = load_dataset("ubaada/booksum-complete-cleaned", "chapters")


	# or pick a specific split
	train = booksum["train"]
	# val = booksum["validation"]
	# test = booksum["test"]
	
	# inspect one sample
	print(train.column_names)
	df_booksum = to_df_booksum(booksum)
	#df_booksum = df_booksum[df_booksum["summary_type"] == "book"]
	return df_booksum


def to_df_booksum(ds):
    import re
    import pandas as pd
    from datasets import DatasetDict

    # If user passed the full DatasetDict, use its "train" split
    if isinstance(ds, DatasetDict):
        ds = ds["train"]

    def clean(s):
        if not isinstance(s, str):
            return ""
        s = s.lower().strip()
        s = re.sub(r"[\W_]+", " ", s)
        return s

    return pd.DataFrame({
        "title": [clean(t) for t in ds["title"]],
        "text": [t for t in ds["text"]],
        "summary": [s for s in ds["summary"]]
    })





# --------------------------------------
# NarrativeQA
# --------------------------------------
def load_narrativeqa():
	nqa = load_dataset("narrativeqa")
	
	# or load a specific split
	train = nqa["train"]
	# val = nqa["validation"]
	# test = nqa["test"]
	
	print(train.column_names)
	df_nqa = to_df_nqa(nqa)
	return df_nqa


def to_df_nqa(ds):
    import pandas as pd
    from datasets import DatasetDict

    # Handle DatasetDict (e.g., from load_dataset without ["train"])
    if isinstance(ds, DatasetDict):
        ds = ds["train"]

    docs = ds["document"]
    return pd.DataFrame({
        "title": [d.get("title", "").strip().lower() for d in docs],
        "author": [d.get("author", "").strip().lower() for d in docs],
        "nqa_id": [d.get("id") for d in docs],
    })





# --------------------------------------
# Cross-Reference
# --------------------------------------
def normalize_title(t):
    t = re.sub(r"[\W_]+", " ", t)  # remove punctuation
    return t.strip()

def merge_dataframes(df1, df2, suffix1, suffix2, key_columns):
	for key in key_columns:
		df1[key] = df1[key].map(normalize_title)
		df1[key] = df1[key].map(normalize_title)
	merged = pd.merge(
	    df1, df2,
	    on=key_columns,
	    how="inner",
	    suffixes=(suffix1, suffix2)
	)
	print(f"Exact matches: {len(merged)}")





# git clone https://github.com/salesforce/booksum.git datasets/booksum
if __name__ == "__main__":
	df_booksum = load_booksum()
	df_nqa = load_narrativeqa()
	merge_dataframes(df_booksum, df_nqa, "_booksum", "_nqa", ["title"])
