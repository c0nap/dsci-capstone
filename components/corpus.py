from datasets import load_dataset
import re

# --------------------------------------
# BookSum
# --------------------------------------
def load_booksum():
	# full dataset with all subsets
	booksum = load_dataset("kmfoda/booksum")
	
	# or pick a specific split
	train = booksum["train"]
	# val = booksum["validation"]
	# test = booksum["test"]
	print(train[0])
	
	# inspect one sample
	df_booksum = to_df_booksum(booksum)
	return df_booksum


def to_df_booksum(ds):
    return pd.DataFrame({
        "title": [x.get("book_title", "").strip().lower() for x in ds],
        "author": [x.get("book_author", "").strip().lower() for x in ds],
        "booksum_id": [x.get("book_id") for x in ds],
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
	print(train[0])
	
	df_nqa = to_df_nqa(nqa)
	return df_nqa


def to_df_nqa(ds):
    docs = [d["document"] for d in ds]
    return pd.DataFrame({
        "title": [d.get("title", "").strip().lower() for d in docs],
        "author": [d.get("author", "").strip().lower() for d in docs],
        "nqa_id": [d.get("id") for d in docs],
    })



if __name__ == "__main__":
	df_booksum = load_booksum()
	df_nqa = load_narrativeqa()
