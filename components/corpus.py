from datasets import load_dataset  # type: ignore
import pandas as pd
from rapidfuzz import fuzz, process
import re


# --------------------------------------
# BookSum
# --------------------------------------
def load_booksum():
    # full dataset with all subsets
    # booksum = load_dataset("kmfoda/booksum")
    # booksum = load_dataset(
    #     "json",
    #     data_files="./datasets/booksum/booksum-data/book-level/book_summaries.json"
    # )
    booksum = load_dataset("ubaada/booksum-complete-cleaned", "books")
    # chapters_ds = load_dataset("ubaada/booksum-complete-cleaned", "chapters")

    # or pick a specific split
    train = booksum["train"]
    # val = booksum["validation"]
    # test = booksum["test"]

    # inspect one sample
    print(train.column_names)
    df_booksum = to_df_booksum(booksum)
    # df_booksum = df_booksum[df_booksum["summary_type"] == "book"]
    return df_booksum


def to_df_booksum(ds):
    from datasets import DatasetDict  # type: ignore
    import pandas as pd
    import re

    # If user passed the full DatasetDict, use its "train" split
    if isinstance(ds, DatasetDict):
        ds = ds["train"]

    def clean(s):
        if not isinstance(s, str):
            return ""
        s = s.lower().strip()
        s = re.sub(r"[\W_]+", " ", s)
        return s

    return pd.DataFrame({"title": [clean(t) for t in ds["title"]], "text": [t for t in ds["text"]], "summary": [s for s in ds["summary"]]})


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
    from datasets import DatasetDict  # type: ignore
    import pandas as pd

    # Handle DatasetDict (e.g., from load_dataset without ["train"])
    if isinstance(ds, DatasetDict):
        ds = ds["train"]

    docs = ds["document"]
    return pd.DataFrame(
        {
            "title": [d.get("title", "").strip().lower() for d in docs],
            "author": [d.get("author", "").strip().lower() for d in docs],
            "nqa_id": [d.get("id") for d in docs],
        }
    )


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
    return pd.merge(df1, df2, on=key_columns, how="inner", suffixes=(suffix1, suffix2))


def fuzzy_merge_titles(df1, df2, suffix1, suffix2, key="title", threshold=90, scorer=fuzz.token_sort_ratio):
    """Perform a two-way fuzzy merge between two DataFrames on a text column (e.g., book titles).
    @details
    For each row in the left DataFrame, the function searches the right DataFrame for the most
    similar string in the specified key column using RapidFuzz. It returns a merged DataFrame
    containing the best matches above a similarity threshold.
    @param df1  The left-hand DataFrame containing a text column to match on.
    @param df2  The right-hand DataFrame containing a text column to match against.
    @param suffix1  The name of the left-hand column.
    @param suffix2  The name of the right-hand column.
    @param key  The name of the column containing the strings to compare (default: "title").
    @param threshold  Minimum similarity score (0–100) required to consider a match valid. Defaults to 90.
    @param scorer  A RapidFuzz scoring function such as `fuzz.token_sort_ratio` or `fuzz.token_set_ratio`.
    @return  A new pandas DataFrame containing the compared strings, score, and all other columns.
    @note
        This function performs a one-to-one best match per left row.
        To ensure only confident matches are kept, adjust the `threshold` parameter.
    """
    matches = []

    # Precompute list of right-hand titles for performance
    right_titles = df2[key].tolist()

    for _, row in df1.iterrows():
        title = row[key]
        best = process.extractOne(title, right_titles, scorer=scorer)
        if best and best[1] >= threshold:
            best_title, score, idx = best
            right_row = df2.iloc[idx]

            merged_row = {
                f"{key}{suffix1}": title,
                f"{key}{suffix2}": best_title,
                "score": score,
                **{f"{col}{suffix1}": row[col] for col in df1.columns if col != key},
                **{f"{col}{suffix2}": right_row[col] for col in df2.columns if col != key},
            }
            matches.append(merged_row)

    return pd.DataFrame(matches)





from components.metrics import Metrics


if __name__ == "__main__":
    df_booksum = load_booksum()
    df_nqa = load_narrativeqa()
    # df = merge_dataframes(df_booksum, df_nqa, "_booksum", "_nqa", ["title"])
    df = fuzzy_merge_titles(df_booksum, df_nqa, "_booksum", "_nqa", key="title", threshold=70)

    df_booksum.to_csv("./datasets/metrics/booksum.csv", index=False)
    df_nqa.to_csv("./datasets/metrics/nqa.csv", index=False)
    df.to_csv("./datasets/metrics/merged.csv", index=False)

    print(f"BookSum rows: {len(df_booksum)}")
    print(f"NarrativeQA rows: {len(df_nqa)}")
    print(f"Fuzzy matches: {len(df)}")

    m = Metrics()
    m.post_basic_metrics("1", df.loc[0, 'title_nqa'], df.loc[0, 'summary_booksum'], df.loc[0, 'summary_booksum'])
