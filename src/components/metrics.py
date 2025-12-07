from dotenv import load_dotenv
import os
from typing import Any, Dict, List


"""Contains functions to score a summary or knowledge graph.
    @details
    - Most imports are kept isolated inside functions.
    - Reduces heavy baggage during Worker imports."""


class Metrics:
    """Utility class for posting evaluation metrics to Blazor web application."""

    def __init__(self) -> None:
        load_dotenv(".env")
        ## Where to POST the book chunk payload.
        self.blazor_url: str = self.get_blazor_url()
        ## Number of seconds to wait on Blazor to accept the POST before timing out.
        self.timeout_seconds: float = 60

    def get_blazor_url(self) -> str:
        """Read environment variables to construct the Blazor URL.
        @return  A string URL specifying the network location of Blazor metrics endpoint."""
        blazor_host = os.environ["BLAZOR_HOST"]
        blazor_port = os.environ["BLAZOR_PORT"]
        return f"http://{blazor_host}:{blazor_port}/api/metrics"

    @staticmethod
    def get_metrics_template(
        rouge1_f1: float = 0.0,
        rouge2_f1: float = 0.0,
        rougeL_f1: float = 0.0,
        rougeLsum_f1: float = 0.0,
        bert_precision: float = 0.0,
        bert_recall: float = 0.0,
        bert_f1: float = 0.0,
        booook_score: float = 0.0,
        questeval_score: float = 0.0,
        qa_question1: str = "UNKNOWN",
        qa_gold1: str = "UNKNOWN",
        qa_generated1: str = "UNKNOWN",
        qa_correct1: bool = False,
        qa_accuracy1: float = 0.0,
        qa_question2: str = "UNKNOWN",
        qa_gold2: str = "UNKNOWN",
        qa_generated2: str = "UNKNOWN",
        qa_correct2: bool = False,
        qa_accuracy2: float = 0.0,
    ) -> Dict[str, Any]:
        """Generate the metrics sub-payload with customizable default values.
        @param rouge1_f1 The ROUGE-1 evaluation metric.
        @param rouge2_f1 The ROUGE-2 evaluation metric.
        @param rougeL_f1 The ROUGE-L evaluation metric.
        @param rougeLsum_f1 The ROUGE-Lsum evaluation metric.
        @param bert_precision The BERTScore precision score.
        @param bert_recall The BERTScore recall score.
        @param bert_f1 The BERTScore F1 score.
        @param booook_score The BooookScore evaluation metric.
        @param questeval_score The QuestEval evaluation metric.
        @param qa_question1 A question about the book.
        @param qa_gold1 The correct answer to the question.
        @param qa_generated1 A generated answer to judge.
        @param qa_correct1 Whether our answer is correct.
        @param qa_accuracy1 The accuracy score for this QA sample.
        @param qa_question2 A question about the book.
        @param qa_gold2 The correct answer to the question.
        @param qa_generated2 A generated answer to judge.
        @param qa_correct2 Whether our answer is correct.
        @param qa_accuracy2 The accuracy score for this QA sample.
        @return  Dictionary containing various nested evaluation metrics."""
        return {
            "PRF1Metrics": [
                {
                    "Name": "BERTScore",
                    "Precision": bert_precision,
                    "Recall": bert_recall,
                    "F1Score": bert_f1,
                },
            ],
            "ScalarMetrics": [
                {"Name": "BooookScore (Chang 2024)", "Value": booook_score},
                {"Name": "QuestEval (Scialom 2021)", "Value": questeval_score},
                {"Name": "ROUGE-1", "Value": rouge1_f1},
                {"Name": "ROUGE-2", "Value": rouge2_f1},
                {"Name": "ROUGE-L", "Value": rougeL_f1},
                {"Name": "ROUGE-Lsum", "Value": rougeLsum_f1},
            ],
            "QA": {
                "QAItems": [
                    {
                        "Question": qa_question1,
                        "GoldAnswer": qa_gold1,
                        "GeneratedAnswer": qa_generated1,
                        "IsCorrect": qa_correct1,
                        "Accuracy": qa_accuracy1,
                    },
                    {
                        "Question": qa_question2,
                        "GoldAnswer": qa_gold2,
                        "GeneratedAnswer": qa_generated2,
                        "IsCorrect": qa_correct2,
                        "Accuracy": qa_accuracy2,
                    },
                ]
            },
        }

    @staticmethod
    def get_book_template(book_id: str, book_title: str, summary: str, gold_summary: str, metrics: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create the full Blazor payload for a single book.
        @param book_id  Unique identifier for one book.
        @param book_title  String containing the title of a book.
        @param summary  String containing a book summary.
        @param gold_summary  Optional summary to compare against.
        @param metrics  Dictionary containing various nested evaluation metrics.
        @return  A dictionary with C#-style key names."""
        return {
            "BookID": str(book_id),
            "BookTitle": book_title,
            "SummaryText": summary,
            "GoldSummaryText": gold_summary,
            "Metrics": metrics or Metrics.get_metrics_template(),
            "QAResults": [],
        }

    @staticmethod
    def generate_example() -> Dict[str, Any]:
        """Create a placeholder payload with dummy values.
        @return Full payload with nested metrics."""
        return Metrics.get_book_template(
            "book-42",
            "Example Book",
            "This is an AI-generated summary of the entire book. It captures the key plot points and themes.",
            "No gold-standard summary available.",
            # Override some defaults with example values
            Metrics.get_metrics_template(
                rouge1_f1=0.83,
                rouge2_f1=0.86,
                rougeL_f1=0.89,
                rougeLsum_f1=0.80,
                bert_precision=0.89,
                bert_recall=0.90,
                bert_f1=0.89,
                booook_score=0.76,
                questeval_score=0.81,
                qa_question1="Who is the protagonist?",
                qa_gold1="Alice",
                qa_generated1="Alice",
                qa_correct1=True,
                qa_accuracy1=1.0,
                qa_question2="Where does the story start?",
                qa_gold2="Wonderland",
                qa_generated2="Forest",
                qa_correct2=False,
                qa_accuracy2=0.0,
            ),
        )

    ###################################################################################
    # POST directly to Blazor (soon to be deprecated)
    ###################################################################################
    def post_payload(self, payload: Dict[str, Any]) -> bool:
        """Verify and POST a given payload using the requests API.
        @param payload JSON dictionary containing data for a single book.
        @return Whether the POST operation was successful."""
        import requests

        try:
            print(f"Sending payload to Blazor at {self.blazor_url}")
            response = requests.post(self.blazor_url, json=payload)

            if response.ok:  # handles 200–299
                print("POST succeeded")
                print(response.json())
                return True
            else:
                print(f"POST failed: {response.status_code}")
                print(response.text)
                print(payload)
                return False

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return False

    def post_basic(self, book_id: str, book_title: str, summary: str, gold_summary: str = "", text: str = "", **kwargs: Any) -> None:
        """POST basic evaluation scores to Blazor (ROUGE, BERTScore).
        @param book_id  Unique identifier for one book.
        @param book_title  String containing the title of a book.
        @param summary  String containing a book summary.
        @param gold_summary  Optional summary to compare against.
        @param text  A string containing text from the book.
        @param kwargs  Any additional named arguments will be added to the payload."""
        results = compute_basic(summary, gold_summary, text)
        metrics = Metrics.get_metrics_template(
            rouge1_f1=results["rouge"]["rouge1"],
            rouge2_f1=results["rouge"]["rouge2"],
            rougeL_f1=results["rouge"]["rougeL"],
            rougeLsum_f1=results["rouge"]["rougeLsum"],
            bert_precision=results["bertscore"]["precision"][0],
            bert_recall=results["bertscore"]["recall"][0],
            bert_f1=results["bertscore"]["f1"][0],
            **kwargs,
        )
        payload = Metrics.get_book_template(book_id, book_title, summary, gold_summary, metrics)
        self.post_payload(payload)

    def post_example(self, book_id: str, book_title: str, summary: str) -> None:
        """POST dummy date to Blazor.
        @param book_id  Unique identifier for one book.
        @param book_title  String containing the title of a book.
        @param summary  String containing a book summary."""
        payload = Metrics.generate_example()
        self.post_payload(payload)

    








def compute_basic(summary: str, gold_summary: str, chunk: str) -> Dict[str, Any]:
    """Compute ROUGE and BERTScore to compare against a gold-quality reference summary.
    @param summary  A text string containing a book summary
    @param gold_summary  A summary to compare against
    @param chunk  The original text of the chunk.
    @return  Dict containing 'rouge' and 'bertscore' keys.
        Scores are nested with inconsistent schema."""
    rouge_result = run_rouge(summary, gold_summary)
    bertscore_result = run_bertscore(summary, gold_summary)
    return {"rouge": rouge_result, "bertscore": bertscore_result}


def run_rouge(prediction: str, reference: str) -> Dict[str, float]:
    """Run the ROUGE evaluation metric given one reference and one prediction to judge.
    @param prediction  Text string containing the generated summary.
    @param reference  Text string to compare against.
    @return  ROUGE results directly from 'evaluate' library.
    Values correspond to F1 score since this is the standard ROUGE metric.
    Example schema: { "rouge1": 0.87, ... }
    Valid keys: rouge1, rouge2, rougeL, rougeLsum."""
    import evaluate

    model = evaluate.load("rouge")
    result = model.compute(predictions=[prediction], references=[reference])
    return result


def run_bertscore(prediction: str, reference: str) -> Dict[str, List[float]]:
    """Run the BERTScore evaluation metric given one reference and one prediction to judge.
    @param prediction  Text string containing the generated summary.
    @param reference  Text string to compare against.
    @return  BERTScore results directly from 'evaluate' library.
    Example schema: { "precision": [0.87], ... }
    Valid keys: precision, recall, f1."""
    import evaluate

    model = evaluate.load("bertscore")
    result = model.compute(predictions=[prediction], references=[reference], model_type="roberta-large")
    return result


def run_questeval(
    chunk: Dict[str, Any], *, qeval_task: str = "summarization", use_cuda: bool = False, use_question_weighter: bool = True
) -> Dict[str, Any]:
    """Run QuestEval metric calculation.
    @details  Question-answering based evaluation.
        Generates questions from source/reference, and checks if answers can be found in the summary.
        For more parameters, see: https://github.com/ThomasScialom/QuestEval/blob/main/questeval/questeval_metric.py
    @param chunk  MongoDB document containing keys:
        - summary: Generated summary (required)
        - text: Source document text (required)
        - gold_summary: Reference summary (optional, filters for better questions)
    @param qeval_task  Task performed by QuestEval (optional, default is summarization).
        Must be one of the following: generation / nlg, qa, dialogue, data2text, translation.
    @param use_cuda  Run transformers with GPU enabled.
    @param use_question_weighter  Make some questions more important based on relevancy.
    @return  Dict containing a score (range 0-1) and metadata for the provided summary.
        questeval_score: Overall semantic precision–recall score for one example (a Summary to evaluate, Source text, and Reference summary).
        has_reference: True if a gold reference summary was provided.
    @throws ImportError  If questeval package not installed.
    @throws KeyError  If required fields are missing from chunk.
    """
    from questeval.questeval_metric import QuestEval

    if qeval_task != "summarization":
        use_question_weighter = False

    questeval = QuestEval(
        task=qeval_task,
        no_cuda=not use_cuda,
        do_weighter=use_question_weighter,
    )
    # TODO: Other parameters?
    #   answer_types: Tuple = ('NER', 'NOUN'),
    #   list_scores: Tuple = ('answerability', 'bertscore', 'f1'),
    #   do_consistency: bool = False,
    #   qg_batch_size: int = 36,
    #   clf_batch_size: int = 48,
    #   limit_sent: int = 5,
    #   reduction_multi_refs: Callable = max,
    #   use_cache: bool = True

    src, summ, ref = chunk["text"], chunk["summary"], chunk.get("gold_summary")
    if ref:
        result = questeval.corpus_questeval([summ], [src], [[ref]])
    else:
        result = questeval.corpus_questeval([summ], [src])

    # Unused outputs:
    #   ex_level_scores: List of per-example QuestEval scores.
    #       "ex_level_scores": result.get("ex_level_scores", []),
    #   corpus_score: Mean score across all examples.
    #       "corpus_score": result.get("corpus_score", 0),
    return {
        "value": result.get("ex_level_scores", [0])[0],
        "has_reference": ref is not None,
    }


def run_bookscore(chunk: Dict[str, Any], *, model: str = "gpt-3.5-turbo", batch_size: int = 10, use_v2: bool = True) -> Dict[str, Any]:
    """Run BooookScore metric for long-form summarization.
    @details  LLM-based coherence evaluation using BooookScore. Runs in CLI via subprocess.
        Handles full workflow: scoring summary, postprocessing.
        Can be run on a single chunk or entire book (if already chunked).
    @param chunk  MongoDB document containing:
        - summary: Generated summary (required)
        - text: Full or partial book text (required)
        - book_title: Book title for identification (optional, for pickling)
    @param model  Model name (optional, default 'gpt-4')
    @param batch_size  Sentences per batch for v2 (optional, default 10)
    @param use_v2  Use batched evaluation (optional, default True)
    @return  Dict containing a score (range 0-1) and metadata for the provided summary.
        bookscore: Coherence score for one summary.
        annotations: True if a gold reference summary was provided.
        model_used: String describing the LLM model and API used.
    @throws KeyError  If required fields are missing from chunk.
    @throws RuntimeError  If subprocess execution fails.
    """
    import importlib.util
    import json
    import os
    import pickle
    import subprocess
    import tempfile

    # Find the installed package path
    pkg_path = importlib.util.find_spec("booookscore").submodule_search_locations[0]

    book_text = chunk['text']
    summary = chunk['summary']
    book_title = chunk.get('book_title', 'Unkown Book')  # TODO: convert to arg
    api_key = os.environ["BOOKSCORE_API_KEY"]
    timeout_seconds: float = 300

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1: Write book text as pickle
        books_pkl = os.path.join(tmpdir, 'books.pkl')
        with open(books_pkl, 'wb') as f:
            pickle.dump({book_title: book_text}, f)

        # 2: Write summary to JSON file
        summ_path = os.path.join(tmpdir, 'summary.json')
        with open(summ_path, 'w') as f:
            json.dump({book_title: summary}, f)

        # 3: Write API key to file
        key_path = os.path.join(tmpdir, 'api_key.txt')
        with open(key_path, 'w') as f:
            f.write(api_key)

        # 4: Run BookScore as subprocess
        annot_path = os.path.join(tmpdir, 'annotations.json')
        score_cmd = [
            'python',
            '-m',
            'booookscore.score',
            '--summ_path',
            summ_path,
            '--annot_path',
            annot_path,
            '--model',
            model,
            '--openai_key',
            key_path,
        ]
        if use_v2:
            score_cmd.extend(['--v2', '--batch_size', str(batch_size)])

        try:
            # Run from inside the package so relative paths resolve
            subprocess.run(
                score_cmd,
                cwd=pkg_path,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=True,
                # start_new_session=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"BooookScore scoring failed: {e.stderr}") from e
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"BookScore scoring timed out after {Metrics.timeout_seconds}s") from e

        # 5: Read annotations output (we dont care about their scores)
        if not os.path.exists(annot_path):
            raise RuntimeError("BooookScore did not produce annotations file")

        with open(annot_path, 'r') as f:
            annotations = json.load(f)

        # 6. Compute score from annotations
        book_annot = annotations.get(book_title, {})
        if not isinstance(book_annot, dict):
            raise RuntimeError(f"Invalid annotations: {book_annot}")

        # Based on code from official repo:
        # https://github.com/lilakk/BooookScore/blob/094bf69bc55317b728b4b2bb679c287e0176ae6c/booookscore/score.py#L167
        confusing = 0
        total = len(book_annot)
        for sent, annot in book_annot.items():
            if annot.get("questions") or annot.get("types"):
                confusing += 1
        overall_score = 1 - (confusing / total) if total else 0.0

        return {'value': overall_score, 'annotations': book_annot, 'model_used': f"openai-model_{model}"}


def chunk_bookscore(book_text: str, book_title: str = 'book', chunk_size: int = 2048) -> str:
    """Chunk a book into BooookScore segments.
    @details  Standardizes long-form input into chunks BooookScore can process.
              Creates a temporary directory and writes chunked pickle for later scoring.
              This step can be reused independently for multiple summaries.
    @param book_text  Full book text to be chunked.
    @param book_title  Name or identifier for the book (default 'book').
    @param chunk_size  Maximum chunk size for book text (default 2048).
    @return  Path to chunked pickle file containing BooookScore-ready segments.
    @throws RuntimeError  If BooookScore chunking fails.
    """
    import os
    import pickle
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        # Step 1: Create pickle file with book text
        books_pkl = os.path.join(tmpdir, 'books.pkl')
        with open(books_pkl, 'wb') as f:
            pickle.dump({book_title: book_text}, f)

        # Step 2: Chunk the book
        chunked_pkl = os.path.join(tmpdir, 'books_chunked.pkl')
        chunk_cmd = ['python', '-m', 'booookscore.chunk', '--chunk_size', str(chunk_size), '--input_path', books_pkl, '--output_path', chunked_pkl]

        try:
            subprocess.run(
                chunk_cmd,
                capture_output=True,
                text=True,
                timeout=Metrics.timeout_seconds,
                check=True,
                # start_new_session=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"BooookScore chunking failed: {e.stderr}") from e
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(f"BookScore chunking timed out after {Metrics.timeout_seconds}s") from e

        return chunked_pkl


# -----------------------------------------------------------------------------------
# Fast core metrics
# 1. Surface-level overlap between original chunk and its summary.
# -----------------------------------------------------------------------------------

from typing import Dict, Any, List


def run_rouge_l(summary: str, source: str) -> Dict[str, float]:
    """
    ROUGE-L Recall (Coverage Score)

    @param summary: The model-generated summary text.
    @param source: The original source text being summarized.
    @return: A dictionary containing ROUGE-L recall.
    @details
    ROUGE-L measures longest-common-subsequence overlap.  
    This metric exists to capture **structural and lexical coverage**, acting as the
    baseline sanity-check that the summary is not missing the core backbone of the text.
    """
    from rouge_score import rouge_scorer
    scorer = rouge_scorer.RougeScorer(["rougeL"], use_stemmer=True)
    scores = scorer.score(source, summary)
    return {"rougeL_recall": scores["rougeL"].recall}


def run_bertscore(summary: str, source: str) -> Dict[str, Any]:
    """
    BERTScore embedding similarity

    @param summary: Summary text.
    @param source: Original text.
    @return: The BERTScore dict from `evaluate`.
    @details
    BERTScore measures **semantic similarity**, not lexical overlap.  
    It exists because summaries can be phrased differently while still
    preserving meaning—ROUGE cannot capture that. This protects you against
    false negatives on paraphrased but faithful summaries.
    """
    import evaluate
    model = evaluate.load("bertscore")
    return model.compute(
        predictions=[summary],
        references=[source]
    )


def run_novel_ngrams(summary: str, source: str, n: int = 3) -> Dict[str, float]:
    """
    Novel n-gram Percentage

    @param summary: Summary text.
    @param source: Source text.
    @param n: N-gram length (default 3).
    @return: Ratio of novel n-grams.
    @details
    Novel n-grams quantify how much of the summary is **newly written** rather than
    lifted lexically from the source.  
    This exists because high abstraction is good, but high novelty can indicate:
      - unwanted hallucination  
      - off-topic writing  
    It complements ROUGE (overlap) by measuring *invention*.
    """
    from nltk import ngrams, word_tokenize

    src_tokens = word_tokenize(source.lower())
    sum_tokens = word_tokenize(summary.lower())

    src_set = set(ngrams(src_tokens, n))
    sum_list = list(ngrams(sum_tokens, n))

    novel = sum(1 for g in sum_list if g not in src_set)
    pct = novel / (len(sum_list) or 1)
    return {"novel_ngram_pct": pct}


def run_ncd(summary: str, source: str) -> Dict[str, float]:
    """
    Normalized Compression Distance (NCD)

    @param summary: Summary text.
    @param source: Source text.
    @return: NCD score (placeholder until logic added).
    @details
    NCD uses compression to estimate **universal similarity**, independent of tokens
    or embeddings.  
    It exists because it can detect similarity even when:
      - wording is heavily paraphrased  
      - structure changes  
      - there is low lexical overlap  
    This complements both ROUGE and BERTScore.
    """
    # Placeholder — you said no new logic yet
    return {"ncd": 0.0}


# -----------------------------------------------------------------------------------
# Fast core metrics
# 2. High-level comparison between original chunk and its summary.
# -----------------------------------------------------------------------------------

def run_entity_coverage(summary: str, source: str) -> Dict[str, float]:
    """
    Entity Coverage & Hallucination (spaCy)

    @param summary: Summary text.
    @param source: Source text.
    @return: Coverage and hallucination ratios.
    @details
    This metric checks **who/what appears in the text**:
      - Coverage = Are key characters/places preserved?
      - Hallucination = Did the summary invent new entities?
    This exists because summaries can be semantically similar (BERTScore)
    while still **dropping crucial actors** or **inventing new ones**.
    """
    import spacy
    nlp = spacy.load("en_core_web_sm")

    src = nlp(source)
    summ = nlp(summary)

    src_ents = {e.text.lower() for e in src.ents}
    sum_ents = {e.text.lower() for e in summ.ents}

    coverage = len(src_ents & sum_ents) / (len(src_ents) or 1)
    halluc = len(sum_ents - src_ents) / (len(sum_ents) or 1)

    return {"entity_coverage": coverage, "entity_hallucination": halluc}


def run_readability_delta(summary: str, source: str) -> Dict[str, float]:
    """
    Readability Delta (textstats)

    @param summary: Summary text.
    @param source: Source text.
    @return: Difference in Flesch-Kincaid grade.
    @details
    This metric exists because summaries should generally be **simpler** than
    their sources. A huge negative shift = oversimplification; no shift =
    overly extractive.
    """
    import textstats
    fk_source = textstats.flesch_kincaid_grade(source)
    fk_summary = textstats.flesch_kincaid_grade(summary)
    return {"readability_delta": fk_source - fk_summary}


def run_jsd_distribution(summary: str, source: str) -> Dict[str, float]:
    """
    Jensen-Shannon Divergence (JSD)

    @param summary: Summary text.
    @param source: Source text.
    @return: JSD value between the two token distributions.
    @details
    JSD measures **distributional drift** — whether the summary is using
    different kinds of words (topics, style, frequency profile).  
    This exists because even if meaning is similar (BERTScore), the *lexical
    signal* can shift, revealing stylistic mismatch or omissions.
    """
    from collections import Counter
    import numpy as np
    from scipy.stats import entropy

    def jsd(p: np.ndarray, q: np.ndarray) -> float:
        p = p / p.sum()
        q = q / q.sum()
        m = 0.5 * (p + q)
        return 0.5 * (entropy(p, m) + entropy(q, m))

    src_counts = Counter(source.lower().split())
    sum_counts = Counter(summary.lower().split())

    vocab = list(set(src_counts) | set(sum_counts))
    p = np.array([src_counts[w] for w in vocab], dtype=float)
    q = np.array([sum_counts[w] for w in vocab], dtype=float)

    return {"jsd": jsd(p, q)}


def run_salience_recall(summary: str, source: str) -> Dict[str, float]:
    """
    TF-IDF Salience Recall

    @param summary: Summary text.
    @param source: Source text.
    @return: Fraction of top-salience words preserved.
    @details
    Unlike BERTScore (semantic) or ROUGE (lexical), salience recall targets
    **rare-but-important words** by ranking terms using TF-IDF.

    This exists because summaries can preserve general meaning while still
    *dropping precise, important anchors* (e.g., names, unique objects,
    specific locations, magical items, plot-significant vocabulary in fiction).
    """
    # Placeholder — no extra business logic added
    return {"salience_recall": 0.0}


def run_nli_faithfulness(summary: str, source: str) -> Dict[str, float]:
    """
    NLI-based Faithfulness Score

    @param summary: Summary text.
    @param source: Source text.
    @return: Placeholder contradiction/entailment metrics.
    @details
    NLI detects:
      - Entailment: The summary is supported by the text.
      - Neutral: The summary adds unverifiable info.
      - Contradiction: The summary states something *false*.

    This metric exists because:
      - Entity metrics detect invented nouns,  
      - Novel n-grams detect invented phrasing,  
      - BUT NLI detects invented **claims**.

    It is your “lie detector.”
    """
    # Placeholder — no extra business logic added
    return {"nli_faithfulness": 0.0}
