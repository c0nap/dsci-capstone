from typing import Dict, Any
# Keep most imports inside a class method, dont pull them along during Worker imports

class Metrics:
    """Utility class for computing and posting evaluation metrics."""

    def __init__(self):
        from dotenv import load_dotenv
        import os

        # Read environment variables
        load_dotenv(".env")
        self.HOST = os.getenv("BLAZOR_HOST")
        self.PORT = os.getenv("BLAZOR_PORT")
        self.url = f"http://{self.HOST}:{self.PORT}/api/metrics"


    @staticmethod
    def compute_basic_metrics(summary, gold_summary, chunk):
        """Compute ROUGE and BERTScore.
        @param summary  A text string containing a book summary
        @param gold_summary  A summary to compare against
        @param chunk  The original text of the chunk."""
        import evaluate
        rouge = evaluate.load("rouge")
        bertscore = evaluate.load("bertscore")

        rouge_result = rouge.compute(predictions=[summary],
                                     references=[gold_summary])["rougeL"]
        bertscore_result = bertscore.compute(predictions=[summary],
                                             references=[gold_summary],
                                             model_type="roberta-large")
        return {
            "rouge": rouge_result,
            "bertscore": bertscore_result
        }



    def post_basic_metrics(self, book_id, book_title, summary, gold_summary="", chunk="", **kwargs):
        results = Metrics.compute_basic_metrics(book_id, book_title, summary, gold_summary, chunk, **kwargs)
        metrics = Metrics.generate_default_metrics(
            rouge_precision = results["rouge"]["precision"][0],
            rouge_recall = results["rouge"]["recall"][0],
            rouge_f1 = results["rouge"]["f1"][0],
            bert_precision = results["bertscore"]["precision"][0],
            bert_recall = results["bertscore"]["recall"][0],
            bert_f1 = results["bertscore"]["f1"][0],
            **kwargs)
        payload = Metrics.create_summary_payload(book_id, book_title, summary, metrics)
        self.post_payload(payload)


    def post_basic_output(self, book_id, book_title, summary):
        metrics = Metrics.generate_default_metrics()
        payload = Metrics.create_summary_payload(book_id, book_title, summary, metrics)
        self.post_payload(payload)
    
    
    @staticmethod
    def generate_default_metrics(
        rouge_precision=0.0,
        rouge_recall=0.0,
        rouge_f1=0.0,
        bert_precision=0.0,
        bert_recall=0.0,
        bert_f1=0.0,
        booook_score=0.0,
        questeval_score=0.0,
        qa_question1="UNKNOWN",
        qa_gold1="UNKNOWN",
        qa_generated1="UNKNOWN",
        qa_correct1=False,
        qa_accuracy1=0.0,
        qa_question2="UNKNOWN",
        qa_gold2="UNKNOWN",
        qa_generated2="UNKNOWN",
        qa_correct2=False,
        qa_accuracy2=0.0,
    ):
        """Generate metrics payload with customizable default values"""
        return {
            "PRF1Metrics": [
                {
                    "Name": "ROUGE",
                    "Precision": rouge_precision,
                    "Recall": rouge_recall,
                    "F1Score": rouge_f1,
                },
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
    def create_summary_payload(book_id, book_title, summary, metrics=None):
        """Create the full summary payload for the API"""
        if metrics is None:
            metrics = Metrics.generate_default_metrics()
    
        return {
            "BookID": str(book_id),
            "BookTitle": book_title,
            "SummaryText": summary,
            "Metrics": metrics,
            "QAResults": [],
        }
    
    
    def post_payload(self, payload):
        """Verify and post any given payload using the requests API."""
        import requests
        try:
            print(f"Sending payload to Blazor at {self.url}")
            response = requests.post(self.url, json=payload)
    
            if response.ok:  # handles 200–299
                print("POST succeeded")
                print(response.json())
                return True
            else:
                print(f"POST failed: {response.status_code}")
                print(response.text)
                return False
    
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return False
    
    
    @staticmethod
    def generate_example_metrics():
        """Send placeholder values to the web app."""
        return Metrics.generate_default_metrics(
            "book-42",
            "Example Book",
            "This is an AI-generated summary of the entire book. It captures the key plot points and themes.",
            # Override some defaults with example values
            rouge_precision=0.80,
            rouge_recall=0.85,
            rouge_f1=0.82,
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
        )
    
    
    













def run_questeval(chunk: Dict[str, Any], *, qeval_task: str = "summarization", use_cuda = False, use_question_weighter = True) -> Dict[str, Any]:
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
        task = qeval_task,
        no_cuda = not use_cuda,
        do_weighter = use_question_weighter,
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
        "questeval_score": result.get("ex_level_scores", [0])[0],
        "has_reference": ref is not None,
    }











def run_bookscore(chunk: Dict[str, Any], *,
    api_key: str = None, model: str = "gpt-4",
    batch_size: int = 10, use_v2: bool = True) -> Dict[str, Any]:
    """Run BooookScore metric for long-form summarization.
    @details  LLM-based coherence evaluation using BooookScore. Runs in CLI via subprocess.
        Handles full workflow: scoring summary, postprocessing.
        Can be run on a single chunk or entire book (if already chunked).
    @param chunk  MongoDB document containing:
        - summary: Generated summary (required)
        - text: Full or partial book text (required)
        - book_title: Book title for identification (optional, for pickling)
    @param api_key  API key for LLM provider (required)
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
    import json
    import pickle
    import subprocess
    import tempfile
    import os
    import importlib.util

    # Find the installed package path
    pkg_path = importlib.util.find_spec("booookscore").submodule_search_locations[0]

    book_text = chunk['text']
    summary = chunk['summary']
    book_title = chunk.get('book_title', 'Unkown Book')  # TODO: convert to arg
    if api_key is None:
        raise RuntimeError("You must provide an API key to use BookScore!")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Step 1: Write book text as pickle
        books_pkl = os.path.join(tmpdir, 'books.pkl')
        with open(books_pkl, 'wb') as f:
            pickle.dump({book_title: book_text}, f)

        # Step 2: Write summary to JSON file
        summ_path = os.path.join(tmpdir, 'summary.json')
        with open(summ_path, 'w') as f:
            json.dump({book_title: summary}, f)

        # Step 3: Write API key to file
        key_path = os.path.join(tmpdir, 'api_key.txt')
        with open(key_path, 'w') as f:
            f.write(api_key)

        # Step 4: Score the summary
        annot_path = os.path.join(tmpdir, 'annotations.json')
        score_cmd = [
            'python', '-m', 'booookscore.score',
            '--summ_path', summ_path,
            '--annot_path', annot_path,
            '--model', model,
            '--openai_key', key_path
        ]
        if use_v2:
            score_cmd.extend(['--v2', '--batch_size', str(batch_size)])

        try:
            # Run from inside the package so relative paths resolve
            subprocess.run(score_cmd, cwd=pkg_path, capture_output=True, text=True, timeout=600, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"BooookScore scoring failed: {e.stderr}") from e
        except subprocess.TimeoutExpired as e:
            raise RuntimeError("BooookScore scoring timed out after 600s") from e

        # Step 5: Read annotations output
        if not os.path.exists(annot_path):
            raise RuntimeError("BooookScore did not produce annotations file")

        with open(annot_path, 'r') as f:
            annotations = json.load(f)

        # Extract scores from annotations
        book_annot = annotations.get(book_title, {})
        sentence_scores = []

        if isinstance(book_annot, dict):
            if 'sentence_scores' in book_annot:
                sentence_scores = book_annot['sentence_scores']
            elif 'scores' in book_annot:
                sentence_scores = book_annot['scores']

        overall_score = sum(sentence_scores) / len(sentence_scores) if sentence_scores else 0.0

        return {
            'bookscore': overall_score,
            'annotations': book_annot,
            'model_used': f"openai-model_{model}"
        }


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
    import pickle
    import subprocess
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmpdir:
        # Step 1: Create pickle file with book text
        books_pkl = os.path.join(tmpdir, 'books.pkl')
        with open(books_pkl, 'wb') as f:
            pickle.dump({book_title: book_text}, f)

        # Step 2: Chunk the book
        chunked_pkl = os.path.join(tmpdir, 'books_chunked.pkl')
        chunk_cmd = [
            'python', '-m', 'booookscore.chunk',
            '--chunk_size', str(chunk_size),
            '--input_path', books_pkl,
            '--output_path', chunked_pkl
        ]

        try:
            subprocess.run(chunk_cmd, capture_output=True, text=True, timeout=60, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"BooookScore chunking failed: {e.stderr}") from e

        return chunked_pkl


