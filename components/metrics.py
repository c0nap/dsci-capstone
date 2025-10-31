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
    def compute_basic_metrics(summary, gold_summary, chunk) -> Dict[str, Any]:
        """Compute ROUGE and BERTScore.
        @param summary  A text string containing a book summary
        @param gold_summary  A summary to compare against
        @param chunk  The original text of the chunk.
        @return  Dict containing 'rouge' and 'bertscore' keys.
            Scores are nested in inconsistent schema."""
        import evaluate
        rouge = evaluate.load("rouge")
        bertscore = evaluate.load("bertscore")

        rouge_result = rouge.compute(predictions=[summary],
                                     references=[gold_summary],
                                     use_aggregator=False)
        bertscore_result = bertscore.compute(predictions=[summary],
                                             references=[gold_summary],
                                             model_type="roberta-large")
        return {
            "rouge": rouge_result,
            "bertscore": bertscore_result
        }



    def post_basic_metrics(self, book_id, book_title, summary, gold_summary="", chunk="", **kwargs):
        results = Metrics.compute_basic_metrics(summary, gold_summary, chunk)
        metrics = Metrics.generate_default_metrics(
            rouge1_precision = results["rouge"]["rouge1"]["precision"],
            rouge1_recall = results["rouge"]["rouge1"]["recall"],
            rouge1_f1 = results["rouge"]["rouge1"]["fmeasure"],

            rouge2_precision = results["rouge"]["rouge2"]["precision"],
            rouge2_recall = results["rouge"]["rouge2"]["recall"],
            rouge2_f1 = results["rouge"]["rouge2"]["fmeasure"],

            rougeL_precision = results["rouge"]["rougeL"]["precision"],
            rougeL_recall = results["rouge"]["rougeL"]["recall"],
            rougeL_f1 = results["rouge"]["rougeL"]["fmeasure"],

            rougeLsum_precision = results["rouge"]["rougeLsum"]["precision"],
            rougeLsum_recall = results["rouge"]["rougeLsum"]["recall"],
            rougeLsum_f1 = results["rouge"]["rougeLsum"]["fmeasure"],

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
        rouge1_precision=0.0,
        rouge1_recall=0.0,
        rouge1_f1=0.0,
        rouge2_precision=0.0,
        rouge2_recall=0.0,
        rouge2_f1=0.0,
        rougeL_precision=0.0,
        rougeL_recall=0.0,
        rougeL_f1=0.0,
        rougeLsum_precision=0.0,
        rougeLsum_recall=0.0,
        rougeLsum_f1=0.0,
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
                    "Name": "ROUGE-1",
                    "Precision": rouge1_precision,
                    "Recall": rouge1_recall,
                    "F1Score": rouge1_f1,
                },
                {
                    "Name": "ROUGE-2",
                    "Precision": rouge2_precision,
                    "Recall": rouge2_recall,
                    "F1Score": rouge2_f1,
                },
                {
                    "Name": "ROUGE-L",
                    "Precision": rougeL_precision,
                    "Recall": rougeL_recall,
                    "F1Score": rougeL_f1,
                },
                {
                    "Name": "ROUGE-Lsum",
                    "Precision": rougeLsum_precision,
                    "Recall": rougeLsum_recall,
                    "F1Score": rougeLsum_f1,
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
            rouge1_precision=0.81,
            rouge1_recall=0.82,
            rouge1_f1=0.83,
            rouge2_precision=0.84,
            rouge2_recall=0.85,
            rouge2_f1=0.86,
            rougeL_precision=0.87,
            rougeL_recall=0.88,
            rougeL_f1=0.89,
            rougeLsum_precision=0.80,
            rougeLsum_recall=0.80,
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
    model: str = "gpt-3.5-turbo",
    batch_size: int = 10, use_v2: bool = True) -> Dict[str, Any]:
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
    api_key = os.environ["BOOKSCORE_API_KEY"]

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
            subprocess.run(
                score_cmd,
                cwd=pkg_path,
                capture_output=True,
                text=True,
                timeout=300,
                check=True,
                #start_new_session=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"BooookScore scoring failed: {e.stderr}") from e
        except subprocess.TimeoutExpired as e:
            raise RuntimeError("BookScore scoring timed out after 300s") from e

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
    import tempfile
    import subprocess
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
            subprocess.run(
                chunk_cmd,
                capture_output=True,
                text=True,
                timeout=300,
                check=True,
                #start_new_session=True
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"BooookScore chunking failed: {e.stderr}") from e
        except subprocess.TimeoutExpired as e:
            raise RuntimeError("BookScore chunking timed out after 300s") from e

        return chunked_pkl


