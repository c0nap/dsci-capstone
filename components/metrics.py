
# Keep these imports inside a class so Worker imports dont pull them along


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

    # formerly post_basic_output
    def compute_basic_metrics(self, book_id, book_title, summary, gold_summary, chunk, **kwargs):
        """Compute ROUGE and BERTScore.
        @param book_id  Integer book identifier
        @param book_title  Book title
        @param summary  A text string containing a book summary
        @param gold_summary  A summary to compare against
        @param chunk  The original text of the chunk.
        @param **kwargs  Any other metric parameters to override defaults (e.g., rouge_f1=0.75)
        """
        import evaluate
        from questeval.questeval_metric import QuestEval

        rouge = evaluate.load("rouge")
        bertscore = evaluate.load("bertscore")
        questeval = QuestEval(no_cuda=True)

        rouge_result = rouge.compute(predictions=[summary],
                                     references=[gold_summary])["rougeL"]
        print("Computed ROUGE!")
        bertscore_result = bertscore.compute(predictions=[summary],
                                             references=[gold_summary],
                                             model_type="roberta-large")
        print("Computed BERTScore!")

        return {
            "rouge": rouge_result,
            "bertscore": bertscore_result
        }



    def post_basic_output(book_id, book_title, summary, gold_summary="", chunk="", **kwargs):
        
    
        rouge_result = rouge.compute(predictions=[summary],
                                 references=[gold_summary])["rougeL"]
        
        bertscore_result = bertscore.compute(predictions=[summary],
                                         references=[gold_summary],
                                         model_type="roberta-large")
        
        qeval_score = questeval.corpus_questeval(
            hypothesis=summary, 
            sources=[chunk],
            list_reference=[gold_summary]
        )['corpus_score']
        print("Computed QuestEval!")
        metrics = generate_default_metrics(
            rouge_precision = rouge_result["precision"][0],
            rouge_recall = rouge_result["recall"][0],
            rouge_f1 = rouge_result["f1"][0],
            bert_precision = bertscore_result["precision"][0],
            bert_recall = bertscore_result["recall"][0],
            bert_f1 = bertscore_result["f1"][0],
            questeval_score = qeval_score,
            **kwargs)
        payload = create_summary_payload(book_id, book_title, summary, metrics)
        return post_payload(payload)

    
    
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
    
    
    def create_summary_payload(book_id, book_title, summary, metrics=None):
        """Create the full summary payload for the API"""
        if metrics is None:
            metrics = generate_default_metrics()
    
        return {
            "BookID": str(book_id),
            "BookTitle": book_title,
            "SummaryText": summary,
            "Metrics": metrics,
            "QAResults": [],
        }
    
    
    def post_payload(payload):
        """Verify and post any given payload using the requests API."""
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
    
    
    def post_example_results():
        """Send placeholder values to the web app."""
        return post_basic_output(
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
    
    
    













def run_questeval(chunk_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Run QuestEval metric calculation.
    @details  Question-answering based evaluation. Generates questions from source/reference
              and checks if answers can be found in the summary. Score range: 0-1.
    @param chunk_doc  MongoDB document containing:
                      - text: Source document text (required)
                      - summary: Generated summary (required)
                      - gold_summary: Reference summary (optional)
                      - task: Task type (optional, default 'summarization')
                      - no_cuda: Disable GPU (optional, default False)
    @return  Dict with 'questeval_score', 'ex_level_scores', 'corpus_score', 'has_reference'.
    @throws ImportError  If questeval package not installed.
    @throws KeyError  If required fields missing from chunk_doc.
    """
    from questeval.questeval_metric import QuestEval
    
    questeval = QuestEval(
        task=chunk_doc.get('task', 'summarization'),
        no_cuda=chunk_doc.get('no_cuda', False)
    )
    
    source = chunk_doc['text']
    summary = chunk_doc['summary']
    reference = chunk_doc.get('gold_summary')
    
    if reference:
        result = questeval.corpus_questeval(
            hypothesis=[summary],
            sources=[source],
            list_references=[[reference]]
        )
    else:
        result = questeval.corpus_questeval(
            hypothesis=[summary],
            sources=[source]
        )
    
    return {
        'questeval_score': result.get('ex_level_scores', [0])[0],
        'ex_level_scores': result.get('ex_level_scores', []),
        'corpus_score': result.get('corpus_score', 0),
        'has_reference': reference is not None
    }














def run_bookscore(chunk_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Run BooookScore metric for long-form summarization.
    @details  LLM-based coherence evaluation using BooookScore CLI via subprocess.
              Handles full workflow: scoring summary, postprocessing.
              Can be run on a single chunk or entire book (if already chunked).
              Requires booookscore to be installed: pip install booookscore
    @param chunk_doc  MongoDB document containing:
                      - book_text: Full or partial book text (required)
                      - summary: Generated summary (required)
                      - book_title: Book title for identification (optional, default 'book')
                      - api_key: API key for LLM provider (required)
                      - model: Model name (optional, default 'gpt-4')
                      - api: API provider (optional, default 'openai')
                      - batch_size: Sentences per batch for v2 (optional, default 10)
                      - use_v2: Use batched evaluation (optional, default True)
    @return  Dict with 'bookscore', 'annotations', 'model_used'.
    @throws KeyError  If required fields missing from chunk_doc.
    @throws RuntimeError  If subprocess execution fails.
    """
    import json
    import pickle
    import subprocess
    import tempfile
    import os

    book_text = chunk_doc['book_text']
    summary = chunk_doc['summary']
    book_title = chunk_doc.get('book_title', 'book')
    api_key = chunk_doc['api_key']
    model = chunk_doc.get('model', 'gpt-4')
    api = chunk_doc.get('api', 'openai')
    batch_size = chunk_doc.get('batch_size', 10)
    use_v2 = chunk_doc.get('use_v2', True)

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
            '--api', api,
            '--api_key', key_path
        ]

        if use_v2:
            score_cmd.extend(['--v2', '--batch_size', str(batch_size)])

        try:
            subprocess.run(score_cmd, capture_output=True, text=True, timeout=600, check=True)
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
            'model_used': model
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


