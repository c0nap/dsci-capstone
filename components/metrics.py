from dotenv import load_dotenv
import os
import requests
import evaluate
from questeval.questeval_metric import QuestEval

bertscore = evaluate.load("bertscore")
rouge = evaluate.load("rouge")
questeval = QuestEval(no_cuda=True)

# Read environment variables at compile time
load_dotenv(".env")

# REST API endpoint
HOST = os.getenv(f"BLAZOR_HOST")
url = f"http://{HOST}:5055/api/metrics"


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
        print(f"Sending payload to Blazor at {url}")
        response = requests.post(url, json=payload)

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


def post_basic_output(book_id, book_title, summary, gold_summary="", chunk="", **kwargs):
    """Send book information and a summary to the web app.
    @param book_id  Integer book identifier
    @param book_title  Book title
    @param summary  A text string containing a book summary
    @param summary  A summary to compare against
    @param **kwargs  Any other metric parameters to override defaults (e.g., rouge_f1=0.75)
    """

    rouge_result = rouge.compute(predictions=[summary],
                             references=[gold_summary])["rougeL"]
    print("Computed ROUGE!")
    bertscore_result = bertscore.compute(predictions=[summary],
                                     references=[gold_summary],
                                     model_type="roberta-large")
    print("Computed BERTScore!")
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
