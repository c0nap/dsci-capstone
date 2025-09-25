import requests

# REST API endpoint
url = "http://172.30.48.1:5055/api/metrics"

def generate_default_metrics(
    rouge_precision=0.0, rouge_recall=0.0, rouge_f1=0.0,
    bert_precision=0.0, bert_recall=0.0, bert_f1=0.0,
    booook_score=0.0, questeval_score=0.0,
    qa_question1="UNKNOWN", qa_gold1="UNKNOWN", qa_generated1="UNKNOWN", qa_correct1=False, qa_accuracy1=0.0,
    qa_question2="UNKNOWN", qa_gold2="UNKNOWN", qa_generated2="UNKNOWN", qa_correct2=False, qa_accuracy2=0.0
):
    """Generate metrics payload with customizable default values"""
    return {
        "PRF1Metrics": [
            {"Name": "ROUGE", "Precision": rouge_precision, "Recall": rouge_recall, "F1Score": rouge_f1},
            {"Name": "BERTScore", "Precision": bert_precision, "Recall": bert_recall, "F1Score": bert_f1}
        ],
        "ScalarMetrics": [
            {"Name": "BooookScore (Chang 2024)", "Value": booook_score},
            {"Name": "QuestEval (Scialom 2021)", "Value": questeval_score}
        ],
        "QA": {
            "QAItems": [
                {"Question": qa_question1, "GoldAnswer": qa_gold1, "GeneratedAnswer": qa_generated1, "IsCorrect": qa_correct1, "Accuracy": qa_accuracy1},
                {"Question": qa_question2, "GoldAnswer": qa_gold2, "GeneratedAnswer": qa_generated2, "IsCorrect": qa_correct2, "Accuracy": qa_accuracy2}
            ]
        }
    }

def create_summary_payload(book_id, book_title, summary, metrics=None):
    """Create the full summary payload for the API"""
    if metrics is None:
        metrics = generate_default_metrics()
    
    return {
        "BookID": book_id,
        "BookTitle": book_title,
        "SummaryText": summary,
        "Metrics": metrics,
        "QAResults": []
    }

def post_payload(payload):
    """Verify and post any given payload to the API"""
    try:
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
    """Post example results using default parameter values"""
    return post_basic_output(
        "book-42", 
        "Example Book", 
        "This is an AI-generated summary of the entire book. It captures the key plot points and themes.",
        # Override some defaults with example values
        rouge_precision=0.80, rouge_recall=0.85, rouge_f1=0.82,
        bert_precision=0.89, bert_recall=0.90, bert_f1=0.89,
        booook_score=0.76, questeval_score=0.81,
        qa_question1="Who is the protagonist?", qa_gold1="Alice", qa_generated1="Alice", qa_correct1=True, qa_accuracy1=1.0,
        qa_question2="Where does the story start?", qa_gold2="Wonderland", qa_generated2="Forest", qa_correct2=False, qa_accuracy2=0.0
    )

def post_basic_output(book_id, book_title, summary, **kwargs):
    """Post basic output with customizable default metrics to the API
    
    Args:
        book_id: Book identifier
        book_title: Book title
        summary: Summary text
        **kwargs: Any metric parameters to override defaults (e.g., rouge_f1=0.75)
    """
    metrics = generate_default_metrics(**kwargs)
    payload = create_summary_payload(book_id, book_title, summary, metrics)
    return post_payload(payload)