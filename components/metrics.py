import requests

# REST API endpoint
url = "http://172.30.48.1:5055/api/metrics"

# Build payload matching Blazor's SummaryData model
summary_payload = {
    "BookID": "book-42",
    "BookTitle": "Example Book",
    "SummaryText": "This is an AI-generated summary of the entire book. It captures the key plot points and themes.",

    "Metrics": {
        "PRF1Metrics": [
            {"Name": "ROUGE", "Precision": 0.80, "Recall": 0.85, "F1Score": 0.82},
            {"Name": "BERTScore", "Precision": 0.89, "Recall": 0.90, "F1Score": 0.89}
        ],
        "ScalarMetrics": [
            {"Name": "BooookScore (Chang 2024)", "Value": 0.76},
            {"Name": "QuestEval (Scialom 2021)", "Value": 0.81}
        ],
        "QA": {
            "QAItems": [
                {"Question": "Who is the protagonist?", "GoldAnswer": "Alice", "GeneratedAnswer": "Alice", "IsCorrect": True, "Accuracy": 1.0},
                {"Question": "Where does the story start?", "GoldAnswer": "Wonderland", "GeneratedAnswer": "Forest", "IsCorrect": False, "Accuracy": 0.0}
            ]
        }
    },

    # Optional: additional QA results if you want to store multiple
    "QAResults": []
}

def post_example_results():
	# Send POST request
	response = requests.post(url, json=summary_payload)
	
	if response.ok:  # handles 200–299
	    print("POST succeeded")
	    print(response.json())
	else:
	    print(f"POST failed: {response.status_code}")
	    print(response.text)
