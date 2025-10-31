namespace BlazorApp.Models;

public class SummaryMetrics
{
    public List<PRF1Metric> PRF1Metrics { get; set; } = new();
    public QAMetric QA { get; set; } = new();
    public List<ScalarMetric> ScalarMetrics { get; set; } = new();

    public static SummaryMetrics GetDefault() {
        return new SummaryMetrics
        { PRF1Metrics = new List<PRF1Metric>
          { new PRF1Metric
            { Name = "ROUGE-1", Precision = 0.0, Recall = 0.0, F1Score = 0.0 },
            new PRF1Metric
            { Name = "ROUGE-2", Precision = 0.0, Recall = 0.0, F1Score = 0.0 },
            new PRF1Metric
            { Name = "ROUGE-L", Precision = 0.0, Recall = 0.0, F1Score = 0.0 },
            new PRF1Metric
            { Name = "ROUGE-Lsum", Precision = 0.0, Recall = 0.0, F1Score = 0.0 },
            new PRF1Metric
            { Name = "BERTScore", Precision = 0.0, Recall = 0.0, F1Score = 0.0 } },
          ScalarMetrics = new List<ScalarMetric>
          { new ScalarMetric
            { Name = "BooookScore (Chang 2024)", Value = 0.0 },
            new ScalarMetric
            { Name = "QuestEval (Scialom 2021)", Value = 0.0 } },
          QA = new QAMetric
          { QAItems = new List<QAItem>
            { new QAItem
              { Question = "UNKNOWN", GoldAnswer = "UNKNOWN", GeneratedAnswer = "UNKNOWN", IsCorrect = false,
                Accuracy = 0.0 },
              new QAItem
              { Question = "UNKNOWN", GoldAnswer = "UNKNOWN", GeneratedAnswer = "UNKNOWN", IsCorrect = false,
                Accuracy = 0.0 } } } };
    }
}