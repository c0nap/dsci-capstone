namespace BlazorApp.Models;

public class SummaryMetrics
{
    public List<PRF1Metric> PRF1Metrics { get; set; } = new();
    public QAMetrics QA { get; set; } = new();
    public List<ScalarMetric> ScalarMetrics { get; set; } = new();
}