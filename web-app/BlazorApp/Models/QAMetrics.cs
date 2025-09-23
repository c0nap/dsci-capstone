namespace BlazorApp.Models;

public class QAMetrics
{
    public List<QAItem> QAItems { get; set; } = new();
    public double AverageAccuracy => 
        QAItems.Count > 0 ? QAItems.Average(q => q.Accuracy ?? (q.IsCorrect == true ? 1.0 : 0.0)) : 0;
}