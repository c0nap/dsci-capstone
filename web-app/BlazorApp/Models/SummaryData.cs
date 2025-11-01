using System.Text.Json.Serialization;

namespace BlazorApp.Models;

public class SummaryData
{
    public string BookID { get; set; } // Identifier for the book
    public string BookTitle { get; set; } // Optional human-readable title
    public string SummaryText { get; set; } // The generated summary
    public string GoldSummaryText { get; set; } // A summary to compare against
    
    
    // Optional: nodes used in constructing the summary
    //public List<NodeReference> NodeReferences { get; set; } = new();

    // Metrics for this summary
    public SummaryMetrics Metrics { get; set; } = new();

    // QA results (if any)
    public List<QAMetric> QAResults { get; set; } = new();
}