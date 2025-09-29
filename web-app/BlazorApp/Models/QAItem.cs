namespace BlazorApp.Models;

public class QAItem
{
    public string Question { get; set; }
    public string GoldAnswer { get; set; }
    public string GeneratedAnswer { get; set; }
    public bool? IsCorrect { get; set; }
    public double? Accuracy { get; set; }
}