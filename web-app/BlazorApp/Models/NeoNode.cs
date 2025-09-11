using Blazor.Diagrams.Core.Models;
using Blazor.Diagrams.Core.Geometry;

namespace BlazorApp.Models
{
public class NeoNode : NodeModel
{
    public string Label { get; set; }

    public NeoNode(string id, Point position, string? label = null) : base(id, position)
    {
        Label = label ?? id;
    }
}
}