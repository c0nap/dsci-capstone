using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using BlazorApp.Models;
using BlazorApp.Hubs;

namespace BlazorApp.Controllers;

[ApiController]
[Route("api/[controller]")]
public class MetricsController : ControllerBase
{
    private readonly ILogger<MetricsController> _logger;
    private readonly IHubContext<MetricsHub> _hubContext;

    private static readonly List<SummaryData> Summaries = new();

    public MetricsController(ILogger<MetricsController> logger, IHubContext<MetricsHub> hubContext)
    {
        _logger = logger;
        _hubContext = hubContext;
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] SummaryData summary)
    {
        Summaries.Add(summary);
        _logger.LogInformation("POST received for BookID: {BookID}", summary.BookID);

        // Push update to all connected Blazor clients
        _logger.LogInformation("Pushing update to Hub...");
        await _hubContext.Clients.All.SendAsync("ReceiveUpdate", summary);
        _logger.LogInformation("Hub update sent.");

        return CreatedAtAction(nameof(GetIndex), new { id = Summaries.Count - 1 }, summary);
    }

    [HttpGet("{id}")]
    public IActionResult GetIndex(int id)
    {
        if (id < 0 || id >= Summaries.Count)
            return NotFound();
        return Ok(Summaries[id]);
    }

    [HttpGet]
    public IActionResult GetAll() => Ok(Summaries);
}