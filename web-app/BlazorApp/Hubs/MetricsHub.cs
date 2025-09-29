using Microsoft.AspNetCore.SignalR;

namespace BlazorApp.Hubs;

// Hubs/MetricsHub.cs
using Microsoft.AspNetCore.SignalR;

public class MetricsHub : Hub
{
    // This is the absolute minimum - just inheriting from Hub
    // SignalR handles connection management automatically
        
    // Optional: Add logging for debugging
    private readonly ILogger<MetricsHub>? _logger;

    public MetricsHub(ILogger<MetricsHub>? logger = null)
    {
        _logger = logger;
    }

    public override async Task OnConnectedAsync()
    {
        //_logger?.LogInformation($"Client connected");
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        //_logger?.LogInformation($"Client disconnected");
        await base.OnDisconnectedAsync(exception);
    }
}
