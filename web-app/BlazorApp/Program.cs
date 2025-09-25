using BlazorApp.Components;
using BlazorApp.Hubs;
using Microsoft.AspNetCore.Components.Server;
using Neo4j.Driver;
using Syncfusion.Blazor;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();
builder.Services.AddSyncfusionBlazor();
builder.Services.AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.PropertyNameCaseInsensitive = true;
    });
builder.Services.AddSignalR();

// Kestrel listen on all interfaces
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5055);
});


// Register Neo4j Driver
builder.Services.AddSingleton<IDriver>(provider =>
{
    var uri = builder.Configuration.GetConnectionString("Neo4j") ?? "bolt://localhost:7687";
    var user = builder.Configuration["Neo4j:Username"] ?? "neo4j";
    var password = builder.Configuration["Neo4j:Password"] ?? "password";
    
    return GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
});

// SyncFusion license key - removes watermark, and free with account verification.
var licenseKey = builder.Configuration["Syncfusion:LicenseKey"];
if (!string.IsNullOrEmpty(licenseKey) && !licenseKey.Equals("your_key_optional"))
    Syncfusion.Licensing.SyncfusionLicenseProvider.RegisterLicense(licenseKey);

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment()) {
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAntiforgery();

app.UseStaticFiles();  // Syncfusion.Blazor resources in App.Razor
app.UseWebSockets();  // SignalR requirement

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();
app.MapControllers();
app.MapHub<MetricsHub>("/metricshub");

app.Run();