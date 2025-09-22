using BlazorApp.Components;
using Neo4j.Driver;
using Syncfusion.Blazor;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddSyncfusionBlazor();

// Register Neo4j Driver
builder.Services.AddSingleton<IDriver>(provider =>
{
    var uri = builder.Configuration.GetConnectionString("Neo4j") ?? "bolt://localhost:7687";
    var user = builder.Configuration["Neo4j:Username"] ?? "neo4j";
    var password = builder.Configuration["Neo4j:Password"] ?? "password";
    
    return GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment()) {
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();


app.UseAntiforgery();

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();