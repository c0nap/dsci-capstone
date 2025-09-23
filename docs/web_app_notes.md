
Data Science Capstone - Patrick Conan
---


#### Tips for designing a Blazor.Diagrams page

1. On NuGet, `Z.Blazor.Diagrams` might work for you, but `Syncfusion.Blazor.Diagram` was easier to use and has more documentation. However, you should install only `Syncfusion.Blazor` to avoid complicating the locations of dependencies.

2. Follow the instructions on this page: [Getting Started with Syncfusion](https://blazor.syncfusion.com/documentation/diagram/getting-started). Note: must add `builder.Services.AddSyncfusionBlazor()` and `app.UseStaticFiles()` before `app.MapStaticAssets()` in your `Program.cs`. Also, add the static web assets to your `App.razor`: JavaScript `_content/Syncfusion.Blazor/scripts/syncfusion-blazor.min.js` and Styles `_content/Syncfusion.Blazor/styles/bootstrap5-lite.css`. Sometimes refreshing the page will fix it too.

3. IMPORTANT: The latest versions of Blazor require a render mode specification for elements to be interactive. `@rendermode InteractiveServer` at the top of the Razor page, or in `_Imports.razor` to apply it globally.

4. In Blazor apps, the normal print function `Console.WriteLine` does not work. Use `@inject ILogger<Graph> Logger` and `Logger.LogInformation("Hello World")`. Alternatively, open your browser's console with `Ctrl+Shift+I` or `F12`.
  
5. The execution order of overridden tasks can be tricky. Be careful when moving code between these functions: `OnInitializedAsync`, `OnAfterRenderAsync`, and `OnDiagramCreated`

