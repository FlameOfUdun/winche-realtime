// A host for trying Winche Realtime (and the SDKs' end-to-end tests), NOT for production: anyone may sign in as
// anyone. The "token" is the uid itself (?access_token=ann or Authorization: Bearer ann).
//
//   WINCHE_REALTIME_DB="Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=realtime" \
//     dotnet run --project samples/Winche.Realtime.Sample --urls http://localhost:5199
using Winche;
using Winche.Realtime;
using Winche.Realtime.AspNetCore;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddWincheRealtime(o =>
{
    o.ConnectionString = Environment.GetEnvironmentVariable("WINCHE_REALTIME_DB")
        ?? throw new InvalidOperationException("Set WINCHE_REALTIME_DB to a Postgres connection string.");
    o.UseRulesText("""
        match sessions/{session} {
          allow get: if request.auth != null;
          allow create: if request.auth != null && request.resource.device == request.auth.uid;
          allow update: if request.auth != null;
        }
        """);
});
builder.Services.AddWincheRealtimeWsApi();
builder.Services.AddWincheCaller(http =>
{
    var token = http.Request.Query["access_token"].ToString();
    if (token.Length == 0 && http.Request.Headers.Authorization.ToString() is { } auth && auth.StartsWith("Bearer ")) token = auth[7..];
    return token.Length == 0 ? null : new Dictionary<string, object?> { ["uid"] = token, ["sub"] = token };
});

var app = builder.Build();
app.UseWebSockets();
app.UseWincheCaller();
app.MapWincheRealtimeWsApi();
app.MapGet("/", () => "Winche Realtime sample: ws://…/realtime/v1/ws?access_token=<uid>");
app.Run();
