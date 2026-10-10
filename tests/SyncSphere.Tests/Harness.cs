using System.IdentityModel.Tokens.Jwt;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using StackExchange.Redis;
using SyncSphere.Client;
using SyncSphere.Contracts;
using SyncSphere.Server;
using Testcontainers.Redis;
using Xunit;

namespace SyncSphere.Tests;

public sealed class RedisFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder("public.ecr.aws/docker/library/redis:8").Build();

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    public async Task FlushAsync()
    {
        using var mux = await ConnectionMultiplexer.ConnectAsync(ConnectionString + ",allowAdmin=true");
        await mux.GetServer(mux.GetEndPoints()[0]).FlushAllDatabasesAsync();
    }
}

[CollectionDefinition("redis")]
public sealed class RedisCollection : ICollectionFixture<RedisFixture>;

public static class Tokens
{
    public const string Key = "test-signing-key-test-signing-key-123456";
    public const string Issuer = "tests";
    public const string Audience = "syncsphere";

    public static string Create(string sub, string tenant, string? role = null, TimeSpan? lifetime = null, string? key = null)
    {
        var claims = new List<Claim> { new("sub", sub), new("tenant", tenant) };
        foreach (var r in (role ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            claims.Add(new Claim("role", r));
        }

        var now = DateTime.UtcNow;
        return new JwtSecurityTokenHandler().WriteToken(new JwtSecurityToken(
            Issuer,
            Audience,
            claims,
            notBefore: now.Add(lifetime ?? TimeSpan.FromMinutes(30)).AddMinutes(-10) < now.AddMinutes(-10) ? now.Add(lifetime ?? TimeSpan.FromMinutes(30)).AddMinutes(-10) : now.AddMinutes(-10),
            expires: now.Add(lifetime ?? TimeSpan.FromMinutes(30)),
            signingCredentials: new SigningCredentials(new SymmetricSecurityKey(Encoding.UTF8.GetBytes(key ?? Key)), SecurityAlgorithms.HmacSha256)));
    }
}

public sealed class DropBatches(Func<DeltaBatch, bool> predicate) : ISyncFaults
{
    public bool Enabled { get; set; } = true;

    public int Dropped;

    public bool DropBatch(DeltaBatch batch)
    {
        if (Enabled && predicate(batch))
        {
            Interlocked.Increment(ref Dropped);
            return true;
        }

        return false;
    }
}

public sealed record ServerOptions
{
    public int ConflationWindowMs { get; init; }
    public int ReplayRetention { get; init; } = 10_000;
    public int PresenceTtlSeconds { get; init; } = 30;
    public int MaxWidgetsPerDashboard { get; init; } = 500;
    public ISyncFaults? Faults { get; init; }
}

public sealed class ServerNode : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly HttpClient _http;

    private ServerNode(WebApplication app)
    {
        _app = app;
        Address = app.Urls.First();
        _http = new HttpClient { BaseAddress = new Uri(Address) };
    }

    public string Address { get; }
    public Uri HubUrl => new(new Uri(Address), SyncSphereHost.HubPath);
    public SyncStats Stats => _app.Services.GetRequiredService<SyncStats>();

    public static async Task<ServerNode> StartAsync(string redis, ServerOptions? options = null)
    {
        options ??= new ServerOptions();
        var app = SyncSphereHost.Build(
            ["--environment", "Development"],
            b =>
            {
                b.Logging.ClearProviders();
                b.Logging.AddConsole();
                b.Logging.SetMinimumLevel(LogLevel.Error);
                b.WebHost.UseUrls("http://127.0.0.1:0");
                b.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["SyncSphere:RedisConnectionString"] = redis,
                    ["SyncSphere:ConflationWindowMs"] = options.ConflationWindowMs.ToString(),
                    ["SyncSphere:ReplayRetention"] = options.ReplayRetention.ToString(),
                    ["SyncSphere:PresenceTtlSeconds"] = options.PresenceTtlSeconds.ToString(),
                    ["SyncSphere:MaxWidgetsPerDashboard"] = options.MaxWidgetsPerDashboard.ToString(),
                    ["SyncSphere:Jwt:SigningKey"] = Tokens.Key,
                    ["SyncSphere:Jwt:Issuer"] = Tokens.Issuer,
                    ["SyncSphere:Jwt:Audience"] = Tokens.Audience,
                });
            },
            services =>
            {
                if (options.Faults is not null)
                {
                    services.RemoveAll<ISyncFaults>();
                    services.AddSingleton(options.Faults);
                }
            });

        await app.StartAsync();
        return new ServerNode(app);
    }

    public DashboardClient Client(string dashboard, string token, double heartbeatSeconds = 10, Action<double>? latency = null)
        => new(new DashboardClientOptions
        {
            HubUrl = HubUrl,
            DashboardId = dashboard,
            AccessToken = () => Task.FromResult<string?>(token),
            GapTimeoutMs = 100,
            HeartbeatSeconds = heartbeatSeconds,
            OnLatencyMs = latency,
        });

    public async Task<(int Status, UpdateResult? Result)> PutAsync(string dashboard, string widget, string json, string token, long expected = -1)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, $"/api/dashboards/{dashboard}/widgets/{widget}")
        {
            Content = JsonContent.Create(new UpdateWidgetRequest(json, expected)),
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await _http.SendAsync(request);
        UpdateResult? result = null;
        if (response.Content.Headers.ContentType?.MediaType == "application/json" && (response.IsSuccessStatusCode || (int)response.StatusCode == 409))
        {
            result = await response.Content.ReadFromJsonAsync<UpdateResult>();
        }

        return ((int)response.StatusCode, result);
    }

    public async Task<Snapshot?> GetSnapshotAsync(string dashboard, string token)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"/api/dashboards/{dashboard}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await _http.SendAsync(request);
        return response.IsSuccessStatusCode ? await response.Content.ReadFromJsonAsync<Snapshot>() : null;
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
