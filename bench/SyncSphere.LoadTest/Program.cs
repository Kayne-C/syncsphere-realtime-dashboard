using System.Diagnostics;
using System.Net.Http.Json;
using SyncSphere.Client;
using SyncSphere.Contracts;

var options = Args.Parse(args);
Console.WriteLine($"servers={string.Join(",", options.Servers)} clients={options.Clients} rate={options.Rate}/s duration={options.Seconds}s widgets={options.Widgets} dashboard={options.Dashboard}");

using var http = new HttpClient(new SocketsHttpHandler { MaxConnectionsPerServer = 16, PooledConnectionLifetime = TimeSpan.FromMinutes(10) });
async Task<string> TokenAsync(string sub, string roles)
{
    var response = await http.GetFromJsonAsync<TokenResponse>($"{options.Servers[0]}/dev/token?sub={sub}&tenant=acme&role={roles}");
    return response!.Token;
}

var userToken = await TokenAsync("load-viewer", string.Empty);
var ingestToken = await TokenAsync("load-bridge", "ingest");

var histogram = new Histogram();
var clients = new List<DashboardClient>(options.Clients);
for (var i = 0; i < options.Clients; i++)
{
    var server = options.Servers[i % options.Servers.Length];
    clients.Add(new DashboardClient(new DashboardClientOptions
    {
        HubUrl = new Uri(new Uri(server), "/hubs/dashboard"),
        DashboardId = options.Dashboard,
        AccessToken = () => Task.FromResult<string?>(userToken),
        HeartbeatSeconds = 15,
        WebSocketsOnly = true,
        OnLatencyMs = histogram.Record,
    }));
}

var connectWatch = Stopwatch.StartNew();
var failures = 0;
var errors = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
using (var gate = new SemaphoreSlim(options.ConnectConcurrency))
{
    await Task.WhenAll(clients.Select(async client =>
    {
        await gate.WaitAsync();
        try
        {
            await client.ConnectAsync();
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref failures);
            errors.AddOrUpdate((ex.GetBaseException().GetType().Name + ": " + ex.GetBaseException().Message)[..Math.Min(110, (ex.GetBaseException().GetType().Name + ": " + ex.GetBaseException().Message).Length)], 1, (_, n) => n + 1);
        }
        finally
        {
            gate.Release();
        }
    }));
}

connectWatch.Stop();
foreach (var (message, count) in errors.OrderByDescending(e => e.Value).Take(4))
{
    Console.WriteLine($"  connect error x{count}: {message}");
}

var connected = clients.Count(c => c.State == Microsoft.AspNetCore.SignalR.Client.HubConnectionState.Connected);
Console.WriteLine($"connected {connected}/{options.Clients} in {connectWatch.Elapsed.TotalSeconds:0.0}s (failures: {failures})");

var sent = 0L;
var rejected = 0L;
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(options.Seconds));
var publishers = Enumerable.Range(0, options.Publishers).Select(p => Task.Run(async () =>
{
    var perPublisher = (double)options.Rate / options.Publishers;
    var watch = Stopwatch.StartNew();
    long n = 0;
    while (!cts.IsCancellationRequested)
    {
        var due = (long)(watch.Elapsed.TotalSeconds * perPublisher);
        while (n < due && !cts.IsCancellationRequested)
        {
            var server = options.Servers[(int)((n + p) % options.Servers.Length)];
            var widget = $"w{(n * options.Publishers + p) % options.Widgets}";
            using var request = new HttpRequestMessage(HttpMethod.Put, $"{server}/api/dashboards/{options.Dashboard}/widgets/{widget}")
            {
                Content = JsonContent.Create(new UpdateWidgetRequest($$"""{"value":{{n}},"at":{{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}}}""")),
            };
            request.Headers.Authorization = new("Bearer", ingestToken);
            try
            {
                using var response = await http.SendAsync(request, cts.Token);
                if (response.IsSuccessStatusCode)
                {
                    Interlocked.Increment(ref sent);
                }
                else
                {
                    Interlocked.Increment(ref rejected);
                }
            }
            catch (OperationCanceledException)
            {
            }

            n++;
        }

        await Task.Delay(1);
    }
})).ToArray();

await Task.WhenAll(publishers);
var publishSeconds = options.Seconds;

Console.WriteLine($"published {sent} updates ({sent / (double)publishSeconds:0}/s), rejected {rejected}");

var finalSnapshot = await FetchHeadAsync(http, options.Servers[0], options.Dashboard, ingestToken);
var convergeDeadline = DateTime.UtcNow.AddSeconds(30);
while (DateTime.UtcNow < convergeDeadline && clients.Count(c => c.State == Microsoft.AspNetCore.SignalR.Client.HubConnectionState.Connected && c.ContiguousVersion < finalSnapshot) > 0)
{
    await Task.Delay(100);
}

var live = clients.Where(c => c.State == Microsoft.AspNetCore.SignalR.Client.HubConnectionState.Connected).ToList();
var converged = live.Count(c => c.ContiguousVersion >= finalSnapshot);
Console.WriteLine($"head version {finalSnapshot}; converged {converged}/{live.Count} connected clients");
Console.WriteLine($"deliveries={histogram.Count} ({histogram.Count / (double)publishSeconds:0}/s) latency ms: p50={histogram.Percentile(0.50):0.0} p95={histogram.Percentile(0.95):0.0} p99={histogram.Percentile(0.99):0.0} max={histogram.Max:0.0}");
Console.WriteLine($"resyncs={clients.Sum(c => c.Resyncs)} snapshots={clients.Sum(c => c.Snapshots)} batches/client={clients.Average(c => c.Batches):0}");
Console.WriteLine($"client process working set={Environment.WorkingSet / (1024 * 1024)} MB");

foreach (var server in options.Servers)
{
    Console.WriteLine($"{server}/stats -> {await http.GetStringAsync($"{server}/stats")}");
}

await Task.WhenAll(clients.Select(c => c.DisposeAsync().AsTask()));
return 0;

static async Task<long> FetchHeadAsync(HttpClient http, string server, string dashboard, string token)
{
    using var request = new HttpRequestMessage(HttpMethod.Get, $"{server}/api/dashboards/{dashboard}");
    request.Headers.Authorization = new("Bearer", token);
    using var response = await http.SendAsync(request);
    return (await response.Content.ReadFromJsonAsync<Snapshot>())!.Version;
}

internal sealed record TokenResponse(string Token);

internal sealed class Histogram
{
    private const double Resolution = 0.1;
    private readonly long[] _buckets = new long[50_000];
    private long _count;
    private long _max;

    public long Count => Interlocked.Read(ref _count);

    public double Max => Interlocked.Read(ref _max) * Resolution;

    public void Record(double ms)
    {
        var bucket = (int)Math.Min(_buckets.Length - 1, ms / Resolution);
        Interlocked.Increment(ref _buckets[bucket]);
        Interlocked.Increment(ref _count);
        long current;
        while (bucket > (current = Interlocked.Read(ref _max)))
        {
            Interlocked.CompareExchange(ref _max, bucket, current);
        }
    }

    public double Percentile(double q)
    {
        var target = (long)Math.Ceiling(q * Count);
        long seen = 0;
        for (var i = 0; i < _buckets.Length; i++)
        {
            seen += Interlocked.Read(ref _buckets[i]);
            if (seen >= target)
            {
                return i * Resolution;
            }
        }

        return 0;
    }
}

internal sealed record Args(string[] Servers, int Clients, int Rate, int Seconds, int Widgets, int Publishers, string Dashboard, int ConnectConcurrency)
{
    public static Args Parse(string[] args)
    {
        string Get(string name, string fallback)
        {
            var i = Array.IndexOf(args, "--" + name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
        }

        return new Args(
            Get("servers", "http://127.0.0.1:5101,http://127.0.0.1:5102").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            int.Parse(Get("clients", "1000")),
            int.Parse(Get("rate", "200")),
            int.Parse(Get("seconds", "15")),
            int.Parse(Get("widgets", "20")),
            int.Parse(Get("publishers", "4")),
            Get("dashboard", "live"),
            int.Parse(Get("connect-concurrency", "250")));
    }
}
