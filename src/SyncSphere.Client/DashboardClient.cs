using Microsoft.AspNetCore.SignalR.Client;
using SyncSphere.Contracts;

namespace SyncSphere.Client;

public sealed class DashboardClientOptions
{
    public required Uri HubUrl { get; init; }
    public required string DashboardId { get; init; }
    public required Func<Task<string?>> AccessToken { get; init; }
    public int GapTimeoutMs { get; init; } = 500;
    public double HeartbeatSeconds { get; init; } = 10;
    public bool WebSocketsOnly { get; init; }
    public Action<double>? OnLatencyMs { get; init; }
    public Action<Delta>? OnDelta { get; init; }
}

public sealed class DashboardClient : IAsyncDisposable
{
    private readonly DashboardClientOptions _options;
    private readonly HubConnection _hub;
    private readonly object _gate = new();
    private readonly Dictionary<string, WidgetState> _widgets = new(StringComparer.Ordinal);
    private readonly SortedSet<long> _ahead = [];
    private readonly CancellationTokenSource _cts = new();
    private long _contiguous = -1;
    private long? _gapSince;
    private long _batches;
    private long _deltas;
    private long _resyncs;
    private long _snapshots;
    private string[] _viewers = [];
    private Task? _background;
    private int _resyncing;
    private int _disposed;

    public DashboardClient(DashboardClientOptions options)
    {
        _options = options;
        _hub = new HubConnectionBuilder()
            .WithUrl(options.HubUrl, o =>
            {
                o.AccessTokenProvider = options.AccessToken;
                if (options.WebSocketsOnly)
                {
                    o.SkipNegotiation = true;
                    o.Transports = Microsoft.AspNetCore.Http.Connections.HttpTransportType.WebSockets;
                }
            })
            .WithAutomaticReconnect([TimeSpan.Zero, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)])
            .Build();

        _hub.On<DeltaBatch>(ClientMethods.Deltas, batch => Apply(batch, live: true));
        _hub.On<string, string[]>(ClientMethods.PresenceChanged, (dashboard, viewers) =>
        {
            if (dashboard == options.DashboardId)
            {
                lock (_gate)
                {
                    _viewers = viewers;
                }
            }
        });
        _hub.Reconnected += async _ => await JoinAsync();
    }

    public long ContiguousVersion
    {
        get
        {
            lock (_gate)
            {
                return _contiguous;
            }
        }
    }

    public long Batches => Interlocked.Read(ref _batches);
    public long DeltasApplied => Interlocked.Read(ref _deltas);
    public long Resyncs => Interlocked.Read(ref _resyncs);
    public long Snapshots => Interlocked.Read(ref _snapshots);

    public string[] Viewers
    {
        get
        {
            lock (_gate)
            {
                return _viewers;
            }
        }
    }

    public IReadOnlyDictionary<string, WidgetState> Widgets
    {
        get
        {
            lock (_gate)
            {
                return new Dictionary<string, WidgetState>(_widgets);
            }
        }
    }

    public HubConnectionState State => _hub.State;

    public async Task ConnectAsync()
    {
        await _hub.StartAsync(_cts.Token);
        await JoinAsync();
        _background = Task.Run(() => BackgroundAsync(_cts.Token));
    }

    public async Task DisconnectAsync() => await _hub.StopAsync();

    public async Task ReconnectAsync()
    {
        if (_hub.State == HubConnectionState.Disconnected)
        {
            await _hub.StartAsync(_cts.Token);
            await JoinAsync();
        }
    }

    public Task<UpdateResult> UpdateWidgetAsync(string widgetId, string json, long expectedWidgetVersion = -1)
        => _hub.InvokeAsync<UpdateResult>(HubMethods.UpdateWidget, _options.DashboardId, widgetId, new UpdateWidgetRequest(json, expectedWidgetVersion), _cts.Token);

    public async Task LeaveAsync() => await _hub.InvokeAsync(HubMethods.Leave, _options.DashboardId, _cts.Token);

    public async Task<bool> WaitForVersionAsync(long version, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (ContiguousVersion >= version)
            {
                return true;
            }

            await Task.Delay(5);
        }

        return ContiguousVersion >= version;
    }

    private async Task JoinAsync()
    {
        long? last;
        lock (_gate)
        {
            last = _contiguous >= 0 ? _contiguous : null;
        }

        var result = await _hub.InvokeAsync<JoinResult>(HubMethods.Join, _options.DashboardId, last, _cts.Token);
        lock (_gate)
        {
            _viewers = result.Viewers;
        }

        if (result.Snapshot is { } snapshot)
        {
            ApplySnapshot(snapshot);
        }
        else if (result.Replay is { } replay)
        {
            Apply(replay, live: false);
        }
    }

    private void ApplySnapshot(Snapshot snapshot)
    {
        Interlocked.Increment(ref _snapshots);
        lock (_gate)
        {
            foreach (var w in snapshot.Widgets)
            {
                if (!_widgets.TryGetValue(w.WidgetId, out var existing) || w.Version > existing.Version)
                {
                    _widgets[w.WidgetId] = w;
                }
            }

            _contiguous = Math.Max(_contiguous, snapshot.Version);
            _ahead.RemoveWhere(v => v <= _contiguous);
            AdvanceLocked();
        }
    }

    private void Apply(DeltaBatch batch, bool live)
    {
        Interlocked.Increment(ref _batches);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        lock (_gate)
        {
            foreach (var d in batch.Deltas)
            {
                if (!_widgets.TryGetValue(d.WidgetId, out var existing) || d.WidgetVersion > existing.Version)
                {
                    _widgets[d.WidgetId] = new WidgetState(d.WidgetId, d.WidgetVersion, d.Json);
                    Interlocked.Increment(ref _deltas);
                    _options.OnDelta?.Invoke(d);
                    if (live)
                    {
                        _options.OnLatencyMs?.Invoke(Math.Max(0, now - d.PublishedAtUnixMs));
                    }
                }
            }

            foreach (var v in batch.Versions)
            {
                if (v > _contiguous)
                {
                    _ahead.Add(v);
                }
            }

            AdvanceLocked();
        }
    }

    private void AdvanceLocked()
    {
        var before = _contiguous;
        while (_ahead.Count > 0 && _ahead.Min == _contiguous + 1)
        {
            _contiguous++;
            _ahead.Remove(_contiguous);
        }

        if (_ahead.Count == 0)
        {
            _gapSince = null;
        }
        else if (_contiguous != before || _gapSince is null)
        {
            _gapSince = Environment.TickCount64;
        }
    }

    private async Task BackgroundAsync(CancellationToken ct)
    {
        var heartbeat = DateTime.UtcNow;
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(25));
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                if (_hub.State != HubConnectionState.Connected)
                {
                    continue;
                }

                bool gap;
                lock (_gate)
                {
                    gap = _gapSince is { } since && Environment.TickCount64 - since >= _options.GapTimeoutMs;
                }

                if (gap)
                {
                    await ResyncAsync(ct);
                }

                if ((DateTime.UtcNow - heartbeat).TotalSeconds >= _options.HeartbeatSeconds)
                {
                    heartbeat = DateTime.UtcNow;
                    try
                    {
                        var head = await _hub.InvokeAsync<long>(HubMethods.Heartbeat, _options.DashboardId, ct);
                        lock (_gate)
                        {
                            if (head > _contiguous)
                            {
                                _gapSince ??= Environment.TickCount64;
                            }
                        }
                    }
                    catch (Exception) when (!ct.IsCancellationRequested)
                    {
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ResyncAsync(CancellationToken ct)
    {
        if (Interlocked.Exchange(ref _resyncing, 1) == 1)
        {
            return;
        }

        try
        {
            Interlocked.Increment(ref _resyncs);
            long from;
            lock (_gate)
            {
                from = _contiguous;
            }

            var replay = await _hub.InvokeAsync<ReplayResult>(HubMethods.Replay, _options.DashboardId, from, ct);
            if (replay.NeedSnapshot)
            {
                ApplySnapshot(await _hub.InvokeAsync<Snapshot>(HubMethods.SnapshotRequest, _options.DashboardId, ct));
            }
            else if (replay.Batch is { } batch)
            {
                Apply(batch, live: false);
            }

            lock (_gate)
            {
                _gapSince = _ahead.Count > 0 ? Environment.TickCount64 : null;
            }
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
        }
        finally
        {
            Interlocked.Exchange(ref _resyncing, 0);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _cts.CancelAsync();
        if (_background is not null)
        {
            try
            {
                await _background;
            }
            catch (OperationCanceledException)
            {
            }
        }

        await _hub.DisposeAsync();
        _cts.Dispose();
    }
}
