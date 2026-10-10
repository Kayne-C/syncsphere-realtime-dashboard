using System.Security.Claims;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SyncSphere.Contracts;

namespace SyncSphere.Server;

public sealed partial class DashboardService(DashboardStore store, DeltaBroadcaster broadcaster, SyncStats stats, IOptions<SyncOptions> options, TimeProvider time)
{
    [GeneratedRegex("^[A-Za-z0-9_-]{1,64}$")]
    private static partial Regex IdPattern();

    public static bool ValidId(string? id) => id is not null && IdPattern().IsMatch(id);

    public async Task<UpdateResult> ApplyAsync(string tenant, string dashboard, string widgetId, UpdateWidgetRequest request)
    {
        if (!ValidId(dashboard) || !ValidId(widgetId))
        {
            throw new HubException("Invalid dashboard or widget id.");
        }

        if (string.IsNullOrEmpty(request.Json) || System.Text.Encoding.UTF8.GetByteCount(request.Json) > options.Value.MaxWidgetBytes)
        {
            throw new HubException($"Widget payload must be 1..{options.Value.MaxWidgetBytes} bytes.");
        }

        var now = time.GetUtcNow().ToUnixTimeMilliseconds();
        var result = await store.ApplyAsync(tenant, dashboard, widgetId, request.Json, request.ExpectedWidgetVersion, now);

        switch (result.Status)
        {
            case ApplyStatus.TooManyWidgets:
                throw new HubException($"A dashboard holds at most {options.Value.MaxWidgetsPerDashboard} widgets.");
            case ApplyStatus.Conflict:
                return new UpdateResult(false, 0, result.WidgetVersion, result.CurrentJson);
        }

        stats.Applied();
        broadcaster.Enqueue(tenant, dashboard, new Delta(result.Version, widgetId, result.WidgetVersion, request.Json, now));
        return new UpdateResult(true, result.Version, result.WidgetVersion, null);
    }
}

[Authorize]
public sealed class DashboardHub(
    DashboardService service,
    DashboardStore store,
    PresenceStore presence,
    SyncStats stats,
    IOptions<SyncOptions> options) : Hub
{
    private const string JoinedKey = "joined";

    private string Tenant => Context.User?.FindFirstValue("tenant") ?? throw new HubException("Token has no tenant.");

    private string User => Context.User?.FindFirstValue("sub") ?? throw new HubException("Token has no subject.");

    private HashSet<string> Joined
    {
        get
        {
            if (!Context.Items.TryGetValue(JoinedKey, out var value))
            {
                value = new HashSet<string>(StringComparer.Ordinal);
                Context.Items[JoinedKey] = value;
            }

            return (HashSet<string>)value!;
        }
    }

    public override Task OnConnectedAsync()
    {
        stats.ConnectionOpened();
        return base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        stats.ConnectionClosed();
        string[] dashboards;
        lock (Joined)
        {
            dashboards = [.. Joined];
            Joined.Clear();
        }

        foreach (var dashboard in dashboards)
        {
            try
            {
                var viewers = await presence.LeaveAsync(Tenant, dashboard, Context.ConnectionId, User);
                await Clients.Group(DeltaBroadcaster.GroupName(Tenant, dashboard)).SendAsync(ClientMethods.PresenceChanged, dashboard, viewers);
            }
            catch (Exception)
            {
            }
        }

        await base.OnDisconnectedAsync(exception);
    }

    public async Task<JoinResult> Join(string dashboardId, long? lastVersion)
    {
        Require(dashboardId);
        var tenant = Tenant;
        lock (Joined)
        {
            if (!Joined.Contains(dashboardId) && Joined.Count >= options.Value.MaxDashboardsPerConnection)
            {
                throw new HubException($"At most {options.Value.MaxDashboardsPerConnection} dashboards per connection.");
            }

            Joined.Add(dashboardId);
        }

        var group = DeltaBroadcaster.GroupName(tenant, dashboardId);
        await Groups.AddToGroupAsync(Context.ConnectionId, group);
        var viewers = await presence.JoinAsync(tenant, dashboardId, Context.ConnectionId, User);
        await Clients.OthersInGroup(group).SendAsync(ClientMethods.PresenceChanged, dashboardId, viewers);

        if (lastVersion is { } last)
        {
            var replay = await store.ReplayAsync(tenant, dashboardId, last);
            if (!replay.NeedSnapshot)
            {
                var current = replay.Batch is { Versions.Length: > 0 } b ? b.Versions[^1] : last;
                return new JoinResult(null, replay.Batch, current, viewers);
            }
        }

        var snapshot = await store.SnapshotAsync(tenant, dashboardId);
        return new JoinResult(snapshot, null, snapshot.Version, viewers);
    }

    public async Task Leave(string dashboardId)
    {
        Require(dashboardId);
        lock (Joined)
        {
            Joined.Remove(dashboardId);
        }

        var group = DeltaBroadcaster.GroupName(Tenant, dashboardId);
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
        var viewers = await presence.LeaveAsync(Tenant, dashboardId, Context.ConnectionId, User);
        await Clients.Group(group).SendAsync(ClientMethods.PresenceChanged, dashboardId, viewers);
    }

    public async Task<ReplayResult> Replay(string dashboardId, long fromVersion)
    {
        Require(dashboardId);
        EnsureJoined(dashboardId);
        stats.Replayed();
        return await store.ReplayAsync(Tenant, dashboardId, fromVersion);
    }

    public async Task<Snapshot> GetSnapshot(string dashboardId)
    {
        Require(dashboardId);
        EnsureJoined(dashboardId);
        return await store.SnapshotAsync(Tenant, dashboardId);
    }

    public async Task<UpdateResult> UpdateWidget(string dashboardId, string widgetId, UpdateWidgetRequest request)
    {
        Require(dashboardId);
        EnsureJoined(dashboardId);
        if (Context.User?.IsInRole("viewer") == true)
        {
            throw new HubException("Viewers cannot edit.");
        }

        return await service.ApplyAsync(Tenant, dashboardId, widgetId, request);
    }

    public async Task<long> Heartbeat(string dashboardId)
    {
        Require(dashboardId);
        EnsureJoined(dashboardId);
        await presence.RefreshAsync(Tenant, dashboardId, Context.ConnectionId, User);
        return await store.HeadVersionAsync(Tenant, dashboardId);
    }

    private static void Require(string dashboardId)
    {
        if (!DashboardService.ValidId(dashboardId))
        {
            throw new HubException("Invalid dashboard id.");
        }
    }

    private void EnsureJoined(string dashboardId)
    {
        bool joined;
        lock (Joined)
        {
            joined = Joined.Contains(dashboardId);
        }

        if (!joined)
        {
            throw new HubException("Join the dashboard first.");
        }
    }
}
