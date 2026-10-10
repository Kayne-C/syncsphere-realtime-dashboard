using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using SyncSphere.Client;
using SyncSphere.Contracts;
using Xunit;

namespace SyncSphere.Tests;

[Collection("redis")]
public class RealtimeTests(RedisFixture redis)
{
    private static string Ingest(string tenant = "acme") => Tokens.Create("erp-bridge", tenant, "ingest");

    private static string User(string name, string tenant = "acme", string? role = null) => Tokens.Create(name, tenant, role);

    private static async Task AssertSameAsServerAsync(ServerNode node, DashboardClient client, string dashboard, string tenant = "acme")
    {
        var truth = await node.GetSnapshotAsync(dashboard, Ingest(tenant));
        Assert.NotNull(truth);
        Assert.True(await client.WaitForVersionAsync(truth.Version, TimeSpan.FromSeconds(20)), $"client stuck at {client.ContiguousVersion}, server at {truth.Version}");
        var widgets = client.Widgets;
        Assert.Equal(truth.Widgets.Length, widgets.Count);
        foreach (var w in truth.Widgets)
        {
            Assert.Equal(w, widgets[w.WidgetId]);
        }
    }

    [Fact]
    public async Task Updates_ingested_on_one_node_reach_every_client_on_both_nodes_in_a_consistent_state()
    {
        await redis.FlushAsync();
        await using var a = await ServerNode.StartAsync(redis.ConnectionString);
        await using var b = await ServerNode.StartAsync(redis.ConnectionString);
        var clients = new List<DashboardClient>();
        for (var i = 0; i < 100; i++)
        {
            clients.Add(a.Client("sales", User($"u{i}")));
            clients.Add(b.Client("sales", User($"v{i}")));
        }

        await Task.WhenAll(clients.Select(c => c.ConnectAsync()));

        for (var i = 0; i < 200; i++)
        {
            var (status, _) = await a.PutAsync("sales", $"kpi-{i % 10}", $$"""{"value":{{i}}}""", Ingest());
            Assert.Equal(200, status);
        }

        foreach (var client in clients)
        {
            await AssertSameAsServerAsync(a, client, "sales");
        }

        Assert.All(clients, c => Assert.Equal(0, c.Snapshots - 1));
        foreach (var client in clients)
        {
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task Concurrent_publishers_on_different_nodes_get_a_gapless_version_sequence()
    {
        await redis.FlushAsync();
        await using var a = await ServerNode.StartAsync(redis.ConnectionString);
        await using var b = await ServerNode.StartAsync(redis.ConnectionString);
        await using var watcher = b.Client("ops", User("w"));
        await watcher.ConnectAsync();

        var versions = new ConcurrentBag<long>();
        await Task.WhenAll(Enumerable.Range(0, 300).Select(async i =>
        {
            var node = i % 2 == 0 ? a : b;
            var (status, result) = await node.PutAsync("ops", $"w{i % 7}", $$"""{"n":{{i}}}""", Ingest());
            Assert.True(status == 200, $"request {i} returned HTTP {status}");
            versions.Add(result!.Version);
        }));

        Assert.Equal(Enumerable.Range(1, 300).Select(i => (long)i), versions.Order());
        await AssertSameAsServerAsync(a, watcher, "ops");
    }

    [Fact]
    public async Task A_late_joiner_starts_from_a_snapshot_and_then_follows_live_updates()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString);
        for (var i = 0; i < 20; i++)
        {
            await node.PutAsync("crm", $"deal-{i % 4}", $$"""{"stage":{{i}}}""", Ingest());
        }

        await using var late = node.Client("crm", User("late"));
        await late.ConnectAsync();
        Assert.Equal(20, late.ContiguousVersion);
        Assert.Equal(1, late.Snapshots);

        await node.PutAsync("crm", "deal-0", """{"stage":99}""", Ingest());

        await AssertSameAsServerAsync(node, late, "crm");
        Assert.Equal("""{"stage":99}""", late.Widgets["deal-0"].Json);
    }

    [Fact]
    public async Task A_client_that_reconnects_replays_only_what_it_missed_without_a_new_snapshot()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString);
        await using var client = node.Client("hr", User("ayse"));
        await client.ConnectAsync();
        await node.PutAsync("hr", "headcount", """{"n":1}""", Ingest());
        Assert.True(await client.WaitForVersionAsync(1, TimeSpan.FromSeconds(5)));

        await client.DisconnectAsync();
        for (var i = 2; i <= 40; i++)
        {
            await node.PutAsync("hr", $"w{i % 5}", $$"""{"n":{{i}}}""", Ingest());
        }

        await client.ReconnectAsync();

        await AssertSameAsServerAsync(node, client, "hr");
        Assert.Equal(1, client.Snapshots);
        Assert.Equal(40, client.ContiguousVersion);
    }

    [Fact]
    public async Task When_the_replay_log_no_longer_reaches_back_the_client_falls_back_to_a_snapshot()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString, new ServerOptions { ReplayRetention = 20 });
        await using var client = node.Client("fin", User("mehmet"));
        await client.ConnectAsync();
        await client.DisconnectAsync();
        for (var i = 1; i <= 100; i++)
        {
            await node.PutAsync("fin", $"w{i % 6}", $$"""{"n":{{i}}}""", Ingest());
        }

        await client.ReconnectAsync();

        await AssertSameAsServerAsync(node, client, "fin");
        Assert.Equal(2, client.Snapshots);
    }

    [Fact]
    public async Task A_burst_to_one_widget_is_conflated_but_the_final_state_and_version_are_exact()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString, new ServerOptions { ConflationWindowMs = 50 });
        await using var client = node.Client("trading", User("t1"));
        await client.ConnectAsync();

        for (var i = 1; i <= 500; i++)
        {
            await node.PutAsync("trading", "price", $$"""{"p":{{i}}}""", Ingest());
        }

        await AssertSameAsServerAsync(node, client, "trading");
        Assert.Equal("""{"p":500}""", client.Widgets["price"].Json);
        Assert.Equal(500, client.ContiguousVersion);
        Assert.True(client.DeltasApplied < 400, $"expected conflation, got {client.DeltasApplied} deltas for 500 updates");
        Assert.Equal(0, client.Resyncs);
    }

    [Fact]
    public async Task Lost_pushes_are_detected_by_version_gaps_and_repaired_from_the_log()
    {
        await redis.FlushAsync();
        var faults = new DropBatches(batch => batch.Versions[0] % 4 == 0);
        await using var node = await ServerNode.StartAsync(redis.ConnectionString, new ServerOptions { Faults = faults });
        await using var client = node.Client("ops2", User("u"), heartbeatSeconds: 0.5);
        await client.ConnectAsync();

        for (var i = 1; i <= 120; i++)
        {
            await node.PutAsync("ops2", $"w{i % 9}", $$"""{"n":{{i}}}""", Ingest());
        }

        await AssertSameAsServerAsync(node, client, "ops2");
        Assert.True(faults.Dropped >= 25);
        Assert.True(client.Resyncs >= 1);
        Assert.Equal(1, client.Snapshots);
    }

    [Fact]
    public async Task A_lost_final_push_is_found_through_the_heartbeat_head_version()
    {
        await redis.FlushAsync();
        var faults = new DropBatches(batch => batch.Versions[^1] == 10);
        await using var node = await ServerNode.StartAsync(redis.ConnectionString, new ServerOptions { Faults = faults });
        await using var client = node.Client("tail", User("u"), heartbeatSeconds: 0.5);
        await client.ConnectAsync();

        for (var i = 1; i <= 10; i++)
        {
            await node.PutAsync("tail", "w", $$"""{"n":{{i}}}""", Ingest());
        }

        await AssertSameAsServerAsync(node, client, "tail");
        Assert.Equal("""{"n":10}""", client.Widgets["w"].Json);
        Assert.Equal(1, faults.Dropped);
    }

    [Fact]
    public async Task Two_editors_racing_on_one_widget_cannot_both_win()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString);
        await using var first = node.Client("layout", User("ayse", role: "editor"));
        await using var second = node.Client("layout", User("mehmet", role: "editor"));
        await Task.WhenAll(first.ConnectAsync(), second.ConnectAsync());
        var seed = await first.UpdateWidgetAsync("chart", """{"x":0}""");
        Assert.True(seed.Applied);

        var results = await Task.WhenAll(
            first.UpdateWidgetAsync("chart", """{"x":1}""", expectedWidgetVersion: seed.WidgetVersion),
            second.UpdateWidgetAsync("chart", """{"x":2}""", expectedWidgetVersion: seed.WidgetVersion));

        Assert.Equal(1, results.Count(r => r.Applied));
        var loser = results.Single(r => !r.Applied);
        Assert.NotNull(loser.ConflictJson);
        Assert.Equal(2, loser.WidgetVersion);
        var winnerJson = results.Single(r => r.Applied) == results[0] ? """{"x":1}""" : """{"x":2}""";
        Assert.Equal(winnerJson, loser.ConflictJson);

        var retry = await (results[0].Applied ? second : first).UpdateWidgetAsync("chart", """{"x":3}""", expectedWidgetVersion: loser.WidgetVersion);
        Assert.True(retry.Applied);
        await AssertSameAsServerAsync(node, first, "layout");
        await AssertSameAsServerAsync(node, second, "layout");
    }
}

[Collection("redis")]
public class SecurityAndLimitTests(RedisFixture redis)
{
    private static string Ingest(string tenant) => Tokens.Create("bridge", tenant, "ingest");

    [Fact]
    public async Task Tenants_are_isolated_even_when_they_use_the_same_dashboard_id()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString);
        await using var acme = node.Client("board", Tokens.Create("a1", "acme"));
        await using var globex = node.Client("board", Tokens.Create("g1", "globex"));
        await Task.WhenAll(acme.ConnectAsync(), globex.ConnectAsync());

        await node.PutAsync("board", "secret", """{"revenue":"acme-only"}""", Ingest("acme"));
        await node.PutAsync("board", "other", """{"x":1}""", Ingest("globex"));

        Assert.True(await acme.WaitForVersionAsync(1, TimeSpan.FromSeconds(10)));
        Assert.True(await globex.WaitForVersionAsync(1, TimeSpan.FromSeconds(10)));
        await Task.Delay(300);
        Assert.Equal(["secret"], acme.Widgets.Keys);
        Assert.Equal(["other"], globex.Widgets.Keys);
        Assert.Equal(1, acme.ContiguousVersion);
        Assert.Equal(1, globex.ContiguousVersion);
        Assert.Equal(["a1"], acme.Viewers);
        Assert.Equal(["g1"], globex.Viewers);
    }

    [Fact]
    public async Task Connections_without_a_valid_token_are_refused()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString);

        var wrongKey = node.Client("x", Tokens.Create("u", "acme", key: "another-key-another-key-another-key-0000"));
        var expired = node.Client("x", Tokens.Create("u", "acme", lifetime: TimeSpan.FromMinutes(-20)));
        var anonymous = node.Client("x", string.Empty);

        await Assert.ThrowsAnyAsync<Exception>(() => wrongKey.ConnectAsync());
        await Assert.ThrowsAnyAsync<Exception>(() => expired.ConnectAsync());
        await Assert.ThrowsAnyAsync<Exception>(() => anonymous.ConnectAsync());
        Assert.Equal(0, node.Stats.Connections);
    }

    [Fact]
    public async Task Only_ingest_tokens_may_write_through_the_api_and_viewers_cannot_edit_through_the_hub()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString);
        await using var viewer = node.Client("ro", Tokens.Create("v", "acme", "viewer"));
        await viewer.ConnectAsync();

        var plain = await node.PutAsync("ro", "w", """{"a":1}""", Tokens.Create("someone", "acme"));
        var ok = await node.PutAsync("ro", "w", """{"a":1}""", Ingest("acme"));
        var edit = await Assert.ThrowsAsync<HubException>(() => viewer.UpdateWidgetAsync("w", """{"a":2}"""));

        Assert.Equal(403, plain.Status);
        Assert.Equal(200, ok.Status);
        Assert.Contains("Viewers cannot edit", edit.Message);
    }

    [Fact]
    public async Task Bad_input_is_rejected_before_it_reaches_the_store()
    {
        await redis.FlushAsync();
        await using var node = await ServerNode.StartAsync(redis.ConnectionString, new ServerOptions { MaxWidgetsPerDashboard = 3 });
        await using var client = node.Client("lim", Tokens.Create("u", "acme"));
        await client.ConnectAsync();

        var huge = new string('x', 20_000);
        Assert.Equal(400, (await node.PutAsync("lim", "w", $$"""{"blob":"{{huge}}"}""", Ingest("acme"))).Status);
        Assert.Equal(400, (await node.PutAsync("bad id!", "w", "{}", Ingest("acme"))).Status);
        Assert.Equal(400, (await node.PutAsync("lim", "bad widget", "{}", Ingest("acme"))).Status);

        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(200, (await node.PutAsync("lim", $"w{i}", "{}", Ingest("acme"))).Status);
        }

        Assert.Equal(400, (await node.PutAsync("lim", "w3", "{}", Ingest("acme"))).Status);
        Assert.Equal(200, (await node.PutAsync("lim", "w0", """{"again":true}""", Ingest("acme"))).Status);
    }

    [Fact]
    public async Task Presence_is_shared_across_nodes_and_stale_entries_expire_without_heartbeats()
    {
        await redis.FlushAsync();
        await using var a = await ServerNode.StartAsync(redis.ConnectionString, new ServerOptions { PresenceTtlSeconds = 2 });
        await using var b = await ServerNode.StartAsync(redis.ConnectionString, new ServerOptions { PresenceTtlSeconds = 2 });
        await using var ayse = a.Client("room", Tokens.Create("ayse", "acme"), heartbeatSeconds: 0.5);
        await using var mehmet = b.Client("room", Tokens.Create("mehmet", "acme"), heartbeatSeconds: 1000);
        await ayse.ConnectAsync();
        await mehmet.ConnectAsync();

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (ayse.Viewers.Length < 2 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50);
        }

        Assert.Equal(["ayse", "mehmet"], ayse.Viewers);

        await Task.Delay(TimeSpan.FromSeconds(3));
        await using var observer = a.Client("room", Tokens.Create("zeynep", "acme"));
        await observer.ConnectAsync();

        Assert.Equal(["ayse", "zeynep"], observer.Viewers);

        await mehmet.DisposeAsync();
        await ayse.DisposeAsync();
    }
}
