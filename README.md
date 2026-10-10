# SyncSphere — A Real-Time Collaborative Dashboard Backend

**English** · [Türkçe](README.tr.md)

A backend that pushes live ERP/CRM data changes to thousands of connected browsers spread over several server nodes, **in order, without loss and with low latency**.
ASP.NET Core SignalR + a Redis backplane, .NET 10.

SignalR with a Redis backplane is an "at-most-once" transport: messages can be lost and can arrive out of order. On top of it this repo builds a stream with version numbers and a replay log in which
**the client notices loss and reordering and repairs itself**, and it tests that by dropping messages on purpose.

## What it does

| Area | Approach | Evidence |
|---|---|---|
| **Multi-node fan-out** | Hub groups + a Redis backplane; a change accepted on one node reaches clients on every node | Test: 2 nodes, 200 clients, 200 updates → every client's state is identical to the server snapshot |
| **One version per dashboard** | A single Redis Lua script: conflict check + version increment + state write + log append, atomically | Test: 300 concurrent publishers on 2 nodes → versions are exactly `1..300`, no gaps or repeats |
| **Repair of loss and reordering** | The client tracks versions; if a gap persists for `GapTimeoutMs` it fills it from the log with `Replay(version)`, falling back to a snapshot if the log is too short | Test: every 4th push is dropped on purpose → the client still reaches the same state without a snapshot. If the last push is dropped, the heartbeat (head version) finds it |
| **Conflation** | Updates to the same widget within a window collapse into one message; the message carries **all versions it covers**, so the client does not mistake that for loss | Test: 500 updates to one widget → the client applies <400 deltas, final state and version exact, 0 resyncs |
| **Reconnection** | Resumes from the last version: only what was missed is sent | Test: connection drops, 39 updates are missed, it reconnects → no extra snapshot; if the log is too short it falls back to a snapshot |
| **Multi-tenancy** | The group name is derived from the token's `tenant` claim; a client cannot address another tenant's dashboard | Test: two tenants using the same dashboard id never see each other's data or viewers |
| **Authorization** | JWT (`access_token` query for WebSockets); the write API needs the `ingest` role; the `viewer` role cannot edit through the hub | Test: 401/403 and hub rejection |
| **Collaborative editing** | Optimistic concurrency with `expectedWidgetVersion`; a conflict returns the current state | Test: two editors on one widget → exactly one wins, the loser sees the current JSON and retries |
| **Presence** | A Redis sorted set with TTL; refreshed by the heartbeat and shared between nodes | Test: visible across two nodes; a user whose heartbeat stops drops out after the TTL |
| **Limits** | Widget ≤16 KB, ≤500 widgets per dashboard, ≤20 dashboards per connection, authentication | Test (except the per-connection dashboard limit) |

## Measurements

AMD Ryzen 7 9800X3D (8 cores / 16 threads), Windows, Release build. **Two server nodes + Redis 8 (Docker) + the load generator on the same machine.** Delivery latency =
the time between the server accepting a change (ms timestamp) and the client applying it; one clock, millisecond resolution. The conflation window is 25 ms (adds ~12 ms on average).

| Connected clients | Update rate | Deliveries | Latency p50 / p95 / p99 / max | Result |
|---|---|---|---|---|
| 2,000 | 200/s, 20 s | 7.89M (394k/s) | 46 / 78 / 91 / 470 ms | 2000/2000 converged, 0 resyncs |
| 5,000 | 100/s, 20 s | 9.97M (499k/s) | 106 / 246 / 356 / 501 ms | 5000/5000 converged, 0 resyncs |
| 10,000 | 100/s | invalid | saturated | 10,000 connections were established but the load-generator process swelled to ~5 GB, the server `keep-alive` limit was exceeded and connections dropped |

**The 10,000 row is not a server result.** On one machine the load generator (10k `HubConnection`s) + 2 servers + Redis share the same CPU and memory; the measurement rig saturated.
Because load generation was not done on separate machines, a single server handling 10k concurrent connections was **not proven**. The correct reading: on this one machine, at 5k connections,
~500k deliveries per second, p99 < 400 ms, lossless.

Tool: [bench/SyncSphere.LoadTest](bench/SyncSphere.LoadTest) (`--clients --rate --seconds --servers`).

## Quick start

```bash
docker compose up -d --build        # Redis + 2 nodes (5101, 5102); Development environment, /dev/token is available
ING=$(curl -s "localhost:5101/dev/token?sub=erp&tenant=acme&role=ingest" | jq -r .token)
curl -X PUT localhost:5101/api/dashboards/sales/widgets/revenue -H "authorization: Bearer $ING" \
     -H 'content-type: application/json' -d '{"json":"{\"today\":128450}"}'
curl localhost:5102/api/dashboards/sales -H "authorization: Bearer $ING"      # the same state from the other node
curl localhost:5101/stats
```

The client: [SyncSphere.Client](src/SyncSphere.Client): connection, version tracking, gap repair, automatic reconnection, snapshot and replay.

```csharp
await using var client = new DashboardClient(new DashboardClientOptions
{
    HubUrl = new Uri("http://localhost:5101/hubs/dashboard"),
    DashboardId = "sales",
    AccessToken = () => Task.FromResult<string?>(token),
});
await client.ConnectAsync();
var widgets = client.Widgets;
```

## Architecture

```
ERP/CRM ──PUT──▶ Node A ─┐          ┌─▶ Node A clients
                          ├─ Lua ─▶ Redis ──backplane──┤
browser ──hub───▶ Node B ─┘  (version++, state, log)     └─▶ Node B clients
                                 │
             Replay(version) ◀───┘   (the client pulls from the log when it sees a gap)
```

Server rule: on join the connection is added to the group first, then the snapshot/replay is read (a push in between may overlap, but no gap can occur; the client drops repeats using the version and the widget version).
Decisions: [docs/adr/en](docs/adr/en).

## Tests

```bash
dotnet test --solution SyncSphere.slnx     # needs Docker (real Redis 8 via Testcontainers), servers are real Kestrel
```

14 tests; eight consecutive full runs passed.

**Bugs the tests and the load test found:**
1. The Redis connection was created lazily; under a sudden burst of 200 connections the thread pool saturated, the connection timed out and a few clients hung. It is now created at startup with `AbortOnConnectFail=false`.
2. The heartbeat triggered a resync as soon as it saw "head version greater than the client's", which is normal in a live stream, so all clients entered repair at once (~1,700 needless resyncs at 2,000 clients).
   A gap is now repaired only if it persists **without progress** for `GapTimeoutMs`; result: 0 resyncs at the same load.
3. The gap age was counted from the first gap; in a continuous stream there is always a tiny gap, so the age never reset. Any continuous version progress now resets the timer.
4. The 10,000-client attempt hit client-side port exhaustion: every `HubConnection` held a separate TCP connection for negotiate (2 sockets per client, ~16k ephemeral ports on Windows). A `WebSocketsOnly` option skips negotiate.

## Known limits (an honest list)

- Measurements were taken on one machine over loopback; distributed load generation from separate machines was not done. 10,000 concurrent connections were **not demonstrated**.
- Redis backplane pub/sub delivers "at most once". Loss and reordering are repaired with versions and replay (tested with dropped pushes), but a **real Redis outage** (stopping the container) was not tested.
- Sticky sessions behind a load balancer and automatic reconnection after a node death were not tried end to end; in the tests clients connect directly to one node.
- Each dashboard writes to a single Redis key group (one hash tag); a very hot dashboard is limited by the Lua throughput of one Redis shard.
- The replay log is bounded (10,000 versions by default). A client that stays offline longer falls back to a snapshot.
- Conflation is windowed and per node: subscribers are not guaranteed to see every intermediate value, only the last value and the version coverage. A consumer that needs every event should use the replay log.
- If a node crashes, presence records stay until the TTL (30 s) expires; there is no immediate broadcast.
- JSON protocol; MessagePack would reduce bandwidth and CPU but was not tried. Widget content is opaque JSON with no schema validation.
- JWT validation was tested with a symmetric key; not tried against a real OIDC `Authority`. `/stats` is unauthenticated.
- There is no per-user rate limit (only size and count limits).
