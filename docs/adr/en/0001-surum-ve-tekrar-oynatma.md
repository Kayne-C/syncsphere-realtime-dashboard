# ADR 0001 — Version numbers + a replay log: a reliable stream over an unreliable transport

**Status:** Accepted

## Context

SignalR group broadcast and the Redis backplane (pub/sub) give no delivery guarantee: messages are lost while a node restarts, when the Redis connection drops or while a client reconnects.
On a dashboard, one lost update means showing a wrong number until the next update arrives (or never correcting it).

## Decision

- Every dashboard has a single, increasing **version**. A Lua script increments the version atomically, writes the widget and appends the change to a Redis Stream (`ID = version-0`). The stream is capped with `MAXLEN`.
- The server answers "snapshot" and "changes with version > x" from the same structure (`Join`, `Replay`, `GetSnapshot`).
- The client keeps a contiguous version boundary (`contiguous`) and accumulates versions that arrive ahead of it in a set. If progress stalls for `GapTimeoutMs` it calls `Replay(contiguous)`; if the log does not reach back far enough it falls back to a snapshot.
- The apply rule is "the newest wins" by **widget version**; repeats, reordering and conflation cannot stop the state from converging.
- For loss at the tail (the last push is dropped and nothing follows) the heartbeat returns the server's current version; if the client is behind, it repairs.

## Consequences

- There is no "exactly once"; there is "converges, and loss is detected". The cost is one Redis key group and a bounded log.
- Test: with fault injection that drops pushes (every 4th, and separately only the last one) the client ends in the same state as the server in both cases.
