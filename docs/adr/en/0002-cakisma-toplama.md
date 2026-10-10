# ADR 0002 — Conflation and version accounting

**Status:** Accepted

- For high-frequency KPIs, sending every change to every client wastes network and client CPU. A node buffers a short window per dashboard (default 25 ms) and sends **only the latest update per widget**.
- Conflation must not break gap detection. So every message carries the **list of all versions it covers** (`Versions`); the client counts those versions as accounted for even if their deltas were skipped. A gap appears only for a version that no message covers.
- The window is per node; if two nodes write to the same dashboard, the versions are split between two messages. The client merges them through its version set, and reordering is resolved by the widget-version rule.
- Latency cost: on average half the window. `ConflationWindowMs=0` sends every change immediately.
- Test: 500 consecutive updates → the client applies <400 deltas, the final value and version are exact, and the resync count is 0.
