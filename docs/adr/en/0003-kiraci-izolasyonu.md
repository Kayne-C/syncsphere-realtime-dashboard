# ADR 0003 — Tenant isolation is derived from the token

**Status:** Accepted

- Which tenant a client belongs to comes **only from the `tenant` claim of the validated JWT**; hub methods have no tenant parameter. The group name (`{tenant}/{dashboard}`), the Redis keys (`ss:{tenant:dashboard}:…`) and the presence records are all built from the same value.
  A client has no way to address another tenant's dashboard (even if an authorization check were forgotten).
- A token without a `tenant` claim cannot reach any endpoint (fallback policy).
- Dashboard and widget ids are validated with `^[A-Za-z0-9_-]{1,64}$`; characters such as `/`, `:` and `{` cannot get into a Redis key or a group name.
- Writing: the REST endpoint needs the `ingest` role; editing through the hub is closed to the `viewer` role. `Join` is required before editing.
- Test: the same dashboard id for two tenants: data, versions and viewers are completely separate.
- Limit: there is no per-tenant quota or rate limit.
