# Iceberg REST frontend, phase 3: observability — design

Date: 2026-07-27
Branch: `feature/iceberg-rest-fe-phase1`
Status: approved

## Goal

Make REST traffic visible in the proxy's existing observability stack. Metrics only:
per-RPC audit events already flow through `RoutingMetaStoreProxy` with the right
`authenticatedUser`, and no separate HTTP audit log is added.

## Metrics

All in the existing hand-rolled `PrometheusMetrics` (same style, same label-cardinality
cap, no new dependencies):

- `hms_proxy_rest_requests_total{prefix, route, status}` — counter, one increment per
  HTTP request handled by the Iceberg REST listener.
- `hms_proxy_rest_request_duration_seconds{prefix, route}` — histogram, same buckets as
  `hms_proxy_request_duration_seconds`.
- `hms_proxy_rest_listener_info{bind_host, port}` — info gauge set to 1 when the
  listener starts (mirrors `hms_proxy_synthetic_read_lock_store_info`).

Label rules:

- `route` is never the raw URL. It is the adapter's parsed `Route` enum name in lower
  case (`config`, `list_namespaces`, `load_table`, ...), or one of the pseudo-routes
  `unknown_prefix`, `unknown_route`, `bad_request` for requests refused before dispatch.
- `prefix` is the catalog prefix, or `unknown` when the request failed before a
  configured catalog was resolved.
- `status` is the numeric HTTP status actually written.

Cardinality: prefixes are bounded by the catalog list plus `unknown`; routes by the
enum plus three pseudo-values; status by HTTP codes — all inside the existing series cap.

## Wiring

`ProxyObservability.metrics()` is already available in `HmsProxyApplication`. It is
passed through `RestCatalogServer.open(config, services, metrics)` into the
`IcebergHttpHandler` constructor. `doHandle` is wrapped with a timer; the record happens
in `finally` with the status that was actually written (the handler tracks the last
written status). The info gauge is set in `RestCatalogServer.open` after a successful
bind. Metrics are required (non-null): tests construct a real `PrometheusMetrics` and
may assert on its `render()` output — no null/no-op path exists.

## Error handling

No behavioral change to responses. Metric recording must never fail a request: recording
happens after the response bytes are written.

## Testing

- Unit: route normalization (enum name mapping, pseudo-routes) and label values.
- Integration (`IcebergRestEndpointIntegrationTest`): after hitting 200/400/404 paths,
  `PrometheusMetrics.render()` contains the expected series.
- Smoke: `--scenario rest` gains a metrics step — after the REST checks, fetch the
  management `/metrics` endpoint (`HMS_SMOKE_REST_METRICS_URL`, stand:
  `http://localhost:19090/metrics`) and grep `hms_proxy_rest_requests_total` and
  `hms_proxy_rest_listener_info`; skipped when the variable is unset.
- Docs: README/CHANGELOG both locales (metrics table extended), stand TEST-MATRIX row
  G17 after the stand run.

## Out of scope

- Separate HTTP-level audit events.
- Readiness/health changes.
- Any change to REST response behavior.
