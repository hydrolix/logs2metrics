# hydrolix-metrics-go

A minimal, production-ready application for collecting data from Hydrolix and exporting to various time-series metric sinks.

Inspiration for this project came from the development of a similar metric exporter that I wrote for Kafka and based on the Fitstar stats-collector.

## Quickstart

```bash
Usage:
  hydrolix-collector [flags]
  hydrolix-collector [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  version     Print version info

Flags:
  -C, --concurrency uint16             concurrency level for the exporter, how many simultaneous requests to make (default 1)
  -e, --environment string             environment that the exporter runs in (default "dev")
  -f, --flush int32                    how frequently the metric sinks should export metrics in seconds (default 15)
      --healthz string                 healthcheck endpoint path (default "/healthz")
      --healthz-addr string            address for the healthcheck listener; ignored when the prometheus sink is enabled, which serves healthz on its own port (default ":2112")
  -h, --help                           help for hydrolix-collector
  -i, --interval int32                 polling interval in seconds for the exporter, how frequently the exporter polls (default 15)
  -n, --namespace string               override metric namespace, default uses "hydrolix"
      --offset-end-minutes int         override lag offset in minutes for the query window end (0 = use config default)
      --offset-start-minutes int       override how many minutes back the query window starts (0 = use config default)
  -c, --config string                  path to queries.yaml config file (default: embedded config)
  -S, --sink strings                   metrics sinks to enable, options: datadog, prometheus, otel, statsd
  -B, --sink-datadog-backoff int       [DataDog Sink] initial backoff time for retries (default 1)
  -b, --sink-datadog-batch-size int    [DataDog Sink] size of batch of custom metrics for a payload (default 200)
  -M, --sink-datadog-max-retries int   [DataDog Sink] max retries before failure to send custom metrics (default 3)
  -q, --sink-datadog-queue-size int    [DataDog Sink] size of payload queue for custom metrics (default 25)
  -o, --sink-otel-endpoint string      [OpenTelemetry Sink] comma-separated list of host:port for endpoint
  -m, --sink-prometheus-path string    [Prometheus Sink] path for endpoint (default "/metrics")
  -d, --sink-statsd-addr string        [StatsD Sink] comma-separated list of host:port for StatsD daemons
  -p, --sink-prometheus-port string    [Prometheus Sink] port for prometheus endpoint (default ":2112")
  -s, --subsystem string               provide a subsystem for the namespace (default "exporter")
  -v, --verbose                        enable verbose output

Use "hydrolix-collector [command] --help" for more information about a command.
```

## Authentication

The collector reads Hydrolix credentials from environment variables:

| Variable       | Description                                  |
|----------------|----------------------------------------------|
| `HDX_HOST`     | Hydrolix host (required)                     |
| `HDX_TOKEN`    | Bearer token (preferred)                     |
| `HDX_USERNAME` | Username (used if token not set)             |
| `HDX_PASSWORD` | Password (used if token not set)             |

### Datadog sink credentials

When running with `--sink datadog`, the Datadog API client reads its own
credentials from environment variables (there is no corresponding CLI flag):

| Variable     | Description                                              |
|--------------|-----------------------------------------------------------|
| `DD_API_KEY` | Datadog API key (required)                               |
| `DD_SITE`    | Datadog site, e.g. `datadoghq.com`, `datadoghq.eu` (optional; defaults to `datadoghq.com`) |

## Deployment notes

### Single replica only

The poller has no leader election or other cross-instance coordination.
Running two or more instances against the same Hydrolix host and query
config causes problems: duplicate queries hit Hydrolix unnecessarily, the
push sinks (Datadog, StatsD, OTel) receive double-counted metrics, and the
Prometheus sink gets confused by multiple scrape targets exposing identical
series. Run exactly one replica per configuration. See `DESIGN.md` for why
this is safe in practice — the query window's backfill makes short restarts
self-healing without needing a second replica for availability.

## Build the Go Binary

```bash
make clean
make prepare
make lint
make test
make build
```

## Playground - Docker Compose

Spins up the collector (prometheus sink) + Prometheus + Grafana:

```bash
# Copy and fill in credentials
cp .env.example .env

docker compose up
```

| Service              | URL                     |
|----------------------|-------------------------|
| Prometheus           | http://localhost:9090   |
| Grafana              | http://localhost:3000   |
| Collector metrics    | http://localhost:2112/metrics |
| Collector health     | http://localhost:2112/healthz |

## Health Monitoring

The collector exposes an HTTP health endpoint (path from `--healthz`, default
`/healthz`) that returns `200` while the poller loop is running and the most
recent poll round didn't fail with auth errors on every query, and `503`
otherwise (for example, an expired or invalid token).

This narrowly targets the auth-dead failure mode, not "is the collector
working" in general: connectivity failures, server-side errors, and
malformed responses do **not** flip it to `503` as long as at least one
query in the round didn't fail on auth specifically. Alert separately on the
`hydrolix.collector.poll` self-metric (`status=error` vs `status=success`)
for those cases.

### Docker / Compose

The image ships a `HEALTHCHECK` instruction, and the example
`docker-compose.yml` declares the same check, surfacing the result as
`healthy`/`unhealthy` in `docker ps` / `docker compose ps` on a fixed
interval.

## Query Configuration

Queries are defined in a YAML config file (`queries/queries.yaml`). The embedded default config ships with Akamai CDN queries, but you can provide your own via `--config`:

```bash
hydrolix-collector --config ./my-queries.yaml --sink prometheus
```

To add a new query, create a `.sql` file and add an entry to the YAML -- no Go code changes required. See `queries/queries.yaml` for the full schema including global/per-query tags, offset overrides, dimension mappings, and array column support for percentile metrics.

Note that `sql_file` paths are resolved **relative to the config file's directory**, not the working directory. A config at `examples/prod/queries.yaml` referring to `sql_file: edge.sql` loads `examples/prod/edge.sql`, whatever directory you run the collector from.

## Features
- YAML-driven query configuration -- add queries without writing Go
- Cobra command structure (`root`, `version`)
- Verbose logging via `log/slog`
- ldflags-based versioning (version, commit, date)
- Makefile with build/test/format/vet

## Releasing
TODO: add Goreleaser, CI, etc.
