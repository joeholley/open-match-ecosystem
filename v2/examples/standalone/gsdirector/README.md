# Standalone Game Server Director (`gsdirector`)

The backend half of the [standalone example](../../README.md). Each matchmaking cycle it asks a (mock) game server manager what matches are needed, sends the resulting profiles to Open Match 2 (`om-core`) with `InvokeMatchmakingFunctions`, evaluates the proposed matches, allocates game servers for the accepted ones, and publishes the assignments to Google Cloud Pub/Sub for the [mmqueue](../mmqueue/README.md) instances. It runs the same [`internal/gsdirector`](../../../internal/gsdirector) package as the all-in-one [matchmaker](../../matchmaker/README.md) harness. Run **one** instance (or one per partition of your fleet): it keeps match state in memory.

On startup it checks the connection to `OM_CORE_ADDR` once (with a 10 second limit) and exits with an error if `om-core` can't be reached or rejects the request. When the director stops on its own (`NUM_MM_CYCLES`, or `NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT` empty cycles in a row), the process exits.

## Run locally

Start `om-core` (port `8080` by default) and the [mmf](../../mmf/README.md) (port `50090` by default), create a Pub/Sub topic for assignments, then from `v2/examples/standalone/gsdirector/`:

```bash
LOGGING_FORMAT=text LOGGING_LEVEL=debug OTEL_SIDECAR=false \
GCP_PROJECT_ID=${PROJECT_ID} ASSIGNMENT_TOPIC_ID=${TOPIC} \
go run .
```

The director listens on `8083` (`curl localhost:8083/` shows the latest cycle's status) and serves Prometheus metrics on `2225`. Run the [mmqueue](../mmqueue/README.md) (port `8082`) to create tickets.

`SOLODUEL_ADDR`/`SOLODUEL_PORT` aren't called by the director: they're passed to `om-core`, which calls the MMF. They must be reachable **from `om-core`**.

## HTTP endpoints

| Endpoint | Description |
| :--- | :--- |
| `GET /` | Plain-text status of the latest matchmaking cycle (accepted/rejected matches and tickets, cycle duration, backoff). |
| `/debug/pprof/` | Go profiling endpoints. They have no authentication, so only deploy with internal, authenticated ingress (as the Cloud Run deploy below does). |

## Environment variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8083` | HTTP port. Cloud Run sets this itself. |
| `OM_CORE_ADDR` | `"http://localhost:8080"` | Base URL of `om-core`. On Cloud Run, use the exact `https://` service URL (see below). |
| `OM_CORE_REQUEST_TIMEOUT_MS` | `5000` | Per-attempt timeout for unary `om-core` calls (ticket re-activation, the startup connection check). |
| `OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL` | `500` | Maximum ticket IDs per re-activation call. Must be less than or equal to `om-core`'s `OM_MAX_STATE_UPDATES_PER_CALL`, or every activation call fails. |
| `OM_CORE_MAX_CONCURRENT_ACTIVATION_CALLS` | `10` | Maximum concurrent re-activation calls. |
| `SOLODUEL_ADDR` | `"http://localhost"` | Address of the `fifo` MMF, as `om-core` should call it. On Cloud Run, the MMF's `https://` URL: `om-core` only authenticates to MMFs (with an ID token from its own identity, which needs `roles/run.invoker` on the MMF) when the scheme is `https`. |
| `SOLODUEL_PORT` | `50090` | Port of the `fifo` MMF (`443` on Cloud Run). Matches the standalone mmf's default local `PORT`. |
| `DEBUGMMF_ADDR` | `"none"` | Address of the optional `debug` MMF. `"none"` (or empty) disables it. |
| `DEBUGMMF_PORT` | `50081` | Port of the `debug` MMF (`443` on Cloud Run). |
| `NUM_MM_CYCLES` | `2147483647` | Number of matchmaking cycles to run before the process exits. |
| `MM_CYCLE_MIN_DURATION_MS` | `5000` | Base minimum cycle length; the director sleeps for the rest (scaled up by adaptive backoff when it sees collisions). |
| `MMF_INVOCATION_TIMEOUT_SECS` | `600` | Director-side deadline for each cycle's `InvokeMatchmakingFunctions` calls. The default matches `om-core`'s `OM_MMF_TIMEOUT_SECS` default; `0` means no director-side deadline. |
| `NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT` | `2147483647` | Exit after this many consecutive cycles with no proposed matches. |
| `REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT` | `false` | Reject matches that `om-core` flagged with `deactivation_timeout=true`. |
| `WAIT_FOR_BACKFILL_MMFS` | `false` | Run `backfill.` profiles to completion before the other profiles each cycle. |
| `RANDOM_MATCH_REJECT_PERCENT` | `1` | Percentage of proposed matches randomly rejected, to exercise the ticket re-activation path. `0` disables it. |
| `ASSIGNMENT_TTL_MS` | `1200000` | How long (20 minutes) the director remembers assigned tickets, to reject proposals that contain one while `om-core` is still replicating its deactivation. Keep it at least as long as `om-core`'s `OM_CACHE_TICKET_TTL_MS`. |
| `ASSIGNMENT_DISTRIBUTION_PATH` | `""` | Anything other than `"channel"` publishes to Pub/Sub. `"channel"` only works in a single process with the queue, so here it discards assignments. |
| `GCP_PROJECT_ID` | `"my-project-id"` | Project containing the assignment topic. |
| `ASSIGNMENT_TOPIC_ID` | `"replace-me"` | Existing Pub/Sub **topic** to publish assignments to (the same topic the mmqueue instances subscribe to). |
| `LOGGING_FORMAT` | `"json"` | `"json"` (Cloud Logging) or `"text"`. |
| `LOGGING_LEVEL` | `"info"` | `trace`, `debug`, `info`, `warn`, `error`, `fatal`. |
| `LOG_CALLER` | `"false"` | Include caller file and line in logs. |
| `OTEL_SIDECAR` | `"true"` | `true` exports metrics over OTLP to a collector sidecar; `false` serves Prometheus metrics on `OTEL_PROM_PORT`. |
| `OTEL_PROM_PORT` | `2225` | Prometheus port used when `OTEL_SIDECAR=false`. |

## Deploy to Cloud Run

Build the image from `v2/` (see the [matchmaker README](../../matchmaker/README.md#build), with `_GOOGLE_BUILDABLE=./examples/standalone/gsdirector`), then run [`deploy/cloudbuild.yaml`](deploy/cloudbuild.yaml) from `deploy/`:

```bash
cd v2/examples/standalone/gsdirector/deploy
gcloud builds submit --async --no-source \
  --substitutions=_OM_CORE_ADDR=${OM_URL},_SOLODUEL_ADDR=${FIFO_URL},_GCP_PROJECT_ID=${PROJECT_ID},_ASSIGNMENT_TOPIC_ID=${TOPIC}
```

* **`OM_CORE_ADDR` must be the exact `https://` URL of the `om-core` service.** When `K_REVISION` is set (Cloud Run sets it), the client authenticates every `om-core` call with an ID token whose audience is that URL, and the service account needs `roles/run.invoker` on `om-core`.
* **`SOLODUEL_ADDR` must include `https://`** (with `_SOLODUEL_PORT=443`), and `om-core`'s service account needs `roles/run.invoker` on the MMF.
* The service account needs `roles/pubsub.publisher` on the topic.
* The deploy pins a single instance (`--min-instances=1 --max-instances=1`), allocates CPU always (`--no-cpu-throttling`, needed for the background loop), uses internal ingress with authentication required, and runs an OpenTelemetry Collector sidecar.
* Shutdown is bounded to fit Cloud Run's ~10 second `SIGTERM` grace period: stop accepting requests, stop the director, flush metrics, then flush and stop the publisher.
