# Standalone Matchmaking Queue (`mmqueue`)

The game client-facing half of the [standalone example](../../README.md): it receives player matchmaking requests, creates and activates tickets in Open Match 2 (`om-core`), and receives the assignments the [game server director](../gsdirector/README.md) publishes over Google Cloud Pub/Sub. It runs the same [`internal/mmqueue`](../../../internal/mmqueue) package as the all-in-one [matchmaker](../../matchmaker/README.md) harness, but on its own so you can scale it horizontally.

On startup it checks the connection to `OM_CORE_ADDR` once (with a 10 second limit) and exits with an error if `om-core` can't be reached or rejects the request. Only then does it create its Pub/Sub subscription.

## Run locally

Start `om-core` (it listens on `8080` by default), create a Pub/Sub topic for assignments, then from `v2/examples/standalone/mmqueue/`:

```bash
LOGGING_FORMAT=text LOGGING_LEVEL=debug OTEL_SIDECAR=false \
GCP_PROJECT_ID=${PROJECT_ID} ASSIGNMENT_TOPIC_ID=${TOPIC} \
go run .
```

The queue listens on `8082` and serves Prometheus metrics on `2224`. Run the [gsdirector](../gsdirector/README.md) (port `8083`) and the [mmf](../../mmf/README.md) (port `50090`) alongside it to get matches.

```bash
curl -X POST localhost:8082/tickets   # create one ticket
curl localhost:8082/tpc/100            # generate 100 tickets per cycle
```

## HTTP endpoints

| Endpoint | Description |
| :--- | :--- |
| `POST /tickets` | Creates one mock ticket (`gameclient.Simple`) and waits until it has been created and activated in `om-core`. Responds `200 OK` when the ticket is active, `502 Bad Gateway` if `om-core`'s `CreateTicket` or `ActivateTickets` call failed, and `503 Service Unavailable` while the queue is shutting down. If the client disconnects first, no response is written, and the queue skips creating the ticket or makes sure it ends up inactive in `om-core` (counted in `mmqueue.ticket.abandonments`), so the player isn't matched. Other methods get `405 Method Not Allowed`. |
| `/tpc/{tixPerCycle}` | Sets how many mock tickets the background generator creates each `TICKET_CREATION_CYCLE_DURATION_MS` cycle (`0`–`10000`; `0` stops it). `400 Bad Request` for anything else. |
| `/debug/pprof/` | Go profiling endpoints. They have no authentication, so only deploy with internal, authenticated ingress (as the Cloud Run deploy below does). |

Metrics are served on a separate port (`OTEL_PROM_PORT`) when `OTEL_SIDECAR=false`, otherwise exported to the OpenTelemetry Collector sidecar.

## Environment variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8082` | HTTP port. Cloud Run sets this itself. |
| `OM_CORE_ADDR` | `"http://localhost:8080"` | Base URL of `om-core`. On Cloud Run, use the exact `https://` service URL (see below). |
| `OM_CORE_REQUEST_TIMEOUT_MS` | `5000` | Per-attempt timeout for unary `om-core` calls (`CreateTicket`, `ActivateTickets`, `DeactivateTickets`, the startup connection check). |
| `OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL` | `500` | Maximum ticket IDs per `ActivateTickets` call. Must be less than or equal to `om-core`'s `OM_MAX_STATE_UPDATES_PER_CALL`, or every activation call fails. |
| `OM_CORE_MAX_CONCURRENT_ACTIVATION_CALLS` | `10` | Maximum concurrent `ActivateTickets` calls. When reached, backpressure propagates to ticket creation. |
| `MAX_CONCURRENT_TICKET_CREATIONS` | `500` | Maximum concurrent `CreateTicket` calls. |
| `MMQUEUE_TICKET_TTL_MS` | `600000` | How long the queue tracks a ticket before treating it as expired in `om-core`. Keep it equal to `om-core`'s `OM_CACHE_TICKET_TTL_MS`. |
| `INITIAL_TPC` | `0` | Tickets per cycle the background generator starts with (`0`–`10000`; `0` = off until `/tpc` is called). |
| `TICKET_CREATION_CYCLE_DURATION_MS` | `1000` | Length of one ticket generation cycle. |
| `TICKET_CREATION_CYCLES` | `2147483647` | Number of generation cycles before the generator stops. |
| `TICKET_ARRIVAL_SEED` | `0` | Seed for the Poisson arrival schedule; `0` picks a random seed each run. |
| `ASSIGNMENT_DISTRIBUTION_PATH` | `"pubsub"` | `"pubsub"` receives assignments from Pub/Sub. `"channel"` only works in a single process with the director, so here it discards assignments. |
| `GCP_PROJECT_ID` | `"my-project-id"` | Project containing the assignment topic. |
| `ASSIGNMENT_TOPIC_ID` | `"replace-me"` | Existing Pub/Sub **topic** the director publishes assignments to. Each instance creates its own subscription to it and deletes it on shutdown. |
| `LOGGING_FORMAT` | `"json"` | `"json"` (Cloud Logging) or `"text"`. |
| `LOGGING_LEVEL` | `"info"` | `trace`, `debug`, `info`, `warn`, `error`, `fatal`. |
| `LOG_CALLER` | `"false"` | Include caller file and line in logs. |
| `OTEL_SIDECAR` | `"true"` | `true` exports metrics over OTLP to a collector sidecar; `false` serves Prometheus metrics on `OTEL_PROM_PORT`. |
| `OTEL_PROM_PORT` | `2224` | Prometheus port used when `OTEL_SIDECAR=false`. |

## Deploy to Cloud Run

Build the image from `v2/` (see the [matchmaker README](../../matchmaker/README.md#build), with `_GOOGLE_BUILDABLE=./examples/standalone/mmqueue`), then run [`deploy/cloudbuild.yaml`](deploy/cloudbuild.yaml) from `deploy/`:

```bash
cd v2/examples/standalone/mmqueue/deploy
gcloud builds submit --async --no-source \
  --substitutions=_OM_CORE_ADDR=${OM_URL},_GCP_PROJECT_ID=${PROJECT_ID},_ASSIGNMENT_TOPIC_ID=${TOPIC}
```

* **`OM_CORE_ADDR` must be the exact `https://` URL of the `om-core` service.** When `K_REVISION` is set (Cloud Run sets it), the client authenticates every `om-core` call with an ID token whose audience is that URL, and the service account needs `roles/run.invoker` on `om-core`.
* The service account also needs permission to create and delete subscriptions on the topic (for example `roles/pubsub.editor`). Every instance gets its own subscription, so every instance receives every assignment and ignores the ones for tickets it isn't tracking (counted in `mmqueue.ticket.assignments.untracked`); see [Fan-out](../../../internal/assignmentdistributor/README.md#fan-out-every-queue-instance-receives-every-assignment).
* The deploy uses internal ingress with authentication required, CPU always allocated (`--no-cpu-throttling`, needed for the background loops), and an OpenTelemetry Collector sidecar.
* Shutdown is bounded to fit Cloud Run's ~10 second `SIGTERM` grace period: stop accepting requests, stop background work, flush metrics, then delete the subscription.
