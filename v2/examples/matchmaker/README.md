# Development

This example matchmaker is a golang application designed to be built using [CNCF Buildpacks](https://www.cncf.io/projects/buildpacks/), and our primary build process specifically uses [Google Cloud's buildpacks](https://cloud.google.com/docs/buildpacks/overview). It requires the use of Open Match 2, and won't startup if it can't contact an OM2 instance.

## Build

This repo includes the files required to build [remotely using Cloud Build on Google Cloud](https://cloud.google.com/docs/buildpacks/build-application#remote_builds).  A typical build goes something like this, with the repo cloned,  `gcloud` initialized, and an existing [Docker Artifact Registry](https://cloud.google.com/artifact-registry/docs/docker/store-docker-container-images) called `myregistry`:
```
# Update the dependencies to the latest versions and kick off a Cloud Build
go get && go mod tidy && gcloud builds submit --async --substitutions=COMMIT_SHA=abc123
```
There is no `cloudbuild.yaml` or `Dockerfile` required for this. [Google Cloud's buildpacks](https://cloud.google.com/docs/buildpacks/overview) takes care of everything.

## Deploy
The `deploy/cloudbuild.yaml` file contains steps to deploy a copy of the `matchmaker-example` service to Cloud Run in Google Cloud.
This file should be populated with the following substitutions:
* `_ARTIFACT_REGISTRY_URI` and `_OTEL_SIDECAR_IMAGE`: Your Artifact Registry repository URI and the `otel-collector-sidecar` image URI (required when `OTEL_SIDECAR=true` so `gsdirector.*` and `mmqueue.*` OpenTelemetry metrics are exported to Google Cloud Monitoring).
* `_SERVICE_ACCOUNT`: Your Service Account created for `matchmaker-example`. The section below has steps on how to create it with the proper IAM roles.
* `_OM_CORE_ADDR`: Your `om-core` Cloud Run HTTPS URL reachable from Cloud Run.
* `_SOLODUEL_ADDR` / `_SOLODUEL_PORT` and `_DEBUGMMF_ADDR` / `_DEBUGMMF_PORT`: Your Cloud Run MMF service URLs (and port `443` when deployed on Cloud Run with `--use-http2`), or `http://localhost` to use the in-process local test MMF servers.

Because `examples/matchmaker` runs continuous background loops (`mmqueue.Run` and `gsdirector.Run`) outside incoming HTTP requests and maintains in-memory ticket and assignment state, `deploy/cloudbuild.yaml` deploys it as an always-on, non-throttled singleton by default (`--no-cpu-throttling --min-instances=1 --max-instances=1`) alongside the `opentelemetry-collector` sidecar container.

Alternatively, with a `service.yaml` file populated with your configuration, you can deploy the service with a command like this:
```
gcloud run services replace service.yaml
```
You may need to adjust the scaling configuration and amount of memory `om-core` is allowed to use depending on your matchmaker.

`core` is just a gRPC/HTTP golang application in a container image and can be deployed in most environments (local Docker/Minikube/Kubernetes/kNative) with a little effort. 

## Service Account
Best practices dictate that you deploy applications to Cloud Run with minimal IAM permissions. This creates a service account named `matchmaker-example-identity` that only has Cloud Run Invoker permissions (so it can talk to `om-core` running in Cloud Run). This service account is usable only by you and by the default Cloud Build service account.

```
export PROJECT_ID=$(gcloud config get project)
export PROJECT_NUMBER=$(gcloud projects list --filter=$(gcloud config get project) --format="value(PROJECT_NUMBER)")
export GCLOUD_ACCOUNT=$(gcloud config get account)

gcloud iam service-accounts create matchmaker-example-identity \
  --display-name="Example Matchmaker Service Account"

gcloud iam service-accounts add-iam-policy-binding \
  matchmaker-example-identity@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/iam.serviceAccountUser \
  --member=user:${GCLOUD_ACCOUNT}

gcloud iam service-accounts add-iam-policy-binding \
  matchmaker-example-identity@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/iam.serviceAccountUser \
  --member=serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
--member=serviceAccount:matchmaker-example-identity@${PROJECT_ID}.iam.gserviceaccount.com \
--role=roles/run.invoker
```

In addition, if you are trying to deploy using the step in the `cloudbuild.yaml`, your Cloud Build service account (`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`) will need permission to deploy to Cloud Run. This is not necessary if you are just manually deploying the built container image to Cloud Run using the Cloud Console UI or the `gcloud` CLI.

## Using the example matchmaker

The behavior of the example matchmaker can be controlled with environment variables. Here are some variables you may want to set when running locally:
* `LOGGING_FORMAT=text` - Make logs easier to read in the console.
* `LOGGING_LEVEL=debug` - See more logs for easier troubleshooting.
* `OTEL_SIDECAR=false` - Don't try to export metrics to an Open Telemetry instance; instead serve Prometheus metrics locally on `localhost:${OTEL_PROM_PORT}/metrics` (default `:2227/metrics`).
* `SOLODUEL_ADDR` / `SOLODUEL_PORT` (defaults `http://localhost` / `50080`) and `DEBUGMMF_ADDR` / `DEBUGMMF_PORT` (defaults `http://localhost` / `50081`) - Target host and port for the `fifo` and `debug` MMFs. When set to `http://localhost`, the matchmaker starts an in-process gRPC server on the configured port; when set to a remote URL (such as a Cloud Run `https://...` URL with port `443`), `om-core` invokes the remote MMF service over gRPC/TLS.
* `REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT=false` (default `false`) - Controls how `gsdirector` handles matches returned with `extensions["deactivation_timeout"] = true` (set by `om-core` when `OM_MATCH_TICKET_DEACTIVATION_WAIT=true` and local cache replication of ticket deactivations was not confirmed within `OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS`). When `false`, the director accepts the match (relying on the `assignments` map to catch cross-cycle collisions), logs a warning, records `gsdirector.matchmaking_cycle.match.deactivation_timeouts` / `gsdirector.matchmaking_cycle.ticket.deactivation_timeouts`, and increases next-cycle sleep backoff. When `true`, the director rejects annotated matches and re-activates their tickets via `ActivateTickets`.
* `WAIT_FOR_BACKFILL_MMFS=false` (default `false`) - Controls whether `gsdirector` waits for backfill `MmfRequest` invocations to complete before launching fleet `MmfRequest` invocations within the same cycle. When `false` (default), backfill `MmfRequest`s are kicked off first and fleet `MmfRequest`s are started immediately afterward so both see the same player pool. When `true`, `gsdirector` waits for each backfill `MmfRequest` stream to finish (and for `om-core` to deactivate its matched tickets) before starting subsequent `MmfRequest`s.
* `MM_CYCLE_MIN_DURATION_MS=5000` (default `5000`) - Minimum duration in milliseconds for each matchmaking cycle. Should be set `>=` your observed p99 `om_core.cache.replication.lag` so ticket deactivations and re-activations have time to replicate across `om-core` instances before the next cycle begins. When cross-cycle ticket collisions or `deactivation_timeout` annotations occur, `gsdirector` adaptively multiplies this sleep duration (`assignedBackoffFactor`) to give `om-core` replication time to catch up.
* `OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL=500` (default `500`) - Maximum number of ticket IDs sent in a single `ActivateTickets` call. Must be `<=` `om-core`'s `OM_MAX_STATE_UPDATES_PER_CALL` (default `500`), which also bounds the number of ticket IDs packed into a single replication stream entry when `OM_CACHE_PACK_TICKET_STATE_UPDATES=true` on `om-core`.
* `NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT` (default `math.MaxInt32`) - Causes `gsdirector` to exit `Run()` after N consecutive matchmaking cycles with zero match proposals. Useful for deterministic exit in local testing and automated load tests.

For instructions on running replication and tickets-per-cycle (TPC) load tests against `om-core`, see [LOADTEST.md](./LOADTEST.md).

Note that the example matchmaker is an example of **all of the pieces of code you'll need to write and maintain on your own** in order to make a matchmaker using Open Match 2.  It is **explicitly not** an example of how that code should be packaged for production. In particular, in production you will want to make horizontally scalable services out of your matchmaking queue and your matchmaking functions with enough instances to handle all the load from all of your game clients requesting matches, and you very likely do **not** want to run dozens of identical copies of your game server manager and director. The `v2/examples/standalone` directory contains individual copies of the components as a model for how to package and deploy your matchmaker in production.  However, spinning up 5+ microservices on your local machine and getting them all talking to each other successfully is a lot of friction when you're trying to quickly iterate on your matching logic and ticket parameters during development.  This 'all-in-one' matchmaker application spins up all the actual code you would run as services in production inside one process and allows you to quickly pin down your matchmaking approach without having to 'port' all your code into a new codebase before running it in production. (For users of the 'original' Open Match: this application is analogous to `minimatch`.)

### Future Refactoring Proposal (Director Testability)

`MockDirector.Run()` in `internal/gsdirector/gsdirector.go` intentionally keeps `ProposalsLoop` inline so that the example reads linearly top-to-bottom as an instructional walkthrough. When adapting `gsdirector` for a production codebase with comprehensive unit test suites, we recommend:
1. **Extracting `ProposalsLoop` into a standalone pure function**: e.g., `evaluateProposal(match *pb.Match, assignments map[string]bool, pendingMmfRequestsByHash map[string]*pb.MmfRequest, cfg *viper.Viper) (decision, reason, stats)`, separating pure match proposal evaluation (checking `deactivation_timeout`, validating `mmfName`/`profileName`/`score`, checking `assignments` for ticket collisions, and extracting backfill `MmfRequest` extensions) from side effects (queueing ticket re-activations, allocating game servers, and recording metrics).
2. **Extracting `OmClient` behind an interface**: Defining a minimal interface for `InvokeMatchmakingFunctions` and `ActivateTicketsWithResults` so every proposal rejection/acceptance branch and cycle loop behavior can be table-tested directly without standing up an HTTP test server.

