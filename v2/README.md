この文書を日本語で [Open Match v2.x Ecosystem（日本語版）](README-JP.md)
# Open Match v2.x Ecosystem

This is the subdirectory holding the examples and dev framework for building an matchmaker based on Open Match v2.x. The following are included:

* the `examples/` directory contains examples of how to write a matchmaker application that interfaces with Open Match. See [`examples/README.md`](examples/README.md) (or [`examples/README-JP.md`](examples/README-JP.md)) for more details.
* the `internal/` directory contains modules used by the applications in the `examples/` directory. 
* the [`cloudbuild.yaml`](cloudbuild.yaml) file can be used to build container images of the applications in the `examples/` directory. The instructions for using it are in the comments at the top of the file. Instructions for deploying the application container images can be found in the application directories themselves.
  
# Development

This example local-development matchmaker, standalone game server director, standalone matchmaking queue, and matchmaking functions in the `examples/` directory are golang applications designed to be built using [CNCF Buildpacks](https://www.cncf.io/projects/buildpacks/), and our primary build process specifically uses [Google Cloud's buildpacks](https://cloud.google.com/docs/buildpacks/overview). It requires the use of Open Match 2, and won't startup if it can't contact an OM2 instance.

### For Maintainers

The following represent best practices but not required when doing a new release:

* Go to [cloud.google.com](https://cloud.google.com) and open a clean Cloud Shell terminal. Run `go version` to find the version offered by default in Cloud Shell. Then kick off a Cloud Build using pack (as in the command below) and find the lines that specify the default version of Golang being used there.  You should see lines similar to this in the build logs:
```
Step #1: ===> BUILDING
Step #1: [builder] target distro name/version labels not found, reading /etc/os-release file
Step #1: [builder] === Go - Runtime (google.go.runtime@0.9.1) ===
Step #1: [builder] Go version not specified, using latest available Go runtime for the stack "ubuntu2204"
Step #1: [builder] 2025/08/27 06:00:39 [DEBUG] GET https://go.dev/dl/?mode=json
Step #1: [builder] ***** CACHE MISS: "go"
Step #1: [builder] Installing Go v1.24.6.
```
use the lower of these two versions (typically the pack builder lags behind) in the `go.mod` files in Open Match 2 related projects using `go mod edit -go=<VERSION>` (currently Go `1.25+` in [`go.mod`](go.mod)). This is to ensure that users following the instructions to build in their own projects can do their own builds using the default Cloud Shell environment and the latest Google Cloud buildpacks.
* After verifying that the build works with the latest dependencies, commit any changes to `go.mod` to the repo.

## Build

This repo includes [`cloudbuild.yaml`](cloudbuild.yaml) to build [remotely using Cloud Build on Google Cloud](https://cloud.google.com/docs/buildpacks/build-application#remote_builds). A typical build goes something like this, with the repo cloned, `gcloud` initialized, and an existing [Docker Artifact Registry](https://cloud.google.com/artifact-registry/docs/docker/store-docker-container-images) called `myregistry`:
```
# Update the dependencies to the latest versions, test, and kick off a Cloud Build
go get $(go list -m -f '{{if not (or .Main .Indirect)}}{{.Path}}@latest{{end}}' all) && go mod tidy && go vet ./... && go test -race ./... && gcloud builds submit --async --substitutions=COMMIT_SHA=abc123,_ARTIFACT_REGISTRY_URI=myregistry,_GOOGLE_BUILDABLE=./examples/mmf,_JOB_NAME=soloduel
```
The `_GOOGLE_BUILDABLE` should be one of the applications in the `examples/`
directory (containing a `main.go` file), and the `_JOB_NAME` will be the name of
the resulting container image (tagged with the `COMMIT_SHA` substitution, by
default).

The root [`cloudbuild.yaml`](cloudbuild.yaml) invokes [Google Cloud's buildpacks](https://cloud.google.com/docs/buildpacks/overview) (`gcr.io/k8s-skaffold/pack`), so no `Dockerfile` is required.

## Deploy & Run

For architecture details, deployment manifests, and runtime configuration, see the documentation in each subdirectory:

* **Architecture & Design Overview**: [`examples/README.md`](examples/README.md) ([日本語版](examples/README-JP.md)) covers the core components, reconciliation loop, backfill workflow, ticket collision handling (`assignments` vs. `acceptedInCycle`), and OpenTelemetry metric pairing with `om-core`.
* **All-in-One Development Matchmaker**: [`examples/matchmaker/README.md`](examples/matchmaker/README.md) and [`examples/matchmaker/deploy/cloudbuild.yaml`](examples/matchmaker/deploy/cloudbuild.yaml) cover service account setup, Cloud Run deployment, HTTP endpoints, and environment variables for running the combined queue, director, and local MMF harness.
* **Matchmaking Functions (MMFs)**: [`examples/mmf/README.md`](examples/mmf/README.md) and [`examples/mmf/deploy/cloudbuild.yaml`](examples/mmf/deploy/cloudbuild.yaml) cover building, deploying, and authoring standalone gRPC MMF services (`fifo` and `debug`).
* **Standalone Production Components**: [`examples/standalone/gsdirector/deploy/cloudbuild.yaml`](examples/standalone/gsdirector/deploy/cloudbuild.yaml) and [`examples/standalone/mmqueue/deploy/cloudbuild.yaml`](examples/standalone/mmqueue/deploy/cloudbuild.yaml) provide Cloud Build deployment pipelines for running the Game Server Director and Matchmaking Queue as independent Cloud Run services connected via Google Cloud Pub/Sub.


