// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// NOTE: WIP, this is a testbed right now.
package main

import (
	"context"
	"io"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/metric"

	// Required for protojson to correctly parse JSON when unmarshalling to protobufs that contain
	// 'well-known types' https://github.com/golang/protobuf/issues/1156

	_ "google.golang.org/protobuf/types/known/wrapperspb"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"open-match.dev/open-match-ecosystem/v2/internal/assignmentdistributor"
	"open-match.dev/open-match-ecosystem/v2/internal/gsdirector"
	"open-match.dev/open-match-ecosystem/v2/internal/logging"
	"open-match.dev/open-match-ecosystem/v2/internal/metrics"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

var (
	meterptr         *metric.Meter
	otelShutdownFunc func(context.Context) error
)

func main() {

	// Quit this process on the signals sent by kubernetes/knative/Cloud Run/ctrl+c.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Read config
	cfg := viper.New()

	// Connection config.
	cfg.SetDefault("OM_CORE_ADDR", "http://localhost:8080")
	// Default HTTP port is 8080 (matching Cloud Run and GKE container conventions);
	// override via PORT env var when running multiple services locally on one host.
	cfg.SetDefault("PORT", 8080)
	cfg.SetDefault("SOLODUEL_ADDR", "http://localhost")
	cfg.SetDefault("SOLODUEL_PORT", 50090)
	// Optional DEBUG MMF endpoint config (disabled by default in standalone
	// gsdirector unless DEBUGMMF_ADDR is explicitly configured).
	cfg.SetDefault("DEBUGMMF_ADDR", "")
	cfg.SetDefault("DEBUGMMF_PORT", 50081)

	// OM core config that the matchmaker needs to respect.
	// Must be <= om-core's OM_MAX_STATE_UPDATES_PER_CALL (default 500), which
	// also bounds packed replication stream entries when
	// OM_CACHE_PACK_TICKET_STATE_UPDATES=true.
	cfg.SetDefault("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)

	// InvokeMatchmaking Function config
	// In production, you will want this to be functionally infinite, thus the
	// use of the largest Int32 number possible.  However, when doing local
	// development of matching logic, it is often useful to run a deterministic
	// number of cycles.
	cfg.SetDefault("NUM_MM_CYCLES", math.MaxInt32)
	cfg.SetDefault("MM_CYCLE_MIN_DURATION_MS", 5000) // Director will sleep if the invokeMMFs didn't take at least this long.
	// Exit if 3 matchmaking cycles come back empty. Again, you probably don't
	// want to do this in production, but it can be very useful in local
	// testing.
	cfg.SetDefault("NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT", math.MaxInt32)

	// When false (default), matches annotated by om-core with
	// deactivation_timeout=true are still accepted (relying on the director's
	// assignments map to catch any cross-cycle duplicate tickets and scaling
	// up the next cycle sleep backoff). When true, annotated matches are
	// immediately rejected and their tickets are sent for re-activation.
	cfg.SetDefault("REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT", false)

	// When false (default), backfill MmfRequests (ordered first by GetMMFParams)
	// are kicked off first, and fleet MmfRequests are started immediately
	// afterward in the same cycle so both see the same active player pool.
	// When true, the director waits for each backfill MmfRequest stream to
	// complete (and for om-core to deactivate its matched tickets) before
	// starting subsequent MmfRequests.
	cfg.SetDefault("WAIT_FOR_BACKFILL_MMFS", false)

	// Override these with env vars when doing local development.
	// Suggested values in that case are "text", "debug", and "false",
	// respectively
	cfg.SetDefault("LOGGING_FORMAT", "json")
	cfg.SetDefault("LOGGING_LEVEL", "info")
	cfg.SetDefault("LOG_CALLER", "false")

	// Metrics config
	cfg.SetDefault("OTEL_SIDECAR", "true")
	cfg.SetDefault("OTEL_PROM_PORT", 2225)

	// Assignment distribution config
	// Returning assignments via channel only works if the mmqueue and the
	// gsdirector are running in the same process, so you'll need to provide a
	// different assignment return data flow.  We sugest using a distributed
	// message bus, pub/sub system, or your platform service's notification
	// service when returning assignments.
	cfg.SetDefault("ASSIGNMENT_TTL_MS", 1200000) // default 1200 secs = 20 mins
	cfg.SetDefault("ASSIGNMENT_DISTRIBUTION_PATH", "")
	cfg.SetDefault("GCP_PROJECT_ID", "my-project-id")
	cfg.SetDefault("ASSIGNMENT_TOPIC_ID", "replace-me")

	// Read overrides from env vars
	cfg.AutomaticEnv()

	// Set up structured logging
	// Default logging configuration is json that plays nicely with Google Cloud Run.
	log := logging.NewSharedLogger(cfg)
	logger := log.WithFields(logrus.Fields{"component": "game_server_director"})
	logger.Debugf("%v cycles", cfg.GetInt("NUM_MM_CYCLES"))

	// Initialize Metrics
	if cfg.GetBool("OTEL_SIDECAR") {
		meterptr, otelShutdownFunc = metrics.InitializeOtel()
	} else {
		meterptr, otelShutdownFunc = metrics.InitializeOtelWithLocalProm(cfg.GetInt("OTEL_PROM_PORT"))
	}
	// Use a fresh bounded context for OpenTelemetry shutdown because the main
	// application ctx is cancelled when a termination signal arrives, and
	// otelShutdownFunc needs an active context deadline to flush final metrics.
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = otelShutdownFunc(shutdownCtx)
	}()

	// Initialize assignment distribution
	var publisher assignmentdistributor.Sender
	switch cfg.GetString("ASSIGNMENT_DISTRIBUTION_PATH") {
	case "channel":
		log.Info("Using Go channels for assignment distribution")
		log.Error("Using Go channels for assignment distribution isn't possible unless the matchmaking queue and game server director are running in the same process, which should only be done when doing active local development. Since this is a standalone game server director process, the go channel will never be read, so assignments are effectively discarded with this configuration.")
		assignmentsChan := make(chan *pb.Roster)
		publisher = assignmentdistributor.NewChannelSender(assignmentsChan)
	case "pubsub":
		fallthrough // default is 'pubsub' in the standalone mmqueue
	default:
		// note: if using pubsub to send assignments from your director to your
		// matchmaking queue, make sure you have sufficient quota for
		// subscriptions in your GCP project. Each instance of the mmqueue will
		// make a unique topic subscription.
		log.Println("Using Google Cloud Pub/Sub for assignment distribution")

		// Instantiate the Pub/Sub receiver
		publisher = assignmentdistributor.NewPubSubPublisher(
			cfg.GetString("GCP_PROJECT_ID"),
			cfg.GetString("ASSIGNMENT_TOPIC_ID"),
			log,
		)
	}

	// Initialize the director.
	// Use a pooled HTTP transport for the director's OmClient so concurrent
	// InvokeMatchmakingFunctions streams and ticket re-activation calls reuse
	// keep-alive connections instead of falling back to Go's default 2 idle
	// connections per host. Do NOT set a static 30s http.Client.Timeout here:
	// InvokeMatchmakingFunctions is a server-streaming RPC whose lifetime is
	// bounded server-side by om-core's OM_MMF_TIMEOUT_SECS and
	// OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS, so a short static client timeout
	// could sever valid long-running MMF streams mid-cycle.
	// Also pass OtelMeterPtr to MockAgonesIntegration so game server allocation
	// metrics are registered on the shared OpenTelemetry meter.
	d := &gsdirector.MockDirector{
		OmClient: omclient.New(cfg, log, &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,              // Maximum idle connections to keep open
				MaxIdleConnsPerHost: 100,              // Maximum idle connections per host
				IdleConnTimeout:     90 * time.Second, // How long to keep idle connections open
				DisableKeepAlives:   false,            // Make sure keep-alives are enabled
			},
		}),
		GSManager: &gsdirector.MockAgonesIntegration{
			Log:          log,
			OtelMeterPtr: meterptr,
		},
		Cfg:                 cfg,
		Log:                 log,
		AssignmentPublisher: publisher,
		OtelMeterPtr:        meterptr,
	}

	// Apply configured MMF endpoint to the sample SoloDuel game mode and
	// re-serialize its backfill MmfRequest extensions before initializing GSManager.
	gsdirector.Fifo.Host = cfg.GetString("SOLODUEL_ADDR")
	gsdirector.Fifo.Port = cfg.GetInt32("SOLODUEL_PORT")
	if cfg.GetString("DEBUGMMF_ADDR") != "" {
		gsdirector.SoloDuel.MMFs = append(gsdirector.SoloDuel.MMFs, &pb.MatchmakingFunctionSpec{
			Name: "DEBUG",
			Host: cfg.GetString("DEBUGMMF_ADDR"),
			Port: cfg.GetInt32("DEBUGMMF_PORT"),
			Type: pb.MatchmakingFunctionSpec_GRPC,
		})
	}
	if err := gsdirector.SoloDuel.SetBackfillMMFs(gsdirector.Fifo); err != nil {
		logger.Fatalf("Failed to configure SoloDuel backfill MMFs: %v", err)
	}
	// HeroShooter is currently disabled in GameModesInZone because no sample
	// BalancedTeam MMF implementation is provided yet (matching examples/matchmaker).
	// if err := gsdirector.HeroShooter.SetBackfillMMFs(gsdirector.Fifo); err != nil {
	// 	logger.Fatalf("Failed to configure HeroShooter backfill MMFs: %v", err)
	// }

	// In a real stand-alone director, you would read in your actual game server configuration and construct
	// the FleetConfig, ZonePools, and GameModesInZone. For automated testing, we use the sample ones
	// defined in the internal/gsdirector module.
	err := d.GSManager.Init(gsdirector.FleetConfig, gsdirector.ZonePools, gsdirector.GameModesInZone)
	if err != nil {
		log.Errorf("Failure initializing game server manager matchmaking parameters: %v", err)
	}

	//Check connection before spinning everything up.
	err = d.OmClient.ValidateConnection(ctx, cfg.GetString("OM_CORE_ADDR"))
	if err != nil {
		logger.Errorf("OM Connection test failure: %v", err)
		if strings.Contains(err.Error(), "connect: connection refused") {
			logger.Fatal("Unrecoverable error. Is the OM_CORE_ADDR config set correctly?")
		}
	}

	// Start the director. This is where profiles get sent to the InvokeMMFs()
	// call, and matches returned.
	go d.Run(ctx)

	// Return the latest matchmaking cycle status on exact GET / requests ("GET /{$}"
	// in Go 1.22+ ServeMux matches only the root path "/" and not arbitrary subpaths).
	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, d.Status())
	})

	// Start http server so this application can be run on serverless platforms.
	logger.Infof("PORT '%v' detected", cfg.GetString("PORT"))
	srv := &http.Server{Addr: ":" + cfg.GetString("PORT")}
	go func() {
		// ErrServerClosed is the error returned by http.Server on graceful exit.
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal(err)
		}
	}()

	// Wait for quit signal, then cancel ctx first so d.Run stops its matchmaking
	// loop before shutting down the assignment publisher and HTTP server.
	<-signalChan
	cancel()
	publisher.Stop()
	// Pass a fresh bounded context to srv.Shutdown rather than the already-cancelled
	// application ctx so in-flight HTTP requests have a grace period to complete.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Fatal(err)
	}
	logger.Info("Exiting...")
}
