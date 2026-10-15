// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This example is part of one approach to writing a matchmaker using Open
// Match v2.x. Many approaches are viable, and we encourage you to read the
// documentation at open-match.dev for more ideas and patterns.
//
// Minimally, a matchmaker using open-match is expected to:
// 1) queue player matchmaking requests,
// 2) request matches and distribute them to servers,
// 3) notify players of their match assignments,
// 4) and provide custom matchmaking logic.
// These components can be one in the same, or have their duties federated
// across a number of platform services. Regardless of the pattern used, the
// collection of the above functionality is referred to by Open Match v2.x as
// your 'matchmaker'.
//
// NOTE: This example does not use om-core for assignments as this
// functionality is deprecated. It is recommended you use a player status
// service for storing and retrieving player assignments in production
// environments.
//
// This file mocks a component of a matchmaker that receives player matchmaking
// requests and creates tickets in Open Match (#1 above). A typical approach is
// a horizontally-scalable server process with:
// - an endpoint the client hits when it wants to begin matchmaking that does
//   some or all of these things:
//   - Generate a Open Match Ticket protobuf message containing the game
//     client-provided matching attributes (e.g. ping values, game mode and
//     character selections)
//   - Add additional data useful for matchmaking from your own authoritative
//     systems as necessary (e.g. MMR values, character progression, purchased
//     unlocks)
//   - Queues this ticket for creation in Open Match. This is a
//     production best practice and allows you to manage ticket pressure in Open
//     Match during spikes in demand (launch, immediately after a maintenance
//     window, after unexpected downtime, etc.)
// - An asynchronous loop to process queued tickets, handling these tasks:
//   - Creates queued tickets in Open Match (POST /tickets -> receive TicketID in
//     response). These TicketIDs need to be activated before they are available
//     for matching.
//   - Stores the ClientID to TicketID mapping. This mapping can be kept local
//     to this process (not scalable, not persistent, not highly available), but
//     it is recommended that it is stored in a player status service instead.
//   - POST batches of TicketIDs to the /tickeds:activate endpoint to mark those
//     tickets as available for matching.
//

package main

import (
	"context"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/metric"

	// Required for protojson to correctly parse JSON when unmarshalling to protobufs that contain
	// 'well-known types' https://github.com/golang/protobuf/issues/1156

	_ "google.golang.org/protobuf/types/known/wrapperspb"

	//soloduelServer "open-match.dev/functions/golang/soloduel"
	//mmf "open-match.dev/mmf/server"
	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"open-match.dev/open-match-ecosystem/v2/internal/assignmentdistributor"
	"open-match.dev/open-match-ecosystem/v2/internal/logging"
	"open-match.dev/open-match-ecosystem/v2/internal/metrics"
	"open-match.dev/open-match-ecosystem/v2/internal/mmqueue"
	"open-match.dev/open-match-ecosystem/v2/internal/mocks/gameclient"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

var (
	otelShutdownFunc func(context.Context) error
	meterptr         *metric.Meter
)

func main() {

	// Quit this process on the signals sent by kubernetes/knative/Cloud Run/ctrl+c.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Read config
	cfg := viper.New()
	cfg.SetDefault("OM_CORE_ADDR", "http://localhost:8080")
	cfg.SetDefault("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)
	cfg.SetDefault("MAX_CONCURRENT_TICKET_CREATIONS", 20)
	// Test ticket load generation config (defaults INITIAL_TPC to 0 in the
	// standalone queue so synthetic tickets are only generated when opted into
	// via INITIAL_TPC or the /tpc/{tixPerCycle} endpoint).
	cfg.SetDefault("INITIAL_TPC", 0)
	cfg.SetDefault("TICKET_CREATION_CYCLE_DURATION_MS", 1000)
	cfg.SetDefault("TICKET_CREATION_CYCLES", math.MaxInt32)
	// Default HTTP port is 8080 (matching Cloud Run and GKE container conventions);
	// override via PORT env var when running multiple services locally on one host.
	cfg.SetDefault("PORT", 8080)

	// Override these with env vars when doing local development.
	// Suggested values in that case are "text", "debug", and "false",
	// respectively
	cfg.SetDefault("LOGGING_FORMAT", "json")
	cfg.SetDefault("LOGGING_LEVEL", "info")
	cfg.SetDefault("LOG_CALLER", "false")

	// Open Telemetry metrics config
	cfg.SetDefault("OTEL_SIDECAR", "true")
	cfg.SetDefault("OTEL_PROM_PORT", 2224)

	// Assignment distribution config
	// Returning assignments via channel only works if the mmqueue and the
	// gsdirector are running in the same process, so you'll need to provide a
	// different assignment return data flow.  We sugest using a distributed
	// message bus, pub/sub system, or your platform service's notification
	// service when returning assignments.
	cfg.SetDefault("ASSIGNMENT_DISTRIBUTION_PATH", "pubsub")
	cfg.SetDefault("GCP_PROJECT_ID", "my-project-id")
	cfg.SetDefault("ASSIGNMENT_TOPIC_ID", "replace-me")

	// Read overrides from env vars
	cfg.AutomaticEnv()

	// initialize shared structured logging
	log := logging.NewSharedLogger(cfg)
	logger := log.WithFields(logrus.Fields{"component": "matchmaking_queue"})

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
	var receiver assignmentdistributor.Receiver
	switch cfg.GetString("ASSIGNMENT_DISTRIBUTION_PATH") {
	case "channel":
		log.Info("Using Go channels for assignment distribution")
		log.Error("Using Go channels for assignment distribution isn't possible unless the matchmaking queue and game server director are running in the same process, which should only be done when doing active local development. Since this is a standalone matchmaking queue process, there is no game server director sending data on the go channel - assignments are effectively discarded with this configuration.")
		assignmentsChan := make(chan *pb.Roster)
		receiver = assignmentdistributor.NewChannelReceiver(assignmentsChan)
	case "pubsub":
		fallthrough // default is 'pubsub' in the standalone mmqueue
	default:
		// note: if using pubsub to send assignments from your director to your
		// matchmaking queue, make sure you have sufficient quota for
		// subscriptions in your GCP project. Each instance of the mmqueue will
		// make a unique topic subscription.
		log.Println("Using Google Cloud Pub/Sub for assignment distribution")

		// Instantiate the Pub/Sub receiver
		receiver = assignmentdistributor.NewPubSubSubscriber(
			cfg.GetString("GCP_PROJECT_ID"),
			cfg.GetString("ASSIGNMENT_TOPIC_ID"),
			log,
		)
	}

	// Initialize the queue with a pooled HTTP transport matching examples/matchmaker
	// so concurrent CreateTicket and ActivateTickets requests reuse keep-alive
	// connections under high ticket creation concurrency (MAX_CONCURRENT_TICKET_CREATIONS).
	q := &mmqueue.MatchmakerQueue{
		OmClient: omclient.New(cfg, log, &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,              // Maximum idle connections to keep open
				MaxIdleConnsPerHost: 100,              // Maximum idle connections per host
				IdleConnTimeout:     90 * time.Second, // How long to keep idle connections open
				DisableKeepAlives:   false,            // Make sure keep-alives are enabled
			},
			Timeout: 30 * time.Second,
		}),
		Cfg:                cfg,
		Log:                log,
		ClientRequestChan:  make(chan *mmqueue.ClientRequest),
		AssignmentReceiver: receiver,
		OtelMeterPtr:       meterptr,
	}

	//Check connection before spinning everything up.
	err := q.OmClient.ValidateConnection(ctx, cfg.GetString("OM_CORE_ADDR"))
	if err != nil {
		logger.Errorf("OM Connection test failure: %v", err)
		if strings.Contains(err.Error(), "connect: connection refused") {
			logger.Fatal("Unrecoverable error. Is the OM_CORE_ADDR config set correctly?")
		}
	}
	// Start the queue. This is where requests are processed and added to Open Match.
	go q.Run(ctx)

	// Handler for client matchmaking requests (registered on both /tickets to
	// match examples/matchmaker and / for backward compatibility).
	handleTicketRequest := func(w http.ResponseWriter, r *http.Request) {

		// Make a buffered channel on which the queue will return its http.Status
		// result so the queue worker never blocks if the HTTP caller disconnects early.
		resultChan := make(chan int, 1)
		req := &mmqueue.ClientRequest{
			ResultChan: resultChan,
			Ticket: func(r *http.Request) *pb.Ticket {
				// In a real game client, you'd probably use whatever communication
				// protocol/format (JSON string, binary encoding, a custom format,
				// etc) your game engine or dev kit encourages, and parse that data
				// from the http.Request 'r' into the Open Match protobuf `ticket`
				// protobuf here.
				//
				// This example uses the provided internal library to generate a simple
				// ticket with a few example attributes for testing.

				// TODO: replace the gameclient.Simple() call with your own code to
				// process the request 'r' into an Open Match *pb.Ticket.
				return gameclient.Simple(ctx)
			}(r),
		}

		// Guard the enqueue on ctx.Done() and r.Context().Done() so in-flight HTTP
		// requests do not block during graceful shutdown after q.Run(ctx) exits.
		select {
		case q.ClientRequestChan <- req:
		case <-ctx.Done():
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		case <-r.Context().Done():
			w.WriteHeader(http.StatusRequestTimeout)
			return
		}

		// Write the http.Status code returned by the queue
		select {
		case status := <-resultChan:
			w.WriteHeader(status)
		case <-r.Context().Done():
			w.WriteHeader(http.StatusRequestTimeout)
		}
	}
	http.HandleFunc("/tickets", handleTicketRequest)
	http.HandleFunc("/", handleTicketRequest)

	// FOR TESTING ONLY
	// Asynchronous goroutine that loops TICKET_CREATION_CYCLES number of
	// times, attempting to queue the given number of tickets each cycle.
	// Update the tpc by sending an http GET to /tpc/<NUM> as per the handler
	// below.
	q.SetTestTicketConfig(cfg.GetInt64("INITIAL_TPC"), gameclient.Simple)
	go q.GenerateTestTickets(ctx)

	// Handler to update the (best effort) number of test tickets we want to
	// generate and put into the queue per second. If the requested TPC is more
	// than the matchmaker can manage in a second, it will do as many as it
	// can. Set it to 0 to stop automatically creating tickets.
	http.HandleFunc("/tpc/{tixPerCycle}", func(w http.ResponseWriter, r *http.Request) {

		// Attempt to convert requested TPC string into an int64
		rTPC, err := strconv.ParseInt(r.PathValue("tixPerCycle"), 10, 64)
		if err != nil {
			// Couldn't convert to int; maybe a typo in the http request?
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Reject negative values and enforce a hard cap of 10k per second to
		// protect against mistyped http requests.
		if rTPC < 0 || rTPC > 10000 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Update tixPerSec number, and generate tickets using the gameclient.Simple function.
		q.SetTestTicketConfig(rTPC, gameclient.Simple)

		logger.Infof("TPC set to %v", rTPC)
		w.WriteHeader(http.StatusOK)
	})

	// Start http server.
	logger.Infof("PORT %v detected", cfg.GetString("PORT"))
	srv := &http.Server{Addr: ":" + cfg.GetString("PORT")}
	go func() {
		// ErrServerClosed is the error returned by http.Server on graceful exit.
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal(err)
		}
	}()

	// Wait for quit signal, then cancel ctx first so q.Run stops processing new
	// ticket batches before shutting down the assignment receiver and HTTP server.
	<-signalChan
	cancel()
	receiver.Stop()
	// Pass a fresh bounded context to srv.Shutdown rather than the already-cancelled
	// application ctx so in-flight HTTP requests have a grace period to complete.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Fatal(err)
	}
	logger.Info("Exiting...")
}
