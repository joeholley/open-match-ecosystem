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
package mmqueue

import (
	"context"
	"fmt"
	"io"
	"net/http"
	_ "net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/metric"

	"github.com/spf13/viper"

	// Required for protojson to correctly parse JSON when unmarshalling to protobufs that contain
	// 'well-known types' https://github.com/golang/protobuf/issues/1156
	_ "google.golang.org/protobuf/types/known/wrapperspb"
	//soloduelServer "open-match.dev/functions/golang/soloduel"
	//mmf "open-match.dev/mmf/server"
	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"open-match.dev/open-match-ecosystem/v2/internal/assignmentdistributor"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

// Mock of the game platform services frontend that handles player matchmaking
// requests, queueing them to be sent to Open Match, and processing them
// asynchronously.
type MatchmakerQueue struct {
	OmClient           *omclient.RestfulOMGrpcClient
	Cfg                *viper.Viper
	Log                *logrus.Logger
	Tickets            sync.Map
	ClientRequestChan  chan *ClientRequest
	AssignmentReceiver assignmentdistributor.Receiver
	AssignmentsChan    chan *pb.Roster
	OtelMeterPtr       *metric.Meter
	TPC                int64
	// tpcMutex protects concurrent reads and writes of per-instance TPC and
	// newTicket across SetTestTicketConfig, GenerateTestTickets, and the OTel
	// gauge callback so multiple MatchmakerQueue instances remain isolated.
	tpcMutex                              sync.Mutex
	newTicket                             func(context.Context) *pb.Ticket
	metricsOnce                           sync.Once
	otelTicketAssignments                 metric.Int64Counter
	otelTicketActivations                 metric.Int64Counter
	otelTicketActivationFailures          metric.Int64Counter
	otelTicketCreationRetries             metric.Int64Counter
	otelTicketCreations                   metric.Int64Counter
	otelTicketDeletionFailures            metric.Int64Counter
	otelTicketsGeneratedPerCycle          metric.Int64ObservableGauge
	otelTicketGenerationsAchievedPerCycle metric.Int64Histogram
	otelTicketGenerationCycleDurations    metric.Int64Histogram
	otelTicketQueuedDurations             metric.Float64Histogram
}

type ClientRequest struct {
	Ticket     *pb.Ticket
	ResultChan chan int
}

// ensureMetrics initializes the MatchmakerQueue OpenTelemetry instruments and
// TPC gauge callback at most once per MatchmakerQueue instance. Both Run and
// GenerateTestTickets call ensureMetrics before recording any metrics so
// starting them concurrently (e.g., with INITIAL_TPC=0 where the first
// GenerateTestTickets cycle finishes immediately) never races on uninitialized
// metric instruments.
func (q *MatchmakerQueue) ensureMetrics(logger *logrus.Entry) {
	q.metricsOnce.Do(func() {
		if q.OtelMeterPtr != nil {
			logger.Tracef("Initializing otel metrics")
			q.registerMetrics(q.OtelMeterPtr)
			// This inline function is a simple callback that open telemetry uses to read
			// the current TPC
			meter := *q.OtelMeterPtr
			_, err := meter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					// TPC can be updated asynchronously, so we protect reading it with a lock.
					q.tpcMutex.Lock()
					o.ObserveInt64(q.otelTicketsGeneratedPerCycle, q.TPC)
					q.tpcMutex.Unlock()
					return nil
				},
				q.otelTicketsGeneratedPerCycle,
			)
			if err != nil {
				logger.Fatalf("Failed to set up tpc gauge: %v", err)
			}
		} else {
			logger.Warnf("Unable to add matchmaking queue metrics to the OTEL configuration")
		}
	})
}

// Run the matchmaker queue, asynchronously queueing tickets and
// activating them.
func (q *MatchmakerQueue) Run(ctx context.Context) {
	logger := q.Log.WithFields(logrus.Fields{
		"app":       "matchmaker",
		"component": "matchmaking_queue",
	})

	// Var init
	var (
		wg                sync.WaitGroup
		pendingMu         sync.Mutex
		activationsClosed bool
	)
	ticketIdsToActivate := make(chan string, 10000)
	// pendingActivations maps each created ticket ID to the client ResultChan(s)
	// waiting for ActivateTicketsWithResults to confirm that the ticket is active
	// in om-core before returning HTTP 200 OK (or HTTP 500 on activation failure).
	pendingActivations := make(map[string][]chan int)

	// Initialize metrics
	q.ensureMetrics(logger)

	// Control number of concurrent requests by creating ticket creation 'slots'.
	// Each slot represents one in-flight proxyCreateTicket call to om-core.
	slots := make(chan struct{}, q.Cfg.GetInt("MAX_CONCURRENT_TICKET_CREATIONS"))

	// Goroutine to read from the incoming ticket request channel, and create
	// one ticket for each incoming request. Limit the number of concurrent
	// creations by blocking until one of the MAX_CONCURRENT_TICKET_CREATIONS
	// 'slots' is available, and release the slot inside the worker goroutine
	// after proxyCreateTicket finishes.
	// Don't exit the Run() function as long as this goroutine is running.
	wg.Add(1)
	go func() {
		defer wg.Done()

		var createWg sync.WaitGroup
		defer func() {
			// Wait for all in-flight proxyCreateTicket worker goroutines to
			// finish enqueueing created ticket IDs before closing
			// ticketIdsToActivate so ActivateTicketsWithResults drains cleanly.
			createWg.Wait()
			close(ticketIdsToActivate)
		}()

		cLogger := q.Log.WithFields(logrus.Fields{
			"operation": "ticket_creation",
		})
		cLogger.Debug("Processing queued ticket creation requests")

		// Loop until ctx is cancelled or ClientRequestChan is closed.
		for {
			logger.Trace("waiting for a new ticket to enter the queue")
			var (
				request *ClientRequest
				ok      bool
			)
			select {
			case <-ctx.Done():
				return
			case request, ok = <-q.ClientRequestChan:
				if !ok {
					return
				}
			}
			if request == nil {
				continue
			}

			// Acquire a concurrency slot after receiving a client request (so an
			// idle queue never holds a slot while waiting for requests) and abort
			// cleanly if ctx is cancelled while waiting for a free slot.
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				request.ResultChan <- http.StatusRequestTimeout
				close(request.ResultChan)
				return
			}

			// Asynchronous concurrent ticket creation.
			// Track the worker in createWg and release the concurrency slot inside
			// the worker goroutine when proxyCreateTicket completes.
			createWg.Add(1)
			go func(request *ClientRequest) {
				defer createWg.Done()
				defer func() { <-slots }()

				// proxyCreateTicket implements retries exp bo + jitter
				id, err := q.proxyCreateTicket(ctx, request.Ticket)
				if err != nil {
					// Either a permanent error, or retries timed out
					logger.Errorf("CreateTicket failed: %v", err)
					request.ResultChan <- http.StatusRequestTimeout
					close(request.ResultChan)
					return
				}

				// Successful ticket creation: record the ticket in q.Tickets and
				// register request.ResultChan in pendingActivations BEFORE sending
				// id on ticketIdsToActivate. This two-stage pipeline ensures:
				//   1. q.Tickets is populated before the ticket can become active
				//      and matched by the director.
				//   2. The client's ResultChan only receives HTTP 200 OK after
				//      ActivateTicketsWithResults confirms the ticket is active in
				//      om-core (or HTTP 500 if activation fails).
				pendingMu.Lock()
				if activationsClosed {
					pendingMu.Unlock()
					request.ResultChan <- http.StatusRequestTimeout
					close(request.ResultChan)
					return
				}
				q.Tickets.Store(id, struct{}{})
				pendingActivations[id] = append(pendingActivations[id], request.ResultChan)
				pendingMu.Unlock()

				q.otelTicketCreations.Add(ctx, 1)
				select {
				case ticketIdsToActivate <- id:
				case <-ctx.Done():
				}
			}(request)
		}

	}()

	// goroutine to activate pending tickets.
	// Don't exit the Run() function as long as this goroutine is running.
	wg.Add(1)
	go func() {
		defer wg.Done()

		aLogger := q.Log.WithFields(logrus.Fields{
			"operation": "ticket_activation",
		})
		aLogger.Debug("activating created tickets")

		// Make a derived context with the activationType 'activate', which is used
		// by the omclient to distinguish between logs for initial ticket activation
		// and re-activation after a failed matching attempt.
		ctx := context.WithValue(ctx, "activationType", "activate")

		activationResults := make(chan omclient.ActivationResult, 1000)
		var resultsWg sync.WaitGroup
		resultsWg.Add(1)
		go func() {
			defer resultsWg.Done()
			for res := range activationResults {
				pendingMu.Lock()
				waiters := pendingActivations[res.TicketId]
				delete(pendingActivations, res.TicketId)
				pendingMu.Unlock()

				if res.Err != nil {
					aLogger.WithField("ticket_id", res.TicketId).Errorf("Ticket activation failed: %v", res.Err)
					q.otelTicketActivationFailures.Add(ctx, 1)
					q.Tickets.Delete(res.TicketId)
					for _, ch := range waiters {
						ch <- http.StatusInternalServerError
						close(ch)
					}
					continue
				}

				// Record otelTicketActivations only after confirming res.Err == nil
				// so failed activations are counted solely under otelTicketActivationFailures.
				q.otelTicketActivations.Add(ctx, 1)

				for _, ch := range waiters {
					ch <- http.StatusOK
					close(ch)
				}
			}

			// If ActivateTicketsWithResults exited (e.g., because ctx was cancelled)
			// while tickets were still awaiting activation, mark activationsClosed and
			// drain all remaining ResultChans with StatusRequestTimeout so no caller
			// hangs waiting on a response.
			pendingMu.Lock()
			activationsClosed = true
			for id, waiters := range pendingActivations {
				delete(pendingActivations, id)
				q.Tickets.Delete(id)
				for _, ch := range waiters {
					ch <- http.StatusRequestTimeout
					close(ch)
				}
			}
			pendingMu.Unlock()
		}()

		q.OmClient.ActivateTicketsWithResults(ctx, ticketIdsToActivate, activationResults)
		close(activationResults)
		resultsWg.Wait()
	}()

	// goroutine to process incoming assignments.
	//
	// NOTE: the assignment api endpoints in om-core are deprecated, and
	// shouldn't be used in production.  This implementation assumes you have
	// some other way of getting assignments from your game server director to
	// your matchmaking queue OTHER than going through om-core, but isn't
	// specific about what that 'other way' is. This just reads all incoming
	// assignments from the AssignmentsChan and processes them. In a real
	// implementation, you'd write some code to get assignments from whatever
	// is handling them and put them in the channel.
	//
	// It is recommended that you incorporate your assignment storage and
	// retreival into the part of your online services suite responsible for
	// returning player online status, like a player online event bus or
	// presence service.
	// Don't exit the Run() function as long as this goroutine is running.
	wg.Add(1)
	go func() {
		defer wg.Done()

		bLogger := q.Log.WithFields(logrus.Fields{
			"operation": "ticket_assignment",
		})
		bLogger.Debug("processing incoming ticket assignments")

		var assigned sync.Map

		assignmentHandler := func(ctx context.Context, roster *pb.Roster) {
			// Get the assignment string
			assignment := roster.GetAssignment().GetConnection()

			// Loop through all tickets in the assignment roster
			var ticket *pb.Ticket
			for _, ticket = range roster.GetTickets() {
				// TODO: in reality, this is where your matchmaking queue would
				// return the assignment to the game client. This sample
				// instead just logs the assignment.

				// Stop tracking this ticket; it's matchmaking is complete.
				// Your matchmaker may wish to instead keep this for a time (in
				// case the game client should be allowed to ask for the same
				// assignment again later).
				if _, existed := q.Tickets.LoadAndDelete(ticket.GetId()); existed {
					bLogger.Debugf("Received ticket %v assignment: %v", ticket.GetId(), assignment)
					assigned.Store(ticket.GetId(), true)
					// Update metric
					q.otelTicketAssignments.Add(ctx, 1)
					// Get diff between now and ticket creation time, in milliseconds
					queuedDur := time.Now().Sub(ticket.GetAttributes().GetCreationTime().AsTime())
					// TODO add metric.WithAttributes(attribute.String()) for
					// useful ways of slicing up the durations in a dashboard
					// Record queue duration in fractional milliseconds
					q.otelTicketQueuedDurations.Record(ctx, float64(queuedDur.Microseconds()/1000))
				} else {
					q.otelTicketDeletionFailures.Add(ctx, 1)
					if _, previouslyassigned := assigned.Load(ticket.GetId()); previouslyassigned {
						bLogger.Debugf("PREVIOUSLY ASSIGNED ticket %v assignment: %v", ticket.GetId(), assignment)
					} else {
						bLogger.Debugf("FAILED UNTRACKED ticket %v assignment: %v", ticket.GetId(), assignment)
					}
				}
			}

		}
		bLogger.Info("Starting assignment receiver")
		err := q.AssignmentReceiver.Receive(ctx, assignmentHandler)
		if err != nil {
			bLogger.Errorf("Assignment receiver error: %v", err)
		}
	}()

	// Don't exit the Run() function as long as any of the goroutines are
	// running (in normal operation, this is forever).
	wg.Wait()
}

// SetTestTicketConfig instructs the test function GenerateTestTickets() to make
// `newTPC` tickets per second using `ticketCreationFunc()` on this
// MatchmakerQueue instance. Example mock client ticket creation functions can
// be found in internal/mocks/gameclient.
func (q *MatchmakerQueue) SetTestTicketConfig(newTPC int64,
	ticketCreationFunc func(context.Context) *pb.Ticket) {

	// Protect per-instance TPC and newTicket updates with q.tpcMutex so callers
	// can reconfigure ticket generation at any time without racing with
	// GenerateTestTickets or other MatchmakerQueue instances.
	q.tpcMutex.Lock()
	q.TPC = newTPC
	q.newTicket = ticketCreationFunc
	q.tpcMutex.Unlock()
}

// Test the queue by generating tickets every second.
// This is only suitable for automated testing; in a real matchmaker, the
// server that instantiated the mmqueue will be inserting all the ClientRequest
// objects as client matchmaking requests come in.  You can find example mock client
// ticket creation functions in internal/mocks/gameclient.
// To use this function:
//   - instante your mmqueue object (by convention, in a variable named 'q')
//   - asynchronously run the queue: `go q.Run(ctx)`
//   - to set up ticket generation, send a non-zero TPC & a ticket creation function to the SetTPC function:
//     `q.SetTestTicketConfig(10, <ticket creation function>)`
//   - asynchronously run the test ticket generator: `go q.GenerateTestTickets(ctx)`
//   - to stop generating tickets, send 0 to the SetTPC function:
//     `q.SetTestTicketConfig(0, <ticket creation function>)`
func (q *MatchmakerQueue) GenerateTestTickets(ctx context.Context) {
	tpcLogger := q.Log.WithFields(logrus.Fields{"component": "tester", "operation": "generate_tickets"})
	q.ensureMetrics(tpcLogger)

	for i := q.Cfg.GetInt("TICKET_CREATION_CYCLES"); i > 0; i-- {
		// Stop starting new ticket-generation cycles once ctx has been cancelled.
		if ctx.Err() != nil {
			return
		}

		// Derive a per-cycle context so that when the 1-second cycle finishes
		// (or ctx is cancelled), the producer and result-reader goroutines from
		// this cycle immediately exit rather than leaking across cycles.
		cycleCtx, cycleCancel := context.WithCancel(ctx)

		// Channel where we will put one struct for each ticket we want to create this second.
		tq := make(chan struct{})

		// Use a wait group to wait for the 1-second deadline to be reached.
		var wg sync.WaitGroup
		wg.Add(1)

		// Loop for 1 second, making as many tickets as we can, up to the requested TPC.
		tpcLogger.Trace("generating tickets until deadline")
		go func() {
			defer wg.Done()
			defer cycleCancel()

			// Start a new cycle after 1 second, whether we queued the requested TPC or not.
			deadlineDur := time.Millisecond * time.Duration(q.Cfg.GetInt("TICKET_CREATION_CYCLE_DURATION_MS"))
			deadline := time.NewTimer(deadlineDur)
			defer deadline.Stop()
			startTime := time.Now()

			var numTixQueued int64
			var ticketCounterMutex sync.Mutex
			durationRecorded := false
			var dur time.Duration
			tqCh := tq

			finishCycle := func() {
				ticketCounterMutex.Lock()
				numTixCreationsAchieved := numTixQueued
				ticketCounterMutex.Unlock()

				if !durationRecorded {
					dur = time.Since(startTime)
					// Record length of the cycle
					q.otelTicketGenerationCycleDurations.Record(ctx, dur.Milliseconds())
					durationRecorded = true
				}
				// Record successful ticket generations this cycle
				q.otelTicketGenerationsAchievedPerCycle.Record(ctx, numTixCreationsAchieved)

				q.tpcMutex.Lock()
				reqTPC := q.TPC
				q.tpcMutex.Unlock()

				// Cycle status log once per cycle
				tpcLogger.Infof("matchmaker queue ticket creation cycle %010d/%010d: ticket creations %05d/%05d (achieved/requested), time elapsed %.3f/%.3f seconds",
					q.Cfg.GetInt("TICKET_CREATION_CYCLES")+1-i,
					q.Cfg.GetInt("TICKET_CREATION_CYCLES")+1,
					numTixCreationsAchieved,
					reqTPC,
					float64(dur.Milliseconds())/1000.0,
					float64(deadlineDur.Milliseconds())/1000.0,
				)
			}

			for {
				select {
				case <-ctx.Done():
					return
				// Read all the structs put into the channel by the tpc goroutine below.
				case _, ok := <-tqCh:
					if ok {
						// There are still structs in the channel representing tickets we want
						// created.

						// Make a buffered channel on which the queue will return
						// its http.Status result so sending on ResultChan never blocks
						// if the cycle deadline has already elapsed.
						rChan := make(chan int, 1)
						q.tpcMutex.Lock()
						ticketFunc := q.newTicket
						q.tpcMutex.Unlock()
						// Guard the send on deadline.C and ctx.Done() so this goroutine
						// neither overruns the cycle deadline when ClientRequestChan is
						// full nor deadlocks if q.Run(ctx) has already exited.
						select {
						case q.ClientRequestChan <- &ClientRequest{
							ResultChan: rChan,
							// Use the provided internal library to generate a simple
							// ticket with a few example attributes for testing.
							Ticket: ticketFunc(ctx),
						}:
						case <-deadline.C:
							finishCycle()
							return
						case <-ctx.Done():
							return
						}

						// In this example, we simply count the ticket creation results.
						go func() {
							select {
							case status := <-rChan:
								success := int64(1)
								if status != http.StatusOK {
									success = int64(0)
								}
								ticketCounterMutex.Lock()
								numTixQueued += success
								ticketCounterMutex.Unlock()
							case <-cycleCtx.Done():
								return
							}
						}()

					} else {
						tqCh = nil
						if !durationRecorded {
							// no more tickets to create this second, record how long elapsed
							dur = time.Since(startTime)
							q.otelTicketGenerationCycleDurations.Record(ctx, dur.Milliseconds())
							durationRecorded = true
						}
					}

				case <-deadline.C:
					// deadline reached; return from this function and call the deferred
					// waitgroup Done(), which signals that we've processed
					// as many tickets as we can this second
					finishCycle()
					return
				}
			}
		}()

		// Simple tpc goroutine that puts one struct in the channel for every ticket
		// we want to generate this second.
		go func(ch chan struct{}) {
			defer close(ch)
			// TPC can be updated at any time, so protect it with a mutex.
			q.tpcMutex.Lock()
			thisTPC := q.TPC
			q.tpcMutex.Unlock()
			for i := thisTPC; i > 0; i-- {
				// Select on cycleCtx.Done() so this producer goroutine immediately
				// exits when the 1-second cycle deadline fires or ctx is cancelled.
				select {
				case ch <- struct{}{}:
				case <-cycleCtx.Done():
					return
				}
			}
		}(tq)

		// Wait for ticket creation goroutine to hit the 1 second deadline.
		wg.Wait()
	} // end for loop

}

// proxyCreateTicket Example
// Your platform services layer should take the matchmaking request (in this
// example, we assume it is from a game client, but it could come from another
// of your game platform services as well), add attributes your platform
// services have authority over (ex: MMR, ELO, inventory, etc), then call Open
// Match core on the client's behalf to make a matchmaking ticket. In this sense,
// your platform service acts as a 'proxy' for the player's game client from
// the viewpoint of Open Match.
func (q *MatchmakerQueue) proxyCreateTicket(ctx context.Context, ticket *pb.Ticket) (string, error) {
	// Derive from the caller's ctx (rather than context.Background()) so queue
	// shutdown cancellation propagates into CreateTicket and stops retry backoff.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Local var declarations
	var id string
	var err error
	mmr := 0.0

	logger := q.Log.WithFields(logrus.Fields{
		"component": "matchmaking_queue",
		"operation": "proxy_CreateTicket",
	})
	q.ensureMetrics(logger)
	logger.Trace("creating ticket")

	// Here is where your matchmaker would make additional calls to your
	// game platform services to add additional matchmaking attributes.
	// Ex: mmr, err := mmrService.Get(ticket.Id)
	if ticket.GetAttributes() == nil {
		ticket.Attributes = &pb.Ticket_FilterableData{}
	}
	if ticket.GetAttributes().GetDoubleArgs() == nil {
		ticket.Attributes.DoubleArgs = make(map[string]float64)
	}
	ticket.Attributes.DoubleArgs["example_mmr"] = mmr

	// With all matchmaking attributes collected from the client and the
	// game backend services, we're now ready to put the ticket in open match.
	err = backoff.RetryNotify(
		func() error {
			// The API call we want to retry with exponential backoff and jitter
			id, err = q.OmClient.CreateTicket(ctx, ticket)
			return err
		},
		// Wrap the exponential backoff policy with backoff.WithContext so retries
		// abort immediately if ctx is cancelled during shutdown.
		// TODO: expose max elapsed retry time as a configurable deadline parameter
		backoff.WithContext(backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(5*time.Second)), ctx),
		func(err error, bo time.Duration) {
			q.otelTicketCreationRetries.Add(ctx, 1)
			logger.Warnf("CreateTicket temporary failure (backoff for %v): %v", err, bo)
		},
	)

	if err == nil {
		// Log successful ticket creation
		logger.Debugf("CreateTicket %v successful", id)
	}
	return id, err
}

// readAllBody is a simple helper function to make sure an HTTP body is completely read and closed.
func readAllBody(resp http.Response, logger *logrus.Entry) ([]byte, error) {
	// Get results
	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		logger.Errorf("cannot read bytes from http response body")
		return nil, err
	}

	return body, err
}

func syncMapDump(sm *sync.Map) map[string]interface{} {
	out := map[string]interface{}{}
	sm.Range(func(key, value interface{}) bool {
		out[fmt.Sprint(key)] = value
		return true
	})
	return out
}
