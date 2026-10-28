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
	"math/rand/v2"
	"net/http"
	// revive: blank-imports — pprof registers profiling HTTP handlers via side-effect import.
	// revive:disable-next-line:blank-imports
	_ "net/http/pprof"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/metric"

	// Required for protojson to correctly parse JSON when unmarshalling to protobufs that contain
	// 'well-known types' https://github.com/golang/protobuf/issues/1156
	// revive:disable-next-line:blank-imports
	_ "google.golang.org/protobuf/types/known/wrapperspb"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"open-match.dev/open-match-ecosystem/v2/internal/assignmentdistributor"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

// Mock of the game platform services frontend that handles player matchmaking
// requests, queueing them to be sent to Open Match, and processing them
// asynchronously.
type MatchmakerQueue struct {
	OmClient *omclient.RestfulOMGrpcClient
	Cfg      *viper.Viper
	Log      *logrus.Logger
	// Tickets maps the ID of each ticket this queue created and is waiting
	// on (for an assignment) to the time.Time at which om-core will expire
	// it. Entries are removed when the ticket is assigned, when its
	// request fails or is abandoned, or by the expiry sweep in Run.
	Tickets            sync.Map
	ClientRequestChan  chan *ClientRequest
	AssignmentReceiver assignmentdistributor.Receiver
	OtelMeterPtr       *metric.Meter
	TPC                int64

	tpcMu         sync.Mutex
	ticketGenFunc func(context.Context) *pb.Ticket
	metricsOnce   sync.Once

	ticketAssignmentsCounter          metric.Int64Counter
	ticketAssignmentsUntrackedCounter metric.Int64Counter
	ticketCreationRetriesCounter      metric.Int64Counter
	ticketCreationsCounter            metric.Int64Counter
	ticketExpirationsCounter          metric.Int64Counter
	ticketAbandonmentsCounter         metric.Int64Counter
	tpcRequestGauge                   metric.Int64ObservableGauge
	tpcActualHistogram                metric.Int64Histogram
	tpcConfirmedHistogram             metric.Int64Histogram
	ticketGenerationDurationHistogram metric.Int64Histogram
	ticketActivationsCounter          metric.Int64Counter
	ticketActivationFailuresCounter   metric.Int64Counter
	ticketQueuedDurationsHistogram    metric.Float64Histogram
	ticketArrivalLagHistogram         metric.Float64Histogram
}

// ClientRequest is one game client's matchmaking request, handed to the
// queue on ClientRequestChan.
type ClientRequest struct {
	// Ctx is the client's request context (for an HTTP frontend, the
	// request's r.Context()). If it is done before the queue has reported a
	// result, the client has gone away and its player shouldn't be matched:
	// the queue skips ticket creation if it hasn't started yet, and
	// otherwise makes sure the ticket ends up inactive in om-core.
	//
	// Ctx is only watched until the result is reported. After that, taking
	// the player out of matchmaking (they cancel, or go offline) is up to
	// your platform: call DeactivateTickets with their ticket ID.
	//
	// nil means the request is never abandoned.
	Ctx context.Context

	// Ticket is the ticket to create on the client's behalf.
	// proxyCreateTicket overwrites the attributes the platform has authority
	// over before sending it to om-core.
	Ticket *pb.Ticket

	// ResultChan, if non-nil, receives the outcome of the request as an
	// HTTP status code:
	//   - 200 OK: om-core created and activated the ticket; the player is
	//     now in matchmaking.
	//   - 502 Bad Gateway: om-core's CreateTicket or ActivateTickets failed.
	//   - 503 Service Unavailable: the queue is shutting down.
	//
	// The queue writes to ResultChan exactly once and then closes it. The
	// one exception is an abandoned request (Ctx done), whose ResultChan is
	// closed without a value, since nobody is listening.
	//
	// ResultChan must be buffered (capacity >= 1) and used for a single
	// request, so the queue never blocks writing to it.
	ResultChan chan int
}

// defaultTicketTTL is om-core's default OM_CACHE_TICKET_TTL_MS, used if
// MMQUEUE_TICKET_TTL_MS isn't set.
const defaultTicketTTL = 10 * time.Minute

// pendingActivations lets each request goroutine wait for its own ticket's
// activation result. ActivateTicketsWithResults reports every result on one
// channel, so a single goroutine in Run reads them and hands each one to the
// waiting request goroutine through that ticket's channel.
type pendingActivations struct {
	mu      sync.Mutex
	waiters map[string]chan int // ticket ID -> activation status (buffered, capacity 1)
}

// add registers ticket id as awaiting activation and returns the channel its
// result will arrive on.
func (p *pendingActivations) add(id string) <-chan int {
	ch := make(chan int, 1)
	p.mu.Lock()
	p.waiters[id] = ch
	p.mu.Unlock()
	return ch
}

// resolve delivers ticket id's activation status to its waiter, if it is
// still waiting. It never blocks: each channel has room for its one result,
// and the send happens after the lock is released.
func (p *pendingActivations) resolve(id string, status int) {
	p.mu.Lock()
	ch, ok := p.waiters[id]
	delete(p.waiters, id)
	p.mu.Unlock()
	if ok {
		ch <- status
	}
}

// remove forgets ticket id, for a waiter that stops waiting early.
func (p *pendingActivations) remove(id string) {
	p.mu.Lock()
	delete(p.waiters, id)
	p.mu.Unlock()
}

// Run the matchmaker queue, asynchronously queueing tickets and
// activating them.
func (q *MatchmakerQueue) Run(ctx context.Context) {
	logger := q.Log.WithFields(logrus.Fields{
		"app":       "matchmaker",
		"component": "matchmaking_queue",
	})

	// Var init
	var wg sync.WaitGroup
	ticketIdsToActivate := make(chan string, 10000)
	pending := &pendingActivations{waiters: make(map[string]chan int)}

	// Initialize metrics
	if q.OtelMeterPtr != nil {
		logger.Tracef("Initializing otel metrics")
		q.ensureMetricsRegistered()
	} else {
		logger.Warnf("Unable to add matchmaking queue metrics to the OTEL configuration")
	}

	// Control number of concurrent requests by creating ticket creation 'slots'
	slots := make(chan struct{}, q.Cfg.GetInt("MAX_CONCURRENT_TICKET_CREATIONS"))

	// Goroutine to read from the incoming ticket request channel, and create
	// one ticket for each incoming request. Limit the number of concurrent
	// creations by blocking until one of the MAX_CONCURRENT_TICKET_CREATIONS
	// 'slots' is available.
	wg.Add(1)
	go func() {
		defer wg.Done()

		cLogger := q.Log.WithFields(logrus.Fields{
			"operation": "ticket_creation",
		})
		cLogger.Debug("Processing queued ticket creation requests")

		var creationWg sync.WaitGroup
		defer func() {
			creationWg.Wait()
			close(ticketIdsToActivate)
		}()

		// On shutdown, answer requests already waiting in ClientRequestChan
		// rather than leaving their clients hanging. This is best effort:
		// a request sent after this point is never read, so clients should
		// also stop waiting when their own context is done.
		defer func() {
			for {
				select {
				case request, ok := <-q.ClientRequestChan:
					if !ok {
						return
					}
					if request != nil {
						respond(request, http.StatusServiceUnavailable)
					}
				default:
					return
				}
			}
		}()

		for {
			logger.Trace("waiting for a new ticket to enter the queue")
			var request *ClientRequest
			var ok bool
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

			// Block until one of the MAX_CONCURRENT_TICKET_CREATIONS 'slots' is available
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				respond(request, http.StatusServiceUnavailable)
				return
			}

			// handleRequest releases the slot once CreateTicket returns.
			creationWg.Add(1)
			go func(req *ClientRequest) {
				defer creationWg.Done()
				q.handleRequest(ctx, req, slots, pending, ticketIdsToActivate)
			}(request)
		}
	}()

	// goroutine to activate pending tickets.
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
		atCtx := omclient.WithActivationType(ctx, "activate")

		activationResults := make(chan omclient.ActivationResult, 1000)
		var resultsWg sync.WaitGroup
		resultsWg.Add(1)
		go func() {
			defer resultsWg.Done()
			// Hand each ticket's result to the request goroutine waiting
			// for it (see handleRequest), which reports it to the client.
			for res := range activationResults {
				status := http.StatusOK
				if res.Err != nil {
					aLogger.Errorf("ActivateTickets failed for ticket %s: %v", res.TicketId, res.Err)
					if q.ticketActivationFailuresCounter != nil {
						q.ticketActivationFailuresCounter.Add(ctx, 1)
					}
					status = http.StatusBadGateway
				} else if q.ticketActivationsCounter != nil {
					q.ticketActivationsCounter.Add(ctx, 1)
				}
				pending.resolve(res.TicketId, status)
			}
		}()

		q.OmClient.ActivateTicketsWithResults(atCtx, ticketIdsToActivate, activationResults)
		close(activationResults)
		resultsWg.Wait()
	}()

	// goroutine to process incoming assignments.
	//
	// NOTE: the assignment api endpoints in om-core are deprecated, and
	// shouldn't be used in production.  This implementation assumes you have
	// some other way of getting assignments from your game server director to
	// your matchmaking queue OTHER than going through om-core, but isn't
	// specific about what that 'other way' is. This just processes every
	// assignment delivered by the AssignmentReceiver (see
	// internal/assignmentdistributor for the Go channel and Pub/Sub
	// implementations). In a real implementation, you'd write a Receiver
	// that gets assignments from whatever is handling them.
	//
	// It is recommended that you incorporate your assignment storage and
	// retreival into the part of your online services suite responsible for
	// returning player online status, like a player online event bus or
	// presence service.
	wg.Add(1)
	go func() {
		defer wg.Done()

		bLogger := q.Log.WithFields(logrus.Fields{
			"operation": "ticket_assignment",
		})
		bLogger.Debug("processing incoming ticket assignments")

		assignmentHandler := func(ctx context.Context, roster *pb.Roster) {
			// Get the assignment string
			assignment := roster.GetAssignment().GetConnection()

			// Loop through all tickets in the assignment roster
			for _, ticket := range roster.GetTickets() {
				// Stop tracking this ticket; its matchmaking is complete.
				// Your matchmaker may wish to instead keep this for a time (in
				// case the game client should be allowed to ask for the same
				// assignment again later).
				if _, existed := q.Tickets.LoadAndDelete(ticket.GetId()); existed {
					bLogger.Debugf("Received ticket %v assignment: %v", ticket.GetId(), assignment)
					// Update metric
					if q.ticketAssignmentsCounter != nil {
						q.ticketAssignmentsCounter.Add(ctx, 1)
					}
					// Time from ticket creation (set by proxyCreateTicket)
					// to receiving its assignment, in fractional milliseconds.
					queuedDur := time.Since(ticket.GetAttributes().GetCreationTime().AsTime())
					if q.ticketQueuedDurationsHistogram != nil {
						q.ticketQueuedDurationsHistogram.Record(ctx, float64(queuedDur.Microseconds())/1000.0)
					}
				} else {
					// Not a ticket this instance is tracking. With Pub/Sub
					// distribution every mmqueue instance has its own
					// subscription and receives every assignment, so most
					// of these are other instances' tickets: this count
					// grows with the number of instances. The rest are
					// redelivered or duplicate assignments, and tickets
					// this instance stopped tracking (expired or abandoned).
					// Telling those apart would need a record of every
					// ticket ever seen, which grows without bound.
					if q.ticketAssignmentsUntrackedCounter != nil {
						q.ticketAssignmentsUntrackedCounter.Add(ctx, 1)
					}
					bLogger.Tracef("Ignoring assignment for untracked ticket %v: %v", ticket.GetId(), assignment)
				}
			}
		}
		bLogger.Info("Starting assignment receiver")
		err := q.AssignmentReceiver.Receive(ctx, assignmentHandler)
		if err != nil {
			bLogger.Errorf("Assignment receiver error: %v", err)
		}
	}()

	// goroutine to stop tracking tickets that om-core has expired.
	//
	// om-core deletes every ticket OM_CACHE_TICKET_TTL_MS after its creation
	// time, whether or not it was matched, and has no DeleteTicket RPC. A
	// ticket that's still here after that will never get an assignment, so
	// it is dropped from q.Tickets. A real platform would also tell the
	// player's client that its matchmaking request timed out, so it can
	// re-queue (which creates a new ticket).
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(expirySweepInterval(q.ticketTTL()))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				q.sweepExpiredTickets(ctx, now)
			}
		}
	}()

	// Don't exit the Run() function as long as any of the goroutines are
	// running (in normal operation, this is forever).
	wg.Wait()
}

// handleRequest creates and activates one client's ticket and reports the
// outcome on its ResultChan (see ClientRequest). It is called holding one of
// the ticket creation slots, and releases it as soon as CreateTicket returns,
// because activation is batched separately.
func (q *MatchmakerQueue) handleRequest(ctx context.Context, req *ClientRequest, slots <-chan struct{}, pending *pendingActivations, ticketIdsToActivate chan<- string) {
	logger := q.Log.WithFields(logrus.Fields{
		"component": "matchmaking_queue",
		"operation": "handle_request",
	})

	// A nil Ctx is never abandoned; a context that's never done keeps the
	// code below uniform.
	clientCtx := req.Ctx
	if clientCtx == nil {
		clientCtx = context.Background()
	}
	// Normalise a missing ticket once, so the ticket sent to om-core and the
	// one its expiry is computed from are the same.
	if req.Ticket == nil {
		req.Ticket = &pb.Ticket{}
	}

	// The client left while its request was queued: don't create a ticket
	// for a player who isn't there.
	if clientCtx.Err() != nil {
		<-slots
		q.abandon(ctx, req, "")
		return
	}

	// proxyCreateTicket retries with exponential backoff and jitter.
	id, err := q.proxyCreateTicket(ctx, req.Ticket)
	<-slots
	if err != nil {
		if ctx.Err() != nil {
			respond(req, http.StatusServiceUnavailable)
			return
		}
		// Either a permanent error, or retries timed out.
		logger.Errorf("CreateTicket failed: %v", err)
		respond(req, http.StatusBadGateway)
		return
	}
	if q.ticketCreationsCounter != nil {
		q.ticketCreationsCounter.Add(ctx, 1)
	}

	// om-core creates tickets in the inactive state, so a ticket that's
	// never activated never appears in a pool; om-core just expires it.
	// That makes these two cases simple: don't activate it.
	if ctx.Err() != nil {
		respond(req, http.StatusServiceUnavailable)
		return
	}
	if clientCtx.Err() != nil {
		q.abandon(ctx, req, id)
		return
	}

	q.Tickets.Store(id, q.ticketExpiry(req.Ticket))
	result := pending.add(id)
	select {
	case ticketIdsToActivate <- id:
	case <-ctx.Done():
		pending.remove(id)
		q.Tickets.Delete(id)
		respond(req, http.StatusServiceUnavailable)
		return
	}

	var status int
	select {
	case status = <-result:
	case <-ctx.Done():
		// Shutting down: the activation results stop arriving. The
		// activation may still take effect in om-core; the ticket then
		// stays in the pools until om-core expires it.
		pending.remove(id)
		q.Tickets.Delete(id)
		respond(req, http.StatusServiceUnavailable)
		return
	case <-clientCtx.Done():
		// The client left while its ticket was being activated. Wait for
		// the activation call to finish before deactivating: om-core
		// applies ticket state changes in the order it receives them, so
		// a deactivation sent now could be undone by the activation still
		// in flight, leaving a ghost player in the pools.
		select {
		case <-result:
		case <-ctx.Done():
			pending.remove(id)
		}
		q.Tickets.Delete(id)
		q.deactivate(ctx, id, "client abandoned the request")
		q.abandon(ctx, req, id)
		return
	}

	if status != http.StatusOK {
		// ActivateTickets reported a failure, but the update may still
		// have reached om-core (for example, a timeout after om-core
		// applied it). Deactivate the ticket so a player who was told
		// matchmaking failed can't be matched anyway.
		q.Tickets.Delete(id)
		respond(req, status)
		q.deactivate(ctx, id, "activation failed")
		return
	}
	respond(req, http.StatusOK)
}

// respond writes status to req.ResultChan, if any, and closes it. It must be
// called at most once per request.
func respond(req *ClientRequest, status int) {
	if req.ResultChan == nil {
		return
	}
	req.ResultChan <- status
	close(req.ResultChan)
}

// abandon finishes a request whose client has gone away (its Ctx is done).
// Nobody is listening for the result, so ResultChan is closed without one.
// ticketID is empty if no ticket was created.
func (q *MatchmakerQueue) abandon(ctx context.Context, req *ClientRequest, ticketID string) {
	q.Log.WithFields(logrus.Fields{
		"component": "matchmaking_queue",
		"operation": "handle_request",
	}).Debugf("client abandoned matchmaking request (ticket %q)", ticketID)
	if q.ticketAbandonmentsCounter != nil {
		q.ticketAbandonmentsCounter.Add(ctx, 1)
	}
	if req.ResultChan != nil {
		close(req.ResultChan)
	}
}

// deactivate takes ticket id out of matchmaking in om-core. Failure is
// logged but otherwise ignored: om-core expires the ticket eventually
// either way.
//
// This makes one call per ticket for simplicity. A matchmaker that
// deactivates tickets often should batch them, the way activations are.
func (q *MatchmakerQueue) deactivate(ctx context.Context, id string, reason string) {
	if err := q.OmClient.DeactivateTickets(ctx, []string{id}); err != nil {
		q.Log.WithFields(logrus.Fields{
			"component": "matchmaking_queue",
			"operation": "deactivate_ticket",
		}).Warnf("could not deactivate ticket %s (%s); om-core will expire it: %v", id, reason, err)
	}
}

// ticketTTL is how long om-core keeps a ticket after its creation time
// (MMQUEUE_TICKET_TTL_MS, which should match om-core's
// OM_CACHE_TICKET_TTL_MS).
func (q *MatchmakerQueue) ticketTTL() time.Duration {
	if ms := q.Cfg.GetInt("MMQUEUE_TICKET_TTL_MS"); ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return defaultTicketTTL
}

// ticketExpiry returns when om-core will expire ticket: its creation time
// plus the ticket TTL, or the ticket's own ExpirationTime if that is earlier
// (om-core accepts a shorter expiration, but not a longer one).
func (q *MatchmakerQueue) ticketExpiry(ticket *pb.Ticket) time.Time {
	expiry := ticket.GetAttributes().GetCreationTime().AsTime().Add(q.ticketTTL())
	if et := ticket.GetExpirationTime(); et.IsValid() && et.AsTime().Before(expiry) {
		expiry = et.AsTime()
	}
	return expiry
}

// expirySweepInterval is how often Run looks for expired tickets: a tenth of
// the TTL, so a ticket is dropped at most 10% late, between 100ms and 1
// minute.
func expirySweepInterval(ttl time.Duration) time.Duration {
	return min(max(ttl/10, 100*time.Millisecond), time.Minute)
}

// sweepExpiredTickets stops tracking every ticket whose expiry is before now.
func (q *MatchmakerQueue) sweepExpiredTickets(ctx context.Context, now time.Time) {
	expired := 0
	q.Tickets.Range(func(id, expiry any) bool {
		if exp, ok := expiry.(time.Time); ok && now.After(exp) && q.Tickets.CompareAndDelete(id, expiry) {
			q.Log.WithFields(logrus.Fields{
				"component": "matchmaking_queue",
				"operation": "expire_tickets",
			}).Debugf("ticket %v expired in om-core before it was assigned", id)
			expired++
		}
		return true
	})
	if expired > 0 && q.ticketExpirationsCounter != nil {
		q.ticketExpirationsCounter.Add(ctx, int64(expired))
	}
}

// SetTestTicketConfig instructs the test function GenerateTestTickets() to make `newTPC`
// tickets per second using `ticketCreationFunc()`. Example mock client ticket
// creations functions can be found in internal/mocks/gameclient
func (q *MatchmakerQueue) SetTestTicketConfig(newTPC int64,
	ticketCreationFunc func(context.Context) *pb.Ticket) {

	// This can be updated at any time by the calling code, so protect it with a mutex
	q.tpcMu.Lock()
	q.TPC = newTPC
	q.ticketGenFunc = ticketCreationFunc
	q.tpcMu.Unlock()
}

// GenerateTestTickets simulates players joining the matchmaking queue
// organically over time, at an average rate of TPC tickets per cycle
// (TICKET_CREATION_CYCLE_DURATION_MS, default 1 second).
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
//
// Arrivals: at the start of each cycle, TPC arrival times are scheduled
// across the cycle as a Poisson process (see poissonArrivalOffsets), so
// players trickle in with natural clusters and lulls rather than all at once.
// Changes to TPC take effect at the next cycle boundary. Set
// TICKET_ARRIVAL_SEED to a non-zero value for a reproducible arrival schedule.
//
// Backpressure: arrivals stay on schedule regardless of how fast the queue
// accepts them. If the queue blocks, overdue players are handed to it as soon
// as it unblocks (as a real backlog would drain), and the delay is recorded
// in mmqueue.ticket.arrival_lag. Players not accepted by the end of the cycle
// are the cycle's shortfall (requested - mmqueue.tpc.actual); the one player
// waiting at the deadline is carried over to the front of the next cycle.
//
// Each generated ticket behaves like a real game client request (see the
// /tickets handler in examples/standalone/mmqueue): it carries a ResultChan and
// waits for the queue to report whether om-core confirmed the ticket was
// created and activated.
//
// Each cycle reports two numbers:
//   - mmqueue.tpc.actual: tickets accepted by the queue, recorded at the deadline.
//   - mmqueue.tpc.confirmed: of those, tickets confirmed created and activated,
//     recorded once every result for that cycle has come back. Confirmations
//     routinely arrive after the cycle deadline (creation retries, activation
//     batching), so they are awaited independently of the cycle timer.
//
// When TICKET_CREATION_CYCLES is finite, GenerateTestTickets returns after
// the final cycle's results are in, or when ctx is cancelled.
func (q *MatchmakerQueue) GenerateTestTickets(ctx context.Context) {
	q.ensureMetricsRegistered()

	tpcLogger := q.Log.WithFields(logrus.Fields{"component": "tester", "operation": "generate_tickets"})

	numCycles := q.Cfg.GetInt("TICKET_CREATION_CYCLES")

	// Random source for arrival schedules. Only used from this goroutine.
	seed := q.Cfg.GetUint64("TICKET_ARRIVAL_SEED")
	if seed == 0 {
		seed = rand.Uint64()
	}
	rng := rand.New(rand.NewPCG(seed, seed))
	tpcLogger.Debugf("test ticket arrival seed: %d", seed)

	// A request that was generated but not accepted by the queue before its
	// cycle's deadline. It is sent first in the next cycle (consuming one of
	// that cycle's arrivals) instead of being discarded. carryAt is the time
	// that player originally arrived, for arrival lag reporting.
	var carry *ClientRequest
	var carryAt time.Time

	// Tracks per-cycle confirmation reporters so we don't return before the
	// last cycle's results have been recorded.
	var reportersWg sync.WaitGroup
	defer reportersWg.Wait()

	for cycle := 1; cycle <= numCycles; cycle++ {
		if ctx.Err() != nil {
			return
		}

		cycleCtx, cycleCancel := context.WithCancel(ctx)

		q.tpcMu.Lock()
		thisTPC := q.TPC
		ticketFunc := q.ticketGenFunc
		q.tpcMu.Unlock()

		tpcLogger.Trace("generating tickets until deadline")
		deadlineDur := time.Millisecond * time.Duration(q.Cfg.GetInt("TICKET_CREATION_CYCLE_DURATION_MS"))
		arrivals := poissonArrivalOffsets(rng, thisTPC, deadlineDur)
		deadline := time.NewTimer(deadlineDur)
		startTime := time.Now()

		// Arrival goroutine: sends each player's scheduled arrival time on tq
		// once that time has come. tq is unbuffered, so if the main loop is
		// blocked handing a request to the queue, later arrivals wait here
		// and are released back-to-back once it unblocks.
		tq := make(chan time.Time)
		go func() {
			defer close(tq)
			timer := time.NewTimer(time.Hour)
			timer.Stop()
			defer timer.Stop()
			for _, offset := range arrivals {
				at := startTime.Add(offset)
				// Arrivals closer together than the timer granularity are
				// released immediately rather than sleeping for each one.
				if wait := time.Until(at); wait > arrivalTimerGranularity {
					timer.Reset(wait)
					select {
					case <-cycleCtx.Done():
						return
					case <-timer.C:
					}
				}
				select {
				case <-cycleCtx.Done():
					return
				case tq <- at:
				}
			}
		}()

		// Only accessed from this goroutine.
		var numTixQueued int64
		durationRecorded := false
		var dur time.Duration

		// Confirmation results for tickets queued this cycle; updated by
		// per-ticket watcher goroutines that may outlive the cycle.
		var confirmWg sync.WaitGroup
		var numConfirmed, numFailed atomic.Int64

		recordDuration := func() {
			if durationRecorded {
				return
			}
			dur = time.Since(startTime)
			if q.ticketGenerationDurationHistogram != nil {
				q.ticketGenerationDurationHistogram.Record(ctx, dur.Milliseconds())
			}
			durationRecorded = true
		}

		finishCycle := func() {
			recordDuration()
			if q.tpcActualHistogram != nil {
				q.tpcActualHistogram.Record(ctx, numTixQueued)
			}

			tpcLogger.Infof("matchmaker queue ticket creation cycle %010d/%010d: tickets queued %05d/%05d (achieved/requested), time elapsed %.3f/%.3f seconds",
				cycle,
				numCycles,
				numTixQueued,
				thisTPC,
				float64(dur.Milliseconds())/1000.0,
				float64(deadlineDur.Milliseconds())/1000.0,
			)
		}

	cycleLoop:
		for {
			select {
			case <-ctx.Done():
				deadline.Stop()
				cycleCancel()
				return

			case arrivedAt, ok := <-tq:
				if !ok {
					// All requested tickets for this cycle have been queued.
					tq = nil
					recordDuration()
					continue
				}

				req := carry
				carry = nil
				if req != nil {
					arrivedAt = carryAt
				} else {
					var ticket *pb.Ticket
					if ticketFunc != nil {
						ticket = ticketFunc(ctx)
					} else {
						ticket = &pb.Ticket{}
					}
					// ResultChan is buffered so the queue never blocks
					// reporting a result, even if we've stopped listening.
					req = &ClientRequest{Ticket: ticket, ResultChan: make(chan int, 1)}
				}

				select {
				case <-ctx.Done():
					deadline.Stop()
					cycleCancel()
					return
				case <-deadline.C:
					carry = req
					carryAt = arrivedAt
					finishCycle()
					break cycleLoop
				case q.ClientRequestChan <- req:
					numTixQueued++
					if q.ticketArrivalLagHistogram != nil {
						q.ticketArrivalLagHistogram.Record(ctx, float64(time.Since(arrivedAt).Microseconds())/1000.0)
					}
					// Wait for om-core's confirmation like a real client
					// would. Uses the parent ctx (not cycleCtx) so results
					// that arrive after the cycle deadline still count.
					confirmWg.Add(1)
					go func(resChan <-chan int) {
						defer confirmWg.Done()
						select {
						case <-ctx.Done():
						case status, ok := <-resChan:
							if ok && status == http.StatusOK {
								numConfirmed.Add(1)
							} else {
								numFailed.Add(1)
							}
						}
					}(req.ResultChan)
				}

			case <-deadline.C:
				finishCycle()
				break cycleLoop
			}
		}

		deadline.Stop()
		cycleCancel()

		// Record this cycle's confirmations once all of its results are in,
		// without holding up the next cycle.
		reportersWg.Add(1)
		go func(cycle int, queued int64) {
			defer reportersWg.Done()
			confirmWg.Wait()
			if ctx.Err() != nil {
				// Shutting down; outstanding results were abandoned.
				return
			}
			confirmed := numConfirmed.Load()
			if q.tpcConfirmedHistogram != nil {
				q.tpcConfirmedHistogram.Record(ctx, confirmed)
			}
			tpcLogger.Infof("matchmaker queue ticket creation cycle %010d/%010d: tickets confirmed %05d/%05d (confirmed/queued), %05d failed, all results in after %.3f seconds",
				cycle,
				numCycles,
				confirmed,
				queued,
				numFailed.Load(),
				time.Since(startTime).Seconds(),
			)
		}(cycle, numTixQueued)
	}
}

// arrivalTimerGranularity is the smallest wait for which the test ticket
// generator sleeps until a scheduled arrival; arrivals due sooner than this
// are released immediately. Keeps timer churn bounded at high TPC, where the
// mean gap between arrivals can be well under a millisecond.
const arrivalTimerGranularity = time.Millisecond

// poissonArrivalOffsets returns n arrival offsets in [0, window), sorted
// ascending.
//
// Placing n points independently and uniformly at random in a window and
// sorting them gives exactly the arrival times of a Poisson process
// conditioned on n arrivals in that window. The gaps between consecutive
// arrivals are (approximately) exponentially distributed, which models
// independently-arriving players with natural clusters and lulls, while
// still producing exactly the requested number of arrivals per cycle.
func poissonArrivalOffsets(rng *rand.Rand, n int64, window time.Duration) []time.Duration {
	if n <= 0 {
		return nil
	}
	offsets := make([]time.Duration, n)
	if window <= 0 {
		// No window to spread across; everyone arrives at once.
		return offsets
	}
	for i := range offsets {
		offsets[i] = time.Duration(rng.Int64N(int64(window)))
	}
	slices.Sort(offsets)
	return offsets
}

// proxyCreateTicket Example
// Your platform services layer should take the matchmaking request (in this
// example, we assume it is from a game client, but it could come from another
// of your game platform services as well), add attributes your platform
// services have authority over (ex: MMR, ELO, inventory, etc), then call Open
// Match core on the client's behalf to make a matchmaking ticket. In this sense,
// your platform service acts as a 'proxy' for the player's game client from
// the viewpoint of Open Match.
//
// ticket must be non-nil (handleRequest normalises a missing one); it is
// modified in place, so the caller sees the attributes that were sent.
func (q *MatchmakerQueue) proxyCreateTicket(ctx context.Context, ticket *pb.Ticket) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q.ensureMetricsRegistered()

	// Local var declarations
	var id string
	var err error

	logger := q.Log.WithFields(logrus.Fields{
		"component": "matchmaking_queue",
		"operation": "proxy_CreateTicket",
	})
	logger.Trace("creating ticket")

	if ticket.GetAttributes() == nil {
		ticket.Attributes = &pb.Ticket_FilterableData{}
	}

	// The platform, not the client, decides when the player entered
	// matchmaking. This is the time the queue's ticket expiry tracking and
	// the queued_durations metric are measured from, and om-core expires
	// the ticket OM_CACHE_TICKET_TTL_MS after it.
	ticket.Attributes.CreationTime = timestamppb.Now()

	// Here is where your matchmaker would make additional calls to your
	// game platform services to add matchmaking attributes it has authority
	// over, overwriting anything the client sent for them. For example:
	//   mmr, err := mmrService.Get(playerID)
	//   ticket.Attributes.DoubleArgs["mmr"] = mmr
	// For simplicity, this example trusts the "mmr" the mock game client
	// (internal/mocks/gameclient) puts in the ticket; the matchmaker's skill
	// tier pools filter on it. Never trust a real client with its own MMR.

	// With all matchmaking attributes collected from the client and the
	// game backend services, we're now ready to put the ticket in open match.
	//
	// Each CreateTicket attempt is bounded by OM_CORE_REQUEST_TIMEOUT_MS
	// (default 5s), so the retry budget has to be well beyond that for an
	// attempt that times out to actually be retried. CreateTicket marks
	// errors that retrying can't fix as permanent, so those aren't retried.
	expBackoff := backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(15 * time.Second))
	err = backoff.RetryNotify(
		func() error {
			// The API call we want to retry with exponential backoff and jitter
			id, err = q.OmClient.CreateTicket(ctx, ticket)
			return err
		},
		// TODO: expose max elapsed retry time as a configurable deadline parameter
		backoff.WithContext(expBackoff, ctx),
		func(err error, bo time.Duration) {
			if q.ticketCreationRetriesCounter != nil {
				q.ticketCreationRetriesCounter.Add(ctx, 1)
			}
			logger.Warnf("CreateTicket temporary failure (backoff for %v): %v", bo, err)
		},
	)

	if err == nil {
		// Log successful ticket creation
		logger.Debugf("CreateTicket %v successful", id)
	}
	return id, err
}
