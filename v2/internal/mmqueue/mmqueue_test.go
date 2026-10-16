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
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"open-match.dev/open-match-ecosystem/v2/internal/assignmentdistributor"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

// testMeter returns a no-op OpenTelemetry Meter pointer so MatchmakerQueue.Run
// initializes its package-level metric instruments without external exporters.
func testMeter() *metric.Meter {
	m := noop.NewMeterProvider().Meter("mmqueue-test")
	return &m
}

// newTestQueue constructs a MatchmakerQueue wired to the given mock om-core URL
// and a synchronous channel-backed assignmentdistributor.ChannelReceiver.
func newTestQueue(omCoreURL string, maxConcurrentCreates int, maxActivationBatch int) (*MatchmakerQueue, chan *pb.Roster) {
	cfg := viper.New()
	cfg.Set("OM_CORE_ADDR", omCoreURL)
	cfg.Set("MAX_CONCURRENT_TICKET_CREATIONS", maxConcurrentCreates)
	cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", maxActivationBatch)

	log := logrus.New()
	log.SetOutput(io.Discard)

	assignmentsChan := make(chan *pb.Roster, 32)
	q := &MatchmakerQueue{
		OmClient:           omclient.New(cfg, log, &http.Client{Timeout: 5 * time.Second}),
		Cfg:                cfg,
		Log:                log,
		ClientRequestChan:  make(chan *ClientRequest, 64),
		AssignmentReceiver: assignmentdistributor.NewChannelReceiver(assignmentsChan),
		AssignmentsChan:    assignmentsChan,
		OtelMeterPtr:       testMeter(),
	}
	return q, assignmentsChan
}

// TestMatchmakerQueueRun_ConcurrencyLimit verifies that MatchmakerQueue.Run
// bounds concurrent in-flight proxyCreateTicket calls to
// MAX_CONCURRENT_TICKET_CREATIONS by releasing each concurrency slot inside the
// worker goroutine's defer rather than immediately in the dispatch loop.
func TestMatchmakerQueueRun_ConcurrencyLimit(t *testing.T) {
	const (
		maxConcurrent = 3
		totalRequests = 10
	)

	var (
		inFlightCreates  atomic.Int32
		peakCreates      atomic.Int32
		totalCreates     atomic.Int32
		reachedMaxOnce   sync.Once
		reachedMaxCh     = make(chan struct{})
		releaseFirstWave = make(chan struct{})
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tickets":
			cur := inFlightCreates.Add(1)
			for {
				prev := peakCreates.Load()
				if cur <= prev || peakCreates.CompareAndSwap(prev, cur) {
					break
				}
			}
			if cur == maxConcurrent {
				reachedMaxOnce.Do(func() {
					close(reachedMaxCh)
				})
			}

			// Hold the first wave until the test verifies no 4th request enters /tickets.
			<-releaseFirstWave
			inFlightCreates.Add(-1)

			id := fmt.Sprintf("1790000000000-%d", totalCreates.Add(1))
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: id})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)

		case "/tickets:activate":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	q, assignmentsChan := newTestQueue(srv.URL, maxConcurrent, 1)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		q.Run(ctx)
	}()
	defer func() {
		cancel()
		close(assignmentsChan)
		<-runDone
	}()

	// Enqueue all 10 client requests immediately.
	resultChans := make([]chan int, totalRequests)
	for i := 0; i < totalRequests; i++ {
		ch := make(chan int, 1)
		resultChans[i] = ch
		q.ClientRequestChan <- &ClientRequest{
			Ticket:     &pb.Ticket{},
			ResultChan: ch,
		}
	}

	// Wait until 3 concurrent /tickets calls are in flight.
	select {
	case <-reachedMaxCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %d concurrent /tickets requests (inFlight=%d)", maxConcurrent, inFlightCreates.Load())
	}

	// Pause briefly while the remaining 7 requests are queued; peak concurrency must stay at 3.
	time.Sleep(30 * time.Millisecond)
	if got := peakCreates.Load(); got != maxConcurrent {
		t.Fatalf("peak concurrent /tickets calls while first wave blocked = %d, want %d", got, maxConcurrent)
	}

	// Release the gate and verify all 10 requests complete with HTTP 200 OK without ever exceeding 3 concurrent calls.
	close(releaseFirstWave)

	for i, ch := range resultChans {
		select {
		case statusCode, ok := <-ch:
			if !ok {
				t.Fatalf("request %d ResultChan closed without status", i)
			}
			if statusCode != http.StatusOK {
				t.Fatalf("request %d status = %d, want %d (http.StatusOK)", i, statusCode, http.StatusOK)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for request %d ResultChan", i)
		}
	}

	if got := peakCreates.Load(); got > maxConcurrent {
		t.Fatalf("peak concurrent /tickets calls = %d, want <= %d", got, maxConcurrent)
	}
	if got := totalCreates.Load(); got != totalRequests {
		t.Fatalf("total /tickets calls = %d, want %d", got, totalRequests)
	}
}

// TestMatchmakerQueueRun_WaitsForActivationBeforeStatusOK verifies that after
// proxyCreateTicket returns a ticket ID, MatchmakerQueue.Run stores the ticket
// in q.Tickets and withholds sending http.StatusOK on request.ResultChan until
// ActivateTicketsWithResults confirms the ticket is active in om-core.
func TestMatchmakerQueueRun_WaitsForActivationBeforeStatusOK(t *testing.T) {
	const ticketID = "1790000000000-42"

	var (
		createDoneOnce    sync.Once
		createDoneCh      = make(chan struct{})
		activateStartOnce sync.Once
		activateStartCh   = make(chan struct{})
		releaseActivateCh = make(chan struct{})
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tickets":
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: ticketID})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
			createDoneOnce.Do(func() {
				close(createDoneCh)
			})

		case "/tickets:activate":
			activateStartOnce.Do(func() {
				close(activateStartCh)
			})
			<-releaseActivateCh
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	q, assignmentsChan := newTestQueue(srv.URL, 5, 1)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		q.Run(ctx)
	}()
	defer func() {
		cancel()
		close(assignmentsChan)
		<-runDone
	}()

	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{
		Ticket: &pb.Ticket{
			Attributes: &pb.Ticket_FilterableData{
				CreationTime: timestamppb.Now(),
			},
		},
		ResultChan: resChan,
	}

	// Wait for /tickets to finish and /tickets:activate to be reached.
	select {
	case <-createDoneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /tickets handler")
	}
	select {
	case <-activateStartCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /tickets:activate handler")
	}

	// While /tickets:activate is still blocked, the ticket ID must already be
	// tracked in q.Tickets, and ResultChan must NOT have received StatusOK yet.
	if _, ok := q.Tickets.Load(ticketID); !ok {
		t.Fatalf("expected ticket %s to be stored in q.Tickets before activation completes", ticketID)
	}
	select {
	case prematureStatus := <-resChan:
		t.Fatalf("ResultChan received premature status %d before /tickets:activate completed", prematureStatus)
	case <-time.After(30 * time.Millisecond):
	}

	// Release /tickets:activate and verify ResultChan now receives http.StatusOK and closes.
	close(releaseActivateCh)

	select {
	case gotStatus, ok := <-resChan:
		if !ok {
			t.Fatal("ResultChan closed without status")
		}
		if gotStatus != http.StatusOK {
			t.Fatalf("ResultChan status = %d, want %d (http.StatusOK)", gotStatus, http.StatusOK)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ResultChan after /tickets:activate completed")
	}

	// Verify that delivering an assignment removes the ticket from q.Tickets.
	assignmentsChan <- &pb.Roster{
		Name:       "match-roster",
		Assignment: &pb.Assignment{Connection: "10.0.0.1:7777"},
		Tickets: []*pb.Ticket{
			{
				Id: ticketID,
				Attributes: &pb.Ticket_FilterableData{
					CreationTime: timestamppb.Now(),
				},
			},
		},
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, exists := q.Tickets.Load(ticketID); !exists {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected ticket %s to be deleted from q.Tickets after assignment delivery", ticketID)
}

// TestMatchmakerQueueRun_ActivationFailureReturns500AndRemovesTicket verifies
// that when ActivateTicketsWithResults reports a permanent error for a ticket,
// MatchmakerQueue.Run removes the ticket from q.Tickets and sends
// http.StatusInternalServerError (500) on request.ResultChan.
func TestMatchmakerQueueRun_ActivationFailureReturns500AndRemovesTicket(t *testing.T) {
	const failedTicketID = "1790000000000-99"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tickets":
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: failedTicketID})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)

		case "/tickets:activate":
			st := status.New(codes.InvalidArgument, "invalid ticket id")
			stWithDetails, err := st.WithDetails(&errdetails.BadRequest{
				FieldViolations: []*errdetails.BadRequest_FieldViolation{
					{
						Field:       "ticket_id/" + failedTicketID,
						Description: "Invalid ticket id",
					},
				},
			})
			if err != nil {
				t.Errorf("failed to attach BadRequest details: %v", err)
			}
			bodyBytes, _ := protojson.Marshal(stWithDetails.Proto())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(bodyBytes)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	q, assignmentsChan := newTestQueue(srv.URL, 5, 1)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		q.Run(ctx)
	}()
	defer func() {
		cancel()
		close(assignmentsChan)
		<-runDone
	}()

	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{
		Ticket:     &pb.Ticket{},
		ResultChan: resChan,
	}

	select {
	case gotStatus, ok := <-resChan:
		if !ok {
			t.Fatal("ResultChan closed without status")
		}
		if gotStatus != http.StatusInternalServerError {
			t.Fatalf("ResultChan status = %d, want %d (http.StatusInternalServerError)", gotStatus, http.StatusInternalServerError)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ResultChan on activation failure")
	}

	if _, exists := q.Tickets.Load(failedTicketID); exists {
		t.Fatalf("expected failed ticket %s to be removed from q.Tickets after activation failure", failedTicketID)
	}
}

// TestMatchmakerQueueRun_ContextCancellationUnblocksAndDrainsPending verifies
// that cancelling ctx unblocks and drains requests across all stages of the
// MatchmakerQueue pipeline:
//  1. WaitingForSlotAndInFlightCreate: A request blocked waiting to acquire a
//     concurrency slot (`select { case slots <- struct{}{}: case <-ctx.Done(): }`)
//     and an in-flight create worker that completes after activation shutdown
//     (`activationsClosed == true`) both receive http.StatusRequestTimeout (408).
//  2. PendingActivationDrained: A ticket already created and registered in
//     pendingActivations when ActivateTicketsWithResults exits on ctx.Done() is
//     removed from q.Tickets and its ResultChan is drained.
func TestMatchmakerQueueRun_ContextCancellationUnblocksAndDrainsPending(t *testing.T) {
	t.Run("WaitingForSlotAndInFlightCreateDrainedOnCancel", func(t *testing.T) {
		var (
			firstCreateOnce sync.Once
			firstCreateInCh = make(chan struct{})
			releaseCreateCh = make(chan struct{})
		)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/tickets":
				firstCreateOnce.Do(func() {
					close(firstCreateInCh)
				})
				<-releaseCreateCh
				respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "1790000000000-201"})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(respBytes)

			case "/tickets:activate":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))

			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		// MAX_CONCURRENT_TICKET_CREATIONS = 1 so Request 1 holds the only slot
		// while Request 2 blocks at `select { case slots <- struct{}{}: case <-ctx.Done(): }`.
		q, assignmentsChan := newTestQueue(srv.URL, 1, 1)
		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			q.Run(ctx)
		}()

		resChan1 := make(chan int, 1)
		q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: resChan1}
		select {
		case <-firstCreateInCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Request 1 to enter /tickets")
		}

		resChan2 := make(chan int, 1)
		q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: resChan2}
		// Give the dispatch loop a moment to read Request 2 and block waiting for the slot.
		time.Sleep(25 * time.Millisecond)

		// Cancel ctx while Request 1 still holds the only concurrency slot.
		cancel()

		// Request 2 must unblock immediately from `select { case slots <- struct{}{}: case <-ctx.Done(): }`
		// with http.StatusRequestTimeout (408) even before Request 1's /tickets call finishes.
		select {
		case gotStatus, ok := <-resChan2:
			if !ok {
				t.Fatal("Request 2 ResultChan closed without status")
			}
			if gotStatus != http.StatusRequestTimeout {
				t.Fatalf("Request 2 status = %d, want %d (http.StatusRequestTimeout)", gotStatus, http.StatusRequestTimeout)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Request 2 (blocked on slot) to unblock on ctx cancellation")
		}

		// Now release Request 1's /tickets handler; because ctx is cancelled and
		// ActivateTicketsWithResults has already exited (activationsClosed == true),
		// Request 1 must also receive http.StatusRequestTimeout (408).
		close(releaseCreateCh)
		close(assignmentsChan)

		select {
		case gotStatus, ok := <-resChan1:
			if !ok {
				t.Fatal("Request 1 ResultChan closed without status")
			}
			if gotStatus != http.StatusRequestTimeout {
				t.Fatalf("Request 1 status = %d, want %d (http.StatusRequestTimeout)", gotStatus, http.StatusRequestTimeout)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Request 1 ResultChan after /tickets completed post-cancel")
		}

		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for MatchmakerQueue.Run(ctx) to exit")
		}
	})

	t.Run("PendingActivationDrainedOnCancel", func(t *testing.T) {
		const pendingTicketID = "1790000000000-202"
		var (
			createdOnce sync.Once
			createdCh   = make(chan struct{})
		)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/tickets":
				respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: pendingTicketID})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(respBytes)
				createdOnce.Do(func() {
					close(createdCh)
				})

			case "/tickets:activate":
				w.WriteHeader(http.StatusServiceUnavailable)

			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		// Set maxActivationBatch = 100 so the single created ticket stays queued
		// inside ActivateTicketsWithResults's 250ms batch timer while we cancel ctx.
		q, assignmentsChan := newTestQueue(srv.URL, 5, 100)
		ctx, cancel := context.WithCancel(context.Background())
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			q.Run(ctx)
		}()

		resChan := make(chan int, 1)
		q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: resChan}

		select {
		case <-createdCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for /tickets handler")
		}

		// Wait until the worker registers pendingTicketID in q.Tickets and pendingActivations.
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if _, ok := q.Tickets.Load(pendingTicketID); ok {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}

		// Cancel ctx while the ticket is registered in pendingActivations (before the 250ms batch timer fires).
		cancel()
		close(assignmentsChan)

		select {
		case gotStatus, ok := <-resChan:
			if !ok {
				t.Fatal("ResultChan closed without status")
			}
			if gotStatus != http.StatusRequestTimeout && gotStatus != http.StatusInternalServerError {
				t.Fatalf("ResultChan status = %d, want %d (http.StatusRequestTimeout) or %d (http.StatusInternalServerError)",
					gotStatus, http.StatusRequestTimeout, http.StatusInternalServerError)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for pending activation ResultChan to drain on cancel")
		}

		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for MatchmakerQueue.Run(ctx) to exit")
		}

		if _, exists := q.Tickets.Load(pendingTicketID); exists {
			t.Fatalf("expected ticket %s to be removed from q.Tickets when drained on cancel", pendingTicketID)
		}
	})
}

// TestProxyCreateTicket_AbortsRetryBackoffOnContextCancel verifies that
// proxyCreateTicket derives its context from the caller's ctx and wraps its
// exponential backoff with backoff.WithContext so cancelling ctx during a
// transient 503 failure aborts the retry loop immediately rather than waiting
// for the full 5-second max elapsed backoff window.
func TestProxyCreateTicket_AbortsRetryBackoffOnContextCancel(t *testing.T) {
	var (
		firstAttemptOnce sync.Once
		firstAttemptCh   = make(chan struct{})
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstAttemptOnce.Do(func() {
			close(firstAttemptCh)
		})
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("transient unavailable"))
	}))
	defer srv.Close()

	q, _ := newTestQueue(srv.URL, 5, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	errCh := make(chan error, 1)
	go func() {
		_, err := q.proxyCreateTicket(ctx, &pb.Ticket{})
		errCh <- err
	}()

	select {
	case <-firstAttemptCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first CreateTicket attempt")
	}

	// Cancel ctx while proxyCreateTicket is in its retry backoff sleep.
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected non-nil error from proxyCreateTicket after ctx cancellation")
		}
		if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
			t.Errorf("proxyCreateTicket took %v after ctx cancel, expected immediate abort (< 1.5s vs 5s max elapsed time)", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for proxyCreateTicket to abort retry backoff on ctx cancellation")
	}
}

// TestGenerateTestTickets_ExitsImmediatelyOnContextCancel verifies that
// MatchmakerQueue.GenerateTestTickets checks ctx.Err() and selects on ctx.Done()
// in both its consumer and producer goroutines so cancelling ctx mid-cycle exits
// immediately even when TICKET_CREATION_CYCLE_DURATION_MS is 30 seconds and no
// reader is draining q.ClientRequestChan.
func TestGenerateTestTickets_ExitsImmediatelyOnContextCancel(t *testing.T) {
	q, _ := newTestQueue("http://127.0.0.1:0", 5, 10)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 100)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 30000) // 30s per cycle

	q.SetTestTicketConfig(500, func(context.Context) *pb.Ticket {
		return &pb.Ticket{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doneCh := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(doneCh)
		q.GenerateTestTickets(ctx)
	}()

	// Read a single ClientRequest to confirm GenerateTestTickets is actively
	// running inside cycle 1, then leave q.ClientRequestChan unbuffered/undrained
	// and cancel ctx.
	select {
	case <-q.ClientRequestChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first generated test ticket")
	}

	cancel()

	select {
	case <-doneCh:
		if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
			t.Errorf("GenerateTestTickets took %v to exit after ctx cancel, want < 1.5s", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for GenerateTestTickets to exit on ctx cancellation")
	}
}

// TestMatchmakerQueueRun_TicketActivationMetricsCounterAndFailureSplit verifies
// that MatchmakerQueue registers "mmqueue.ticket.activations" as a monotonic
// Int64Counter (with description "Total number of tickets activated") rather
// than a histogram, and increments it only when a ticket activation succeeds
// while recording failed activations on "mmqueue.ticket.activations.failures".
func TestMatchmakerQueueRun_TicketActivationMetricsCounterAndFailureSplit(t *testing.T) {
	ticketIDs := []string{"ticket-ok-1", "ticket-fail-1", "ticket-ok-2"}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/tickets":
			var createReq pb.CreateTicketRequest
			if err := protojson.Unmarshal(body, &createReq); err != nil || len(createReq.GetTicket().GetAttributes().GetTags()) == 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			resp := &pb.CreateTicketResponse{TicketId: createReq.GetTicket().GetAttributes().GetTags()[0]}
			b, _ := protojson.Marshal(resp)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(b)
		case "/tickets:activate":
			var actReq pb.ActivateTicketsRequest
			if err := protojson.Unmarshal(body, &actReq); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			hasFailTicket := false
			for _, id := range actReq.GetTicketIds() {
				if id == "ticket-fail-1" {
					hasFailTicket = true
					break
				}
			}
			if !hasFailTicket {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
				return
			}
			// Return 400 InvalidArgument with a BadRequest FieldViolation for
			// "ticket_id/ticket-fail-1" so any valid tickets in the batch
			// succeed and "ticket-fail-1" fails permanently.
			st := status.New(codes.InvalidArgument, "partial activation failure")
			stWithDetails, err := st.WithDetails(&errdetails.BadRequest{
				FieldViolations: []*errdetails.BadRequest_FieldViolation{
					{Field: "ticket_id/ticket-fail-1", Description: "Invalid ticket id"},
				},
			})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			b, _ := protojson.Marshal(stWithDetails.Proto())
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(b)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})
	meter := mp.Meter("mmqueue-metrics-test")

	q, assignmentsChan := newTestQueue(srv.URL, 5, 10)
	q.OtelMeterPtr = &meter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		q.Run(ctx)
	}()

	resChans := make([]chan int, len(ticketIDs))
	for i, tid := range ticketIDs {
		resChans[i] = make(chan int, 1)
		q.ClientRequestChan <- &ClientRequest{
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					Tags: []string{tid},
				},
			},
			ResultChan: resChans[i],
		}
	}

	wantStatuses := []int{http.StatusOK, http.StatusInternalServerError, http.StatusOK}
	for i, wantStatus := range wantStatuses {
		select {
		case gotStatus := <-resChans[i]:
			if gotStatus != wantStatus {
				t.Fatalf("ticket %d (%s) status = %d, want %d", i, ticketIDs[i], gotStatus, wantStatus)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for ticket %d (%s) activation result", i, ticketIDs[i])
		}
	}

	cancel()
	close(assignmentsChan)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for MatchmakerQueue.Run to exit")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}

	metricsByName := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metricsByName[m.Name] = m
		}
	}

	if _, deprecatedExists := metricsByName["mmqueue.ticket.activations.per_call"]; deprecatedExists {
		t.Errorf("deprecated histogram metric mmqueue.ticket.activations.per_call should no longer be registered")
	}

	assertMonotonicCounterSum := func(name, wantDesc string, wantTotal int64) {
		t.Helper()
		m, ok := metricsByName[name]
		if !ok {
			t.Fatalf("expected metric %q to be registered and collected, got metrics: %v", name, metricsByName)
		}
		if wantDesc != "" && m.Description != wantDesc {
			t.Errorf("metric %q Description = %q, want %q", name, m.Description, wantDesc)
		}
		sumData, ok := m.Data.(metricdata.Sum[int64])
		if !ok {
			t.Fatalf("metric %q Data type = %T, want metricdata.Sum[int64] (Int64Counter)", name, m.Data)
		}
		if !sumData.IsMonotonic {
			t.Errorf("metric %q Sum.IsMonotonic = false, want true", name)
		}
		var total int64
		for _, dp := range sumData.DataPoints {
			total += dp.Value
		}
		if total != wantTotal {
			t.Errorf("metric %q total counter value = %d, want %d", name, total, wantTotal)
		}
	}

	// 3 tickets were created; 2 activations succeeded and 1 activation failed.
	assertMonotonicCounterSum("mmqueue.ticket.creations", "Total tickets created since startup", 3)
	assertMonotonicCounterSum("mmqueue.ticket.activations", "Total number of tickets activated", 2)
	assertMonotonicCounterSum("mmqueue.ticket.activations.failures", "Total number of ticket activations that permanently failed", 1)
}

// TestMatchmakerQueueRun_AssignmentDeliveryAndMetrics verifies that the
// AssignmentReceiver.Receive goroutine in MatchmakerQueue.Run (mmqueue.go:302-355):
//  1. Removes tracked tickets ("ticket-assign-1", "ticket-assign-2") from
//     q.Tickets upon receiving an assignment Roster, records each successful
//     assignment on the monotonic Int64Counter "mmqueue.ticket.assignments",
//     and records each ticket's elapsed queue duration on the Float64Histogram
//     "mmqueue.ticket.queued_durations".
//  2. Increments the monotonic Int64Counter "mmqueue.ticket.deletion.failures"
//     when receiving a subsequent Roster containing an already-assigned ticket
//     ("ticket-assign-1") and an untracked ticket ("ticket-untracked-99").
func TestMatchmakerQueueRun_AssignmentDeliveryAndMetrics(t *testing.T) {
	trackedTicketIDs := []string{"ticket-assign-1", "ticket-assign-2"}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/tickets":
			var createReq pb.CreateTicketRequest
			if err := protojson.Unmarshal(body, &createReq); err != nil || len(createReq.GetTicket().GetAttributes().GetTags()) == 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			resp := &pb.CreateTicketResponse{TicketId: createReq.GetTicket().GetAttributes().GetTags()[0]}
			b, _ := protojson.Marshal(resp)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(b)
		case "/tickets:activate":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})
	meter := mp.Meter("mmqueue-assignment-metrics-test")

	q, assignmentsChan := newTestQueue(srv.URL, 5, 1)
	q.OtelMeterPtr = &meter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		q.Run(ctx)
	}()

	// 1. Enqueue and activate 2 tickets ("ticket-assign-1", "ticket-assign-2")
	// through q.ClientRequestChan so they are stored in q.Tickets.
	resChans := make([]chan int, len(trackedTicketIDs))
	for i, tid := range trackedTicketIDs {
		resChans[i] = make(chan int, 1)
		q.ClientRequestChan <- &ClientRequest{
			Ticket: &pb.Ticket{
				Attributes: &pb.Ticket_FilterableData{
					Tags:         []string{tid},
					CreationTime: timestamppb.New(time.Now().Add(-50 * time.Millisecond)),
				},
			},
			ResultChan: resChans[i],
		}
	}

	for i, tid := range trackedTicketIDs {
		select {
		case gotStatus := <-resChans[i]:
			if gotStatus != http.StatusOK {
				t.Fatalf("ticket %s status = %d, want %d (http.StatusOK)", tid, gotStatus, http.StatusOK)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for ticket %s activation", tid)
		}
		if _, exists := q.Tickets.Load(tid); !exists {
			t.Fatalf("expected ticket %s to be stored in q.Tickets after activation", tid)
		}
	}

	// 2. Deliver an assignment Roster for the 2 tracked tickets with CreationTime
	// populated, followed by a second Roster containing a duplicate already-assigned
	// ticket ("ticket-assign-1") and an untracked ticket ("ticket-untracked-99").
	assignmentsChan <- &pb.Roster{
		Name:       "roster-valid-assignments",
		Assignment: &pb.Assignment{Connection: "10.0.0.1:7777"},
		Tickets: []*pb.Ticket{
			{
				Id: "ticket-assign-1",
				Attributes: &pb.Ticket_FilterableData{
					CreationTime: timestamppb.New(time.Now().Add(-60 * time.Millisecond)),
				},
			},
			{
				Id: "ticket-assign-2",
				Attributes: &pb.Ticket_FilterableData{
					CreationTime: timestamppb.New(time.Now().Add(-80 * time.Millisecond)),
				},
			},
		},
	}

	assignmentsChan <- &pb.Roster{
		Name:       "roster-duplicate-and-untracked",
		Assignment: &pb.Assignment{Connection: "10.0.0.1:7777"},
		Tickets: []*pb.Ticket{
			{
				Id: "ticket-assign-1",
				Attributes: &pb.Ticket_FilterableData{
					CreationTime: timestamppb.Now(),
				},
			},
			{
				Id: "ticket-untracked-99",
				Attributes: &pb.Ticket_FilterableData{
					CreationTime: timestamppb.Now(),
				},
			},
		},
	}

	// Close assignmentsChan before cancelling ctx so syncChannelReceiver drains
	// both rosters in order and returns cleanly, then cancel ctx to stop Run().
	close(assignmentsChan)
	deadline := time.Now().Add(2 * time.Second)
	for len(assignmentsChan) > 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for MatchmakerQueue.Run to exit")
	}

	// 3. Verify both tracked tickets were removed from q.Tickets.
	for _, tid := range trackedTicketIDs {
		if _, exists := q.Tickets.Load(tid); exists {
			t.Errorf("expected ticket %s to be removed from q.Tickets after assignment delivery", tid)
		}
	}

	// 4. Collect and verify OpenTelemetry metrics from ManualReader.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}

	metricsByName := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metricsByName[m.Name] = m
		}
	}

	assertCounter := func(name, wantDesc string, wantTotal int64) {
		t.Helper()
		m, ok := metricsByName[name]
		if !ok {
			t.Fatalf("expected metric %q to be registered and collected, got: %v", name, metricsByName)
		}
		if wantDesc != "" && m.Description != wantDesc {
			t.Errorf("metric %q Description = %q, want %q", name, m.Description, wantDesc)
		}
		sumData, ok := m.Data.(metricdata.Sum[int64])
		if !ok {
			t.Fatalf("metric %q Data type = %T, want metricdata.Sum[int64]", name, m.Data)
		}
		if !sumData.IsMonotonic {
			t.Errorf("metric %q Sum.IsMonotonic = false, want true", name)
		}
		var total int64
		for _, dp := range sumData.DataPoints {
			total += dp.Value
		}
		if total != wantTotal {
			t.Errorf("metric %q total value = %d, want %d", name, total, wantTotal)
		}
	}

	// Verify 2 successful assignments and 2 deletion failures (1 duplicate + 1 untracked).
	assertCounter("mmqueue.ticket.assignments", "Total ticket assignments received", 2)
	assertCounter("mmqueue.ticket.deletion.failures", "Total ticket deletion failures", 2)

	// Verify "mmqueue.ticket.queued_durations" recorded 2 observations.
	queuedDurMetric, ok := metricsByName["mmqueue.ticket.queued_durations"]
	if !ok {
		t.Fatalf("expected metric \"mmqueue.ticket.queued_durations\" to be registered and collected, got: %v", metricsByName)
	}
	if wantDesc := "Length of time spent by tickets in the queue"; queuedDurMetric.Description != wantDesc {
		t.Errorf("mmqueue.ticket.queued_durations Description = %q, want %q", queuedDurMetric.Description, wantDesc)
	}
	queuedHist, ok := queuedDurMetric.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("mmqueue.ticket.queued_durations Data type = %T, want metricdata.Histogram[float64]", queuedDurMetric.Data)
	}
	var totalCount uint64
	var totalSum float64
	for _, dp := range queuedHist.DataPoints {
		totalCount += dp.Count
		totalSum += dp.Sum
	}
	if totalCount != 2 {
		t.Errorf("mmqueue.ticket.queued_durations Count = %d, want 2", totalCount)
	}
	if totalSum <= 0 {
		t.Errorf("mmqueue.ticket.queued_durations Sum = %v, want > 0", totalSum)
	}
}

// TestGenerateTestTickets_ZeroInitialTPCInitializesMetricsWithoutPanic verifies
// that starting GenerateTestTickets(ctx) with TPC=0 before Run(ctx) has
// registered OpenTelemetry instruments initializes metrics via ensureMetrics
// without panicking on a nil otelTicketGenerationCycleDurations histogram.
func TestGenerateTestTickets_ZeroInitialTPCInitializesMetricsWithoutPanic(t *testing.T) {
	q, _ := newTestQueue("http://127.0.0.1:0", 5, 10)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 20)
	q.SetTestTicketConfig(0, func(context.Context) *pb.Ticket {
		return &pb.Ticket{}
	})

	if q.otelTicketGenerationCycleDurations != nil || q.otelTicketGenerationsAchievedPerCycle != nil {
		t.Fatal("expected fresh MatchmakerQueue instance to have nil metric instruments before ensureMetrics")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Must complete cleanly without nil-pointer dereference panic.
	q.GenerateTestTickets(ctx)

	if q.otelTicketGenerationCycleDurations == nil {
		t.Fatal("expected GenerateTestTickets to initialize q.otelTicketGenerationCycleDurations via ensureMetrics")
	}
}

// TestGenerateTestTickets_RespectsDeadlineWhenQueueFull verifies that when
// ClientRequestChan is unbuffered and undrained (simulating a saturated queue),
// GenerateTestTickets exits each cycle promptly when the cycle deadline timer
// fires rather than blocking indefinitely on ClientRequestChan.
func TestGenerateTestTickets_RespectsDeadlineWhenQueueFull(t *testing.T) {
	q, _ := newTestQueue("http://127.0.0.1:0", 1, 10)
	q.ClientRequestChan = make(chan *ClientRequest) // unbuffered, no reader
	q.Cfg.Set("TICKET_CREATION_CYCLES", 2)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 50) // 50ms per cycle

	q.SetTestTicketConfig(100, func(context.Context) *pb.Ticket {
		return &pb.Ticket{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		q.GenerateTestTickets(ctx)
	}()

	select {
	case <-doneCh:
		if elapsed := time.Since(start); elapsed > 1*time.Second {
			t.Errorf("GenerateTestTickets took %v for two 50ms cycles with full queue, want < 1s", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for GenerateTestTickets to finish 2 cycles when ClientRequestChan is full")
	}
}

// TestMatchmakerQueue_IsolatedPerInstanceTestTicketConfig verifies REL-08:
// TPC, tpcMutex, and newTicket are per-instance fields on MatchmakerQueue
// (rather than package-level globals) so calling SetTestTicketConfig and
// GenerateTestTickets on two concurrent MatchmakerQueue instances never
// overwrites or cross-contaminates the other instance's ticket generator.
func TestMatchmakerQueue_IsolatedPerInstanceTestTicketConfig(t *testing.T) {
	q1, _ := newTestQueue("http://127.0.0.1:0", 5, 10)
	q1.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q1.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 30)

	q2, _ := newTestQueue("http://127.0.0.1:0", 5, 10)
	q2.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q2.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 30)

	// Configure q1 first and q2 second; with the old package-level newTicket
	// global, q2.SetTestTicketConfig overwrote q1's generator function.
	q1.SetTestTicketConfig(3, func(context.Context) *pb.Ticket {
		return &pb.Ticket{Id: "q1-ticket"}
	})
	q2.SetTestTicketConfig(4, func(context.Context) *pb.Ticket {
		return &pb.Ticket{Id: "q2-ticket"}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		q1.GenerateTestTickets(ctx)
	}()
	go func() {
		defer wg.Done()
		q2.GenerateTestTickets(ctx)
	}()

	wg.Wait()

	drainAndVerify := func(name string, ch chan *ClientRequest, wantCount int, wantID string) {
		t.Helper()
		var gotIDs []string
		for {
			select {
			case req := <-ch:
				gotIDs = append(gotIDs, req.Ticket.GetId())
				req.ResultChan <- http.StatusOK
			default:
				if len(gotIDs) != wantCount {
					t.Fatalf("%s generated %d tickets (%v), want %d", name, len(gotIDs), gotIDs, wantCount)
				}
				for i, id := range gotIDs {
					if id != wantID {
						t.Errorf("%s ticket[%d] ID = %q, want %q", name, i, id, wantID)
					}
				}
				return
			}
		}
	}

	drainAndVerify("q1", q1.ClientRequestChan, 3, "q1-ticket")
	drainAndVerify("q2", q2.ClientRequestChan, 4, "q2-ticket")
}
