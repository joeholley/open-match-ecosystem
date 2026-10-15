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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

// testMeter returns a no-op OpenTelemetry Meter pointer so MatchmakerQueue.Run
// initializes its package-level metric instruments without external exporters.
func testMeter() *metric.Meter {
	m := noop.NewMeterProvider().Meter("mmqueue-test")
	return &m
}

// syncChannelReceiver implements assignmentdistributor.Receiver by blocking on
// the caller goroutine until ctx is cancelled or assignmentsChan is closed.
// This ensures MatchmakerQueue.Run's WaitGroup waits for all in-flight
// assignmentHandler invocations to complete before Run returns.
type syncChannelReceiver struct {
	assignmentsChan <-chan *pb.Roster
}

func (r *syncChannelReceiver) Receive(ctx context.Context, handler func(ctx context.Context, roster *pb.Roster)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case roster, ok := <-r.assignmentsChan:
			if !ok {
				return nil
			}
			handler(ctx, roster)
		}
	}
}

func (r *syncChannelReceiver) Stop() {}

// newTestQueue constructs a MatchmakerQueue wired to the given mock om-core URL
// and a synchronous channel-backed assignment receiver.
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
		AssignmentReceiver: &syncChannelReceiver{assignmentsChan: assignmentsChan},
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
	registerMetrics(q.OtelMeterPtr)

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
	registerMetrics(q.OtelMeterPtr)

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
