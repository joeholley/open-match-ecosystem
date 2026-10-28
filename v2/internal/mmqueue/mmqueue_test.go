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
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	otelmetrics "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"open-match.dev/open-match-ecosystem/v2/internal/assignmentdistributor"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

func newTestLogger() *logrus.Logger {
	log := logrus.New()
	log.SetOutput(io.Discard)
	return log
}

func newTestMeter(t *testing.T) (*otelmetrics.Meter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	meter := provider.Meter("mmqueue.test")
	return &meter, reader
}

func newTestQueue(t *testing.T, serverURL string, maxConcurrent int) (*MatchmakerQueue, *assignmentdistributor.ChannelSender, *sdkmetric.ManualReader) {
	t.Helper()
	cfg := viper.New()
	cfg.Set("OM_CORE_ADDR", serverURL)
	cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)
	cfg.Set("MAX_CONCURRENT_TICKET_CREATIONS", maxConcurrent)
	cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 50)
	cfg.Set("TICKET_CREATION_CYCLES", 1)
	cfg.Set("MMQUEUE_TICKET_TTL_MS", 600000)

	log := newTestLogger()
	meterPtr, reader := newTestMeter(t)
	assignChan := make(chan *pb.Roster, 100)
	sender := assignmentdistributor.NewChannelSender(assignChan)
	receiver := assignmentdistributor.NewChannelReceiver(assignChan)

	q := &MatchmakerQueue{
		OmClient:           omclient.New(cfg, log, &http.Client{Timeout: 5 * time.Second}),
		Cfg:                cfg,
		Log:                log,
		ClientRequestChan:  make(chan *ClientRequest, 100),
		AssignmentReceiver: receiver,
		OtelMeterPtr:       meterPtr,
	}
	return q, sender, reader
}

// 1. Concurrency Slot Limit
func TestRun_ConcurrencySlotLimit(t *testing.T) {
	const maxConcurrent = 3
	const totalRequests = 10

	var inFlight atomic.Int32
	var maxObserved atomic.Int32
	var totalCreated atomic.Int32

	threeInFlight := make(chan struct{})
	var threeOnce sync.Once
	releaseCreate := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/tickets":
			cur := inFlight.Add(1)
			for {
				prev := maxObserved.Load()
				if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
					break
				}
			}
			if cur == int32(maxConcurrent) {
				threeOnce.Do(func() {
					close(threeInFlight)
				})
			}
			<-releaseCreate
			inFlight.Add(-1)

			idx := totalCreated.Add(1)
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{
				TicketId: fmt.Sprintf("ticket-%d", idx),
			})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
		case "/tickets:activate":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	q, sender, _ := newTestQueue(t, srv.URL, maxConcurrent)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	resChans := make([]chan int, totalRequests)
	for i := range totalRequests {
		resChans[i] = make(chan int, 1)
		q.ClientRequestChan <- &ClientRequest{
			Ticket:     &pb.Ticket{},
			ResultChan: resChans[i],
		}
	}

	select {
	case <-threeInFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for 3 concurrent /tickets requests")
	}

	// Allow time for any extra goroutines that might violate the concurrency cap.
	time.Sleep(40 * time.Millisecond)
	if got := maxObserved.Load(); got > int32(maxConcurrent) {
		t.Fatalf("concurrency before release = %d, want <= %d", got, maxConcurrent)
	}

	close(releaseCreate)

	for i, ch := range resChans {
		select {
		case status := <-ch:
			if status != http.StatusOK {
				t.Fatalf("request %d status = %d, want %d", i, status, http.StatusOK)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for request %d completion", i)
		}
	}

	if got := maxObserved.Load(); got != int32(maxConcurrent) {
		t.Fatalf("max concurrent /tickets calls = %d, want %d", got, maxConcurrent)
	}

	cancel()
	sender.Stop()
	<-runDone
}

// 2. Activation Confirmation Barrier Before HTTP 200
func TestRun_ActivationConfirmationBarrierBeforeHTTP200(t *testing.T) {
	const ticketID = "ticket-barrier-1"
	activateEntered := make(chan struct{})
	releaseActivate := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/tickets":
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: ticketID})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
		case "/tickets:activate":
			close(activateEntered)
			<-releaseActivate
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	q, sender, _ := newTestQueue(t, srv.URL, 3)
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{
		Ticket:     &pb.Ticket{},
		ResultChan: resChan,
	}

	select {
	case <-activateEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /tickets:activate to be called")
	}

	// Ticket must already be tracked in q.Tickets before activation finishes.
	if _, exists := q.Tickets.Load(ticketID); !exists {
		t.Fatalf("expected ticket %q to be present in q.Tickets while activation is in-flight", ticketID)
	}

	// ResultChan must NOT receive 200 OK while /tickets:activate is still blocked.
	select {
	case code := <-resChan:
		t.Fatalf("ResultChan received status %d before /tickets:activate completed", code)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseActivate)

	select {
	case code := <-resChan:
		if code != http.StatusOK {
			t.Fatalf("ResultChan status = %d, want %d", code, http.StatusOK)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for 200 OK after /tickets:activate returned")
	}

	// Deliver assignment and verify ticket is removed from q.Tickets.
	err := sender.Send(ctx, &pb.Roster{
		Name:       "roster-1",
		Assignment: &pb.Assignment{Connection: "10.0.0.1:7777"},
		Tickets: []*pb.Ticket{
			{
				Id: ticketID,
				Attributes: &pb.Ticket_FilterableData{
					CreationTime: timestamppb.New(time.Now().Add(-50 * time.Millisecond)),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("sender.Send failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, exists := q.Tickets.Load(ticketID); !exists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, exists := q.Tickets.Load(ticketID); exists {
		t.Fatalf("expected ticket %q to be deleted from q.Tickets after assignment", ticketID)
	}

	cancel()
	sender.Stop()
	<-runDone
}

// 3. Permanent Activation Failure Returns HTTP 502, Evicts and Deactivates Ticket
func TestRun_PermanentActivationFailureReturnsHTTP502AndEvictsTicket(t *testing.T) {
	const ticketID = "ticket-perm-fail-1"
	deactivated := make(chan []string, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/tickets":
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: ticketID})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
		case "/tickets:activate":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":3,"message":"invalid ticket id"}`))
		case "/tickets:deactivate":
			deactivated <- readDeactivateRequest(t, r)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	q, sender, _ := newTestQueue(t, srv.URL, 3)
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{
		Ticket:     &pb.Ticket{},
		ResultChan: resChan,
	}

	select {
	case code := <-resChan:
		if code != http.StatusBadGateway {
			t.Fatalf("ResultChan status = %d, want %d", code, http.StatusBadGateway)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for 502 status on permanent activation failure")
	}

	if _, exists := q.Tickets.Load(ticketID); exists {
		t.Fatalf("expected permanently failed ticket %q to be evicted from q.Tickets", ticketID)
	}

	// The failed activation may have reached om-core anyway, so the queue
	// deactivates the ticket defensively.
	select {
	case ids := <-deactivated:
		if !slices.Equal(ids, []string{ticketID}) {
			t.Fatalf("deactivated %v, want [%s]", ids, ticketID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the failed ticket to be deactivated")
	}

	cancel()
	sender.Stop()
	<-runDone
}

// readDeactivateRequest checks r is om-core's DELETE /tickets:deactivate and
// returns the ticket IDs in it.
func readDeactivateRequest(t *testing.T, r *http.Request) []string {
	t.Helper()
	if r.Method != http.MethodDelete {
		t.Errorf("/tickets:deactivate method = %s, want DELETE", r.Method)
	}
	body, _ := io.ReadAll(r.Body)
	var req pb.DeactivateTicketsRequest
	if err := protojson.Unmarshal(body, &req); err != nil {
		t.Errorf("cannot unmarshal DeactivateTicketsRequest %q: %v", body, err)
	}
	return req.GetTicketIds()
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// 4. Shutdown answers every outstanding request with 503 exactly once, then
// closes its ResultChan, whatever stage the request is in.
func TestRun_ShutdownAnswersEveryPendingRequestOnce(t *testing.T) {
	var createCalls atomic.Int32
	activateEntered := make(chan struct{})
	releaseActivate := make(chan struct{})
	create1Entered := make(chan struct{})
	releaseCreate1 := make(chan struct{})
	create2Entered := make(chan struct{})
	releaseCreate2 := make(chan struct{})

	t.Cleanup(func() {
		select {
		case <-releaseActivate:
		default:
			close(releaseActivate)
		}
		select {
		case <-releaseCreate1:
		default:
			close(releaseCreate1)
		}
		select {
		case <-releaseCreate2:
		default:
			close(releaseCreate2)
		}
	})

	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/tickets":
			callNum := createCalls.Add(1)
			switch callNum {
			case 1:
				respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "seed-activate-ticket"})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(respBytes)),
				}, nil
			case 2:
				close(create1Entered)
				<-releaseCreate1
				respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "pending-act-ticket"})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(respBytes)),
				}, nil
			default:
				close(create2Entered)
				<-releaseCreate2
				respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "post-shutdown-ticket"})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(respBytes)),
				}, nil
			}
		case "/tickets:activate":
			close(activateEntered)
			<-releaseActivate
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
			}, nil
		default:
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}
	})

	// MAX_CONCURRENT_TICKET_CREATIONS = 2 and unbuffered ClientRequestChan for deterministic staging.
	q, sender, _ := newTestQueue(t, "http://om-core.test", 2)
	q.OmClient.Client = &http.Client{Transport: transport}
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)
	q.ClientRequestChan = make(chan *ClientRequest)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	// Ticket 0: enters /tickets:activate and holds ActivateTicketsWithResults open until releaseActivate.
	res0 := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: res0}
	select {
	case <-activateEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for seed ticket to enter /tickets:activate")
	}

	// Ticket 1: occupies concurrency slot 1 inside /tickets.
	res1 := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: res1}
	select {
	case <-create1Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Ticket 1 to enter /tickets")
	}

	// Ticket 2: occupies concurrency slot 2 inside /tickets.
	res2 := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: res2}
	select {
	case <-create2Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Ticket 2 to enter /tickets")
	}

	// Ticket 3: read from unbuffered ClientRequestChan and blocked waiting for a concurrency slot.
	res3 := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: res3}

	// Shut down with one request in every stage.
	cancel()

	// Waiting for a creation slot, and waiting for activation: answered
	// at once, even though om-core hasn't responded.
	expectResultOnceThenClosed(t, "slot-waiter", res3, http.StatusServiceUnavailable)
	expectResultOnceThenClosed(t, "activation-waiter", res0, http.StatusServiceUnavailable)

	// Creation calls that succeed after shutdown began: answered with 503,
	// and the (still inactive) tickets aren't tracked or activated.
	close(releaseCreate1)
	expectResultOnceThenClosed(t, "in-flight create 1", res1, http.StatusServiceUnavailable)
	close(releaseCreate2)
	expectResultOnceThenClosed(t, "in-flight create 2", res2, http.StatusServiceUnavailable)

	close(releaseActivate)
	sender.Stop()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for q.Run to exit")
	}

	q.Tickets.Range(func(id, _ any) bool {
		t.Errorf("ticket %v still tracked after shutdown", id)
		return true
	})
}

// expectResultOnceThenClosed checks the ResultChan contract: exactly one
// status, then the channel is closed.
func expectResultOnceThenClosed(t *testing.T, name string, ch chan int, want int) {
	t.Helper()
	select {
	case code, ok := <-ch:
		if !ok {
			t.Fatalf("%s: ResultChan closed without a status, want %d", name, want)
		}
		if code != want {
			t.Fatalf("%s: status = %d, want %d", name, code, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: timed out waiting for status %d", name, want)
	}
	select {
	case code, ok := <-ch:
		if ok {
			t.Fatalf("%s: got a second status %d; want ResultChan closed after one", name, code)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: ResultChan not closed after its status", name)
	}
}

// 5. Ticket Creation Retry Aborts on Context Cancellation
func TestProxyCreateTicket_RetryAbortsOnContextCancellation(t *testing.T) {
	firstAttemptFailed := make(chan struct{})
	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		once.Do(func() {
			close(firstAttemptFailed)
		})
	}))
	defer srv.Close()

	q, _, _ := newTestQueue(t, srv.URL, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		_, err := q.proxyCreateTicket(ctx, &pb.Ticket{})
		errChan <- err
	}()

	select {
	case <-firstAttemptFailed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first 503 attempt")
	}

	// Wait briefly so proxyCreateTicket enters backoff sleep, then cancel ctx.
	time.Sleep(20 * time.Millisecond)
	cancelStart := time.Now()
	cancel()

	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("expected non-nil error when context is cancelled during retry backoff")
		}
		if elapsed := time.Since(cancelStart); elapsed > 500*time.Millisecond {
			t.Fatalf("proxyCreateTicket took %v to abort after cancel, want < 500ms", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("proxyCreateTicket did not abort immediately on context cancellation")
	}
}

// 6. GenerateTestTickets Immediate Exit on Context Cancellation
func TestGenerateTestTickets_ImmediateExitOnContextCancellation(t *testing.T) {
	q, _, _ := newTestQueue(t, "http://127.0.0.1:0", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 30000) // 30-second cycle duration
	q.Cfg.Set("TICKET_CREATION_CYCLES", 10)
	q.ClientRequestChan = make(chan *ClientRequest) // unbuffered and undrained

	ticketGenerated := make(chan struct{})
	var genOnce sync.Once
	// High TPC so the first Poisson-scheduled arrival lands within a few
	// milliseconds of the start of the 30-second cycle.
	q.SetTestTicketConfig(5000, func(_ context.Context) *pb.Ticket {
		genOnce.Do(func() {
			close(ticketGenerated)
		})
		return &pb.Ticket{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		q.GenerateTestTickets(ctx)
		close(done)
	}()

	select {
	case <-ticketGenerated:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for GenerateTestTickets to start generating tickets")
	}

	cancelStart := time.Now()
	cancel()

	select {
	case <-done:
		if elapsed := time.Since(cancelStart); elapsed > 500*time.Millisecond {
			t.Fatalf("GenerateTestTickets took %v to exit after cancel, want < 500ms", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("GenerateTestTickets did not exit immediately on context cancellation")
	}
}

// 7. Ticket Activation Monotonic Counters and Deprecated Metric Absence
func TestMetrics_TicketActivationCountersAndDeprecatedMetricAbsence(t *testing.T) {
	var ticketSeq atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/tickets":
			n := ticketSeq.Add(1)
			id := fmt.Sprintf("ok-%d", n)
			if n == 3 {
				id = "fail-1"
			}
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: id})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
		case "/tickets:activate":
			body, _ := io.ReadAll(r.Body)
			var req pb.ActivateTicketsRequest
			_ = protojson.Unmarshal(body, &req)
			if len(req.GetTicketIds()) == 1 && req.GetTicketIds()[0] == "fail-1" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":3,"message":"invalid ticket"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	q, sender, reader := newTestQueue(t, srv.URL, 3)
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	expectedStatuses := []int{http.StatusOK, http.StatusOK, http.StatusBadGateway}
	for i, wantStatus := range expectedStatuses {
		resChan := make(chan int, 1)
		q.ClientRequestChan <- &ClientRequest{
			Ticket:     &pb.Ticket{},
			ResultChan: resChan,
		}
		select {
		case got := <-resChan:
			if got != wantStatus {
				t.Fatalf("request %d status = %d, want %d", i+1, got, wantStatus)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for request %d result", i+1)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}

	var foundActivations, foundFailures bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "mmqueue.ticket.activation.requests", "mmqueue.ticket.activations.per_call":
				t.Fatalf("deprecated metric %q must not be registered", m.Name)
			case "mmqueue.ticket.activations":
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("mmqueue.ticket.activations data type = %T, want metricdata.Sum[int64]", m.Data)
				}
				if !sum.IsMonotonic {
					t.Fatal("expected mmqueue.ticket.activations to be monotonic")
				}
				if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 2 {
					t.Fatalf("mmqueue.ticket.activations value = %+v, want 2", sum.DataPoints)
				}
				foundActivations = true
			case "mmqueue.ticket.activations.failures":
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("mmqueue.ticket.activations.failures data type = %T, want metricdata.Sum[int64]", m.Data)
				}
				if !sum.IsMonotonic {
					t.Fatal("expected mmqueue.ticket.activations.failures to be monotonic")
				}
				if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
					t.Fatalf("mmqueue.ticket.activations.failures value = %+v, want 1", sum.DataPoints)
				}
				foundFailures = true
			}
		}
	}

	if !foundActivations {
		t.Fatal("expected mmqueue.ticket.activations metric to be present")
	}
	if !foundFailures {
		t.Fatal("expected mmqueue.ticket.activations.failures metric to be present")
	}

	cancel()
	sender.Stop()
	<-runDone
}

// 8. Assignment Delivery and Queued Duration Metrics
func TestRun_AssignmentDeliveryAndQueuedDurationMetrics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/tickets":
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "tracked-1"})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
		case "/tickets:activate":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	q, sender, reader := newTestQueue(t, srv.URL, 3)
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{
		Ticket:     &pb.Ticket{},
		ResultChan: resChan,
	}
	if code := <-resChan; code != http.StatusOK {
		t.Fatalf("ResultChan status = %d, want %d", code, http.StatusOK)
	}

	createdAt := timestamppb.New(time.Now().Add(-150 * time.Millisecond))
	err := sender.Send(ctx, &pb.Roster{
		Name:       "roster-assign",
		Assignment: &pb.Assignment{Connection: "10.0.0.2:7777"},
		Tickets: []*pb.Ticket{
			{Id: "tracked-1", Attributes: &pb.Ticket_FilterableData{CreationTime: createdAt}},
			{Id: "tracked-1", Attributes: &pb.Ticket_FilterableData{CreationTime: createdAt}}, // duplicate
			{Id: "untracked-99", Attributes: &pb.Ticket_FilterableData{CreationTime: createdAt}},
		},
	})
	if err != nil {
		t.Fatalf("sender.Send failed: %v", err)
	}

	// Wait until tracked-1 is removed and metrics are updated.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, exists := q.Tickets.Load("tracked-1"); !exists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, exists := q.Tickets.Load("tracked-1"); exists {
		t.Fatal("expected tracked-1 to be removed from q.Tickets")
	}

	// Give the receiver loop a moment to finish the remaining two tickets in the same roster slice.
	time.Sleep(30 * time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}

	var assignmentsVal, untrackedVal int64
	var queuedCount uint64
	var queuedSum float64

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "mmqueue.ticket.assignments":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) > 0 {
					assignmentsVal = sum.DataPoints[0].Value
				}
			case "mmqueue.ticket.assignments.untracked":
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) > 0 {
					untrackedVal = sum.DataPoints[0].Value
				}
			case "mmqueue.ticket.queued_durations":
				if hist, ok := m.Data.(metricdata.Histogram[float64]); ok && len(hist.DataPoints) > 0 {
					queuedCount = hist.DataPoints[0].Count
					queuedSum = hist.DataPoints[0].Sum
				}
			}
		}
	}

	if assignmentsVal != 1 {
		t.Fatalf("mmqueue.ticket.assignments = %d, want 1", assignmentsVal)
	}
	// The duplicate and the other instance's ticket are both untracked.
	if untrackedVal != 2 {
		t.Fatalf("mmqueue.ticket.assignments.untracked = %d, want 2", untrackedVal)
	}
	if queuedCount != 1 || queuedSum < 100 {
		t.Fatalf("mmqueue.ticket.queued_durations count=%d sum=%v, want count=1 sum>=100ms", queuedCount, queuedSum)
	}

	cancel()
	sender.Stop()
	<-runDone
}

// 9. Zero Initial TPC Metric Initialization Without Panic
func TestGenerateTestTickets_ZeroInitialTPCMetricInitializationWithoutPanic(t *testing.T) {
	q, _, reader := newTestQueue(t, "http://127.0.0.1:0", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 25)
	q.SetTestTicketConfig(0, func(_ context.Context) *pb.Ticket {
		return &pb.Ticket{}
	})

	// Call GenerateTestTickets before Run with TPC == 0; must not panic.
	q.GenerateTestTickets(context.Background())

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}

	var foundCycleDur, foundTPCActual, foundTPCRequest bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "mmqueue.ticket.generation.cycle_duration":
				if hist, ok := m.Data.(metricdata.Histogram[int64]); ok && len(hist.DataPoints) == 1 && hist.DataPoints[0].Count == 1 {
					foundCycleDur = true
				}
			case "mmqueue.tpc.actual":
				if hist, ok := m.Data.(metricdata.Histogram[int64]); ok && len(hist.DataPoints) == 1 && hist.DataPoints[0].Count == 1 && hist.DataPoints[0].Sum == 0 {
					foundTPCActual = true
				}
			case "mmqueue.tpc.request":
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok && len(gauge.DataPoints) == 1 && gauge.DataPoints[0].Value == 0 {
					foundTPCRequest = true
				}
			}
		}
	}

	if !foundCycleDur || !foundTPCActual || !foundTPCRequest {
		t.Fatalf("missing expected metrics: cycle_duration=%v tpc.actual=%v tpc.request=%v", foundCycleDur, foundTPCActual, foundTPCRequest)
	}
}

// 10. GenerateTestTickets Deadline Enforcement When Queue Full
func TestGenerateTestTickets_DeadlineEnforcementWhenQueueFull(t *testing.T) {
	q, _, reader := newTestQueue(t, "http://127.0.0.1:0", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 2)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 40)
	q.ClientRequestChan = make(chan *ClientRequest) // unbuffered and undrained

	q.SetTestTicketConfig(10, func(_ context.Context) *pb.Ticket {
		return &pb.Ticket{}
	})

	start := time.Now()
	done := make(chan struct{})
	go func() {
		q.GenerateTestTickets(context.Background())
		close(done)
	}()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
			t.Fatalf("GenerateTestTickets took %v for two 40ms cycles, want < 600ms", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("GenerateTestTickets deadlocked when ClientRequestChan was full")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}

	var cycleDurCount, tpcActualCount uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "mmqueue.ticket.generation.cycle_duration":
				if hist, ok := m.Data.(metricdata.Histogram[int64]); ok && len(hist.DataPoints) > 0 {
					cycleDurCount = hist.DataPoints[0].Count
				}
			case "mmqueue.tpc.actual":
				if hist, ok := m.Data.(metricdata.Histogram[int64]); ok && len(hist.DataPoints) > 0 {
					tpcActualCount = hist.DataPoints[0].Count
				}
			}
		}
	}

	if cycleDurCount != 2 {
		t.Fatalf("mmqueue.ticket.generation.cycle_duration count = %d, want 2", cycleDurCount)
	}
	if tpcActualCount != 2 {
		t.Fatalf("mmqueue.tpc.actual count = %d, want 2", tpcActualCount)
	}
}

// 11. Per-Instance Test Ticket Configuration Isolation
func TestSetTestTicketConfig_PerInstanceIsolation(t *testing.T) {
	q1, _, reader1 := newTestQueue(t, "http://127.0.0.1:0", 3)
	q2, _, reader2 := newTestQueue(t, "http://127.0.0.1:0", 3)

	q1.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q1.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 60)
	seedWithArrivalsBefore(t, q1, 2, 60*time.Millisecond, 20*time.Millisecond)
	q2.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q2.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 60)
	seedWithArrivalsBefore(t, q2, 5, 60*time.Millisecond, 20*time.Millisecond)

	q1.SetTestTicketConfig(2, func(_ context.Context) *pb.Ticket {
		return &pb.Ticket{Id: "q1-ticket"}
	})
	q2.SetTestTicketConfig(5, func(_ context.Context) *pb.Ticket {
		return &pb.Ticket{Id: "q2-ticket"}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var q1Tickets, q2Tickets []string
	var mu1, mu2 sync.Mutex

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-q1.ClientRequestChan:
				mu1.Lock()
				q1Tickets = append(q1Tickets, req.Ticket.GetId())
				mu1.Unlock()
				req.ResultChan <- http.StatusOK
				close(req.ResultChan)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-q2.ClientRequestChan:
				mu2.Lock()
				q2Tickets = append(q2Tickets, req.Ticket.GetId())
				mu2.Unlock()
				req.ResultChan <- http.StatusOK
				close(req.ResultChan)
			}
		}
	}()

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

	mu1.Lock()
	got1 := append([]string(nil), q1Tickets...)
	mu1.Unlock()

	mu2.Lock()
	got2 := append([]string(nil), q2Tickets...)
	mu2.Unlock()

	if len(got1) != 2 {
		t.Fatalf("q1 generated %d tickets, want 2", len(got1))
	}
	for _, id := range got1 {
		if id != "q1-ticket" {
			t.Fatalf("q1 generated ticket id %q, want %q", id, "q1-ticket")
		}
	}

	if len(got2) != 5 {
		t.Fatalf("q2 generated %d tickets, want 5", len(got2))
	}
	for _, id := range got2 {
		if id != "q2-ticket" {
			t.Fatalf("q2 generated ticket id %q, want %q", id, "q2-ticket")
		}
	}

	var rm1, rm2 metricdata.ResourceMetrics
	if err := reader1.Collect(ctx, &rm1); err != nil {
		t.Fatalf("reader1.Collect failed: %v", err)
	}
	if err := reader2.Collect(ctx, &rm2); err != nil {
		t.Fatalf("reader2.Collect failed: %v", err)
	}

	assertGauge := func(rm metricdata.ResourceMetrics, want int64) {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "mmqueue.tpc.request" {
					if g, ok := m.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) == 1 && g.DataPoints[0].Value == want {
						return
					}
				}
			}
		}
		t.Fatalf("expected mmqueue.tpc.request gauge = %d", want)
	}
	assertGauge(rm1, 2)
	assertGauge(rm2, 5)
}

// 12. Results that arrive after the cycle deadline are still counted:
// tpc.actual reflects tickets queued during the cycle, and tpc.confirmed is
// recorded once every result for that cycle has come back.
func TestGenerateTestTickets_ConfirmationsAfterDeadlineAreCounted(t *testing.T) {
	const tpc = 5
	q, _, reader := newTestQueue(t, "http://127.0.0.1:0", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 50)
	seedWithArrivalsBefore(t, q, tpc, 50*time.Millisecond, 20*time.Millisecond)
	q.ClientRequestChan = make(chan *ClientRequest)

	q.SetTestTicketConfig(tpc, func(_ context.Context) *pb.Ticket {
		return &pb.Ticket{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Accept every request, but hold the results until after the cycle
	// deadline, simulating creation/activation that outlasts the cycle.
	var mu sync.Mutex
	var held []*ClientRequest
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-q.ClientRequestChan:
				mu.Lock()
				held = append(held, req)
				mu.Unlock()
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		q.GenerateTestTickets(ctx)
		close(done)
	}()

	// Wait well past the 50ms deadline before releasing any results.
	time.Sleep(150 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("GenerateTestTickets returned before the cycle's results were in")
	default:
	}

	mu.Lock()
	if len(held) != tpc {
		mu.Unlock()
		t.Fatalf("queue accepted %d requests, want %d", len(held), tpc)
	}
	// One permanent failure, the rest succeed.
	for i, req := range held {
		if i == 0 {
			req.ResultChan <- http.StatusInternalServerError
		} else {
			req.ResultChan <- http.StatusOK
		}
		close(req.ResultChan)
	}
	mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GenerateTestTickets did not return after all results were delivered")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "mmqueue.tpc.actual" && m.Name != "mmqueue.tpc.confirmed" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[int64])
			if !ok || len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 {
				t.Fatalf("unexpected %s data: %+v", m.Name, m.Data)
			}
			sums[m.Name] = hist.DataPoints[0].Sum
		}
	}
	if got, ok := sums["mmqueue.tpc.actual"]; !ok || got != tpc {
		t.Fatalf("mmqueue.tpc.actual = %d (present=%v), want %d", got, ok, tpc)
	}
	if got, ok := sums["mmqueue.tpc.confirmed"]; !ok || got != tpc-1 {
		t.Fatalf("mmqueue.tpc.confirmed = %d (present=%v), want %d", got, ok, tpc-1)
	}
}

// 13. A ticket generated but not accepted before the cycle deadline is carried
// over to the next cycle instead of being discarded and regenerated.
func TestGenerateTestTickets_CarriesOverUnqueuedTicket(t *testing.T) {
	q, _, _ := newTestQueue(t, "http://127.0.0.1:0", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", 40)
	q.ClientRequestChan = make(chan *ClientRequest) // unbuffered and undrained

	var generated atomic.Int32
	q.SetTestTicketConfig(10, func(_ context.Context) *pb.Ticket {
		n := generated.Add(1)
		return &pb.Ticket{Id: fmt.Sprintf("gen-%d", n)}
	})

	q.GenerateTestTickets(context.Background())

	// Every cycle blocks on its first send until the deadline; without
	// carry-over that would generate (and discard) one ticket per cycle.
	if got := generated.Load(); got != 1 {
		t.Fatalf("ticket generation function called %d times across 3 blocked cycles, want 1", got)
	}
}

// 14. Poisson arrival schedule shape.
func TestPoissonArrivalOffsets(t *testing.T) {
	t.Run("count, ordering, bounds, and exponential-like gaps", func(t *testing.T) {
		const n = 5000
		window := time.Second
		offsets := poissonArrivalOffsets(rand.New(rand.NewPCG(42, 42)), n, window)
		if len(offsets) != n {
			t.Fatalf("got %d offsets, want %d", len(offsets), n)
		}
		if !slices.IsSorted(offsets) {
			t.Fatal("offsets are not sorted ascending")
		}
		if offsets[0] < 0 || offsets[n-1] >= window {
			t.Fatalf("offsets out of range [0, %v): first=%v last=%v", window, offsets[0], offsets[n-1])
		}

		// Gaps of a Poisson process are exponentially distributed, whose
		// coefficient of variation (stddev/mean) is 1. Evenly spaced arrivals
		// would have CV ~0; a burst would have mean ~0.
		gaps := make([]float64, n-1)
		var sum float64
		for i := 1; i < n; i++ {
			gaps[i-1] = float64(offsets[i] - offsets[i-1])
			sum += gaps[i-1]
		}
		mean := sum / float64(len(gaps))
		var sq float64
		for _, g := range gaps {
			sq += (g - mean) * (g - mean)
		}
		cv := math.Sqrt(sq/float64(len(gaps))) / mean
		wantMean := float64(window) / n
		if mean < 0.9*wantMean || mean > 1.1*wantMean {
			t.Fatalf("mean gap = %v, want ~%v", time.Duration(mean), time.Duration(wantMean))
		}
		if cv < 0.9 || cv > 1.1 {
			t.Fatalf("gap coefficient of variation = %.3f, want ~1 (exponential)", cv)
		}
	})

	t.Run("zero arrivals", func(t *testing.T) {
		if got := poissonArrivalOffsets(rand.New(rand.NewPCG(1, 1)), 0, time.Second); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})

	t.Run("zero-length window", func(t *testing.T) {
		got := poissonArrivalOffsets(rand.New(rand.NewPCG(1, 1)), 3, 0)
		if !slices.Equal(got, []time.Duration{0, 0, 0}) {
			t.Fatalf("got %v, want three zero offsets", got)
		}
	})
}

// 15. Arrivals are spread across the cycle rather than fired at its start.
func TestGenerateTestTickets_ArrivalsSpreadAcrossCycle(t *testing.T) {
	const tpc = 20
	cycleDur := 200 * time.Millisecond
	q, _, _ := newTestQueue(t, "http://127.0.0.1:0", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", int(cycleDur.Milliseconds()))
	seedWithArrivalsBefore(t, q, tpc, cycleDur, 30*time.Millisecond)
	seed := q.Cfg.GetUint64("TICKET_ARRIVAL_SEED")
	q.ClientRequestChan = make(chan *ClientRequest)
	q.SetTestTicketConfig(tpc, func(_ context.Context) *pb.Ticket { return &pb.Ticket{} })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var acceptedAt []time.Time
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-q.ClientRequestChan:
				mu.Lock()
				acceptedAt = append(acceptedAt, time.Now())
				mu.Unlock()
				req.ResultChan <- http.StatusOK
				close(req.ResultChan)
			}
		}
	}()

	start := time.Now()
	q.GenerateTestTickets(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(acceptedAt) != tpc {
		t.Fatalf("queue accepted %d requests, want %d", len(acceptedAt), tpc)
	}

	// Compare against the schedule the generator derived from the seed.
	want := poissonArrivalOffsets(rand.New(rand.NewPCG(seed, seed)), tpc, cycleDur)
	if span := want[tpc-1] - want[0]; span < cycleDur/2 {
		t.Fatalf("test precondition: seeded schedule span %v too narrow to be meaningful", span)
	}
	const slack = 20 * time.Millisecond
	for i, at := range acceptedAt {
		got := at.Sub(start)
		if got+slack < want[i] {
			t.Fatalf("arrival %d accepted at %v, before its scheduled time %v", i, got, want[i])
		}
	}
	if got := acceptedAt[tpc-1].Sub(acceptedAt[0]); got < cycleDur/2 {
		t.Fatalf("arrivals spanned only %v of a %v cycle; want them spread across the cycle", got, cycleDur)
	}
}

// 16. When the queue blocks, arrivals stay on schedule: overdue players are
// handed over as soon as it unblocks, and the delay shows up as arrival lag.
func TestGenerateTestTickets_OverdueArrivalsReleasedWhenQueueUnblocks(t *testing.T) {
	const tpc = 10
	cycleDur := 300 * time.Millisecond
	blockFor := 150 * time.Millisecond

	q, _, reader := newTestQueue(t, "http://127.0.0.1:0", 3)
	q.Cfg.Set("TICKET_CREATION_CYCLES", 1)
	q.Cfg.Set("TICKET_CREATION_CYCLE_DURATION_MS", int(cycleDur.Milliseconds()))
	q.ClientRequestChan = make(chan *ClientRequest)
	q.SetTestTicketConfig(tpc, func(_ context.Context) *pb.Ticket { return &pb.Ticket{} })

	// Pick a seed whose schedule has an arrival well inside the blocked
	// window and whose last arrival leaves room before the deadline.
	var schedule []time.Duration
	for seed := uint64(1); ; seed++ {
		if seed >= 1000 {
			t.Fatal("no suitable arrival seed found")
		}
		schedule = poissonArrivalOffsets(rand.New(rand.NewPCG(seed, seed)), tpc, cycleDur)
		if schedule[0] <= blockFor/2 && schedule[tpc-1] <= cycleDur-30*time.Millisecond {
			q.Cfg.Set("TICKET_ARRIVAL_SEED", seed)
			break
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var accepted atomic.Int32
	go func() {
		// Queue is unavailable for the first blockFor of the cycle.
		select {
		case <-ctx.Done():
			return
		case <-time.After(blockFor):
		}
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-q.ClientRequestChan:
				accepted.Add(1)
				req.ResultChan <- http.StatusOK
				close(req.ResultChan)
			}
		}
	}()

	q.GenerateTestTickets(ctx)

	if got := accepted.Load(); got != tpc {
		t.Fatalf("queue accepted %d requests, want all %d (overdue arrivals should catch up)", got, tpc)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}
	var found bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "mmqueue.ticket.arrival_lag" {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(hist.DataPoints) != 1 {
				t.Fatalf("unexpected mmqueue.ticket.arrival_lag data: %+v", m.Data)
			}
			dp := hist.DataPoints[0]
			if dp.Count != tpc {
				t.Fatalf("arrival_lag count = %d, want %d", dp.Count, tpc)
			}
			maxLag, ok := dp.Max.Value()
			wantMin := float64((blockFor - schedule[0]).Milliseconds()) - 10
			if !ok || maxLag < wantMin {
				t.Fatalf("arrival_lag max = %vms (present=%v), want >= %vms", maxLag, ok, wantMin)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("expected mmqueue.ticket.arrival_lag metric to be present")
	}
}

// seedWithArrivalsBefore sets TICKET_ARRIVAL_SEED to a seed whose n-arrival
// schedule within window finishes at least margin before the cycle deadline,
// so tests that expect every arrival to be queued within a single cycle are
// deterministic.
func seedWithArrivalsBefore(t *testing.T, q *MatchmakerQueue, n int64, window, margin time.Duration) {
	t.Helper()
	for seed := uint64(1); seed < 1000; seed++ {
		offsets := poissonArrivalOffsets(rand.New(rand.NewPCG(seed, seed)), n, window)
		if len(offsets) == 0 || offsets[len(offsets)-1] <= window-margin {
			q.Cfg.Set("TICKET_ARRIVAL_SEED", seed)
			return
		}
	}
	t.Fatalf("no seed found with %d arrivals finishing %v before a %v deadline", n, margin, window)
}

// counterValue returns the current value of the int64 counter called name,
// or 0 if it hasn't been recorded.
func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect failed: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && len(sum.DataPoints) > 0 {
				return sum.DataPoints[0].Value
			}
		}
	}
	return 0
}

// fakeOMCore is a minimal om-core REST endpoint: CreateTicket returns
// ticketID, ActivateTickets succeeds once release is closed (nil: at once),
// and each DeactivateTickets request's ticket IDs are sent on deactivated.
func fakeOMCore(t *testing.T, ticketID string, activateEntered chan<- struct{}, release <-chan struct{}, deactivated chan<- []string) *httptest.Server {
	t.Helper()
	var enteredOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/tickets":
			respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: ticketID})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(respBytes)
		case "/tickets:activate":
			if activateEntered != nil {
				enteredOnce.Do(func() { close(activateEntered) })
			}
			if release != nil {
				<-release
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/tickets:deactivate":
			ids := readDeactivateRequest(t, r)
			if deactivated != nil {
				deactivated <- ids
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// 17. Tickets still tracked when om-core expires them are dropped from
// q.Tickets by the expiry sweep, and counted.
func TestRun_ExpirySweepDropsExpiredTickets(t *testing.T) {
	const ticketID = "expiring-1"
	srv := fakeOMCore(t, ticketID, nil, nil, nil)

	q, sender, reader := newTestQueue(t, srv.URL, 3)
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)
	q.Cfg.Set("MMQUEUE_TICKET_TTL_MS", 300)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ticket: &pb.Ticket{}, ResultChan: resChan}
	expectResultOnceThenClosed(t, "request", resChan, http.StatusOK)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, exists := q.Tickets.Load(ticketID); !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected ticket %q to be dropped after its TTL", ticketID)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := counterValue(t, reader, "mmqueue.ticket.expirations"); got != 1 {
		t.Fatalf("mmqueue.ticket.expirations = %d, want 1", got)
	}

	cancel()
	sender.Stop()
	<-runDone
}

// 18. A ticket expires at creation time + TTL, or at its own earlier
// ExpirationTime.
func TestTicketExpiry(t *testing.T) {
	q, _, _ := newTestQueue(t, "http://unused.test", 1)
	created := time.Now().Truncate(time.Millisecond)
	ticket := &pb.Ticket{Attributes: &pb.Ticket_FilterableData{CreationTime: timestamppb.New(created)}}

	if got, want := q.ticketExpiry(ticket), created.Add(10*time.Minute); !got.Equal(want) {
		t.Fatalf("expiry without ExpirationTime = %v, want %v", got, want)
	}
	ticket.ExpirationTime = timestamppb.New(created.Add(time.Minute))
	if got, want := q.ticketExpiry(ticket), created.Add(time.Minute); !got.Equal(want) {
		t.Fatalf("expiry with earlier ExpirationTime = %v, want %v", got, want)
	}
	ticket.ExpirationTime = timestamppb.New(created.Add(time.Hour))
	if got, want := q.ticketExpiry(ticket), created.Add(10*time.Minute); !got.Equal(want) {
		t.Fatalf("expiry with later ExpirationTime = %v, want %v (om-core caps it)", got, want)
	}
}

// 19. A request whose client has already gone never creates a ticket, and its
// ResultChan is closed without a status.
func TestRun_AbandonedBeforeCreateSkipsCreateTicket(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	q, sender, reader := newTestQueue(t, srv.URL, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	clientCtx, clientCancel := context.WithCancel(context.Background())
	clientCancel()
	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ctx: clientCtx, Ticket: &pb.Ticket{}, ResultChan: resChan}

	select {
	case code, ok := <-resChan:
		if ok {
			t.Fatalf("abandoned request got status %d, want ResultChan closed without a status", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for abandoned request's ResultChan to close")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("om-core received %d calls for an abandoned request, want 0", got)
	}
	if got := counterValue(t, reader, "mmqueue.ticket.abandonments"); got != 1 {
		t.Fatalf("mmqueue.ticket.abandonments = %d, want 1", got)
	}

	cancel()
	sender.Stop()
	<-runDone
}

// 20. If the client goes away while its ticket is being activated, the queue
// waits for the activation call to finish and then deactivates the ticket,
// so the deactivation can't be overtaken by the activation.
func TestRun_AbandonedDuringActivationDeactivatesTicket(t *testing.T) {
	const ticketID = "abandon-1"
	activateEntered := make(chan struct{})
	releaseActivate := make(chan struct{})
	deactivated := make(chan []string, 1)
	srv := fakeOMCore(t, ticketID, activateEntered, releaseActivate, deactivated)
	t.Cleanup(func() {
		select {
		case <-releaseActivate:
		default:
			close(releaseActivate)
		}
	})

	q, sender, reader := newTestQueue(t, srv.URL, 3)
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ctx: clientCtx, Ticket: &pb.Ticket{}, ResultChan: resChan}

	select {
	case <-activateEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for /tickets:activate")
	}
	clientCancel()

	// No deactivation while the activation call is still in flight.
	select {
	case ids := <-deactivated:
		t.Fatalf("deactivated %v before activation finished", ids)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseActivate)
	select {
	case ids := <-deactivated:
		if !slices.Equal(ids, []string{ticketID}) {
			t.Fatalf("deactivated %v, want [%s]", ids, ticketID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for abandoned ticket to be deactivated")
	}

	select {
	case code, ok := <-resChan:
		if ok {
			t.Fatalf("abandoned request got status %d, want ResultChan closed without a status", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for abandoned request's ResultChan to close")
	}
	if _, exists := q.Tickets.Load(ticketID); exists {
		t.Fatalf("abandoned ticket %q still tracked", ticketID)
	}
	if got := counterValue(t, reader, "mmqueue.ticket.abandonments"); got != 1 {
		t.Fatalf("mmqueue.ticket.abandonments = %d, want 1", got)
	}

	cancel()
	sender.Stop()
	<-runDone
}

// 21. proxyCreateTicket sets CreationTime itself (the platform, not the
// client, decides when the player entered matchmaking) and passes the
// client's attributes through.
func TestProxyCreateTicket_SetsCreationTime(t *testing.T) {
	var got pb.CreateTicketRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := protojson.Unmarshal(body, &got); err != nil {
			t.Errorf("cannot unmarshal CreateTicketRequest: %v", err)
		}
		respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "t1"})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	defer srv.Close()

	q, _, _ := newTestQueue(t, srv.URL, 1)
	clientTime := time.Now().Add(-time.Hour)
	before := time.Now()
	_, err := q.proxyCreateTicket(context.Background(), &pb.Ticket{Attributes: &pb.Ticket_FilterableData{
		CreationTime: timestamppb.New(clientTime),
		DoubleArgs:   map[string]float64{"mmr": 1500},
	}})
	if err != nil {
		t.Fatalf("proxyCreateTicket failed: %v", err)
	}
	after := time.Now()

	attrs := got.GetTicket().GetAttributes()
	if ct := attrs.GetCreationTime().AsTime(); ct.Before(before) || ct.After(after) {
		t.Fatalf("CreationTime = %v, want between %v and %v (client sent %v)", ct, before, after, clientTime)
	}
	if !maps.Equal(attrs.GetDoubleArgs(), map[string]float64{"mmr": 1500}) {
		t.Fatalf("DoubleArgs = %v, want map[mmr:1500]", attrs.GetDoubleArgs())
	}
}

// 22. A request with a nil Ticket is tracked with a real expiry (creation
// time + TTL), not epoch + TTL, so the expiry sweep doesn't drop it at once.
func TestRun_NilTicketGetsRealExpiry(t *testing.T) {
	const ticketID = "nil-ticket-1"
	srv := fakeOMCore(t, ticketID, nil, nil, nil)

	q, sender, reader := newTestQueue(t, srv.URL, 3)
	q.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1)
	q.Cfg.Set("MMQUEUE_TICKET_TTL_MS", 60000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		q.Run(ctx)
		close(runDone)
	}()

	resChan := make(chan int, 1)
	q.ClientRequestChan <- &ClientRequest{Ticket: nil, ResultChan: resChan}
	expectResultOnceThenClosed(t, "request", resChan, http.StatusOK)

	v, ok := q.Tickets.Load(ticketID)
	if !ok {
		t.Fatalf("ticket %q not tracked after 200", ticketID)
	}
	if exp := v.(time.Time); time.Until(exp) < 50*time.Second {
		t.Fatalf("ticket expiry = %v (in %v), want ~60s from now", exp, time.Until(exp))
	}

	// Run a sweep now (rather than waiting for the 6s ticker) and confirm
	// the ticket survives it.
	q.sweepExpiredTickets(ctx, time.Now())
	if _, ok := q.Tickets.Load(ticketID); !ok {
		t.Fatalf("ticket %q dropped by the expiry sweep", ticketID)
	}
	if got := counterValue(t, reader, "mmqueue.ticket.expirations"); got != 0 {
		t.Fatalf("mmqueue.ticket.expirations = %d, want 0", got)
	}

	cancel()
	sender.Stop()
	<-runDone
}
