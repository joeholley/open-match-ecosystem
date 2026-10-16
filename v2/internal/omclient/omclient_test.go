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

package omclient

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func marshalStatusBody(t *testing.T, code codes.Code, msg string, perTicketErrs map[string]string) []byte {
	t.Helper()
	st := status.New(code, msg)
	if len(perTicketErrs) > 0 {
		br := &errdetails.BadRequest{}
		for id, desc := range perTicketErrs {
			br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
				Field:       "ticket_id/" + id,
				Description: desc,
			})
		}
		var err error
		st, err = st.WithDetails(br)
		if err != nil {
			t.Fatalf("st.WithDetails failed: %v", err)
		}
	}
	b, err := protojson.Marshal(st.Proto())
	if err != nil {
		t.Fatalf("protojson.Marshal status failed: %v", err)
	}
	return b
}

func newTestOMClient(serverURL string, maxBatchSize int) *RestfulOMGrpcClient {
	cfg := viper.New()
	cfg.Set("OM_CORE_ADDR", serverURL)
	cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", maxBatchSize)
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return New(cfg, logger, &http.Client{})
}

func testActivationCtx() context.Context {
	return context.WithValue(context.Background(), "activationType", "test") //nolint:staticcheck // omclient reads the string key "activationType" from context
}

func TestParseTicketStatusBody(t *testing.T) {
	t.Run("valid status with BadRequest field violations", func(t *testing.T) {
		body := marshalStatusBody(t, codes.Internal, "unable to update ticket state", map[string]string{
			"ticket-2": "redis timeout",
			"ticket-5": "stream full",
		})

		code, perTicketErrs, ok := parseTicketStatusBody(body)
		if !ok {
			t.Fatalf("expected ok=true, got false")
		}
		if code != codes.Internal {
			t.Errorf("expected code %v, got %v", codes.Internal, code)
		}
		if len(perTicketErrs) != 2 {
			t.Fatalf("expected 2 per-ticket errors, got %d: %v", len(perTicketErrs), perTicketErrs)
		}
		if perTicketErrs["ticket-2"] != "redis timeout" {
			t.Errorf("unexpected error for ticket-2: %q", perTicketErrs["ticket-2"])
		}
		if perTicketErrs["ticket-5"] != "stream full" {
			t.Errorf("unexpected error for ticket-5: %q", perTicketErrs["ticket-5"])
		}
	})

	t.Run("raw literal grpc-gateway JSON string", func(t *testing.T) {
		rawJSON := []byte(`{"code":3,"message":"Invalid ticket id","details":[{"@type":"type.googleapis.com/google.rpc.BadRequest","fieldViolations":[{"field":"ticket_id/bad-id-1","description":"Invalid ticket id"}]}]}`)

		code, perTicketErrs, ok := parseTicketStatusBody(rawJSON)
		if !ok {
			t.Fatalf("expected ok=true for raw grpc-gateway JSON, got false")
		}
		if code != codes.InvalidArgument {
			t.Errorf("expected code %v, got %v", codes.InvalidArgument, code)
		}
		if len(perTicketErrs) != 1 || perTicketErrs["bad-id-1"] != "Invalid ticket id" {
			t.Errorf("unexpected perTicketErrs: %v", perTicketErrs)
		}
	})

	t.Run("status JSON with no details", func(t *testing.T) {
		body := marshalStatusBody(t, codes.InvalidArgument, "all ticket ids invalid", nil)

		code, perTicketErrs, ok := parseTicketStatusBody(body)
		if ok {
			t.Errorf("expected ok=false when no details are present, got true (%v)", perTicketErrs)
		}
		if code != codes.InvalidArgument {
			t.Errorf("expected code %v, got %v", codes.InvalidArgument, code)
		}
		if len(perTicketErrs) != 0 {
			t.Errorf("expected empty perTicketErrs, got %v", perTicketErrs)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		code, perTicketErrs, ok := parseTicketStatusBody([]byte("503 Service Unavailable"))
		if ok {
			t.Errorf("expected ok=false for plain text body, got true")
		}
		if code != codes.Unknown {
			t.Errorf("expected code %v, got %v", codes.Unknown, code)
		}
		if perTicketErrs != nil {
			t.Errorf("expected nil perTicketErrs, got %v", perTicketErrs)
		}
	})
}

func TestActivateTicketsWithResults_PartialInternalFailureRetriesSubset(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
		}
		var reqPb pb.ActivateTicketsRequest
		if err := protojson.Unmarshal(body, &reqPb); err != nil {
			t.Errorf("unmarshalling ActivateTicketsRequest: %v", err)
		}

		mu.Lock()
		receivedBatches = append(receivedBatches, slices.Clone(reqPb.GetTicketIds()))
		attempt := len(receivedBatches)
		mu.Unlock()

		if attempt == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write(marshalStatusBody(t, codes.Internal, "unable to update ticket state", map[string]string{
				"t-2": "replication error",
				"t-4": "replication error",
			}))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	client := newTestOMClient(srv.URL, 5)
	ctx := testActivationCtx()

	inChan := make(chan string, 5)
	resultsChan := make(chan ActivationResult, 10)
	inputIDs := []string{"t-1", "t-2", "t-3", "t-4", "t-5"}
	for _, id := range inputIDs {
		inChan <- id
	}
	close(inChan)

	client.ActivateTicketsWithResults(ctx, inChan, resultsChan)
	close(resultsChan)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedBatches) != 2 {
		t.Fatalf("expected 2 HTTP attempts, got %d: %v", len(receivedBatches), receivedBatches)
	}
	if !slices.Equal(receivedBatches[0], inputIDs) {
		t.Errorf("attempt 1 batch = %v, want %v", receivedBatches[0], inputIDs)
	}
	wantRetry := []string{"t-2", "t-4"}
	if !slices.Equal(receivedBatches[1], wantRetry) {
		t.Errorf("attempt 2 batch = %v, want only failed subset %v", receivedBatches[1], wantRetry)
	}

	gotResults := make(map[string]error)
	for res := range resultsChan {
		if _, exists := gotResults[res.TicketId]; exists {
			t.Errorf("duplicate result emitted for ticket %q", res.TicketId)
		}
		gotResults[res.TicketId] = res.Err
	}
	if len(gotResults) != len(inputIDs) {
		t.Fatalf("expected results for %d tickets, got %d: %v", len(inputIDs), len(gotResults), gotResults)
	}
	for _, id := range inputIDs {
		if err, ok := gotResults[id]; !ok || err != nil {
			t.Errorf("ticket %q result: ok=%v, err=%v; want ok=true, err=nil", id, ok, err)
		}
	}
}

func TestActivateTicketsWithResults_InvalidArgumentNotRetried(t *testing.T) {
	t.Run("partial invalid IDs with BadRequest details", func(t *testing.T) {
		var mu sync.Mutex
		var attempts int

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			attempts++
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(marshalStatusBody(t, codes.InvalidArgument, "invalid ticket id", map[string]string{
				"bad-1": "ticket id bad-1 is malformed",
				"bad-2": "ticket id bad-2 is malformed",
			}))
		}))
		defer srv.Close()

		client := newTestOMClient(srv.URL, 4)
		ctx := testActivationCtx()

		inChan := make(chan string, 4)
		resultsChan := make(chan ActivationResult, 10)
		for _, id := range []string{"good-1", "bad-1", "good-2", "bad-2"} {
			inChan <- id
		}
		close(inChan)

		client.ActivateTicketsWithResults(ctx, inChan, resultsChan)
		close(resultsChan)

		mu.Lock()
		gotAttempts := attempts
		mu.Unlock()
		if gotAttempts != 1 {
			t.Fatalf("expected exactly 1 HTTP request (no retries on 400), got %d", gotAttempts)
		}

		gotResults := make(map[string]error)
		for res := range resultsChan {
			if _, exists := gotResults[res.TicketId]; exists {
				t.Errorf("duplicate result emitted for ticket %q", res.TicketId)
			}
			gotResults[res.TicketId] = res.Err
		}
		if len(gotResults) != 4 {
			t.Fatalf("expected 4 results, got %d: %v", len(gotResults), gotResults)
		}
		for _, goodID := range []string{"good-1", "good-2"} {
			if err := gotResults[goodID]; err != nil {
				t.Errorf("expected nil error for valid ticket %q, got %v", goodID, err)
			}
		}
		for _, badID := range []string{"bad-1", "bad-2"} {
			if err := gotResults[badID]; err == nil {
				t.Errorf("expected non-nil error for invalid ticket %q, got nil", badID)
			}
		}
	})

	t.Run("all invalid IDs without details", func(t *testing.T) {
		var mu sync.Mutex
		var attempts int

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			attempts++
			mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(marshalStatusBody(t, codes.InvalidArgument, "all ticket ids invalid", nil))
		}))
		defer srv.Close()

		client := newTestOMClient(srv.URL, 2)
		ctx := testActivationCtx()

		inChan := make(chan string, 2)
		resultsChan := make(chan ActivationResult, 10)
		inChan <- "bad-a"
		inChan <- "bad-b"
		close(inChan)

		client.ActivateTicketsWithResults(ctx, inChan, resultsChan)
		close(resultsChan)

		mu.Lock()
		gotAttempts := attempts
		mu.Unlock()
		if gotAttempts != 1 {
			t.Fatalf("expected 1 HTTP request on 400 without details, got %d", gotAttempts)
		}

		gotResults := make(map[string]error)
		for res := range resultsChan {
			gotResults[res.TicketId] = res.Err
		}
		if len(gotResults) != 2 {
			t.Fatalf("expected 2 results, got %d", len(gotResults))
		}
		for _, id := range []string{"bad-a", "bad-b"} {
			if gotResults[id] == nil {
				t.Errorf("expected non-nil error for %q, got nil", id)
			}
		}
	})
}

func TestActivateTicketsWithResults_MixedInvalidAndTransientInternalOn400(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var reqPb pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &reqPb)

		mu.Lock()
		receivedBatches = append(receivedBatches, slices.Clone(reqPb.GetTicketIds()))
		attempt := len(receivedBatches)
		mu.Unlock()

		if attempt == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(marshalStatusBody(t, codes.InvalidArgument, "Invalid ticket id", map[string]string{
				"bad": "Invalid ticket id",
				"2":   "rpc error: code = Internal desc = Unable to update ticket 2 state to active",
			}))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	client := newTestOMClient(srv.URL, 3)
	ctx := testActivationCtx()

	inChan := make(chan string, 3)
	resultsChan := make(chan ActivationResult, 10)
	inputIDs := []string{"1", "bad", "2"}
	for _, id := range inputIDs {
		inChan <- id
	}
	close(inChan)

	client.ActivateTicketsWithResults(ctx, inChan, resultsChan)
	close(resultsChan)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedBatches) != 2 {
		t.Fatalf("expected 2 HTTP attempts, got %d: %v", len(receivedBatches), receivedBatches)
	}
	if !slices.Equal(receivedBatches[0], inputIDs) {
		t.Errorf("attempt 1 batch = %v, want %v", receivedBatches[0], inputIDs)
	}
	wantRetry := []string{"2"}
	if !slices.Equal(receivedBatches[1], wantRetry) {
		t.Errorf("attempt 2 batch = %v, want only transient failed ticket %v", receivedBatches[1], wantRetry)
	}

	gotResults := make(map[string]error)
	for res := range resultsChan {
		if _, exists := gotResults[res.TicketId]; exists {
			t.Errorf("duplicate result emitted for ticket %q", res.TicketId)
		}
		gotResults[res.TicketId] = res.Err
	}
	if len(gotResults) != 3 {
		t.Fatalf("expected 3 results, got %d: %v", len(gotResults), gotResults)
	}
	if gotResults["1"] != nil {
		t.Errorf("expected nil error for valid ticket '1', got %v", gotResults["1"])
	}
	if gotResults["bad"] == nil {
		t.Errorf("expected non-nil permanent error for invalid ticket 'bad', got nil")
	}
	if gotResults["2"] != nil {
		t.Errorf("expected nil error after retry for ticket '2', got %v", gotResults["2"])
	}
}

func TestActivateTicketsWithResults_DeduplicatesTicketIDsInBatch(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var reqPb pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &reqPb)

		mu.Lock()
		receivedBatches = append(receivedBatches, slices.Clone(reqPb.GetTicketIds()))
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	client := newTestOMClient(srv.URL, 10)
	ctx := testActivationCtx()

	inChan := make(chan string, 10)
	resultsChan := make(chan ActivationResult, 10)
	for _, id := range []string{"dup-1", "dup-2", "dup-1", "dup-3", "dup-2"} {
		inChan <- id
	}
	close(inChan)

	client.ActivateTicketsWithResults(ctx, inChan, resultsChan)
	close(resultsChan)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedBatches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(receivedBatches))
	}
	wantBatch := []string{"dup-1", "dup-2", "dup-3"}
	if !slices.Equal(receivedBatches[0], wantBatch) {
		t.Errorf("received batch = %v, want deduplicated %v", receivedBatches[0], wantBatch)
	}

	var results []ActivationResult
	for res := range resultsChan {
		results = append(results, res)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 deduplicated results, got %d: %v", len(results), results)
	}
}

func TestActivateTicketsWithResults_TransientUnavailableRetriesWholeBatch(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var reqPb pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &reqPb)

		mu.Lock()
		receivedBatches = append(receivedBatches, slices.Clone(reqPb.GetTicketIds()))
		attempt := len(receivedBatches)
		mu.Unlock()

		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("upstream unavailable"))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	client := newTestOMClient(srv.URL, 3)
	ctx := testActivationCtx()

	inChan := make(chan string, 3)
	resultsChan := make(chan ActivationResult, 10)
	inputIDs := []string{"t-1", "t-2", "t-3"}
	for _, id := range inputIDs {
		inChan <- id
	}
	close(inChan)

	client.ActivateTicketsWithResults(ctx, inChan, resultsChan)
	close(resultsChan)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedBatches) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(receivedBatches))
	}
	if !slices.Equal(receivedBatches[0], inputIDs) {
		t.Errorf("attempt 1 batch = %v, want %v", receivedBatches[0], inputIDs)
	}
	if !slices.Equal(receivedBatches[1], inputIDs) {
		t.Errorf("attempt 2 batch = %v, want whole batch %v", receivedBatches[1], inputIDs)
	}
}

func TestActivateTickets_ClosedChannelDoesNotSendEmptyTicketID(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var reqPb pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &reqPb)

		mu.Lock()
		receivedBatches = append(receivedBatches, slices.Clone(reqPb.GetTicketIds()))
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	// Set max batch size to 500 so closing the channel after 3 items triggers flush before the 250ms timer.
	client := newTestOMClient(srv.URL, 500)
	ctx := testActivationCtx()

	inChan := make(chan string, 10)
	resultsChan := make(chan ActivationResult, 10)
	inputIDs := []string{"id-1", "id-2", "id-3"}
	for _, id := range inputIDs {
		inChan <- id
	}
	close(inChan)

	client.ActivateTicketsWithResults(ctx, inChan, resultsChan)
	close(resultsChan)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedBatches) != 1 {
		t.Fatalf("expected 1 batch, got %d: %v", len(receivedBatches), receivedBatches)
	}
	if !slices.Equal(receivedBatches[0], inputIDs) {
		t.Errorf("received batch = %q, want %q (with no trailing empty string)", receivedBatches[0], inputIDs)
	}

	var results []ActivationResult
	for res := range resultsChan {
		if res.Err != nil {
			t.Errorf("expected nil error for %q, got %v", res.TicketId, res.Err)
		}
		results = append(results, res)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 activation results, got %d: %v", len(results), results)
	}
}

type staticTestTokenSource struct {
	tok *oauth2.Token
}

func (s *staticTestTokenSource) Token() (*oauth2.Token, error) {
	return s.tok, nil
}

func TestPost_CloudRunTokenCacheConcurrent(t *testing.T) {
	t.Setenv("K_REVISION", "test-rev-001")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer cloud-run-valid-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ticketId":"1790307871351-0"}`))
	}))
	defer srv.Close()

	var factoryCalls atomic.Int32
	client := newTestOMClient(srv.URL, 10)
	client.tokens = newIDTokenCache(func(ctx context.Context, audience string) (oauth2.TokenSource, error) {
		factoryCalls.Add(1)
		return &staticTestTokenSource{
			tok: &oauth2.Token{
				AccessToken: "cloud-run-valid-token",
				Expiry:      time.Now().Add(time.Hour),
			},
		}, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := client.CreateTicket(context.Background(), &pb.Ticket{})
			if err != nil {
				t.Errorf("CreateTicket failed: %v", err)
			}
			if id != "1790307871351-0" {
				t.Errorf("CreateTicket got id %q, want 1790307871351-0", id)
			}
		}()
	}
	wg.Wait()

	if got := factoryCalls.Load(); got != 1 {
		t.Errorf("expected token mint function to be called once across 50 concurrent Post calls, got %d", got)
	}
}

func TestPost_CloudRunTokenRefreshedOn401(t *testing.T) {
	t.Setenv("K_REVISION", "test-rev-001")

	var mu sync.Mutex
	var receivedAuth []string
	var receivedBodies []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		auth := r.Header.Get("Authorization")
		mu.Lock()
		receivedAuth = append(receivedAuth, auth)
		receivedBodies = append(receivedBodies, string(body))
		mu.Unlock()

		if auth == "Bearer stale-valid-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("token rejected"))
			return
		}
		if auth != "Bearer refreshed-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ticketId":"1790307871351-99"}`))
	}))
	defer srv.Close()

	var factoryCalls atomic.Int32
	client := newTestOMClient(srv.URL, 10)
	client.tokens = newIDTokenCache(func(_ context.Context, audience string) (oauth2.TokenSource, error) {
		n := factoryCalls.Add(1)
		tok := "stale-valid-token"
		if n >= 2 {
			tok = "refreshed-token"
		}
		return &staticTestTokenSource{
			tok: &oauth2.Token{
				AccessToken: tok,
				Expiry:      time.Now().Add(time.Hour),
			},
		}, nil
	})

	id, err := client.CreateTicket(context.Background(), &pb.Ticket{})
	if err != nil {
		t.Fatalf("CreateTicket after 401 token refresh failed: %v", err)
	}
	if id != "1790307871351-99" {
		t.Errorf("got ticketId %q, want 1790307871351-99", id)
	}
	if got := factoryCalls.Load(); got != 2 {
		t.Errorf("expected 2 token mint calls (initial + 1 refresh on 401), got %d", got)
	}

	mu.Lock()
	defer mu.Unlock()
	wantAuth := []string{"Bearer stale-valid-token", "Bearer refreshed-token"}
	if !slices.Equal(receivedAuth, wantAuth) {
		t.Errorf("receivedAuth = %v, want %v", receivedAuth, wantAuth)
	}
	if len(receivedBodies) != 2 || receivedBodies[0] == "" || receivedBodies[0] != receivedBodies[1] {
		t.Errorf("expected non-empty identical request body on 401 retry, got %v", receivedBodies)
	}
}

// TestCreateTicket_ContextCancellation verifies that CreateTicket derives its
// request context from the caller's ctx (rather than context.Background()) so
// pre-cancelled contexts and in-flight cancellations abort the HTTP request.
func TestCreateTicket_ContextCancellation(t *testing.T) {
	t.Run("pre-cancelled context aborts without hitting server", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ticketId":"1790307871351-1"}`))
		}))
		defer srv.Close()

		client := newTestOMClient(srv.URL, 10)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		id, err := client.CreateTicket(ctx, &pb.Ticket{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CreateTicket with pre-cancelled ctx error = %v, want context.Canceled", err)
		}
		if id != "" {
			t.Errorf("expected empty ticketId on cancelled ctx, got %q", id)
		}
		if got := hits.Load(); got != 0 {
			t.Errorf("expected 0 HTTP requests on pre-cancelled ctx, got %d", got)
		}
	})

	t.Run("in-flight context cancellation aborts blocked request", func(t *testing.T) {
		enteredCh := make(chan struct{})
		releaseHandlerCh := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(enteredCh)
			<-releaseHandlerCh
		}))
		defer srv.Close()
		defer close(releaseHandlerCh)

		client := newTestOMClient(srv.URL, 10)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			_, err := client.CreateTicket(ctx, &pb.Ticket{})
			errCh <- err
		}()

		select {
		case <-enteredCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for CreateTicket HTTP request to reach server")
		}

		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("CreateTicket error after in-flight cancel = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for CreateTicket to abort after ctx cancellation")
		}
	})
}

// TestValidateConnection verifies that ValidateConnection probes POST /tickets:activate
// with an empty ActivateTicketsRequest (without creating dummy tickets), accepts
// HTTP 200 OK and HTTP 400 Bad Request, and returns an error on auth/routing/server
// error status codes (401, 403, 404, 501, 503).
func TestValidateConnection(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantErr    bool
	}{
		{name: "200 OK succeeds", statusCode: http.StatusOK, wantErr: false},
		{name: "400 Bad Request from live om-core empty ActivateTickets succeeds", statusCode: http.StatusBadRequest, wantErr: false},
		{name: "401 Unauthorized fails", statusCode: http.StatusUnauthorized, wantErr: true},
		{name: "403 Forbidden fails", statusCode: http.StatusForbidden, wantErr: true},
		{name: "404 Not Found fails", statusCode: http.StatusNotFound, wantErr: true},
		{name: "501 Not Implemented fails", statusCode: http.StatusNotImplemented, wantErr: true},
		{name: "503 Service Unavailable fails", statusCode: http.StatusServiceUnavailable, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu         sync.Mutex
				gotMethod  string
				gotPath    string
				gotReqBody []byte
			)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				gotMethod = r.Method
				gotPath = r.URL.Path
				gotReqBody = body
				mu.Unlock()

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(`{"message":"probe response"}`))
			}))
			defer srv.Close()

			client := newTestOMClient(srv.URL, 10)
			err := client.ValidateConnection(context.Background(), srv.URL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateConnection() error = %v, wantErr %v", err, tc.wantErr)
			}

			mu.Lock()
			defer mu.Unlock()
			if gotMethod != http.MethodPost {
				t.Errorf("ValidateConnection HTTP method = %q, want POST", gotMethod)
			}
			if gotPath != "/tickets:activate" {
				t.Errorf("ValidateConnection HTTP path = %q, want /tickets:activate", gotPath)
			}
			var actReq pb.ActivateTicketsRequest
			if uErr := protojson.Unmarshal(gotReqBody, &actReq); uErr != nil {
				t.Errorf("ValidateConnection body did not unmarshal as ActivateTicketsRequest: %v", uErr)
			}
			if len(actReq.GetTicketIds()) != 0 {
				t.Errorf("ValidateConnection ActivateTicketsRequest.TicketIds = %v, want empty", actReq.GetTicketIds())
			}
		})
	}
}

// TestInvokeMatchmakingFunctions_MidStreamErrorEnvelope verifies that
// InvokeMatchmakingFunctions parses the grpc-gateway JSON envelope on streamed
// responses from /matches:fetch, logs mid-stream {"error": ...} objects without
// emitting bogus matches, skips empty {"result": {}} objects, and forwards only
// valid match responses to respChan before closing it.
func TestInvokeMatchmakingFunctions_MidStreamErrorEnvelope(t *testing.T) {
	t.Run("mid-stream error envelope is logged and skipped while valid matches are delivered", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/matches:fetch" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			m1Bytes, _ := protojson.Marshal(&pb.StreamedMmfResponse{Match: &pb.Match{Id: "match-1"}})
			m2Bytes, _ := protojson.Marshal(&pb.StreamedMmfResponse{Match: &pb.Match{Id: "match-2"}})

			// Stream:
			// 1. Valid match-1 result envelope
			// 2. Mid-stream grpc-gateway error envelope
			// 3. Empty result envelope (nil Match)
			// 4. Valid match-2 result envelope
			_, _ = fmt.Fprintf(w, `{"result":%s}`+"\n", string(m1Bytes))
			_, _ = fmt.Fprintln(w, `{"error":{"code":13,"message":"simulated mid-stream MMF error"}}`)
			_, _ = fmt.Fprintln(w, `{"result":{}}`)
			_, _ = fmt.Fprintf(w, `{"result":%s}`+"\n", string(m2Bytes))
		}))
		defer srv.Close()

		var logBuf bytes.Buffer
		cfg := viper.New()
		cfg.Set("OM_CORE_ADDR", srv.URL)
		logger := logrus.New()
		logger.SetOutput(&logBuf)
		client := New(cfg, logger, srv.Client())

		respChan := make(chan *pb.StreamedMmfResponse, 10)
		client.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{
			Profile: &pb.Profile{Name: "test-profile"},
		}, respChan)

		var gotMatchIDs []string
		for res := range respChan {
			if res == nil || res.GetMatch() == nil {
				t.Errorf("received nil StreamedMmfResponse or nil Match on respChan: %v", res)
				continue
			}
			gotMatchIDs = append(gotMatchIDs, res.GetMatch().GetId())
		}

		wantIDs := []string{"match-1", "match-2"}
		if !slices.Equal(gotMatchIDs, wantIDs) {
			t.Errorf("received match IDs = %v, want %v", gotMatchIDs, wantIDs)
		}

		logOutput := logBuf.String()
		if !strings.Contains(logOutput, "simulated mid-stream MMF error") || !strings.Contains(logOutput, "code 13") {
			t.Errorf("expected mid-stream error message and code 13 in logs, got:\n%s", logOutput)
		}
	})

	t.Run("non-200 HTTP status drains body and closes respChan with zero matches", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("service unavailable"))
		}))
		defer srv.Close()

		client := newTestOMClient(srv.URL, 10)
		respChan := make(chan *pb.StreamedMmfResponse, 10)
		client.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{
			Profile: &pb.Profile{Name: "test-profile"},
		}, respChan)

		var count int
		for range respChan {
			count++
		}
		if count != 0 {
			t.Errorf("expected 0 matches on 503 response, got %d", count)
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type failingStreamBody struct {
	firstLine []byte
	readOnce  bool
	readErr   error
}

func (b *failingStreamBody) Read(p []byte) (int, error) {
	if !b.readOnce {
		b.readOnce = true
		n := copy(p, b.firstLine)
		return n, nil
	}
	return 0, b.readErr
}

func (b *failingStreamBody) Close() error { return nil }

// TestInvokeMatchmakingFunctions_LargePayloadAndScannerError verifies REL-02:
//  1. A streamed StreamedMmfResponse whose JSON line exceeds bufio.MaxScanTokenSize
//     (64 KiB) is parsed and delivered intact via the 10 MiB scanner buffer,
//     followed by subsequent matches on the same stream.
//  2. When scanner.Scan() terminates due to a non-EOF stream error while ctx is
//     still active (ctx.Err() == nil), InvokeMatchmakingFunctions logs
//     "Error reading stream, closing".
//  3. When ctx is cancelled mid-stream after receiving a match,
//     InvokeMatchmakingFunctions exits cleanly without logging a spurious
//     "Error reading stream, closing" error.
func TestInvokeMatchmakingFunctions_LargePayloadAndScannerError(t *testing.T) {
	t.Run("large match response exceeding 64 KiB default scanner limit is delivered intact", func(t *testing.T) {
		// Construct a match whose serialized JSON line is ~256 KiB (4x the 64 KiB
		// bufio.MaxScanTokenSize default).
		const numTickets = 256
		largeTickets := make([]*pb.Ticket, numTickets)
		padding := strings.Repeat("x", 1024)
		for i := range numTickets {
			largeTickets[i] = &pb.Ticket{
				Id: fmt.Sprintf("1790000000000-%d", i),
				Attributes: &pb.Ticket_FilterableData{
					Tags: []string{padding},
				},
			}
		}
		largeMatch := &pb.Match{
			Id: "match-large-256k",
			Rosters: map[string]*pb.Roster{
				"roster-large": {
					Name:    "roster-large",
					Tickets: largeTickets,
				},
			},
		}
		largeBytes, err := protojson.Marshal(&pb.StreamedMmfResponse{Match: largeMatch})
		if err != nil {
			t.Fatalf("protojson.Marshal(largeMatch): %v", err)
		}
		if len(largeBytes) <= 4*bufio.MaxScanTokenSize {
			t.Fatalf("expected serialized largeMatch size > %d bytes, got %d", 4*bufio.MaxScanTokenSize, len(largeBytes))
		}
		followUpBytes, err := protojson.Marshal(&pb.StreamedMmfResponse{Match: &pb.Match{Id: "match-after-large"}})
		if err != nil {
			t.Fatalf("protojson.Marshal(followUpMatch): %v", err)
		}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"result":%s}`+"\n", string(largeBytes))
			_, _ = fmt.Fprintf(w, `{"result":%s}`+"\n", string(followUpBytes))
		}))
		defer srv.Close()

		var logBuf bytes.Buffer
		cfg := viper.New()
		cfg.Set("OM_CORE_ADDR", srv.URL)
		logger := logrus.New()
		logger.SetOutput(&logBuf)
		client := New(cfg, logger, srv.Client())

		respChan := make(chan *pb.StreamedMmfResponse, 4)
		client.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{
			Profile: &pb.Profile{Name: "large-profile"},
		}, respChan)

		var gotMatches []*pb.Match
		for res := range respChan {
			if res != nil && res.GetMatch() != nil {
				gotMatches = append(gotMatches, res.GetMatch())
			}
		}

		if len(gotMatches) != 2 {
			t.Fatalf("received %d matches, want 2 (log output: %s)", len(gotMatches), logBuf.String())
		}
		if gotMatches[0].GetId() != "match-large-256k" {
			t.Errorf("first match ID = %q, want \"match-large-256k\"", gotMatches[0].GetId())
		}
		if gotTickets := len(gotMatches[0].GetRosters()["roster-large"].GetTickets()); gotTickets != numTickets {
			t.Errorf("first match ticket count = %d, want %d", gotTickets, numTickets)
		}
		if gotMatches[1].GetId() != "match-after-large" {
			t.Errorf("second match ID = %q, want \"match-after-large\"", gotMatches[1].GetId())
		}
		if strings.Contains(logBuf.String(), "Error reading stream, closing") {
			t.Errorf("unexpected stream error logged on clean large stream:\n%s", logBuf.String())
		}
	})

	t.Run("scanner error when context is active is logged", func(t *testing.T) {
		m1Bytes, _ := protojson.Marshal(&pb.StreamedMmfResponse{Match: &pb.Match{Id: "match-before-stream-err"}})
		firstLine := fmt.Appendf(nil, `{"result":%s}`+"\n", string(m1Bytes))

		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body: &failingStreamBody{
						firstLine: firstLine,
						readErr:   errors.New("simulated mid-stream read failure"),
					},
					Request: req,
				}, nil
			}),
		}

		var logBuf bytes.Buffer
		cfg := viper.New()
		cfg.Set("OM_CORE_ADDR", "http://om-core.example.internal")
		logger := logrus.New()
		logger.SetOutput(&logBuf)
		client := New(cfg, logger, httpClient)

		respChan := make(chan *pb.StreamedMmfResponse, 4)
		client.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{
			Profile: &pb.Profile{Name: "err-profile"},
		}, respChan)

		var gotIDs []string
		for res := range respChan {
			if res != nil && res.GetMatch() != nil {
				gotIDs = append(gotIDs, res.GetMatch().GetId())
			}
		}

		if !slices.Equal(gotIDs, []string{"match-before-stream-err"}) {
			t.Errorf("received match IDs = %v, want [\"match-before-stream-err\"]", gotIDs)
		}
		if !strings.Contains(logBuf.String(), "Error reading stream, closing") ||
			!strings.Contains(logBuf.String(), "simulated mid-stream read failure") {
			t.Errorf("expected \"Error reading stream, closing\" with underlying error in logs, got:\n%s", logBuf.String())
		}
	})

	t.Run("context cancellation mid-stream does not log spurious scanner error", func(t *testing.T) {
		releaseHandlerCh := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			m1Bytes, _ := protojson.Marshal(&pb.StreamedMmfResponse{Match: &pb.Match{Id: "match-before-cancel"}})
			_, _ = fmt.Fprintf(w, `{"result":%s}`+"\n", string(m1Bytes))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-releaseHandlerCh
		}))
		defer srv.Close()
		defer close(releaseHandlerCh)

		var logBuf bytes.Buffer
		cfg := viper.New()
		cfg.Set("OM_CORE_ADDR", srv.URL)
		logger := logrus.New()
		logger.SetOutput(&logBuf)
		client := New(cfg, logger, srv.Client())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		respChan := make(chan *pb.StreamedMmfResponse)
		doneCh := make(chan struct{})
		go func() {
			defer close(doneCh)
			client.InvokeMatchmakingFunctions(ctx, &pb.MmfRequest{
				Profile: &pb.Profile{Name: "cancel-profile"},
			}, respChan)
		}()

		select {
		case res := <-respChan:
			if res.GetMatch().GetId() != "match-before-cancel" {
				t.Fatalf("first match ID = %q, want \"match-before-cancel\"", res.GetMatch().GetId())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for first streamed match before cancel")
		}

		// Cancel ctx while the server stream is still open and blocked in scanner.Scan().
		cancel()

		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for InvokeMatchmakingFunctions to exit after ctx cancel")
		}

		if strings.Contains(logBuf.String(), "Error reading stream, closing") {
			t.Errorf("expected no spurious \"Error reading stream, closing\" log on context cancellation, got:\n%s", logBuf.String())
		}
	})
}

// TestPost_CloudRunTokenRefreshedOn403 verifies REL-07:
//  1. When running on Cloud Run (K_REVISION set) and the server rejects a cached
//     OIDC ID token with 403 Forbidden (matching om-core's mmfauth.IsAuthRejection),
//     Post refreshes the cached token from a fresh TokenSource and retries once
//     with the refreshed token and identical request body.
//  2. When the refreshed token is also rejected with 403 Forbidden, Post retries
//     only once (2 total HTTP requests and 2 token mint calls) and returns the
//     403 response without looping infinitely.
func TestPost_CloudRunTokenRefreshedOn403(t *testing.T) {
	t.Setenv("K_REVISION", "test-rev-rel07")

	t.Run("403 Forbidden triggers single OIDC token refresh and succeeds with refreshed token", func(t *testing.T) {
		var (
			mu             sync.Mutex
			receivedAuth   []string
			receivedBodies []string
		)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			auth := r.Header.Get("Authorization")
			mu.Lock()
			receivedAuth = append(receivedAuth, auth)
			receivedBodies = append(receivedBodies, string(body))
			mu.Unlock()

			if auth == "Bearer stale-403-token" {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte("IAM token expired or rejected"))
				return
			}
			if auth != "Bearer refreshed-403-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ticketId":"1790307871351-403"}`))
		}))
		defer srv.Close()

		var factoryCalls atomic.Int32
		client := newTestOMClient(srv.URL, 10)
		client.tokens = newIDTokenCache(func(_ context.Context, _ string) (oauth2.TokenSource, error) {
			n := factoryCalls.Add(1)
			tok := "stale-403-token"
			if n >= 2 {
				tok = "refreshed-403-token"
			}
			return &staticTestTokenSource{
				tok: &oauth2.Token{
					AccessToken: tok,
					Expiry:      time.Now().Add(time.Hour),
				},
			}, nil
		})

		id, err := client.CreateTicket(context.Background(), &pb.Ticket{})
		if err != nil {
			t.Fatalf("CreateTicket after 403 token refresh failed: %v", err)
		}
		if id != "1790307871351-403" {
			t.Errorf("got ticketId %q, want 1790307871351-403", id)
		}
		if got := factoryCalls.Load(); got != 2 {
			t.Errorf("expected 2 token mint calls (initial + 1 refresh on 403), got %d", got)
		}

		mu.Lock()
		defer mu.Unlock()
		wantAuth := []string{"Bearer stale-403-token", "Bearer refreshed-403-token"}
		if !slices.Equal(receivedAuth, wantAuth) {
			t.Errorf("receivedAuth = %v, want %v", receivedAuth, wantAuth)
		}
		if len(receivedBodies) != 2 || receivedBodies[0] == "" || receivedBodies[0] != receivedBodies[1] {
			t.Errorf("expected non-empty identical request body on 403 retry, got %v", receivedBodies)
		}
	})

	t.Run("403 Forbidden retries at most once when refreshed token is also rejected", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("persistent IAM permission denied"))
		}))
		defer srv.Close()

		var factoryCalls atomic.Int32
		client := newTestOMClient(srv.URL, 10)
		client.tokens = newIDTokenCache(func(_ context.Context, _ string) (oauth2.TokenSource, error) {
			n := factoryCalls.Add(1)
			return &staticTestTokenSource{
				tok: &oauth2.Token{
					AccessToken: fmt.Sprintf("forbidden-tok-%d", n),
					Expiry:      time.Now().Add(time.Hour),
				},
			}, nil
		})

		logger := logrus.New()
		logger.SetOutput(io.Discard)
		resp, err := client.Post(context.Background(), logrus.NewEntry(logger), srv.URL, "/tickets", []byte(`{}`))
		if err != nil {
			t.Fatalf("Post returned unexpected transport error: %v", err)
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("Post status = %d, want %d (http.StatusForbidden)", resp.StatusCode, http.StatusForbidden)
		}
		if gotHits := hits.Load(); gotHits != 2 {
			t.Errorf("expected exactly 2 HTTP requests (initial + 1 retry), got %d", gotHits)
		}
		if gotMints := factoryCalls.Load(); gotMints != 2 {
			t.Errorf("expected exactly 2 token mint calls, got %d", gotMints)
		}
	})
}
