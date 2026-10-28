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
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/cenkalti/backoff/v4"
	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type staticTokenSource struct {
	token *oauth2.Token
	err   error
}

func (s *staticTokenSource) Token() (*oauth2.Token, error) {
	return s.token, s.err
}

func newTestClient(t *testing.T, serverURL string) (*RestfulOMGrpcClient, *bytes.Buffer) {
	t.Helper()
	cfg := viper.New()
	cfg.Set("OM_CORE_ADDR", serverURL)
	cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)

	var logBuf bytes.Buffer
	log := logrus.New()
	log.SetOutput(&logBuf)
	log.SetLevel(logrus.DebugLevel)

	client := New(cfg, log, &http.Client{Timeout: 5 * time.Second})
	return client, &logBuf
}

func marshalStatusWithViolations(t *testing.T, code codes.Code, msg string, violations map[string]string) []byte {
	t.Helper()
	st := status.New(code, msg)
	if len(violations) > 0 {
		br := &errdetails.BadRequest{}
		for ticketID, desc := range violations {
			br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
				Field:       "ticket_id/" + ticketID,
				Description: desc,
			})
		}
		var err error
		st, err = st.WithDetails(br)
		if err != nil {
			t.Fatalf("WithDetails failed: %v", err)
		}
	}
	b, err := protojson.Marshal(st.Proto())
	if err != nil {
		t.Fatalf("protojson.Marshal status failed: %v", err)
	}
	return b
}

// 1. Status Body Parser
func TestParseActivationErrorBody(t *testing.T) {
	t.Run("protojson BadRequest field violations", func(t *testing.T) {
		body := marshalStatusWithViolations(t, codes.InvalidArgument, "Invalid ticket id", map[string]string{
			"bad-1": "Invalid ticket id",
			"bad-2": "Ticket ID failed validation",
		})
		code, ticketErrs, ok := parseActivationErrorBody(body)
		if code != codes.InvalidArgument {
			t.Fatalf("expected codes.InvalidArgument, got %v", code)
		}
		if !ok {
			t.Fatalf("expected non-empty ticketErrs map")
		}
		if len(ticketErrs) != 2 || ticketErrs["bad-1"] != "Invalid ticket id" || ticketErrs["bad-2"] != "Ticket ID failed validation" {
			t.Fatalf("unexpected ticketErrs map: %#v", ticketErrs)
		}
	})

	t.Run("raw literal grpc-gateway JSON string", func(t *testing.T) {
		rawJSON := []byte(`{
			"code": 13,
			"message": "unable to update ticket state",
			"details": [
				{
					"@type": "type.googleapis.com/google.rpc.BadRequest",
					"fieldViolations": [
						{
							"field": "ticket_id/1700000000000-1",
							"description": "Unable to update ticket 1700000000000-1 state to active : redis timeout"
						}
					]
				}
			],
			"unknownField": "ignored"
		}`)
		code, ticketErrs, ok := parseActivationErrorBody(rawJSON)
		if code != codes.Internal {
			t.Fatalf("expected codes.Internal, got %v", code)
		}
		if !ok {
			t.Fatalf("expected non-empty ticketErrs map")
		}
		wantDesc := "Unable to update ticket 1700000000000-1 state to active : redis timeout"
		if got := ticketErrs["1700000000000-1"]; got != wantDesc {
			t.Fatalf("expected description %q, got %q", wantDesc, got)
		}
	})

	t.Run("status JSON without details", func(t *testing.T) {
		rawJSON := []byte(`{"code": 3, "message": "No Valid Ticket IDs in update request"}`)
		code, ticketErrs, ok := parseActivationErrorBody(rawJSON)
		if code != codes.InvalidArgument {
			t.Fatalf("expected codes.InvalidArgument, got %v", code)
		}
		if ok || len(ticketErrs) != 0 {
			t.Fatalf("expected empty ticketErrs map and ok=false, got ok=%v map=%#v", ok, ticketErrs)
		}
	})

	t.Run("invalid plain-text and empty bodies", func(t *testing.T) {
		for _, body := range [][]byte{[]byte("plain text error"), []byte(""), nil} {
			code, ticketErrs, ok := parseActivationErrorBody(body)
			if code != codes.Unknown {
				t.Fatalf("expected codes.Unknown for body %q, got %v", string(body), code)
			}
			if ok || len(ticketErrs) != 0 {
				t.Fatalf("expected ok=false and empty map for body %q, got ok=%v map=%#v", string(body), ok, ticketErrs)
			}
		}
	})
}

// 2. Partial Internal Failure Subset Retry
func TestActivateTicketsWithResults_PartialInternalFailureSubsetRetry(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string
	secondAttemptGate := make(chan struct{})
	firstAttemptDone := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed reading request body: %v", err)
		}
		var req pb.ActivateTicketsRequest
		if err := protojson.Unmarshal(body, &req); err != nil {
			t.Errorf("failed unmarshaling ActivateTicketsRequest: %v", err)
		}

		mu.Lock()
		attempt := len(receivedBatches) + 1
		receivedBatches = append(receivedBatches, slices.Clone(req.GetTicketIds()))
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if attempt == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write(marshalStatusWithViolations(t, codes.Internal, "unable to update ticket state", map[string]string{
				"ticket-2": "Unable to update ticket ticket-2 state to active : redis write error",
			}))
			close(firstAttemptDone)
			return
		}

		<-secondAttemptGate
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 3)

	inChan := make(chan string, 3)
	resChan := make(chan ActivationResult, 10)
	inChan <- "ticket-1"
	inChan <- "ticket-2"
	inChan <- "ticket-3"
	close(inChan)

	ctx := WithActivationType(context.Background(), "activate")
	done := make(chan struct{})
	go func() {
		rc.ActivateTicketsWithResults(ctx, inChan, resChan)
		close(done)
	}()

	<-firstAttemptDone

	// Non-failing tickets ("ticket-1" and "ticket-3") must be reported immediately before retry finishes.
	immediateSuccess := make(map[string]bool)
	for range 2 {
		select {
		case res := <-resChan:
			if res.Err != nil {
				t.Fatalf("expected immediate nil error for %s, got %v", res.TicketId, res.Err)
			}
			immediateSuccess[res.TicketId] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for immediate success results on non-failing tickets")
		}
	}
	if !immediateSuccess["ticket-1"] || !immediateSuccess["ticket-3"] {
		t.Fatalf("expected ticket-1 and ticket-3 to succeed immediately, got %#v", immediateSuccess)
	}

	close(secondAttemptGate)
	<-done

	select {
	case res := <-resChan:
		if res.TicketId != "ticket-2" || res.Err != nil {
			t.Fatalf("expected ticket-2 to succeed on retry, got %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retry result of ticket-2")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedBatches) != 2 {
		t.Fatalf("expected exactly 2 HTTP attempts, got %d: %#v", len(receivedBatches), receivedBatches)
	}
	if !slices.Equal(receivedBatches[0], []string{"ticket-1", "ticket-2", "ticket-3"}) {
		t.Fatalf("unexpected first attempt batch: %#v", receivedBatches[0])
	}
	if !slices.Equal(receivedBatches[1], []string{"ticket-2"}) {
		t.Fatalf("expected second attempt to retry only [ticket-2], got %#v", receivedBatches[1])
	}
}

// 3. InvalidArgument Non-Retry
func TestActivateTicketsWithResults_InvalidArgumentNonRetry(t *testing.T) {
	t.Run("partial invalid IDs with BadRequest details", func(t *testing.T) {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(marshalStatusWithViolations(t, codes.InvalidArgument, "Invalid ticket id", map[string]string{
				"bad-id": "Invalid ticket id",
			}))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 2)

		inChan := make(chan string, 2)
		resChan := make(chan ActivationResult, 4)
		inChan <- "good-id"
		inChan <- "bad-id"
		close(inChan)

		ctx := WithActivationType(context.Background(), "activate")
		rc.ActivateTicketsWithResults(ctx, inChan, resChan)
		close(resChan)

		if got := attempts.Load(); got != 1 {
			t.Fatalf("expected no retry on permanent InvalidArgument (1 attempt), got %d", got)
		}

		results := make(map[string]error)
		for res := range resChan {
			results[res.TicketId] = res.Err
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %#v", results)
		}
		if results["good-id"] != nil {
			t.Fatalf("expected good-id to succeed, got %v", results["good-id"])
		}
		if results["bad-id"] == nil {
			t.Fatalf("expected bad-id to return permanent error, got nil")
		}
	})

	t.Run("all invalid IDs without details", func(t *testing.T) {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(marshalStatusWithViolations(t, codes.InvalidArgument, "No Valid Ticket IDs in update request", nil))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 2)

		inChan := make(chan string, 2)
		resChan := make(chan ActivationResult, 4)
		inChan <- "bad-1"
		inChan <- "bad-2"
		close(inChan)

		ctx := WithActivationType(context.Background(), "activate")
		rc.ActivateTicketsWithResults(ctx, inChan, resChan)
		close(resChan)

		if got := attempts.Load(); got != 1 {
			t.Fatalf("expected 1 attempt without retry, got %d", got)
		}

		results := make(map[string]error)
		for res := range resChan {
			results[res.TicketId] = res.Err
		}
		if len(results) != 2 || results["bad-1"] == nil || results["bad-2"] == nil {
			t.Fatalf("expected both bad-1 and bad-2 to fail permanently, got %#v", results)
		}
	})
}

// 4. Mixed InvalidArgument and Transient Internal on HTTP 400
func TestActivateTicketsWithResults_MixedInvalidArgumentAndInternalOn400(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &req)

		mu.Lock()
		attempt := len(receivedBatches) + 1
		receivedBatches = append(receivedBatches, slices.Clone(req.GetTicketIds()))
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if attempt == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(marshalStatusWithViolations(t, codes.InvalidArgument, "Invalid ticket id", map[string]string{
				"invalid-id":   "Invalid ticket id",
				"transient-id": "Unable to update ticket transient-id state to active : redis timeout",
			}))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 3)

	inChan := make(chan string, 3)
	resChan := make(chan ActivationResult, 6)
	inChan <- "valid-id"
	inChan <- "invalid-id"
	inChan <- "transient-id"
	close(inChan)

	ctx := WithActivationType(context.Background(), "activate")
	rc.ActivateTicketsWithResults(ctx, inChan, resChan)
	close(resChan)

	results := make(map[string][]error)
	for res := range resChan {
		results[res.TicketId] = append(results[res.TicketId], res.Err)
	}

	if len(results["valid-id"]) != 1 || results["valid-id"][0] != nil {
		t.Fatalf("expected single nil result for valid-id, got %#v", results["valid-id"])
	}
	if len(results["invalid-id"]) != 1 || results["invalid-id"][0] == nil {
		t.Fatalf("expected single permanent error for invalid-id, got %#v", results["invalid-id"])
	}
	if len(results["transient-id"]) != 1 || results["transient-id"][0] != nil {
		t.Fatalf("expected single nil result after retry for transient-id, got %#v", results["transient-id"])
	}

	mu.Lock()
	defer mu.Unlock()
	if len(receivedBatches) != 2 {
		t.Fatalf("expected 2 HTTP attempts, got %d: %#v", len(receivedBatches), receivedBatches)
	}
	if !slices.Equal(receivedBatches[0], []string{"valid-id", "invalid-id", "transient-id"}) {
		t.Fatalf("unexpected first attempt batch: %#v", receivedBatches[0])
	}
	if !slices.Equal(receivedBatches[1], []string{"transient-id"}) {
		t.Fatalf("expected second attempt to retry only [transient-id], got %#v", receivedBatches[1])
	}
}

// 5. Batch Ticket ID Deduplication
func TestActivateTicketsWithResults_BatchTicketIDDeduplication(t *testing.T) {
	var mu sync.Mutex
	var receivedIDs []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &req)

		mu.Lock()
		receivedIDs = append(receivedIDs, req.GetTicketIds()...)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 10)

	inChan := make(chan string, 6)
	resChan := make(chan ActivationResult, 6)
	for _, id := range []string{"t-1", "t-2", "t-1", "t-3", "t-2"} {
		inChan <- id
	}
	close(inChan)

	ctx := WithActivationType(context.Background(), "activate")
	rc.ActivateTicketsWithResults(ctx, inChan, resChan)
	close(resChan)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"t-1", "t-2", "t-3"}
	if !slices.Equal(receivedIDs, want) {
		t.Fatalf("expected deduplicated ticket IDs %v, got %v", want, receivedIDs)
	}
}

// 6. Transient HTTP 503 Whole-Batch Retry
func TestActivateTicketsWithResults_Transient503WholeBatchRetry(t *testing.T) {
	var mu sync.Mutex
	var receivedBatches [][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &req)

		mu.Lock()
		attempt := len(receivedBatches) + 1
		receivedBatches = append(receivedBatches, slices.Clone(req.GetTicketIds()))
		mu.Unlock()

		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("service unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 2)

	inChan := make(chan string, 2)
	resChan := make(chan ActivationResult, 4)
	inChan <- "t-100"
	inChan <- "t-200"
	close(inChan)

	ctx := WithActivationType(context.Background(), "activate")
	rc.ActivateTicketsWithResults(ctx, inChan, resChan)
	close(resChan)

	mu.Lock()
	if len(receivedBatches) != 2 {
		t.Fatalf("expected 2 attempts on HTTP 503, got %d", len(receivedBatches))
	}
	wantBatch := []string{"t-100", "t-200"}
	if !slices.Equal(receivedBatches[0], wantBatch) || !slices.Equal(receivedBatches[1], wantBatch) {
		t.Fatalf("expected whole batch %v on both attempts, got %#v", wantBatch, receivedBatches)
	}
	mu.Unlock()

	var gotSuccess []string
	for res := range resChan {
		if res.Err != nil {
			t.Fatalf("unexpected error for %s: %v", res.TicketId, res.Err)
		}
		gotSuccess = append(gotSuccess, res.TicketId)
	}
	if !slices.Equal(gotSuccess, wantBatch) {
		t.Fatalf("expected results for %v, got %v", wantBatch, gotSuccess)
	}
}

// 7. Closed Input Channel Without Empty Ticket ID
func TestActivateTicketsWithResults_ClosedInputChannelWithoutEmptyTicketID(t *testing.T) {
	var mu sync.Mutex
	var rawBodies []string
	var receivedIDs []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req pb.ActivateTicketsRequest
		_ = protojson.Unmarshal(body, &req)

		mu.Lock()
		rawBodies = append(rawBodies, string(body))
		receivedIDs = append(receivedIDs, req.GetTicketIds()...)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)

	inChan := make(chan string, 2)
	resChan := make(chan ActivationResult, 4)
	inChan <- "ticket-a"
	inChan <- "ticket-b"
	close(inChan)

	ctx := WithActivationType(context.Background(), "activate")
	rc.ActivateTicketsWithResults(ctx, inChan, resChan)
	close(resChan)

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(receivedIDs, []string{"ticket-a", "ticket-b"}) {
		t.Fatalf("expected [ticket-a ticket-b] without trailing empty string, got %#v (raw bodies: %v)", receivedIDs, rawBodies)
	}
	for _, id := range receivedIDs {
		if id == "" {
			t.Fatal("received unexpected empty ticket ID string")
		}
	}
}

// 8. Concurrent Cloud Run OIDC Token Caching
func TestCreateTicket_ConcurrentCloudRunOIDCTokenCaching(t *testing.T) {
	var authHeaders sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders.Store(r.Header.Get("Authorization"), true)
		respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "1700000000000-42"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("K_REVISION", "test-rev-001")

	var mintCalls atomic.Int32
	var recordedAudience atomic.Value
	rc.tokenCache = newOIDCTokenCache(func(ctx context.Context, audience string) (oauth2.TokenSource, error) {
		mintCalls.Add(1)
		recordedAudience.Store(audience)
		return &staticTokenSource{
			token: &oauth2.Token{
				AccessToken: "cached-oidc-token",
				Expiry:      time.Now().Add(time.Hour),
			},
		}, nil
	})

	const concurrency = 50
	var wg sync.WaitGroup
	errChan := make(chan error, concurrency)
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			id, err := rc.CreateTicket(context.Background(), &pb.Ticket{})
			if err != nil {
				errChan <- err
				return
			}
			if id != "1700000000000-42" {
				errChan <- fmt.Errorf("unexpected ticket id %q", id)
			}
		}()
	}
	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Fatalf("concurrent CreateTicket failed: %v", err)
	}
	if got := mintCalls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 OIDC token source mint call across %d concurrent requests, got %d", concurrency, got)
	}
	if gotAud, _ := recordedAudience.Load().(string); gotAud != srv.URL {
		t.Fatalf("expected audience %q, got %q", srv.URL, gotAud)
	}
	if _, ok := authHeaders.Load("Bearer cached-oidc-token"); !ok {
		t.Fatal("expected Authorization: Bearer cached-oidc-token header on requests")
	}
}

// 9. Cloud Run OIDC Token Refresh on HTTP 401
func TestPost_CloudRunOIDCTokenRefreshOn401(t *testing.T) {
	var mu sync.Mutex
	var authHeaders []string
	var contentTypes []string
	var bodies []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		contentTypes = append(contentTypes, r.Header.Get("Content-Type"))
		bodies = append(bodies, string(b))
		attempt := len(authHeaders)
		mu.Unlock()

		if attempt == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("expired token"))
			return
		}
		respBytes, _ := protojson.Marshal(&pb.CreateTicketResponse{TicketId: "1700000000000-99"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("K_REVISION", "test-rev-401")

	var mintCalls atomic.Int32
	var audiences []string
	var audMu sync.Mutex
	rc.tokenCache = newOIDCTokenCache(func(ctx context.Context, audience string) (oauth2.TokenSource, error) {
		n := mintCalls.Add(1)
		audMu.Lock()
		audiences = append(audiences, audience)
		audMu.Unlock()
		tok := fmt.Sprintf("token-v%d", n)
		return &staticTokenSource{
			token: &oauth2.Token{
				AccessToken: tok,
				Expiry:      time.Now().Add(time.Hour),
			},
		}, nil
	})

	ticketID, err := rc.CreateTicket(context.Background(), &pb.Ticket{})
	if err != nil {
		t.Fatalf("expected CreateTicket to succeed after 401 token refresh, got %v", err)
	}
	if ticketID != "1700000000000-99" {
		t.Fatalf("expected ticket ID 1700000000000-99, got %q", ticketID)
	}

	if got := mintCalls.Load(); got != 2 {
		t.Fatalf("expected 2 token mints (initial + 1 refresh), got %d", got)
	}

	audMu.Lock()
	for i, aud := range audiences {
		if aud != srv.URL {
			t.Fatalf("attempt %d: expected audience %q (base OM_CORE_ADDR without path), got %q", i, srv.URL, aud)
		}
	}
	audMu.Unlock()

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(authHeaders, []string{"Bearer token-v1", "Bearer token-v2"}) {
		t.Fatalf("unexpected Authorization headers across retry: %v", authHeaders)
	}
	if !slices.Equal(contentTypes, []string{"application/json", "application/json"}) {
		t.Fatalf("expected Content-Type application/json on both attempts, got %v", contentTypes)
	}
	if len(bodies) != 2 || bodies[0] == "" || bodies[0] != bodies[1] {
		t.Fatalf("expected identical non-empty request bodies across 401 retry, got %#v", bodies)
	}
}

// 10. CreateTicket Context Cancellation
func TestCreateTicket_ContextCancellation(t *testing.T) {
	t.Run("pre-cancelled context", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := rc.CreateTicket(ctx, &pb.Ticket{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected error wrapping context.Canceled for pre-cancelled context, got %v", err)
		}
	})

	t.Run("in-flight context cancellation", func(t *testing.T) {
		reqStarted := make(chan struct{})
		unblockServer := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(reqStarted)
			select {
			case <-r.Context().Done():
			case <-unblockServer:
			}
		}))
		defer srv.Close()
		defer close(unblockServer)

		rc, _ := newTestClient(t, srv.URL)
		ctx, cancel := context.WithCancel(context.Background())

		errChan := make(chan error, 1)
		go func() {
			_, err := rc.CreateTicket(ctx, &pb.Ticket{})
			errChan <- err
		}()

		<-reqStarted
		cancel()

		select {
		case err := <-errChan:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected error wrapping context.Canceled on in-flight cancellation, got %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for CreateTicket to abort after context cancellation")
		}
	})
}

// 11. ValidateConnection Status Codes
func TestValidateConnection_StatusCodes(t *testing.T) {
	cases := []struct {
		statusCode int
		wantErr    bool
	}{
		{statusCode: http.StatusOK, wantErr: false},
		{statusCode: http.StatusBadRequest, wantErr: false},
		{statusCode: http.StatusUnauthorized, wantErr: true},
		{statusCode: http.StatusForbidden, wantErr: true},
		{statusCode: http.StatusNotFound, wantErr: true},
		{statusCode: http.StatusNotImplemented, wantErr: true},
		{statusCode: http.StatusServiceUnavailable, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("HTTP_%d", tc.statusCode), func(t *testing.T) {
			var gotPath string
			var gotBody []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotBody, _ = io.ReadAll(r.Body)
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer srv.Close()

			rc, _ := newTestClient(t, srv.URL)
			err := rc.ValidateConnection(context.Background())
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for HTTP %d, got nil", tc.statusCode)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected nil error for HTTP %d, got %v", tc.statusCode, err)
			}
			if gotPath != "/tickets:activate" {
				t.Fatalf("expected POST /tickets:activate, got %q", gotPath)
			}
			var req pb.ActivateTicketsRequest
			if err := protojson.Unmarshal(gotBody, &req); err != nil {
				t.Fatalf("failed unmarshaling ActivateTicketsRequest: %v", err)
			}
			if len(req.GetTicketIds()) != 0 {
				t.Fatalf("expected empty ActivateTicketsRequest, got TicketIds=%v", req.GetTicketIds())
			}
		})
	}

	t.Run("explicit customURL argument takes precedence over OM_CORE_ADDR", func(t *testing.T) {
		var customCalled atomic.Bool
		customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			customCalled.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		defer customSrv.Close()

		rc, _ := newTestClient(t, "http://127.0.0.1:1")
		if err := rc.ValidateConnection(context.Background(), customSrv.URL); err != nil {
			t.Fatalf("expected ValidateConnection with customURL to succeed, got %v", err)
		}
		if !customCalled.Load() {
			t.Fatal("expected request to be sent to customURL")
		}
	})
}

// 12. InvokeMatchmakingFunctions forwards every response and returns stream errors
func TestInvokeMatchmakingFunctions_MidStreamErrorAndEmptyEnvelope(t *testing.T) {
	t.Run("every response forwarded, including one with no match; stream error returned", func(t *testing.T) {
		match1JSON, _ := protojson.Marshal(&pb.StreamedMmfResponse{
			Match: &pb.Match{Id: "match-1"},
		})
		match2JSON, _ := protojson.Marshal(&pb.StreamedMmfResponse{
			Match: &pb.Match{Id: "match-2"},
		})

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, "{\"result\":%s}\n", string(match1JSON))
			_, _ = fmt.Fprintln(w, `{"result":{}}`)
			_, _ = fmt.Fprintf(w, "{\"result\":%s}\n", string(match2JSON))
			// om-core reports MMF failures as a final error message.
			_, _ = fmt.Fprintln(w, `{"error":{"code":13,"message":"1 of 2 matchmaking functions failed: mmf-b: boom"}}`)
		}))
		defer srv.Close()

		rc, logBuf := newTestClient(t, srv.URL)
		respChan := make(chan *pb.StreamedMmfResponse, 10)
		err := rc.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{}, respChan)

		var gotMatchIDs []string
		noMatch := 0
		for resp := range respChan {
			if resp == nil {
				t.Fatal("respChan must never receive a nil response")
			}
			if resp.GetMatch() == nil {
				noMatch++
				continue
			}
			gotMatchIDs = append(gotMatchIDs, resp.GetMatch().GetId())
		}

		if !slices.Equal(gotMatchIDs, []string{"match-1", "match-2"}) {
			t.Fatalf("expected [match-1 match-2], got %v", gotMatchIDs)
		}
		if noMatch != 1 {
			t.Fatalf("expected the response with no match to be forwarded once, got %d", noMatch)
		}
		if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "mmf-b: boom") {
			t.Fatalf("expected the stream's Internal error to be returned, got %v", err)
		}
		if !strings.Contains(logBuf.String(), "mmf-b: boom") {
			t.Fatalf("expected stream error message to be logged, got logs: %s", logBuf.String())
		}
	})

	t.Run("successful stream returns nil", func(t *testing.T) {
		matchJSON, _ := protojson.Marshal(&pb.StreamedMmfResponse{Match: &pb.Match{Id: "ok"}})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, "{\"result\":%s}\n", string(matchJSON))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		respChan := make(chan *pb.StreamedMmfResponse, 4)
		if err := rc.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{}, respChan); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if n := len(respChan); n != 1 {
			t.Fatalf("expected 1 response, got %d", n)
		}
	})

	t.Run("non-200 HTTP response returns om-core's status and closes respChan", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":14,"message":"initial ticket cache replication catch-up did not complete in time"}`))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		respChan := make(chan *pb.StreamedMmfResponse, 4)
		err := rc.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{}, respChan)

		var count int
		for range respChan {
			count++
		}
		if count != 0 {
			t.Fatalf("expected 0 streamed responses on non-200 HTTP status, got %d", count)
		}
		if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "catch-up") {
			t.Fatalf("expected om-core's Unavailable status to be returned, got %v", err)
		}
	})

	t.Run("non-200 HTTP response with a non-status body still returns an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal server error"))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		respChan := make(chan *pb.StreamedMmfResponse, 4)
		err := rc.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{}, respChan)
		for range respChan {
		}
		if err == nil || !strings.Contains(err.Error(), "internal server error") {
			t.Fatalf("expected an error carrying the response body, got %v", err)
		}
	})
}

type failingStreamBody struct {
	firstChunk []byte
	readFirst  bool
	err        error
}

func (f *failingStreamBody) Read(p []byte) (int, error) {
	if !f.readFirst {
		f.readFirst = true
		n := copy(p, f.firstChunk)
		return n, nil
	}
	return 0, f.err
}

func (f *failingStreamBody) Close() error {
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// 13. InvokeMatchmakingFunctions >64 KiB Payload and Scanner Error Suppression
func TestInvokeMatchmakingFunctions_LargePayloadAndScannerErrorSuppression(t *testing.T) {
	t.Run("delivers streamed match larger than 64 KiB (256 KiB payload)", func(t *testing.T) {
		largeValue, err := anypb.New(wrapperspb.String(strings.Repeat("x", 256*1024)))
		if err != nil {
			t.Fatalf("anypb.New failed: %v", err)
		}
		largeResp := &pb.StreamedMmfResponse{
			Match: &pb.Match{
				Id:         "large-256k-match",
				Extensions: map[string]*anypb.Any{"payload": largeValue},
			},
		}
		resultJSON, err := protojson.Marshal(largeResp)
		if err != nil {
			t.Fatalf("protojson.Marshal failed: %v", err)
		}
		envelopeBytes, err := json.Marshal(map[string]json.RawMessage{
			"result": resultJSON,
		})
		if err != nil {
			t.Fatalf("json.Marshal envelope failed: %v", err)
		}
		if len(envelopeBytes) <= 64*1024 {
			t.Fatalf("expected envelope larger than 64 KiB, got %d bytes", len(envelopeBytes))
		}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(append(envelopeBytes, '\n'))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		respChan := make(chan *pb.StreamedMmfResponse, 2)
		rc.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{}, respChan)

		var got []*pb.StreamedMmfResponse
		for resp := range respChan {
			got = append(got, resp)
		}
		if len(got) != 1 || got[0].GetMatch().GetId() != "large-256k-match" {
			t.Fatalf("expected 1 large match with ID large-256k-match, got %#v", got)
		}
	})

	t.Run("mid-stream read error with active context is logged", func(t *testing.T) {
		matchJSON, _ := protojson.Marshal(&pb.StreamedMmfResponse{
			Match: &pb.Match{Id: "match-before-error"},
		})
		firstLine := fmt.Sprintf("{\"result\":%s}\n", string(matchJSON))

		rc, logBuf := newTestClient(t, "http://om-core.local")
		rc.Client = &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body: &failingStreamBody{
						firstChunk: []byte(firstLine),
						err:        io.ErrUnexpectedEOF,
					},
				}, nil
			}),
		}

		respChan := make(chan *pb.StreamedMmfResponse, 2)
		rc.InvokeMatchmakingFunctions(context.Background(), &pb.MmfRequest{}, respChan)

		var got []*pb.StreamedMmfResponse
		for resp := range respChan {
			got = append(got, resp)
		}
		if len(got) != 1 || got[0].GetMatch().GetId() != "match-before-error" {
			t.Fatalf("expected first match before read error, got %#v", got)
		}
		if !strings.Contains(logBuf.String(), "Error reading stream, closing") {
			t.Fatalf("expected stream read error to be logged when context is active, got logs: %s", logBuf.String())
		}
	})

	t.Run("mid-stream context cancellation suppresses spurious scanner error log", func(t *testing.T) {
		matchJSON, _ := protojson.Marshal(&pb.StreamedMmfResponse{
			Match: &pb.Match{Id: "match-before-cancel"},
		})
		firstLine := fmt.Sprintf("{\"result\":%s}\n", string(matchJSON))

		unblockServer := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(firstLine))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
			case <-unblockServer:
			}
		}))
		defer srv.Close()
		defer close(unblockServer)

		rc, logBuf := newTestClient(t, srv.URL)
		errCycleTimeout := errors.New("test MMF cycle timeout")
		ctx, cancel := context.WithCancelCause(context.Background())
		respChan := make(chan *pb.StreamedMmfResponse)

		done := make(chan struct{})
		var invokeErr error
		go func() {
			invokeErr = rc.InvokeMatchmakingFunctions(ctx, &pb.MmfRequest{}, respChan)
			close(done)
		}()

		select {
		case resp := <-respChan:
			if resp == nil || resp.GetMatch().GetId() != "match-before-cancel" {
				t.Fatalf("unexpected match: %#v", resp)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for first match")
		}

		cancel(errCycleTimeout)
		for range respChan {
		}
		<-done

		if !errors.Is(invokeErr, errCycleTimeout) {
			t.Fatalf("expected returned error to wrap the context cause, got %v", invokeErr)
		}
		if strings.Contains(logBuf.String(), "Error reading stream, closing") {
			t.Fatalf("expected scanner error log to be suppressed on context cancellation, got logs: %s", logBuf.String())
		}
	})
}

// countingTokenFactory returns a token factory that mints "<prefix>-<n>"
// tokens and counts how many times it was called.
func countingTokenFactory(prefix string, calls *atomic.Int32) tokenSourceFactory {
	return func(ctx context.Context, audience string) (oauth2.TokenSource, error) {
		n := calls.Add(1)
		return &staticTokenSource{
			token: &oauth2.Token{
				AccessToken: fmt.Sprintf("%s-%d", prefix, n),
				Expiry:      time.Now().Add(time.Hour),
			},
		}, nil
	}
}

// 14. Cloud Run OIDC: HTTP 403 is not retried with a new token, because a
// missing IAM invoker role isn't fixed by refreshing the token.
func TestPost_CloudRunOIDC403NotRetried(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("forbidden"))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("K_REVISION", "test-rev-403")
	var mintCalls atomic.Int32
	rc.tokenCache = newOIDCTokenCache(countingTokenFactory("token-403", &mintCalls))

	resp, err := rc.Post(context.Background(), srv.URL, "/tickets:activate", []byte(`{}`))
	if err != nil {
		t.Fatalf("Post returned unexpected transport error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected HTTP 403 to be returned to the caller, got %d", resp.StatusCode)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected 1 HTTP attempt (no retry on 403), got %d", got)
	}
	if got := mintCalls.Load(); got != 1 {
		t.Fatalf("expected 1 token mint (no refresh on 403), got %d", got)
	}
}

// Cloud Run OIDC: the audience and URL ignore a trailing "/" on OM_CORE_ADDR.
func TestPost_TrailingSlashNormalised(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL+"/")
	rc.Cfg.Set("K_REVISION", "test-rev-slash")
	var audience atomic.Value
	rc.tokenCache = newOIDCTokenCache(func(ctx context.Context, aud string) (oauth2.TokenSource, error) {
		audience.Store(aud)
		return &staticTokenSource{token: &oauth2.Token{AccessToken: "tok", Expiry: time.Now().Add(time.Hour)}}, nil
	})

	resp, err := rc.Post(context.Background(), rc.Cfg.GetString("OM_CORE_ADDR"), "/tickets", []byte(`{}`))
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	resp.Body.Close()
	if gotPath != "/tickets" {
		t.Fatalf("expected path /tickets, got %q", gotPath)
	}
	if got, _ := audience.Load().(string); got != srv.URL {
		t.Fatalf("expected audience %q, got %q", srv.URL, got)
	}
}

// Cloud Run OIDC: a hung token mint fails the request when its context ends
// instead of blocking it forever.
func TestPost_TokenMintBoundedByContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("K_REVISION", "test-rev-hang")
	unblock := make(chan struct{})
	defer close(unblock)
	rc.tokenCache = newOIDCTokenCache(func(ctx context.Context, aud string) (oauth2.TokenSource, error) {
		<-unblock
		return nil, errors.New("unblocked")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := rc.Post(ctx, srv.URL, "/tickets", []byte(`{}`))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded from hung token mint, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Post took %v; expected it to give up when the context expired", elapsed)
	}
}

// The Authorization header is never written to the logs, even at trace level.
func TestPost_AuthorizationHeaderRedacted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rc, logBuf := newTestClient(t, srv.URL)
	rc.Log.SetLevel(logrus.TraceLevel)
	rc.Cfg.Set("K_REVISION", "test-rev-redact")
	rc.tokenCache = newOIDCTokenCache(func(ctx context.Context, aud string) (oauth2.TokenSource, error) {
		return &staticTokenSource{token: &oauth2.Token{AccessToken: "super-secret-token", Expiry: time.Now().Add(time.Hour)}}, nil
	})

	resp, err := rc.Post(context.Background(), srv.URL, "/tickets", []byte(`{}`))
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	resp.Body.Close()

	logs := logBuf.String()
	if strings.Contains(logs, "super-secret-token") {
		t.Fatalf("token leaked into logs: %s", logs)
	}
	if !strings.Contains(logs, "Prepared Headers") || !strings.Contains(logs, "REDACTED") {
		t.Fatalf("expected trace header log with redacted Authorization, got: %s", logs)
	}
}

// CreateTicket marks errors retrying can't fix as backoff.Permanent, and
// leaves transient ones retryable.
func TestCreateTicket_RetryClassification(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		body          []byte
		wantPermanent bool
	}{
		{"400 InvalidArgument", http.StatusBadRequest, marshalStatusWithViolations(t, codes.InvalidArgument, "bad ticket", nil), true},
		{"404 Not Found", http.StatusNotFound, []byte("404 page not found"), true},
		{"401 Unauthenticated", http.StatusUnauthorized, nil, true},
		{"429 ResourceExhausted", http.StatusTooManyRequests, marshalStatusWithViolations(t, codes.ResourceExhausted, "slow down", nil), false},
		{"500 Internal", http.StatusInternalServerError, marshalStatusWithViolations(t, codes.Internal, "oops", nil), false},
		{"503 Unavailable", http.StatusServiceUnavailable, marshalStatusWithViolations(t, codes.Unavailable, "catching up", nil), false},
		{"409 Aborted", http.StatusConflict, marshalStatusWithViolations(t, codes.Aborted, "aborted", nil), false},
		{"501 Unimplemented", http.StatusNotImplemented, marshalStatusWithViolations(t, codes.Unimplemented, "nope", nil), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write(tc.body)
			}))
			defer srv.Close()

			rc, _ := newTestClient(t, srv.URL)
			_, err := rc.CreateTicket(context.Background(), &pb.Ticket{})
			if err == nil {
				t.Fatal("expected an error")
			}
			var perm *backoff.PermanentError
			if got := errors.As(err, &perm); got != tc.wantPermanent {
				t.Fatalf("permanent = %v, want %v (err: %v)", got, tc.wantPermanent, err)
			}
		})
	}

	t.Run("transport error is retryable", func(t *testing.T) {
		rc, _ := newTestClient(t, "http://127.0.0.1:1")
		_, err := rc.CreateTicket(context.Background(), &pb.Ticket{})
		var perm *backoff.PermanentError
		if err == nil || errors.As(err, &perm) {
			t.Fatalf("expected retryable transport error, got %v", err)
		}
	})

	t.Run("per-attempt timeout from OM_CORE_REQUEST_TIMEOUT_MS is retryable", func(t *testing.T) {
		unblock := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-unblock:
			}
		}))
		defer srv.Close()
		defer close(unblock)

		rc, _ := newTestClient(t, srv.URL)
		rc.Cfg.Set("OM_CORE_REQUEST_TIMEOUT_MS", 50)
		_, err := rc.CreateTicket(context.Background(), &pb.Ticket{})
		var perm *backoff.PermanentError
		if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &perm) {
			t.Fatalf("expected retryable context.DeadlineExceeded, got %v", err)
		}
	})
}

// Each /tickets:activate attempt is bounded by OM_CORE_REQUEST_TIMEOUT_MS, so
// a hung om-core can't hold an activation slot forever.
func TestActivateTicketsWithResults_PerAttemptTimeout(t *testing.T) {
	var attempts atomic.Int32
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
	}))
	defer srv.Close()
	defer close(unblock)

	rc, _ := newTestClient(t, srv.URL)
	// 500ms per attempt gives a 1.5s retry budget: long enough that the
	// first timed-out attempt is always retried (the first backoff interval
	// is at most 750ms).
	rc.Cfg.Set("OM_CORE_REQUEST_TIMEOUT_MS", 500)

	inChan := make(chan string, 1)
	resChan := make(chan ActivationResult, 1)
	inChan <- "ticket-hang"
	close(inChan)

	done := make(chan struct{})
	go func() {
		rc.ActivateTicketsWithResults(context.Background(), inChan, resChan)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ActivateTicketsWithResults did not give up on a hung om-core")
	}

	res := <-resChan
	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded result, got %v", res.Err)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected a timed-out attempt to be retried, got %d attempt(s)", got)
	}
}

// Client errors with no per-ticket details (404 wrong address, 413, ...) are
// not retried, and callers get the plain error rather than a
// *backoff.PermanentError.
func TestActivateTicketsWithResults_ClientErrorsNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusRequestEntityTooLarge, http.StatusBadRequest} {
		t.Run(fmt.Sprintf("HTTP_%d", status), func(t *testing.T) {
			var attempts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.WriteHeader(status)
			}))
			defer srv.Close()

			rc, _ := newTestClient(t, srv.URL)
			inChan := make(chan string, 1)
			resChan := make(chan ActivationResult, 1)
			inChan <- "ticket-1"
			close(inChan)
			rc.ActivateTicketsWithResults(context.Background(), inChan, resChan)

			res := <-resChan
			if res.Err == nil {
				t.Fatal("expected an activation error")
			}
			var perm *backoff.PermanentError
			if errors.As(res.Err, &perm) {
				t.Fatalf("result error should not be a *backoff.PermanentError: %#v", res.Err)
			}
			if got := attempts.Load(); got != 1 {
				t.Fatalf("expected 1 attempt, got %d", got)
			}
		})
	}
}

// Concurrent /tickets:activate calls are capped by OM_CORE_MAX_CONCURRENT_ACTIVATION_CALLS.
func TestActivateTicketsWithResults_ConcurrentCallLimit(t *testing.T) {
	const maxConcurrent = 2
	const totalTickets = 6

	var inFlight, maxObserved atomic.Int32
	limitReached := make(chan struct{})
	var limitOnce sync.Once
	release := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			prev := maxObserved.Load()
			if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
				break
			}
		}
		if cur == maxConcurrent {
			limitOnce.Do(func() { close(limitReached) })
		}
		<-release
		inFlight.Add(-1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	rc.Cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 1) // one ticket per call
	rc.Cfg.Set("OM_CORE_MAX_CONCURRENT_ACTIVATION_CALLS", maxConcurrent)

	inChan := make(chan string, totalTickets)
	resChan := make(chan ActivationResult, totalTickets)
	for i := range totalTickets {
		inChan <- fmt.Sprintf("ticket-%d", i)
	}
	close(inChan)

	ctx := WithActivationType(context.Background(), "activate")
	done := make(chan struct{})
	go func() {
		rc.ActivateTicketsWithResults(ctx, inChan, resChan)
		close(done)
	}()

	select {
	case <-limitReached:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %d concurrent activation calls", maxConcurrent)
	}
	// Give any (incorrectly) uncapped calls a chance to arrive.
	time.Sleep(100 * time.Millisecond)
	if got := inFlight.Load(); got != maxConcurrent {
		t.Fatalf("in-flight activation calls = %d while blocked, want %d", got, maxConcurrent)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ActivateTicketsWithResults did not finish after releasing calls")
	}

	if got := maxObserved.Load(); got != maxConcurrent {
		t.Fatalf("max concurrent activation calls = %d, want %d", got, maxConcurrent)
	}
	close(resChan)
	var succeeded int
	for res := range resChan {
		if res.Err != nil {
			t.Fatalf("unexpected activation error for %s: %v", res.TicketId, res.Err)
		}
		succeeded++
	}
	if succeeded != totalTickets {
		t.Fatalf("got %d successful activation results, want %d", succeeded, totalTickets)
	}
}
