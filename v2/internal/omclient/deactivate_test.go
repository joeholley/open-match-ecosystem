// Copyright 2025 Google LLC
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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

// DeactivateTickets sends DELETE /tickets:deactivate (om-core's grpc-gateway
// mapping) with a deduplicated DeactivateTicketsRequest body.
func TestDeactivateTickets_RequestShape(t *testing.T) {
	var gotMethod, gotPath string
	var gotIDs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		body, _ := io.ReadAll(r.Body)
		var req pb.DeactivateTicketsRequest
		if err := protojson.Unmarshal(body, &req); err != nil {
			t.Errorf("cannot unmarshal request body %q: %v", body, err)
		}
		gotIDs = req.GetTicketIds()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	if err := rc.DeactivateTickets(context.Background(), []string{"a", "b", "a", ""}); err != nil {
		t.Fatalf("DeactivateTickets returned %v, want nil", err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/tickets:deactivate" {
		t.Fatalf("request = %s %s, want DELETE /tickets:deactivate", gotMethod, gotPath)
	}
	if !slices.Equal(gotIDs, []string{"a", "b"}) {
		t.Fatalf("ticket ids = %v, want [a b]", gotIDs)
	}
}

// No ticket IDs means no request.
func TestDeactivateTickets_EmptyIsNoop(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rc, _ := newTestClient(t, srv.URL)
	if err := rc.DeactivateTickets(context.Background(), nil); err != nil {
		t.Fatalf("DeactivateTickets(nil) returned %v, want nil", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("DeactivateTickets(nil) made %d requests, want 0", got)
	}
}

// Transient failures (503) are retried; InvalidArgument (400) is not.
func TestDeactivateTickets_RetryClassification(t *testing.T) {
	t.Run("503 then 200 is retried", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"code":14,"message":"catching up"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		if err := rc.DeactivateTickets(context.Background(), []string{"t1"}); err != nil {
			t.Fatalf("DeactivateTickets returned %v, want nil after retry", err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("requests = %d, want 2", got)
		}
	})

	t.Run("400 is permanent", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(marshalStatusWithViolations(t, codes.InvalidArgument, "bad ids", map[string]string{"t1": "invalid id"}))
		}))
		defer srv.Close()

		rc, _ := newTestClient(t, srv.URL)
		if err := rc.DeactivateTickets(context.Background(), []string{"t1"}); err == nil {
			t.Fatal("DeactivateTickets returned nil, want error for InvalidArgument")
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("requests = %d, want 1 (no retry)", got)
		}
	})
}
