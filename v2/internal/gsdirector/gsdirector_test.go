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

package gsdirector

import (
	"context"
	"io"
	"math"
	"net/http"
	"testing"

	"github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/types/known/anypb"
	knownpb "google.golang.org/protobuf/types/known/wrapperspb"
	ex "open-match.dev/open-match-ecosystem/v2/internal/extensions"
	"open-match.dev/open-match-ecosystem/v2/internal/omclient"
)

func testMeter() *metric.Meter {
	m := noop.NewMeterProvider().Meter("gsdirector-test")
	return &m
}

type stubGSManager struct {
	requests []*pb.MmfRequest
	calls    int
}

func (s *stubGSManager) Init(map[string]map[string]string, map[string]*pb.Pool, map[string][]*GameMode) error {
	return nil
}

func (s *stubGSManager) Allocate(string, *pb.Match) string {
	return "127.0.0.1:7000"
}

func (s *stubGSManager) GetMMFParams() []*pb.MmfRequest {
	s.calls++
	return s.requests
}

func (s *stubGSManager) UpdateMMFRequest(spec string, _ *pb.MmfRequest) string {
	return spec
}

func TestMockDirectorRunMissingMMFRequestHashAndEmptyCyclesExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := viper.New()
	cfg.Set("NUM_MM_CYCLES", 10)
	cfg.Set("NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT", 2)
	cfg.Set("MM_CYCLE_MIN_DURATION_MS", 1)
	cfg.Set("ASSIGNMENT_TTL_MS", 60000)
	cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)

	log := logrus.New()
	log.SetOutput(io.Discard)

	gsMgr := &stubGSManager{
		requests: []*pb.MmfRequest{
			// Request missing MMFRequestHashKey in Profile.Extensions (and one with nil Profile)
			// must be discarded without deadlocking wg.Wait().
			{Profile: &pb.Profile{Name: "dev.open-match.missing-hash.1"}},
			{Profile: nil},
		},
	}

	d := &MockDirector{
		OmClient:     omclient.New(cfg, log, &http.Client{}),
		GSManager:    gsMgr,
		Cfg:          cfg,
		Log:          log,
		OtelMeterPtr: testMeter(),
	}

	d.Run(ctx)

	if gsMgr.calls != 2 {
		t.Errorf("GetMMFParams() called %d times, want 2 (NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT=2)", gsMgr.calls)
	}
}

func TestHasDeactivationTimeout(t *testing.T) {
	boolTrueAny, err := anypb.New(&knownpb.BoolValue{Value: true})
	if err != nil {
		t.Fatalf("anypb.New(BoolValue(true)): %v", err)
	}
	boolFalseAny, err := anypb.New(&knownpb.BoolValue{Value: false})
	if err != nil {
		t.Fatalf("anypb.New(BoolValue(false)): %v", err)
	}
	stringAny, err := anypb.New(&knownpb.StringValue{Value: "bad"})
	if err != nil {
		t.Fatalf("anypb.New(StringValue): %v", err)
	}

	tests := []struct {
		name      string
		match     *pb.Match
		want      bool
		wantError bool
	}{
		{
			name:      "nil match",
			match:     nil,
			want:      false,
			wantError: false,
		},
		{
			name:      "nil extensions",
			match:     &pb.Match{Id: "m-nil-ext"},
			want:      false,
			wantError: false,
		},
		{
			name: "absent key",
			match: &pb.Match{
				Id:         "m-no-key",
				Extensions: map[string]*anypb.Any{"other": stringAny},
			},
			want:      false,
			wantError: false,
		},
		{
			name: "BoolValue(true)",
			match: &pb.Match{
				Id: "m-true",
				Extensions: map[string]*anypb.Any{
					ex.MatchDeactivationTimeoutKey: boolTrueAny,
				},
			},
			want:      true,
			wantError: false,
		},
		{
			name: "BoolValue(false)",
			match: &pb.Match{
				Id: "m-false",
				Extensions: map[string]*anypb.Any{
					ex.MatchDeactivationTimeoutKey: boolFalseAny,
				},
			},
			want:      false,
			wantError: false,
		},
		{
			name: "wrong type returns error",
			match: &pb.Match{
				Id: "m-wrong-type",
				Extensions: map[string]*anypb.Any{
					ex.MatchDeactivationTimeoutKey: stringAny,
				},
			},
			want:      false,
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hasDeactivationTimeout(tc.match)
			if (err != nil) != tc.wantError {
				t.Errorf("hasDeactivationTimeout() error = %v, wantError %v", err, tc.wantError)
			}
			if got != tc.want {
				t.Errorf("hasDeactivationTimeout() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCountMatchTickets(t *testing.T) {
	tests := []struct {
		name  string
		match *pb.Match
		want  int64
	}{
		{
			name:  "nil match",
			match: nil,
			want:  0,
		},
		{
			name:  "empty rosters",
			match: &pb.Match{Id: "m-empty"},
			want:  0,
		},
		{
			name: "multiple rosters with tickets",
			match: &pb.Match{
				Id: "m-rosters",
				Rosters: map[string]*pb.Roster{
					"r1": {Tickets: []*pb.Ticket{{Id: "t1"}, {Id: "t2"}}},
					"r2": {Tickets: nil},
					"r3": {Tickets: []*pb.Ticket{{Id: "t3"}}},
				},
			},
			want: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := countMatchTickets(tc.match); got != tc.want {
				t.Errorf("countMatchTickets() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCalculateCycleBackoffFactor(t *testing.T) {
	tests := []struct {
		name                 string
		numProposed          int64
		numAssigned          int64
		numDeactivationTimed int64
		want                 float64
	}{
		{
			name:                 "zero proposed returns 1.0 (not NaN)",
			numProposed:          0,
			numAssigned:          0,
			numDeactivationTimed: 0,
			want:                 1.0,
		},
		{
			name:                 "positive proposed with zero lagging returns 1.0",
			numProposed:          10,
			numAssigned:          0,
			numDeactivationTimed: 0,
			want:                 1.0,
		},
		{
			name:                 "assigned collisions only",
			numProposed:          10,
			numAssigned:          3,
			numDeactivationTimed: 0,
			want:                 1.3,
		},
		{
			name:                 "deactivation timeouts only",
			numProposed:          10,
			numAssigned:          0,
			numDeactivationTimed: 4,
			want:                 1.4,
		},
		{
			name:                 "both present uses max(assigned, deactivationTimeout)",
			numProposed:          10,
			numAssigned:          2,
			numDeactivationTimed: 5,
			want:                 1.5,
		},
		{
			name:                 "both present when assigned > deactivationTimeout",
			numProposed:          10,
			numAssigned:          7,
			numDeactivationTimed: 3,
			want:                 1.7,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := calculateCycleBackoffFactor(tc.numProposed, tc.numAssigned, tc.numDeactivationTimed)
			if math.IsNaN(got) {
				t.Fatalf("calculateCycleBackoffFactor() returned NaN")
			}
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("calculateCycleBackoffFactor(%d, %d, %d) = %v, want %v",
					tc.numProposed, tc.numAssigned, tc.numDeactivationTimed, got, tc.want)
			}
		})
	}
}

func TestGameModeSetBackfillMMFs(t *testing.T) {
	mmf := &pb.MatchmakingFunctionSpec{
		Name: "FIFO",
		Type: pb.MatchmakingFunctionSpec_GRPC,
	}
	gm := &GameMode{
		Name: "SoloDuel",
	}

	// Simulate applying runtime configuration after package initialization.
	mmf.Host = "https://soloduel-xyz.a.run.app"
	mmf.Port = 443
	if err := gm.SetBackfillMMFs(mmf); err != nil {
		t.Fatalf("SetBackfillMMFs failed: %v", err)
	}

	rawAny, ok := gm.ExtensionParams[ex.MMFRequestKey]
	if !ok || rawAny == nil {
		t.Fatalf("expected ExtensionParams[%q] to be populated", ex.MMFRequestKey)
	}

	var backfillReq pb.MmfRequest
	if err := rawAny.UnmarshalTo(&backfillReq); err != nil {
		t.Fatalf("UnmarshalTo(pb.MmfRequest) failed: %v", err)
	}
	if len(backfillReq.GetMmfs()) != 1 {
		t.Fatalf("expected 1 backfill MMF, got %d", len(backfillReq.GetMmfs()))
	}
	got := backfillReq.GetMmfs()[0]
	if got.GetHost() != "https://soloduel-xyz.a.run.app" || got.GetPort() != 443 {
		t.Errorf("backfill MMF host:port = %s:%d, want https://soloduel-xyz.a.run.app:443", got.GetHost(), got.GetPort())
	}
}
