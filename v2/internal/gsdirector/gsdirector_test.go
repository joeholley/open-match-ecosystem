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
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// TestMockAgonesIntegration_MultiFleetExtensionsAndPoolIsolation verifies that
// initializing multiple regional fleets that share a single GameMode and template
// Pool isolates each fleet's Profile.Extensions map, Pool.Extensions map, and
// Pool filter slices, so every fleet receives its own unique AgonesAllocatorKey
// and MMFRequestHashKey and can allocate servers independently.
func TestMockAgonesIntegration_MultiFleetExtensionsAndPoolIsolation(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	// Pre-allocate filter slices with extra capacity (len=1, cap=4) on the shared
	// mode pool template so that append() without cloning would overwrite the
	// backing array across zones.
	tagFilters := make([]*pb.Pool_TagPresentFilter, 1, 4)
	tagFilters[0] = &pb.Pool_TagPresentFilter{Tag: "mode:duel"}
	doubleFilters := make([]*pb.Pool_DoubleRangeFilter, 1, 4)
	doubleFilters[0] = &pb.Pool_DoubleRangeFilter{DoubleArg: "mmr", Minimum: 0, Maximum: 3000}
	stringFilters := make([]*pb.Pool_StringEqualsFilter, 1, 4)
	stringFilters[0] = &pb.Pool_StringEqualsFilter{StringArg: "playlist", Value: "ranked"}

	templatePoolExtVal, err := anypb.New(&knownpb.StringValue{Value: "template-pool-ext"})
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	sharedTemplatePool := &pb.Pool{
		Name:                "shared-template-pool",
		TagPresentFilters:   tagFilters,
		DoubleRangeFilters:  doubleFilters,
		StringEqualsFilters: stringFilters,
		Extensions: map[string]*anypb.Any{
			"template_key": templatePoolExtVal,
		},
	}

	sharedMode := &GameMode{
		Name:  "SharedDuelMode",
		MMFs:  []*pb.MatchmakingFunctionSpec{Fifo},
		Pools: map[string]*pb.Pool{"all": sharedTemplatePool},
		ExtensionParams: ex.AnypbIntMap(map[string]int32{
			"desiredNumRosters": 1,
			"desiredRosterLen":  2,
		}),
	}

	fleetCfg := map[string]map[string]string{
		"APAC": {"JP_Tokyo": "asia-northeast1-a"},
		"EMEA": {"DE_Frankfurt": "europe-west3-a"},
		"NA":   {"US_Ashburn": "us-east4-b"},
	}

	zoneExtTokyo, _ := anypb.New(&knownpb.StringValue{Value: "tokyo"})
	zoneExtFrankfurt, _ := anypb.New(&knownpb.StringValue{Value: "frankfurt"})
	zoneExtAshburn, _ := anypb.New(&knownpb.StringValue{Value: "ashburn"})

	zonePools := map[string]*pb.Pool{
		"asia-northeast1-a": {
			TagPresentFilters:   []*pb.Pool_TagPresentFilter{{Tag: "zone:tokyo"}},
			DoubleRangeFilters:  []*pb.Pool_DoubleRangeFilter{{DoubleArg: "ping.tokyo", Minimum: 1, Maximum: 100}},
			StringEqualsFilters: []*pb.Pool_StringEqualsFilter{{StringArg: "dc", Value: "tokyo"}},
			Extensions:          map[string]*anypb.Any{"zone_ext": zoneExtTokyo},
		},
		"europe-west3-a": {
			TagPresentFilters:   []*pb.Pool_TagPresentFilter{{Tag: "zone:frankfurt"}},
			DoubleRangeFilters:  []*pb.Pool_DoubleRangeFilter{{DoubleArg: "ping.frankfurt", Minimum: 1, Maximum: 100}},
			StringEqualsFilters: []*pb.Pool_StringEqualsFilter{{StringArg: "dc", Value: "frankfurt"}},
			Extensions:          map[string]*anypb.Any{"zone_ext": zoneExtFrankfurt},
		},
		"us-east4-b": {
			TagPresentFilters:   []*pb.Pool_TagPresentFilter{{Tag: "zone:ashburn"}},
			DoubleRangeFilters:  []*pb.Pool_DoubleRangeFilter{{DoubleArg: "ping.ashburn", Minimum: 1, Maximum: 100}},
			StringEqualsFilters: []*pb.Pool_StringEqualsFilter{{StringArg: "dc", Value: "ashburn"}},
			Extensions:          map[string]*anypb.Any{"zone_ext": zoneExtAshburn},
		},
	}

	modesInZone := map[string][]*GameMode{
		"asia-northeast1-a": {sharedMode},
		"europe-west3-a":    {sharedMode},
		"us-east4-b":        {sharedMode},
	}

	agones := &MockAgonesIntegration{Log: log}
	if err := agones.Init(fleetCfg, zonePools, modesInZone); err != nil {
		t.Fatalf("MockAgonesIntegration.Init failed: %v", err)
	}
	t.Cleanup(func() {
		close(agones.gameServerUpdateChan)
	})

	// Verify sharedMode.ExtensionParams was not polluted with per-fleet Agones keys.
	for _, forbiddenKey := range []string{ex.AgonesGSReadyCountKey, ex.AgonesAllocatorKey, ex.MMFRequestHashKey} {
		if _, polluted := sharedMode.ExtensionParams[forbiddenKey]; polluted {
			t.Errorf("sharedMode.ExtensionParams was mutated with per-fleet key %q", forbiddenKey)
		}
	}

	// Verify sharedTemplatePool.Extensions and filter slices were not mutated.
	if len(sharedTemplatePool.Extensions) != 1 {
		t.Errorf("sharedTemplatePool.Extensions len = %d, want 1 (was mutated by appendPoolParams)", len(sharedTemplatePool.Extensions))
	}

	requests := agones.GetMMFParams()
	if len(requests) != 3 {
		t.Fatalf("GetMMFParams() returned %d requests, want 3 (one per fleet)", len(requests))
	}

	seenAllocSpecs := make(map[string]string)
	seenHashes := make(map[string]string)

	for _, req := range requests {
		profName := req.GetProfile().GetName()
		ext := req.GetProfile().GetExtensions()

		allocSpec, err := ex.String(ext, ex.AgonesAllocatorKey)
		if err != nil || allocSpec == "" {
			t.Fatalf("profile %q missing %s: %v", profName, ex.AgonesAllocatorKey, err)
		}
		reqHash, err := ex.String(ext, ex.MMFRequestHashKey)
		if err != nil || reqHash == "" {
			t.Fatalf("profile %q missing %s: %v", profName, ex.MMFRequestHashKey, err)
		}

		if prevProf, dup := seenAllocSpecs[allocSpec]; dup {
			t.Errorf("profile %q shares duplicate AgonesAllocatorKey with %q (map aliasing across fleets)", profName, prevProf)
		}
		seenAllocSpecs[allocSpec] = profName

		if prevProf, dup := seenHashes[reqHash]; dup {
			t.Errorf("profile %q shares duplicate MMFRequestHashKey with %q (map aliasing across fleets)", profName, prevProf)
		}
		seenHashes[reqHash] = profName

		// Verify each composed pool has distinct zone filters rather than overwritten slice elements.
		for poolName, pool := range req.GetProfile().GetPools() {
			if len(pool.GetTagPresentFilters()) != 2 {
				t.Errorf("pool %q TagPresentFilters len = %d, want 2", poolName, len(pool.GetTagPresentFilters()))
			}
			if len(pool.GetDoubleRangeFilters()) != 2 {
				t.Errorf("pool %q DoubleRangeFilters len = %d, want 2", poolName, len(pool.GetDoubleRangeFilters()))
			}
			if len(pool.GetStringEqualsFilters()) != 2 {
				t.Errorf("pool %q StringEqualsFilters len = %d, want 2", poolName, len(pool.GetStringEqualsFilters()))
			}
			zoneVal, err := ex.String(pool.GetExtensions(), "zone_ext")
			if err != nil || zoneVal == "" {
				t.Errorf("pool %q missing zone_ext extension: %v", poolName, err)
			}
		}

		// Verify Allocate succeeds for every fleet's AgonesAllocatorKey.
		conn := agones.Allocate(allocSpec, &pb.Match{Id: "match-for-" + profName})
		if conn == "" {
			t.Errorf("Allocate(%s) failed for fleet profile %q", allocSpec, profName)
		}
	}

	// Verify that mutating the Extensions map returned by GetMMFParams() does not
	// mutate the stored profile extensions on subsequent GetMMFParams() calls.
	requests[0].GetProfile().GetExtensions()["caller_mutated_key"] = templatePoolExtVal
	secondRequests := agones.GetMMFParams()
	for _, req := range secondRequests {
		if _, leaked := req.GetProfile().GetExtensions()["caller_mutated_key"]; leaked {
			t.Errorf("mutation of returned MmfRequest.Profile.Extensions leaked into stored profile %q", req.GetProfile().GetName())
		}
	}
}

// TestMockAgonesIntegration_RefreshesEveryTicketCreationTimeRangeFilter verifies
// that MockAgonesIntegration.Init refreshes the package-level EveryTicket
// CreationTimeRangeFilter relative to the current Init() timestamp rather than
// retaining a stale timestamp from package initialization.
func TestMockAgonesIntegration_RefreshesEveryTicketCreationTimeRangeFilter(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	// Set EveryTicket's CreationTimeRangeFilter to a stale sentinel 10 years in the past.
	staleBase := time.Now().Add(-10 * 365 * 24 * time.Hour)
	EveryTicket.CreationTimeRangeFilter = &pb.Pool_CreationTimeRangeFilter{
		Start: timestamppb.New(staleBase.Add(-1 * time.Hour)),
		End:   timestamppb.New(staleBase.Add(100 * time.Hour)),
	}

	beforeInit := time.Now()
	soloDuelZones := map[string][]*GameMode{
		"asia-northeast1-a": {SoloDuel},
		"europe-west3-a":    {SoloDuel},
		"us-east4-b":        {SoloDuel},
	}
	agones := &MockAgonesIntegration{Log: log}
	if err := agones.Init(FleetConfig, ZonePools, soloDuelZones); err != nil {
		t.Fatalf("MockAgonesIntegration.Init failed: %v", err)
	}
	t.Cleanup(func() {
		close(agones.gameServerUpdateChan)
	})
	afterInit := time.Now()

	gotFilter := EveryTicket.GetCreationTimeRangeFilter()
	if gotFilter == nil || gotFilter.GetStart() == nil || gotFilter.GetEnd() == nil {
		t.Fatalf("EveryTicket.CreationTimeRangeFilter is nil after Init")
	}

	gotStart := gotFilter.GetStart().AsTime()
	gotEnd := gotFilter.GetEnd().AsTime()

	minExpectedStart := beforeInit.Add(-1*time.Hour - 5*time.Second)
	maxExpectedStart := afterInit.Add(-1*time.Hour + 5*time.Second)
	if gotStart.Before(minExpectedStart) || gotStart.After(maxExpectedStart) {
		t.Errorf("EveryTicket.CreationTimeRangeFilter.Start = %v, want within [%v, %v]", gotStart, minExpectedStart, maxExpectedStart)
	}

	minExpectedEnd := beforeInit.Add(100*time.Hour - 5*time.Second)
	maxExpectedEnd := afterInit.Add(100*time.Hour + 5*time.Second)
	if gotEnd.Before(minExpectedEnd) || gotEnd.After(maxExpectedEnd) {
		t.Errorf("EveryTicket.CreationTimeRangeFilter.End = %v, want within [%v, %v]", gotEnd, minExpectedEnd, maxExpectedEnd)
	}

	// Verify composed pools produced by GetMMFParams() across all 3 SoloDuel fleets
	// carry the refreshed filter without mutating SoloDuel.ExtensionParams.
	requests := agones.GetMMFParams()
	if len(requests) != 3 {
		t.Fatalf("expected 3 MmfRequests from GetMMFParams(), got %d", len(requests))
	}
	for _, forbiddenKey := range []string{ex.AgonesGSReadyCountKey, ex.AgonesAllocatorKey, ex.MMFRequestHashKey} {
		if _, polluted := SoloDuel.ExtensionParams[forbiddenKey]; polluted {
			t.Errorf("SoloDuel.ExtensionParams was mutated with per-fleet key %q", forbiddenKey)
		}
	}
	for _, req := range requests {
		for _, pool := range req.GetProfile().GetPools() {
			pf := pool.GetCreationTimeRangeFilter()
			if pf == nil || !pf.GetStart().AsTime().Equal(gotStart) || !pf.GetEnd().AsTime().Equal(gotEnd) {
				t.Errorf("composed pool %q CreationTimeRangeFilter = %v, want Start=%v End=%v", pool.GetName(), pf, gotStart, gotEnd)
			}
		}
	}
}

type trackingGSManager struct {
	mu               sync.Mutex
	requests         []*pb.MmfRequest
	allocatedMatches map[string]string
	updatedSpecs     map[string]*pb.MmfRequest
}

func (m *trackingGSManager) Init(map[string]map[string]string, map[string]*pb.Pool, map[string][]*GameMode) error {
	return nil
}

func (m *trackingGSManager) GetMMFParams() []*pb.MmfRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

func (m *trackingGSManager) UpdateMMFRequest(spec string, nextReq *pb.MmfRequest) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	updatedSpec := spec + "|backfill:" + nextReq.GetProfile().GetName()
	m.updatedSpecs[updatedSpec] = nextReq
	return updatedSpec
}

func (m *trackingGSManager) Allocate(spec string, match *pb.Match) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allocatedMatches[match.GetId()] = spec
	return "198.51.100.10:7463"
}

func (m *trackingGSManager) isAllocated(matchID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.allocatedMatches[matchID]
	return ok
}

type recordingAssignmentSender struct {
	mu      sync.Mutex
	rosters []*pb.Roster
}

func (s *recordingAssignmentSender) Send(_ context.Context, roster *pb.Roster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rosters = append(s.rosters, roster)
	return nil
}

func (s *recordingAssignmentSender) Stop() {}

// TestMockDirectorRun_ProposalsLoopHandlesMissingAndPresentBackfillExtension
// verifies that ProposalsLoop in MockDirector.Run:
//  1. Safely handles a full match whose Extensions map has no ex.MMFRequestKey
//     entry (without panicking on nil *anypb.Any.UnmarshalTo) and allocates
//     using the base allocator spec.
//  2. Unmarshals a valid ex.MMFRequestKey backfill MmfRequest when present and
//     calls GSManager.UpdateMMFRequest before allocating.
func TestMockDirectorRun_ProposalsLoopHandlesMissingAndPresentBackfillExtension(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := logrus.New()
	log.SetOutput(io.Discard)

	const (
		testReqHash   = "hash-fleet-1"
		baseAllocSpec = `{"spec":"fleet-1"}`
		fullMatchID   = "match-full-no-backfill"
		bfMatchID     = "match-partial-with-backfill"
		bfProfileName = "backfill.dev.open-match.fleet-1.12345"
	)

	hashAny, err := anypb.New(&knownpb.StringValue{Value: testReqHash})
	if err != nil {
		t.Fatalf("anypb.New(hash): %v", err)
	}
	allocAny, err := anypb.New(&knownpb.StringValue{Value: baseAllocSpec})
	if err != nil {
		t.Fatalf("anypb.New(alloc): %v", err)
	}
	backfillReqAny, err := anypb.New(&pb.MmfRequest{
		Mmfs: []*pb.MatchmakingFunctionSpec{Fifo},
		Profile: &pb.Profile{
			Name:  bfProfileName,
			Pools: map[string]*pb.Pool{"all": EveryTicket},
		},
	})
	if err != nil {
		t.Fatalf("anypb.New(backfillReq): %v", err)
	}

	gsMgr := &trackingGSManager{
		requests: []*pb.MmfRequest{
			{
				Mmfs: []*pb.MatchmakingFunctionSpec{Fifo},
				Profile: &pb.Profile{
					Name: "dev.open-match.fleet-1.1",
					Extensions: map[string]*anypb.Any{
						ex.MMFRequestHashKey: hashAny,
						ex.AgonesAllocatorKey: allocAny,
					},
				},
			},
		},
		allocatedMatches: make(map[string]string),
		updatedSpecs:     make(map[string]*pb.MmfRequest),
	}

	// Serve /matches:fetch and /tickets:activate. Because ProposalsLoop has a 2%
	// random test rejection (rand.Intn(100) <= 1), the test server streams any
	// not-yet-allocated match until both matches have been accepted and allocated,
	// and then streams an empty response so NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT=1
	// terminates Run() deterministically.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tickets:activate":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case "/matches:fetch":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			var toStream []*pb.Match
			if !gsMgr.isAllocated(fullMatchID) {
				// Full match WITHOUT ex.MMFRequestKey in Extensions.
				toStream = append(toStream, &pb.Match{
					Id: fullMatchID,
					Rosters: map[string]*pb.Roster{
						"r1": {Tickets: []*pb.Ticket{{Id: "ticket-full-1"}, {Id: "ticket-full-2"}}},
					},
					Extensions: map[string]*anypb.Any{
						ex.MMFRequestHashKey: hashAny,
					},
				})
			}
			if !gsMgr.isAllocated(bfMatchID) {
				// Partial match WITH a valid backfill MmfRequest in ex.MMFRequestKey.
				toStream = append(toStream, &pb.Match{
					Id: bfMatchID,
					Rosters: map[string]*pb.Roster{
						"r1": {Tickets: []*pb.Ticket{{Id: "ticket-bf-1"}}},
					},
					Extensions: map[string]*anypb.Any{
						ex.MMFRequestHashKey: hashAny,
						ex.MMFRequestKey:     backfillReqAny,
					},
				})
			}

			for _, m := range toStream {
				raw, mErr := protojson.Marshal(&pb.StreamedMmfResponse{Match: m})
				if mErr != nil {
					t.Errorf("protojson.Marshal(StreamedMmfResponse): %v", mErr)
					return
				}
				_, _ = fmt.Fprintf(w, `{"result":%s}`+"\n", string(raw))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cfg := viper.New()
	cfg.Set("OM_CORE_ADDR", srv.URL)
	cfg.Set("NUM_MM_CYCLES", 50)
	cfg.Set("NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT", 1)
	cfg.Set("MM_CYCLE_MIN_DURATION_MS", 0)
	cfg.Set("ASSIGNMENT_TTL_MS", 60000)
	cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)

	sender := &recordingAssignmentSender{}
	d := &MockDirector{
		OmClient:            omclient.New(cfg, log, srv.Client()),
		GSManager:           gsMgr,
		AssignmentPublisher: sender,
		Cfg:                 cfg,
		Log:                 log,
		OtelMeterPtr:        testMeter(),
	}

	d.Run(ctx)
	cancel()

	gsMgr.mu.Lock()
	defer gsMgr.mu.Unlock()

	if gotSpec := gsMgr.allocatedMatches[fullMatchID]; gotSpec != baseAllocSpec {
		t.Errorf("full match without backfill allocated with spec %q, want %q", gotSpec, baseAllocSpec)
	}

	wantBfSpec := baseAllocSpec + "|backfill:" + bfProfileName
	if gotSpec := gsMgr.allocatedMatches[bfMatchID]; gotSpec != wantBfSpec {
		t.Errorf("partial match with backfill allocated with spec %q, want %q", gotSpec, wantBfSpec)
	}
	if len(gsMgr.updatedSpecs) != 1 || gsMgr.updatedSpecs[wantBfSpec].GetProfile().GetName() != bfProfileName {
		t.Errorf("expected UpdateMMFRequest to be called once for %q, got %v", bfProfileName, gsMgr.updatedSpecs)
	}

	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.rosters) != 2 {
		t.Errorf("AssignmentPublisher.Send called %d times, want 2", len(sender.rosters))
	}
}

type notifyGSManager struct {
	stubGSManager
	firstCallOnce sync.Once
	firstCallCh   chan struct{}
}

func (n *notifyGSManager) GetMMFParams() []*pb.MmfRequest {
	n.firstCallOnce.Do(func() {
		close(n.firstCallCh)
	})
	return n.stubGSManager.GetMMFParams()
}

// TestMockDirectorRun_ContextCancelInterruptsInterCycleSleepAndFlushesReactivations
// verifies that:
//  1. Cancelling ctx during the inter-cycle sleep (MM_CYCLE_MIN_DURATION_MS=30000)
//     wakes MockDirector.Run immediately instead of blocking for 30 seconds.
//  2. When MockDirector.Run finishes its configured NUM_MM_CYCLES without ctx
//     being cancelled, defer close(ticketIdsToActivate) and reactivationWg.Wait()
//     flush any queued rejected tickets to /tickets:activate and exit the
//     background reactivation goroutine before Run returns.
func TestMockDirectorRun_ContextCancelInterruptsInterCycleSleepAndFlushesReactivations(t *testing.T) {
	t.Run("context cancel interrupts long inter-cycle sleep", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := viper.New()
		cfg.Set("NUM_MM_CYCLES", 100)
		cfg.Set("NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT", 0)
		cfg.Set("MM_CYCLE_MIN_DURATION_MS", 30000) // 30s sleep per cycle
		cfg.Set("ASSIGNMENT_TTL_MS", 60000)
		cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)

		log := logrus.New()
		log.SetOutput(io.Discard)

		gsMgr := &notifyGSManager{
			firstCallCh: make(chan struct{}),
		}

		d := &MockDirector{
			OmClient:     omclient.New(cfg, log, &http.Client{}),
			GSManager:    gsMgr,
			Cfg:          cfg,
			Log:          log,
			OtelMeterPtr: testMeter(),
		}

		runDone := make(chan struct{})
		start := time.Now()
		go func() {
			defer close(runDone)
			d.Run(ctx)
		}()

		select {
		case <-gsMgr.firstCallCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for cycle 1 GetMMFParams")
		}

		// Give cycle 1 a few milliseconds to enter the 30-second inter-cycle timer select,
		// then cancel ctx.
		time.Sleep(20 * time.Millisecond)
		cancel()

		select {
		case <-runDone:
			if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
				t.Errorf("MockDirector.Run took %v to exit after ctx cancel, want < 1.5s", elapsed)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for MockDirector.Run to exit during inter-cycle sleep")
		}
	})

	t.Run("Run closes ticketIdsToActivate and flushes rejected tickets on normal cycle completion", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		log := logrus.New()
		log.SetOutput(io.Discard)

		const (
			validReqHash     = "hash-valid-1"
			rejectedTicketID = "ticket-rejected-on-exit"
		)
		validHashAny, _ := anypb.New(&knownpb.StringValue{Value: validReqHash})
		unknownHashAny, _ := anypb.New(&knownpb.StringValue{Value: "hash-unknown-stale"})
		allocAny, _ := anypb.New(&knownpb.StringValue{Value: `{"spec":"fleet-1"}`})

		var (
			mu             sync.Mutex
			reactivatedIDs []string
		)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/matches:fetch":
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				// Stream a match with an unknown MMFRequestHashKey so ProposalsLoop
				// rejects it deterministically and sends rejectedTicketID to ticketIdsToActivate.
				m := &pb.Match{
					Id: "match-with-unknown-hash",
					Rosters: map[string]*pb.Roster{
						"r1": {Tickets: []*pb.Ticket{{Id: rejectedTicketID}}},
					},
					Extensions: map[string]*anypb.Any{
						ex.MMFRequestHashKey: unknownHashAny,
					},
				}
				raw, _ := protojson.Marshal(&pb.StreamedMmfResponse{Match: m})
				_, _ = fmt.Fprintf(w, `{"result":%s}`+"\n", string(raw))
			case "/tickets:activate":
				body, _ := io.ReadAll(r.Body)
				var reqPb pb.ActivateTicketsRequest
				_ = protojson.Unmarshal(body, &reqPb)
				mu.Lock()
				reactivatedIDs = append(reactivatedIDs, reqPb.GetTicketIds()...)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		cfg := viper.New()
		cfg.Set("OM_CORE_ADDR", srv.URL)
		cfg.Set("NUM_MM_CYCLES", 50)
		cfg.Set("NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT", 0)
		cfg.Set("MM_CYCLE_MIN_DURATION_MS", 0)
		cfg.Set("ASSIGNMENT_TTL_MS", 60000)
		// Set batch size to 500 so closing ticketIdsToActivate on Run exit is what
		// flushes the single rejected ticket before Run returns.
		cfg.Set("OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL", 500)

		gsMgr := &stubGSManager{
			requests: []*pb.MmfRequest{
				{
					Mmfs: []*pb.MatchmakingFunctionSpec{Fifo},
					Profile: &pb.Profile{
						Name: "dev.open-match.fleet-1.1",
						Extensions: map[string]*anypb.Any{
							ex.MMFRequestHashKey: validHashAny,
							ex.AgonesAllocatorKey: allocAny,
						},
					},
				},
			},
		}

		// Run 1 cycle when the match is rejected by unknown hash (even if the 2%
		// random test rejection fires first, rejectMatch still queues rejectedTicketID!).
		cfg.Set("NUM_MM_CYCLES", 1)
		d := &MockDirector{
			OmClient:     omclient.New(cfg, log, srv.Client()),
			GSManager:    gsMgr,
			Cfg:          cfg,
			Log:          log,
			OtelMeterPtr: testMeter(),
		}

		// Do NOT cancel ctx before d.Run(ctx) returns; verify d.Run flushes
		// ticketIdsToActivate via defer close(ticketIdsToActivate) + reactivationWg.Wait().
		d.Run(ctx)

		mu.Lock()
		defer mu.Unlock()
		if len(reactivatedIDs) != 1 || reactivatedIDs[0] != rejectedTicketID {
			t.Errorf("reactivatedIDs after Run() returned = %v, want [%q]", reactivatedIDs, rejectedTicketID)
		}
	})
}
