// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	knownpb "google.golang.org/protobuf/types/known/wrapperspb"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/functions/debug"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/functions/fifo"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/server"
	"open-match.dev/open-match-ecosystem/v2/internal/extensions"
)

type fakeRunServer struct {
	grpc.ServerStream
	ctx       context.Context
	chunks    []*pb.ChunkedMmfRunRequest
	recvIdx   int
	recvErr   error
	responses []*pb.StreamedMmfResponse
	sendErr   error
}

func (f *fakeRunServer) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeRunServer) Recv() (*pb.ChunkedMmfRunRequest, error) {
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	if f.recvIdx >= len(f.chunks) {
		return nil, io.EOF
	}
	chunk := f.chunks[f.recvIdx]
	f.recvIdx++
	return chunk, nil
}

func (f *fakeRunServer) Send(resp *pb.StreamedMmfResponse) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.responses = append(f.responses, resp)
	return nil
}

func mustAnyInt32(t *testing.T, v int32) *anypb.Any {
	t.Helper()
	a, err := anypb.New(&knownpb.Int32Value{Value: v})
	if err != nil {
		t.Fatalf("failed to pack int32 any: %v", err)
	}
	return a
}

func mustAnyString(t *testing.T, v string) *anypb.Any {
	t.Helper()
	a, err := anypb.New(&knownpb.StringValue{Value: v})
	if err != nil {
		t.Fatalf("failed to pack string any: %v", err)
	}
	return a
}

func discardLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

// runFIFO runs the fifo MMF against a single-chunk profile and returns the
// matches it streamed back.
func runFIFO(t *testing.T, profile *pb.Profile) []*pb.Match {
	t.Helper()
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	stream := &fakeRunServer{
		chunks: []*pb.ChunkedMmfRunRequest{{NumChunks: 1, Profile: profile}},
	}
	if err := fifo.NewWithLogger(logger).Run(stream); err != nil {
		t.Fatalf("fifo.Run returned unexpected error: %v", err)
	}
	matches := make([]*pb.Match, 0, len(stream.responses))
	for _, r := range stream.responses {
		matches = append(matches, r.GetMatch())
	}
	return matches
}

func ticketAt(id string, created time.Time) *pb.Ticket {
	return &pb.Ticket{
		Id:         id,
		Attributes: &pb.Ticket_FilterableData{CreationTime: timestamppb.New(created)},
	}
}

func matchTicketIDs(m *pb.Match) []string {
	var ids []string
	for _, r := range m.GetRosters() {
		for _, tk := range r.GetTickets() {
			ids = append(ids, tk.GetId())
		}
	}
	sort.Strings(ids)
	return ids
}

func TestBaseProfileName(t *testing.T) {
	cases := map[string]string{
		"dev.open-match.fleet.mode.1730000000":           "dev.open-match.fleet.mode",
		"backfill.dev.open-match.fleet.mode.1730000-3":   "dev.open-match.fleet.mode",
		"backfill.backfill.dev.open-match.fleet.mode.42": "backfill.dev.open-match.fleet.mode",
		"my-profile":                "my-profile",
		"backfill.my-profile.123-0": "my-profile",
	}
	for in, want := range cases {
		if got := server.BaseProfileName(in); got != want {
			t.Errorf("BaseProfileName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFIFOOrdersByCreationTime(t *testing.T) {
	base := time.Unix(1700000000, 0)
	// Tickets are deliberately out of order and split across pools; "tie-a"
	// and "tie-b" share a creation time so the ID tie-break is exercised.
	profile := &pb.Profile{
		Name: "dev.open-match.fleet.mode.1",
		Pools: map[string]*pb.Pool{
			"p1": {Name: "p1", Participants: &pb.Roster{Tickets: []*pb.Ticket{
				ticketAt("newest", base.Add(5*time.Second)),
				ticketAt("tie-b", base.Add(2*time.Second)),
				ticketAt("oldest", base),
			}}},
			"p2": {Name: "p2", Participants: &pb.Roster{Tickets: []*pb.Ticket{
				ticketAt("second", base.Add(1*time.Second)),
				ticketAt("tie-a", base.Add(2*time.Second)),
				ticketAt("oldest", base), // duplicate across pools
			}}},
		},
		Extensions: map[string]*anypb.Any{
			"desiredNumRosters": mustAnyInt32(t, 1),
			"desiredRosterLen":  mustAnyInt32(t, 2),
			"minRosterLen":      mustAnyInt32(t, 2),
		},
	}

	matches := runFIFO(t, profile)
	want := [][]string{{"oldest", "second"}, {"tie-a", "tie-b"}}
	if len(matches) != len(want) {
		t.Fatalf("got %d matches, want %d", len(matches), len(want))
	}
	for i, m := range matches {
		var got []string
		for _, r := range m.GetRosters() {
			for _, tk := range r.GetTickets() {
				got = append(got, tk.GetId())
			}
		}
		if strings.Join(got, ",") != strings.Join(want[i], ",") {
			t.Errorf("match %d tickets = %v, want %v (oldest first)", i, got, want[i])
		}
	}
	// "newest" is left over: one ticket is fewer than minRosterLen.
}

func TestFIFOMatchIDsAndBackfillNaming(t *testing.T) {
	bfReqAny, err := anypb.New(&pb.MmfRequest{})
	if err != nil {
		t.Fatalf("failed to pack MmfRequest: %v", err)
	}
	newProfile := func(name string) *pb.Profile {
		return &pb.Profile{
			Name: name,
			Pools: map[string]*pb.Pool{
				"p1": {Name: "p1", Participants: &pb.Roster{Tickets: []*pb.Ticket{
					{Id: "a"}, {Id: "b"}, {Id: "c"}, {Id: "d"}, {Id: "e"},
				}}},
			},
			Extensions: map[string]*anypb.Any{
				"desiredNumRosters":      mustAnyInt32(t, 1),
				"desiredRosterLen":       mustAnyInt32(t, 2),
				"minRosterLen":           mustAnyInt32(t, 1),
				extensions.MMFRequestKey: bfReqAny,
			},
		}
	}

	seenIDs := map[string]bool{}
	seenRosters := map[string]bool{}
	checkUnique := func(matches []*pb.Match) {
		for _, m := range matches {
			if seenIDs[m.GetId()] {
				t.Errorf("duplicate match ID %q", m.GetId())
			}
			seenIDs[m.GetId()] = true
			for name := range m.GetRosters() {
				if seenRosters[name] {
					t.Errorf("duplicate roster name %q", name)
				}
				seenRosters[name] = true
			}
		}
	}

	// Two invocations of profiles with the same base name: IDs must be
	// unique across invocations, and the part before the last dot must be
	// the same (bounded metric cardinality).
	first := runFIFO(t, newProfile("dev.open-match.fleet.mode.111"))
	second := runFIFO(t, newProfile("dev.open-match.fleet.mode.222"))
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("got %d and %d matches, want 3 each", len(first), len(second))
	}
	checkUnique(first)
	checkUnique(second)

	const wantIDPrefix = "simple-fifo-matchfunction.dev.open-match.fleet.mode."
	for _, m := range append(first, second...) {
		id := m.GetId()
		if !strings.HasPrefix(id, wantIDPrefix) {
			t.Errorf("match ID %q, want prefix %q", id, wantIDPrefix)
		}
		if suffix := strings.TrimPrefix(id, wantIDPrefix); suffix == "" || strings.Contains(suffix, ".") {
			t.Errorf("match ID %q has empty or dotted unique suffix %q", id, suffix)
		}
	}

	// The last match of each run has a single ticket, so it carries a
	// backfill request named backfill.<base profile name>.<unique suffix>.
	bf, err := extensions.MMFRequest(first[2].GetExtensions(), extensions.MMFRequestKey)
	if err != nil {
		t.Fatalf("expected backfill MmfRequest on partial match: %v", err)
	}
	bfName := bf.GetProfile().GetName()
	const wantBfPrefix = "backfill.dev.open-match.fleet.mode."
	if !strings.HasPrefix(bfName, wantBfPrefix) {
		t.Fatalf("backfill profile name = %q, want prefix %q", bfName, wantBfPrefix)
	}
	if suffix := strings.TrimPrefix(bfName, wantBfPrefix); suffix == "" || strings.Contains(suffix, ".") {
		t.Fatalf("backfill profile name %q has empty or dotted unique suffix %q", bfName, suffix)
	}
	// What gsdirector records as a metric attribute: strip after the last dot.
	if got := bfName[:strings.LastIndex(bfName, ".")]; got != "backfill.dev.open-match.fleet.mode" {
		t.Errorf("bounded backfill profile name = %q, want %q", got, "backfill.dev.open-match.fleet.mode")
	}

	// Running the backfill profile must not nest "backfill." prefixes, in
	// either the next backfill profile name or the match IDs.
	bfProfile := bf.GetProfile()
	bfProfile.Pools["p1"].Participants = &pb.Roster{Tickets: []*pb.Ticket{{Id: "z"}}}
	bfProfile.Extensions[extensions.MMFRequestKey] = bfReqAny
	bfProfile.Extensions["desiredRosterLen"] = mustAnyInt32(t, 2) // leave it partial again
	bfMatches := runFIFO(t, bfProfile)
	if len(bfMatches) != 1 {
		t.Fatalf("backfill run got %d matches, want 1", len(bfMatches))
	}
	checkUnique(bfMatches)
	if id := bfMatches[0].GetId(); !strings.HasPrefix(id, "simple-fifo-matchfunction.backfill.dev.open-match.fleet.mode.") {
		t.Errorf("backfill match ID = %q, want prefix simple-fifo-matchfunction.backfill.dev.open-match.fleet.mode.", id)
	}
	bf2, err := extensions.MMFRequest(bfMatches[0].GetExtensions(), extensions.MMFRequestKey)
	if err != nil {
		t.Fatalf("expected backfill MmfRequest on partial backfill match: %v", err)
	}
	if name := bf2.GetProfile().GetName(); !strings.HasPrefix(name, wantBfPrefix) || strings.Contains(strings.TrimPrefix(name, wantBfPrefix), ".") {
		t.Errorf("backfill-of-backfill profile name = %q, want %q<suffix>", name, wantBfPrefix)
	}
}

func TestFIFOBackfillCapacityWithUnevenRosters(t *testing.T) {
	bfReqAny, err := anypb.New(&pb.MmfRequest{})
	if err != nil {
		t.Fatalf("failed to pack MmfRequest: %v", err)
	}
	// Two rosters of 4 with only 7 tickets: rosters end up with 4 and 3
	// players, so exactly one slot is open.
	var tickets []*pb.Ticket
	for _, id := range []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7"} {
		tickets = append(tickets, &pb.Ticket{Id: id})
	}
	matches := runFIFO(t, &pb.Profile{
		Name:  "dev.open-match.fleet.teams.1",
		Pools: map[string]*pb.Pool{"p1": {Name: "p1", Participants: &pb.Roster{Tickets: tickets}}},
		Extensions: map[string]*anypb.Any{
			"desiredNumRosters":      mustAnyInt32(t, 2),
			"desiredRosterLen":       mustAnyInt32(t, 4),
			"minRosterLen":           mustAnyInt32(t, 2),
			extensions.MMFRequestKey: bfReqAny,
		},
	})
	if len(matches) != 1 {
		t.Fatalf("got %d matches, want 1", len(matches))
	}
	if got := len(matchTicketIDs(matches[0])); got != 7 {
		t.Fatalf("match has %d tickets, want 7", got)
	}
	bf, err := extensions.MMFRequest(matches[0].GetExtensions(), extensions.MMFRequestKey)
	if err != nil {
		t.Fatalf("expected backfill MmfRequest: %v", err)
	}
	ex := bf.GetProfile().GetExtensions()
	for key, want := range map[string]int{"desiredNumRosters": 1, "desiredRosterLen": 1, "minRosterLen": 1} {
		if got, err := extensions.Int32(ex, key); err != nil || got != want {
			t.Errorf("backfill %s = %d (err=%v), want %d", key, got, err, want)
		}
	}
}

func TestMMFServerFactoryDispatch(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	validCases := []struct {
		name  string
		input string
	}{
		{name: "empty defaults to fifo", input: ""},
		{name: "lowercase fifo", input: "fifo"},
		{name: "uppercase whitespace fifo", input: "  FIFO\t\n"},
		{name: "lowercase debug", input: "debug"},
		{name: "mixedcase whitespace debug", input: "  DeBuG  "},
	}

	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newMMFServer(tc.input, logger)
			if err != nil {
				t.Fatalf("newMMFServer(%q) returned unexpected error: %v", tc.input, err)
			}
			if srv == nil {
				t.Fatalf("newMMFServer(%q) returned nil server", tc.input)
			}
		})
	}

	invalidInputs := []string{"unknown", "soloduel", "fifo2"}
	for _, invalid := range invalidInputs {
		t.Run("invalid_"+invalid, func(t *testing.T) {
			srv, err := newMMFServer(invalid, logger)
			if err == nil {
				t.Fatalf("newMMFServer(%q) expected error, got nil", invalid)
			}
			if srv != nil {
				t.Fatalf("newMMFServer(%q) expected nil server on error, got %v", invalid, srv)
			}
		})
	}
}

func TestGetChunkedRequestMultiChunkEmptyPoolAndSliceIsolation(t *testing.T) {
	// Allocate chunk 1 ticket slice with extra capacity to verify that subsequent
	// chunk appends do not overwrite chunk 1's backing array.
	chunk1Backing := make([]*pb.Ticket, 2, 4)
	chunk1Backing[0] = &pb.Ticket{Id: "t1"}
	chunk1Backing[1] = &pb.Ticket{Id: "t2"}
	sentinel := &pb.Ticket{Id: "sentinel-do-not-overwrite"}
	fullView := chunk1Backing[:4]
	fullView[2] = sentinel
	fullView[3] = sentinel

	chunk1 := &pb.ChunkedMmfRunRequest{
		NumChunks: 2,
		Profile: &pb.Profile{
			Name: "chunked-profile",
			Pools: map[string]*pb.Pool{
				"empty_pool": {
					Name:         "empty_pool",
					Participants: nil,
				},
				"late_pool": {
					Name:         "late_pool",
					Participants: nil,
				},
				"multi_pool": {
					Name: "multi_pool",
					Participants: &pb.Roster{
						Name:    "",
						Tickets: chunk1Backing,
					},
				},
			},
		},
	}

	chunk2 := &pb.ChunkedMmfRunRequest{
		NumChunks: 2,
		Profile: &pb.Profile{
			Name: "chunked-profile",
			Pools: map[string]*pb.Pool{
				"late_pool": {
					Name: "late_pool",
					Participants: &pb.Roster{
						Name: "late_roster",
						Tickets: []*pb.Ticket{
							{Id: "late-t1"},
						},
					},
				},
				"multi_pool": {
					Name: "multi_pool",
					Participants: &pb.Roster{
						Tickets: []*pb.Ticket{
							{Id: "t3"},
							{Id: "t4"},
						},
					},
				},
			},
		},
	}

	stream := &fakeRunServer{
		chunks: []*pb.ChunkedMmfRunRequest{chunk1, chunk2},
	}

	profile, err := server.GetChunkedRequest(stream, discardLogger())
	if err != nil {
		t.Fatalf("GetChunkedRequest returned unexpected error: %v", err)
	}
	if profile.GetName() != "chunked-profile" {
		t.Fatalf("got profile name %q, want %q", profile.GetName(), "chunked-profile")
	}

	// Verify empty_pool has non-nil Participants with fallback roster name and 0 tickets.
	emptyPool := profile.GetPools()["empty_pool"]
	if emptyPool == nil || emptyPool.GetParticipants() == nil {
		t.Fatalf("expected non-nil Participants for empty_pool")
	}
	if emptyPool.GetParticipants().GetName() != "empty_pool" {
		t.Fatalf("empty_pool roster name = %q, want %q", emptyPool.GetParticipants().GetName(), "empty_pool")
	}
	if len(emptyPool.GetParticipants().GetTickets()) != 0 {
		t.Fatalf("empty_pool ticket count = %d, want 0", len(emptyPool.GetParticipants().GetTickets()))
	}

	// Verify late_pool received tickets in chunk 2 after nil Participants in chunk 1.
	latePool := profile.GetPools()["late_pool"]
	if latePool == nil || len(latePool.GetParticipants().GetTickets()) != 1 {
		t.Fatalf("late_pool tickets = %v, want 1 ticket", latePool.GetParticipants().GetTickets())
	}
	if latePool.GetParticipants().GetTickets()[0].GetId() != "late-t1" {
		t.Fatalf("late_pool ticket[0] = %q, want %q", latePool.GetParticipants().GetTickets()[0].GetId(), "late-t1")
	}

	// Verify multi_pool concatenated all 4 tickets and defaulted its empty roster name to pool key.
	multiPool := profile.GetPools()["multi_pool"]
	if multiPool == nil || multiPool.GetParticipants() == nil {
		t.Fatalf("expected non-nil Participants for multi_pool")
	}
	if multiPool.GetParticipants().GetName() != "multi_pool" {
		t.Fatalf("multi_pool roster name = %q, want %q", multiPool.GetParticipants().GetName(), "multi_pool")
	}
	gotTickets := multiPool.GetParticipants().GetTickets()
	if len(gotTickets) != 4 {
		t.Fatalf("multi_pool ticket count = %d, want 4", len(gotTickets))
	}
	wantIDs := []string{"t1", "t2", "t3", "t4"}
	for i, wantID := range wantIDs {
		if gotTickets[i].GetId() != wantID {
			t.Fatalf("multi_pool ticket[%d] = %q, want %q", i, gotTickets[i].GetId(), wantID)
		}
	}

	// Verify chunk 1's slice backing array was never mutated by appending chunk 2.
	if fullView[2] != sentinel || fullView[3] != sentinel {
		t.Fatalf("chunk 1 slice backing array was mutated: fullView[2]=%v, fullView[3]=%v", fullView[2], fullView[3])
	}
}

func TestStreamRecvErrorPropagation(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	expectedErr := errors.New("simulated stream recv failure")

	t.Run("GetChunkedRequest", func(t *testing.T) {
		stream := &fakeRunServer{recvErr: expectedErr}
		prof, err := server.GetChunkedRequest(stream, discardLogger())
		if !errors.Is(err, expectedErr) {
			t.Fatalf("GetChunkedRequest err = %v, want %v", err, expectedErr)
		}
		if prof != nil {
			t.Fatalf("expected nil profile on error, got %v", prof)
		}
	})

	t.Run("fifo.Run", func(t *testing.T) {
		stream := &fakeRunServer{recvErr: expectedErr}
		fifoSrv := fifo.NewWithLogger(logger)
		err := fifoSrv.Run(stream)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("fifo.Run err = %v, want %v", err, expectedErr)
		}
		if len(stream.responses) != 0 {
			t.Fatalf("fifo.Run sent %d responses on error, want 0", len(stream.responses))
		}
	})

	t.Run("debug.Run", func(t *testing.T) {
		stream := &fakeRunServer{recvErr: expectedErr}
		debugSrv := debug.NewWithLogger(logger)
		err := debugSrv.Run(stream)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("debug.Run err = %v, want %v", err, expectedErr)
		}
		if len(stream.responses) != 0 {
			t.Fatalf("debug.Run sent %d responses on error, want 0", len(stream.responses))
		}
	})
}

func TestFIFOOverlappingPoolDeduplicationFullAndBackfillMatch(t *testing.T) {
	var logBuf bytes.Buffer
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetOutput(&logBuf)

	bfReqAny, err := anypb.New(&pb.MmfRequest{})
	if err != nil {
		t.Fatalf("failed to pack MmfRequest: %v", err)
	}

	// Pool A has 4 tickets (t1..t4); Pool B has 5 tickets (t3..t7), overlapping on t3 and t4.
	// Total unique tickets = 7.
	// With desiredNumRosters=1, desiredRosterLen=4, minRosterLen=3:
	// - Match 0 is a full match (4 tickets, score = 100).
	// - Match 1 is a partial/backfill match (3 tickets, score = 25, backfill profile desiredRosterLen = 1, minRosterLen = 1).
	profile := &pb.Profile{
		Name: "fifo-overlap-test",
		Pools: map[string]*pb.Pool{
			"poolA": {
				Name: "poolA",
				Participants: &pb.Roster{
					Name: "poolA",
					Tickets: []*pb.Ticket{
						{Id: "t1"},
						{Id: "t2"},
						{Id: "t3"},
						{Id: "t4"},
					},
				},
			},
			"poolB": {
				Name: "poolB",
				Participants: &pb.Roster{
					Name: "poolB",
					Tickets: []*pb.Ticket{
						{Id: "t3"},
						{Id: "t4"},
						{Id: "t5"},
						{Id: "t6"},
						{Id: "t7"},
					},
				},
			},
		},
		Extensions: map[string]*anypb.Any{
			"desiredNumRosters":      mustAnyInt32(t, 1),
			"desiredRosterLen":       mustAnyInt32(t, 4),
			"minRosterLen":           mustAnyInt32(t, 3),
			extensions.MMFRequestKey: bfReqAny,
		},
	}

	stream := &fakeRunServer{
		chunks: []*pb.ChunkedMmfRunRequest{
			{
				NumChunks: 1,
				Profile:   profile,
			},
		},
	}

	fifoSrv := fifo.NewWithLogger(logger)
	if err := fifoSrv.Run(stream); err != nil {
		t.Fatalf("fifo.Run returned unexpected error: %v", err)
	}

	logs := logBuf.String()
	if !strings.Contains(logs, "Found 4 tickets in pool poolA") {
		t.Errorf("expected per-pool log 'Found 4 tickets in pool poolA', got logs:\n%s", logs)
	}
	if !strings.Contains(logs, "Found 5 tickets in pool poolB") {
		t.Errorf("expected per-pool log 'Found 5 tickets in pool poolB', got logs:\n%s", logs)
	}
	if !strings.Contains(logs, "Matching among 7 tickets from 2 provided pools") {
		t.Errorf("expected deduplicated ticket count log 'Matching among 7 tickets from 2 provided pools', got logs:\n%s", logs)
	}
	if !strings.Contains(logs, "with roster of 4 tickets") {
		t.Errorf("expected full match log 'with roster of 4 tickets', got logs:\n%s", logs)
	}

	if len(stream.responses) != 2 {
		t.Fatalf("got %d matches, want 2", len(stream.responses))
	}

	// Verify Match 0 (full match: score 100, 4 tickets).
	fullMatch := stream.responses[0].GetMatch()
	fullScore, err := extensions.Int32(fullMatch.GetExtensions(), "score")
	if err != nil || fullScore != 100 {
		t.Fatalf("full match score = %d (err=%v), want 100", fullScore, err)
	}
	fullTicketCount := 0
	seenAcrossMatches := make(map[string]struct{})
	for _, r := range fullMatch.GetRosters() {
		for _, tk := range r.GetTickets() {
			fullTicketCount++
			if _, dup := seenAcrossMatches[tk.GetId()]; dup {
				t.Fatalf("duplicate ticket %q in full match", tk.GetId())
			}
			seenAcrossMatches[tk.GetId()] = struct{}{}
		}
	}
	if fullTicketCount != 4 {
		t.Fatalf("full match ticket count = %d, want 4", fullTicketCount)
	}

	// Verify Match 1 (partial match: score 25, 3 tickets, backfill profile with desiredRosterLen = 1).
	partialMatch := stream.responses[1].GetMatch()
	partialScore, err := extensions.Int32(partialMatch.GetExtensions(), "score")
	if err != nil || partialScore != 25 {
		t.Fatalf("partial match score = %d (err=%v), want 25", partialScore, err)
	}
	partialTicketCount := 0
	for _, r := range partialMatch.GetRosters() {
		for _, tk := range r.GetTickets() {
			partialTicketCount++
			if _, dup := seenAcrossMatches[tk.GetId()]; dup {
				t.Fatalf("duplicate ticket %q across matches", tk.GetId())
			}
			seenAcrossMatches[tk.GetId()] = struct{}{}
		}
	}
	if partialTicketCount != 3 {
		t.Fatalf("partial match ticket count = %d, want 3", partialTicketCount)
	}

	gotBfReq, err := extensions.MMFRequest(partialMatch.GetExtensions(), extensions.MMFRequestKey)
	if err != nil {
		t.Fatalf("expected backfill MmfRequest on partial match, got err: %v", err)
	}
	bfProf := gotBfReq.GetProfile()
	if !strings.HasPrefix(bfProf.GetName(), "backfill.") {
		t.Fatalf("backfill profile name = %q, want prefix 'backfill.'", bfProf.GetName())
	}
	bfDesiredLen, err := extensions.Int32(bfProf.GetExtensions(), "desiredRosterLen")
	if err != nil || bfDesiredLen != 1 {
		t.Fatalf("backfill desiredRosterLen = %d (err=%v), want 1", bfDesiredLen, err)
	}
	bfMinLen, err := extensions.Int32(bfProf.GetExtensions(), "minRosterLen")
	if err != nil || bfMinLen != 1 {
		t.Fatalf("backfill minRosterLen = %d (err=%v), want 1", bfMinLen, err)
	}

	// Verify original request profile extensions were not mutated.
	origDesiredLen, err := extensions.Int32(profile.GetExtensions(), "desiredRosterLen")
	if err != nil || origDesiredLen != 4 {
		t.Fatalf("original profile desiredRosterLen was mutated to %d (err=%v), want 4", origDesiredLen, err)
	}
	if _, hasScore := profile.GetExtensions()["score"]; hasScore {
		t.Fatalf("original profile extensions map was mutated with 'score'")
	}

	// Verify partial match with nil profile Extensions (and absent MMFRequestKey) still streams with score=25 without panicking.
	nilExtStream := &fakeRunServer{
		chunks: []*pb.ChunkedMmfRunRequest{
			{
				NumChunks: 1,
				Profile: &pb.Profile{
					Name:       "fifo-nil-ext-profile",
					Extensions: nil,
					Pools: map[string]*pb.Pool{
						"p1": {
							Name: "p1",
							Participants: &pb.Roster{
								Name: "p1",
								Tickets: []*pb.Ticket{
									{Id: "tk1"},
									{Id: "tk2"},
								},
							},
						},
					},
				},
			},
		},
	}
	if err := fifoSrv.Run(nilExtStream); err != nil {
		t.Fatalf("fifo.Run with nil Extensions returned unexpected error: %v", err)
	}
	if len(nilExtStream.responses) != 1 {
		t.Fatalf("fifo.Run with nil Extensions and 2 tickets sent %d matches, want 1", len(nilExtStream.responses))
	}
}

func TestDebugMMFDiagnosticMatchAndExtensionIsolation(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	debugSrv := debug.NewWithLogger(logger)

	t.Run("non_nil_extensions_not_mutated", func(t *testing.T) {
		origExtensions := map[string]*anypb.Any{
			"customParam": mustAnyString(t, "value1"),
		}
		profile := &pb.Profile{
			Name:       "debug-test-profile",
			Extensions: origExtensions,
			Pools: map[string]*pb.Pool{
				"p1": {
					Name: "p1",
					Participants: &pb.Roster{
						Name:    "p1",
						Tickets: []*pb.Ticket{{Id: "t1"}},
					},
				},
			},
		}

		stream := &fakeRunServer{
			chunks: []*pb.ChunkedMmfRunRequest{
				{NumChunks: 1, Profile: profile},
			},
		}

		if err := debugSrv.Run(stream); err != nil {
			t.Fatalf("debug.Run returned unexpected error: %v", err)
		}
		if len(stream.responses) != 1 {
			t.Fatalf("debug.Run sent %d matches, want 1", len(stream.responses))
		}

		match := stream.responses[0].GetMatch()
		if len(match.GetRosters()) != 0 {
			t.Fatalf("debug match rosters count = %d, want 0", len(match.GetRosters()))
		}
		// The analysis travels in extensions: one ticket count per pool.
		count, err := extensions.Int32(match.GetExtensions(), "debug.pool-ticket-count/p1")
		if err != nil || count != 1 {
			t.Fatalf("debug match pool ticket count = %d (err=%v), want 1", count, err)
		}
		if _, hasScore := match.GetExtensions()["score"]; hasScore {
			t.Fatalf("debug match should not carry a 'score' extension")
		}
		profName, err := extensions.String(match.GetExtensions(), "profileName")
		if err != nil || profName != "debug-test-profile" {
			t.Fatalf("debug match profileName = %q (err=%v), want %q", profName, err, "debug-test-profile")
		}
		if custom, err := extensions.String(match.GetExtensions(), "customParam"); err != nil || custom != "value1" {
			t.Fatalf("debug match customParam = %q (err=%v), want profile extensions copied over", custom, err)
		}
		const wantIDPrefix = "profile-debug-matchfunction.debug-test-profile."
		if id := match.GetId(); !strings.HasPrefix(id, wantIDPrefix) || strings.Contains(strings.TrimPrefix(id, wantIDPrefix), ".") {
			t.Fatalf("debug match ID = %q, want %q<unique suffix without dots>", id, wantIDPrefix)
		}

		// Ensure original extensions map was not mutated.
		if len(origExtensions) != 1 {
			t.Fatalf("original profile Extensions mutated: got %d keys, want 1", len(origExtensions))
		}
		if _, exists := origExtensions["score"]; exists {
			t.Fatalf("original profile Extensions was mutated with 'score' key")
		}
	})

	t.Run("nil_extensions_initialized_safely", func(t *testing.T) {
		profile := &pb.Profile{
			Name:       "debug-nil-ext-profile",
			Extensions: nil,
		}
		stream := &fakeRunServer{
			chunks: []*pb.ChunkedMmfRunRequest{
				{NumChunks: 1, Profile: profile},
			},
		}

		if err := debugSrv.Run(stream); err != nil {
			t.Fatalf("debug.Run with nil Extensions returned unexpected error: %v", err)
		}
		if len(stream.responses) != 1 {
			t.Fatalf("debug.Run sent %d matches, want 1", len(stream.responses))
		}
		match := stream.responses[0].GetMatch()
		if match.GetId() == "" || len(match.GetRosters()) != 0 {
			t.Fatalf("debug match = %v, want a ticketless match with an ID", match)
		}
		if name, err := extensions.String(match.GetExtensions(), "mmfName"); err != nil || name != "profile-debug-matchfunction" {
			t.Fatalf("debug match mmfName = %q (err=%v)", name, err)
		}
	})
}

func TestStartServerWithContextListenErrorAndGracefulStop(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	mmfSrv := fifo.NewWithLogger(logger)

	t.Run("invalid_port_returns_error", func(t *testing.T) {
		err := server.StartServerWithContext(context.Background(), -1, mmfSrv, logger)
		if err == nil {
			t.Fatalf("StartServerWithContext with port -1 expected error, got nil")
		}
	})

	t.Run("graceful_stop_on_context_cancel", func(t *testing.T) {
		// Reserve an ephemeral port, then release it so StartServerWithContext can bind it.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to find free port: %v", err)
		}
		port := int32(ln.Addr().(*net.TCPAddr).Port)
		_ = ln.Close()

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- server.StartServerWithContext(ctx, port, mmfSrv, logger)
		}()

		// Wait until the TCP port is accepting connections, then cancel context.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			conn, dialErr := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strings.TrimSpace(ln.Addr().String()[strings.LastIndex(ln.Addr().String(), ":")+1:])), 50*time.Millisecond)
			if dialErr == nil {
				_ = conn.Close()
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		cancel()

		select {
		case serveErr := <-errCh:
			if serveErr != nil {
				t.Fatalf("StartServerWithContext returned unexpected error on graceful stop: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for StartServerWithContext to return after ctx cancellation")
		}
	})

	t.Run("concurrent_servers_with_own_loggers", func(t *testing.T) {
		// The example matchmaker starts fifo and debug servers in one
		// process; under -race this catches any shared mutable state.
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 2)
		for i := 0; i < 2; i++ {
			l := logrus.New()
			l.SetOutput(io.Discard)
			port := freePort(t)
			go func() { errCh <- server.StartServerWithContext(ctx, port, fifo.NewWithLogger(l), l) }()
			waitForPort(t, port)
		}
		cancel()
		for i := 0; i < 2; i++ {
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("StartServerWithContext returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for servers to stop")
			}
		}
	})

	t.Run("hung_run_is_force_stopped_after_deadline", func(t *testing.T) {
		if testing.Short() {
			t.Skip("waits for the full graceful stop deadline")
		}
		port := freePort(t)
		hung := &hangingMMF{entered: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- server.StartServerWithContext(ctx, port, hung, logger) }()
		waitForPort(t, port)

		conn, err := grpc.NewClient(net.JoinHostPort("127.0.0.1", fmt.Sprint(port)), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		defer conn.Close()
		stream, err := pb.NewMatchMakingFunctionServiceClient(conn).Run(context.Background())
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if err := stream.Send(&pb.ChunkedMmfRunRequest{NumChunks: 1, Profile: &pb.Profile{Name: "p"}}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		select {
		case <-hung.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("MMF Run was never invoked")
		}

		start := time.Now()
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("StartServerWithContext returned %v", err)
			}
			// GracefulStop alone would wait forever; Stop must kick in
			// after the ~8s deadline.
			if elapsed := time.Since(start); elapsed < 5*time.Second {
				t.Errorf("server stopped after %v, expected it to wait for the graceful stop deadline", elapsed)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("server didn't force-stop a hung Run after the graceful stop deadline")
		}
	})
}

// hangingMMF never returns from Run until its stream is torn down, standing
// in for an MMF stuck in a long computation.
type hangingMMF struct {
	pb.UnimplementedMatchMakingFunctionServiceServer
	entered chan struct{}
}

func (h *hangingMMF) Run(stream pb.MatchMakingFunctionService_RunServer) error {
	close(h.entered)
	<-stream.Context().Done()
	return stream.Context().Err()
}

func freePort(t *testing.T) int32 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	defer ln.Close()
	return int32(ln.Addr().(*net.TCPAddr).Port)
}

func waitForPort(t *testing.T, port int32) {
	t.Helper()
	addr := net.JoinHostPort("127.0.0.1", fmt.Sprint(port))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond); err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("port %d never started accepting connections", port)
}
