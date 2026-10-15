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

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	knownpb "google.golang.org/protobuf/types/known/wrapperspb"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/functions/debug"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/functions/fifo"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/server"
	"open-match.dev/open-match-ecosystem/v2/internal/extensions"
)

// fakeMmfRunServer implements pb.MatchMakingFunctionService_RunServer for unit
// testing server.GetChunkedRequest, fifo.Run, and debug.Run without network I/O.
type fakeMmfRunServer struct {
	grpc.ServerStream
	ctx       context.Context
	chunks    []*pb.ChunkedMmfRunRequest
	recvIndex int
	recvErr   error
	sent      []*pb.StreamedMmfResponse
}

func (f *fakeMmfRunServer) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeMmfRunServer) Recv() (*pb.ChunkedMmfRunRequest, error) {
	if f.recvIndex < len(f.chunks) {
		ch := f.chunks[f.recvIndex]
		f.recvIndex++
		return ch, nil
	}
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	return nil, io.EOF
}

func (f *fakeMmfRunServer) Send(resp *pb.StreamedMmfResponse) error {
	f.sent = append(f.sent, resp)
	return nil
}

func mustAnyInt32(t *testing.T, v int32) *anypb.Any {
	t.Helper()
	a, err := anypb.New(&knownpb.Int32Value{Value: v})
	if err != nil {
		t.Fatalf("anypb.New(Int32Value(%d)): %v", v, err)
	}
	return a
}

func TestNewMMFServer(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	fifoType := reflect.TypeOf(fifo.NewWithLogger(logger))
	debugType := reflect.TypeOf(debug.NewWithLogger(logger))

	tests := []struct {
		name     string
		input    string
		wantType reflect.Type
		wantErr  bool
	}{
		{name: "empty defaults to fifo", input: "", wantType: fifoType},
		{name: "lowercase fifo", input: "fifo", wantType: fifoType},
		{name: "uppercase FIFO with whitespace", input: "  FIFO ", wantType: fifoType},
		{name: "lowercase debug", input: "debug", wantType: debugType},
		{name: "uppercase DEBUG", input: "DEBUG", wantType: debugType},
		{name: "unknown function returns error", input: "nonexistent", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := newMMFServer(tc.input, logger)
			if (err != nil) != tc.wantErr {
				t.Fatalf("newMMFServer(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if gotType := reflect.TypeOf(srv); gotType != tc.wantType {
				t.Errorf("newMMFServer(%q) returned %v, want %v", tc.input, gotType, tc.wantType)
			}
		})
	}
}

// TestGetChunkedRequest_MultiChunkEmptyPoolAndSliceIsolation verifies that
// server.GetChunkedRequest:
//  1. Safely logs via the package-level logger even when called before StartServer.
//  2. Preserves empty pools (where Participants is nil on chunk 1) with a
//     non-nil Participants roster and concatenates tickets arriving on later
//     chunks without panicking on a nil Participants pointer.
//  3. Clones the initial chunk's Tickets slice so appending tickets from
//     subsequent chunks never mutates the caller's chunk 1 slice backing array.
func TestGetChunkedRequest_MultiChunkEmptyPoolAndSliceIsolation(t *testing.T) {
	// Allocate chunk1Backing with extra capacity (len 2, cap 6) so an in-place
	// append on chunk 2 would overwrite chunk1Backing[2] and chunk1Backing[3]
	// if GetChunkedRequest did not clone chunk 1's Tickets slice.
	chunk1Backing := make([]*pb.Ticket, 4)
	chunk1Backing[0] = &pb.Ticket{Id: "t-split-1"}
	chunk1Backing[1] = &pb.Ticket{Id: "t-split-2"}
	sentinelTicket := &pb.Ticket{Id: "sentinel-must-not-be-overwritten"}
	chunk1Backing[2] = sentinelTicket
	chunk1Backing[3] = sentinelTicket
	chunk1SliceWithSpareCap := chunk1Backing[:2]

	stream := &fakeMmfRunServer{
		chunks: []*pb.ChunkedMmfRunRequest{
			{
				NumChunks: 2,
				Profile: &pb.Profile{
					Name: "com.example.chunked_profile",
					Pools: map[string]*pb.Pool{
						"pool_always_empty": {
							Name:         "pool_always_empty",
							Participants: nil,
						},
						"pool_empty_on_chunk1": {
							Name:         "pool_empty_on_chunk1",
							Participants: nil,
						},
						"pool_split": {
							Name: "pool_split",
							Participants: &pb.Roster{
								Name:    "pool_split",
								Tickets: chunk1SliceWithSpareCap,
							},
						},
					},
				},
			},
			{
				NumChunks: 2,
				Profile: &pb.Profile{
					Name: "com.example.chunked_profile",
					Pools: map[string]*pb.Pool{
						"pool_always_empty": {
							Name:         "pool_always_empty",
							Participants: nil,
						},
						"pool_empty_on_chunk1": {
							Name: "pool_empty_on_chunk1",
							Participants: &pb.Roster{
								Name: "pool_empty_on_chunk1",
								Tickets: []*pb.Ticket{
									{Id: "t-late-1"},
									{Id: "t-late-2"},
								},
							},
						},
						"pool_split": {
							Name: "pool_split",
							Participants: &pb.Roster{
								Name: "pool_split",
								Tickets: []*pb.Ticket{
									{Id: "t-split-3"},
									{Id: "t-split-4"},
								},
							},
						},
					},
				},
			},
		},
	}

	profile, err := server.GetChunkedRequest(stream)
	if err != nil {
		t.Fatalf("GetChunkedRequest returned unexpected error: %v", err)
	}
	if profile == nil {
		t.Fatal("GetChunkedRequest returned nil profile")
	}

	// 1. Always-empty pool has a non-nil Participants roster with 0 tickets.
	emptyPool := profile.GetPools()["pool_always_empty"]
	if emptyPool == nil || emptyPool.GetParticipants() == nil {
		t.Fatalf("expected pool_always_empty to have a non-nil Participants roster, got %+v", emptyPool)
	}
	if len(emptyPool.GetParticipants().GetTickets()) != 0 {
		t.Fatalf("pool_always_empty ticket count = %d, want 0", len(emptyPool.GetParticipants().GetTickets()))
	}

	// 2. Pool that was empty (nil Participants) on chunk 1 has chunk 2's tickets.
	latePool := profile.GetPools()["pool_empty_on_chunk1"]
	if latePool == nil || latePool.GetParticipants() == nil {
		t.Fatalf("expected pool_empty_on_chunk1 to have a non-nil Participants roster, got %+v", latePool)
	}
	if got := len(latePool.GetParticipants().GetTickets()); got != 2 {
		t.Fatalf("pool_empty_on_chunk1 ticket count = %d, want 2", got)
	}

	// 3. Split pool has all 4 tickets and did not overwrite chunk1Backing[2..3].
	splitPool := profile.GetPools()["pool_split"]
	if splitPool == nil || splitPool.GetParticipants() == nil {
		t.Fatalf("expected pool_split to have a non-nil Participants roster, got %+v", splitPool)
	}
	gotSplitIDs := make([]string, 0, len(splitPool.GetParticipants().GetTickets()))
	for _, tk := range splitPool.GetParticipants().GetTickets() {
		gotSplitIDs = append(gotSplitIDs, tk.GetId())
	}
	wantSplitIDs := []string{"t-split-1", "t-split-2", "t-split-3", "t-split-4"}
	if !reflect.DeepEqual(gotSplitIDs, wantSplitIDs) {
		t.Fatalf("pool_split tickets = %v, want %v", gotSplitIDs, wantSplitIDs)
	}
	if chunk1Backing[2] != sentinelTicket || chunk1Backing[3] != sentinelTicket {
		t.Fatal("GetChunkedRequest mutated chunk 1's underlying Tickets slice backing array in place")
	}
}

// TestGetChunkedRequest_AndMMFsPropagateStreamRecvError verifies that when
// stream.Recv() fails before all chunks arrive, GetChunkedRequest, fifo.Run,
// and debug.Run all return the error immediately without streaming bogus
// matches.
func TestGetChunkedRequest_AndMMFsPropagateStreamRecvError(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	wantErr := status.Error(codes.Unavailable, "simulated stream disconnect")

	t.Run("GetChunkedRequest", func(t *testing.T) {
		stream := &fakeMmfRunServer{recvErr: wantErr}
		profile, err := server.GetChunkedRequest(stream)
		if !errors.Is(err, wantErr) {
			t.Fatalf("GetChunkedRequest error = %v, want %v", err, wantErr)
		}
		if profile != nil {
			t.Fatalf("expected nil profile on error, got %+v", profile)
		}
	})

	t.Run("FifoRun", func(t *testing.T) {
		stream := &fakeMmfRunServer{recvErr: wantErr}
		err := fifo.NewWithLogger(logger).Run(stream)
		if !errors.Is(err, wantErr) {
			t.Fatalf("fifo.Run error = %v, want %v", err, wantErr)
		}
		if len(stream.sent) != 0 {
			t.Fatalf("fifo.Run sent %d responses on stream error, want 0", len(stream.sent))
		}
	})

	t.Run("DebugRun", func(t *testing.T) {
		stream := &fakeMmfRunServer{recvErr: wantErr}
		err := debug.NewWithLogger(logger).Run(stream)
		if !errors.Is(err, wantErr) {
			t.Fatalf("debug.Run error = %v, want %v", err, wantErr)
		}
		if len(stream.sent) != 0 {
			t.Fatalf("debug.Run sent %d responses on stream error, want 0", len(stream.sent))
		}
	})
}

// TestFifoRun_DeduplicatesOverlappingPoolsAndFormsFullAndBackfillMatches verifies:
//  1. Tickets appearing in multiple overlapping pools are deduplicated by ID so
//     the same ticket is never placed into a match twice.
//  2. Per-pool debug logs report the per-pool ticket count rather than the
//     cumulative ticket count across pools.
//  3. Full matches report score = 100 and log the exact ticket count (tixCount = 4
//     for a 2-roster x 2-player match, not 3).
//  4. Partial matches report score = 25 and attach an updated backfill
//     MmfRequest under extensions.MMFRequestKey.
func TestFifoRun_DeduplicatesOverlappingPoolsAndFormsFullAndBackfillMatches(t *testing.T) {
	var logBuf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&logBuf)
	logger.SetLevel(logrus.DebugLevel)

	bfTemplate, err := anypb.New(&pb.MmfRequest{
		Mmfs: []*pb.MatchmakingFunctionSpec{
			{Name: "fifo-mmf", Host: "127.0.0.1", Port: 50052, Type: pb.MatchmakingFunctionSpec_GRPC},
		},
	})
	if err != nil {
		t.Fatalf("anypb.New(MmfRequest): %v", err)
	}

	// Configure 2 rosters of desired length 2 (4 tickets per full match) and
	// minRosterLen = 1 (so 2 remaining tickets can form a partial match needing backfill).
	// Pool A has t-1..t-4 (4 tickets) and Pool B has t-3..t-6 (4 tickets), overlapping on t-3 and t-4
	// for 6 unique tickets total.
	stream := &fakeMmfRunServer{
		chunks: []*pb.ChunkedMmfRunRequest{
			{
				NumChunks: 1,
				Profile: &pb.Profile{
					Name: "com.example.fifo_dedup_and_backfill",
					Extensions: map[string]*anypb.Any{
						"desiredNumRosters":      mustAnyInt32(t, 2),
						"desiredRosterLen":       mustAnyInt32(t, 2),
						"minRosterLen":           mustAnyInt32(t, 1),
						extensions.MMFRequestKey: bfTemplate,
					},
					Pools: map[string]*pb.Pool{
						"pool_a": {
							Name: "pool_a",
							Participants: &pb.Roster{
								Name: "pool_a",
								Tickets: []*pb.Ticket{
									{Id: "1790000000000-1"},
									{Id: "1790000000000-2"},
									{Id: "1790000000000-3"},
									{Id: "1790000000000-4"},
								},
							},
						},
						"pool_b": {
							Name: "pool_b",
							Participants: &pb.Roster{
								Name: "pool_b",
								Tickets: []*pb.Ticket{
									{Id: "1790000000000-3"},
									{Id: "1790000000000-4"},
									{Id: "1790000000000-5"},
									{Id: "1790000000000-6"},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := fifo.NewWithLogger(logger).Run(stream); err != nil {
		t.Fatalf("fifo.Run returned unexpected error: %v", err)
	}

	// With 6 unique tickets, match 0 takes 4 tickets (full match, score 100) and
	// match 1 takes the remaining 2 tickets (partial match, score 25, with backfill).
	if len(stream.sent) != 2 {
		t.Fatalf("fifo.Run streamed %d matches, want 2", len(stream.sent))
	}

	seenAcrossMatches := make(map[string]int)
	for mIdx, resp := range stream.sent {
		m := resp.GetMatch()
		if m == nil {
			t.Fatalf("match %d is nil", mIdx)
		}
		for _, r := range m.GetRosters() {
			for _, tk := range r.GetTickets() {
				seenAcrossMatches[tk.GetId()]++
			}
		}
	}

	if len(seenAcrossMatches) != 6 {
		t.Fatalf("unique tickets across matches = %d, want 6 (%v)", len(seenAcrossMatches), seenAcrossMatches)
	}
	for id, count := range seenAcrossMatches {
		if count != 1 {
			t.Fatalf("ticket %s appeared %d times across matches, want 1 (overlapping pools must be deduplicated)", id, count)
		}
	}

	// Verify Match 0: full 4-ticket match with score = 100.
	match0 := stream.sent[0].GetMatch()
	score0, err := extensions.Int32(match0.GetExtensions(), "score")
	if err != nil || score0 != 100 {
		t.Fatalf("match 0 score = %d (err=%v), want 100", score0, err)
	}

	// Verify Match 1: partial 2-ticket match with score = 25 and backfill profile.
	match1 := stream.sent[1].GetMatch()
	score1, err := extensions.Int32(match1.GetExtensions(), "score")
	if err != nil || score1 != 25 {
		t.Fatalf("match 1 score = %d (err=%v), want 25", score1, err)
	}
	bfReq, err := extensions.MMFRequest(match1.GetExtensions(), extensions.MMFRequestKey)
	if err != nil || bfReq.GetProfile() == nil {
		t.Fatalf("expected backfill MmfRequest on partial match 1, got err=%v, bfReq=%+v", err, bfReq)
	}
	if !strings.HasPrefix(bfReq.GetProfile().GetName(), "backfill.") {
		t.Fatalf("backfill profile name = %q, want prefix 'backfill.'", bfReq.GetProfile().GetName())
	}
	bfDesiredLen, err := extensions.Int32(bfReq.GetProfile().GetExtensions(), "desiredRosterLen")
	if err != nil || bfDesiredLen != 1 {
		t.Fatalf("backfill desiredRosterLen = %d (err=%v), want 1", bfDesiredLen, err)
	}

	// Verify debug logs report per-pool ticket count (4 in pool_a, 4 in pool_b),
	// 6 unique tickets total, and "with roster of 4 tickets" for the full match.
	logs := logBuf.String()
	if !strings.Contains(logs, "Found 4 tickets in pool pool_a") || !strings.Contains(logs, "Found 4 tickets in pool pool_b") {
		t.Fatalf("expected per-pool ticket counts (4 in pool_a, 4 in pool_b) in logs, got:\n%s", logs)
	}
	if !strings.Contains(logs, "Matching among 6 tickets from 2 provided pools") {
		t.Fatalf("expected 'Matching among 6 tickets from 2 provided pools' in logs, got:\n%s", logs)
	}
	if !strings.Contains(logs, "with roster of 4 tickets") {
		t.Fatalf("expected full match completion log 'with roster of 4 tickets', got:\n%s", logs)
	}
}

// TestDebugRun_StreamsDiagnosticMatchWithMinInt32Score verifies that debug.Run
// streams a single empty diagnostic match with score == math.MinInt32 and
// preserves profileName and mmfName extensions without mutating the input profile.
func TestDebugRun_StreamsDiagnosticMatchWithMinInt32Score(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)

	stream := &fakeMmfRunServer{
		chunks: []*pb.ChunkedMmfRunRequest{
			{
				NumChunks: 1,
				Profile: &pb.Profile{
					Name: "com.example.debug_profile",
					Pools: map[string]*pb.Pool{
						"all": {
							Name: "all",
							Participants: &pb.Roster{
								Name:    "all",
								Tickets: []*pb.Ticket{{Id: "1790000000000-1"}},
							},
						},
					},
				},
			},
		},
	}

	if err := debug.NewWithLogger(logger).Run(stream); err != nil {
		t.Fatalf("debug.Run returned unexpected error: %v", err)
	}
	if len(stream.sent) != 1 {
		t.Fatalf("debug.Run streamed %d matches, want 1", len(stream.sent))
	}

	m := stream.sent[0].GetMatch()
	if len(m.GetRosters()) != 0 {
		t.Fatalf("debug match rosters len = %d, want 0", len(m.GetRosters()))
	}
	score, err := extensions.Int32(m.GetExtensions(), "score")
	if err != nil || score != math.MinInt32 {
		t.Fatalf("debug match score = %d (err=%v), want %d (math.MinInt32)", score, err, math.MinInt32)
	}
	profName, err := extensions.String(m.GetExtensions(), "profileName")
	if err != nil || profName != "com.example.debug_profile" {
		t.Fatalf("debug match profileName = %q (err=%v), want %q", profName, err, "com.example.debug_profile")
	}
}
