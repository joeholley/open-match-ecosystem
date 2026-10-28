// Copyright 2019 Google LLC
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

// Package server contains golang implementations for common matchmaking
// function gRPC server functionality, like starting up the server, or parsing
// the chunked profile format sent by om-core.
//
//   - The MMF Server is defined in the proto/mmf.proto file. If you want to
//     write your MMFs in golang, you can use the protoc-generated grpc server
//     module in pkg/pb/mmf*.go. If you want to write it in a different
//     language, consult the protoc documentation for instructions on generating
//     grpc server source files in your language of choice.
//   - This package doesn't include the matchmaking function itself.  You pass
//     in a Server struct when calling StartServer that includes your MMF Run()
//     implementation.
package server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	// grpcMessageSoftLimitBytes is the 4 MiB soft limit on the size of any
	// single gRPC message, in either direction. 4 MiB is gRPC's default
	// maximum received message size, and some middleware layers that gRPC
	// traffic passes through (proxies, service meshes, load balancers)
	// enforce the same limit and silently discard larger messages. So rather
	// than raising limits, everything in Open Match stays under it: om-core
	// splits the profile it sends to an MMF into chunks that each fit (see
	// GetChunkedRequest), and an MMF must keep every match it sends back
	// under it too. The server sets it explicitly (it's also gRPC's default)
	// so the limit is visible here and doesn't change if gRPC's defaults do.
	grpcMessageSoftLimitBytes = 4 * 1024 * 1024

	// GracefulStop waits for every in-flight Run() to finish. A Run() that
	// never returns would block shutdown indefinitely, and Cloud Run sends
	// SIGKILL ~10s after SIGTERM, so give in-flight streams a bounded
	// window to finish and then force-close whatever is left.
	gracefulStopTimeout = 8 * time.Second
)

// logfields are added to every log line written by this package.
var logfields = logrus.Fields{
	"app":       "matchmaker",
	"component": "matchmaking_function",
	"function":  "server",
}

// StartServer creates and starts the Match Function server. It blocks until
// the server stops. See StartServerWithContext.
func StartServer(port int32, mmfServer pb.MatchMakingFunctionServiceServer, l *logrus.Logger) error {
	return StartServerWithContext(context.Background(), port, mmfServer, l)
}

// StartServerWithContext creates and starts the Match Function server and
// blocks until it stops. When ctx is done, the server stops accepting new
// streams and gives in-flight Run() calls up to gracefulStopTimeout to finish
// before closing them. If l is nil, a new logrus.Logger with default settings
// is used.
//
// Each call uses its own logger, so it is safe to start several servers in
// one process (the example matchmaker runs fifo and debug MMFs in-process).
func StartServerWithContext(ctx context.Context, port int32, mmfServer pb.MatchMakingFunctionServiceServer, l *logrus.Logger) error {
	if l == nil {
		l = logrus.New()
	}
	logger := l.WithFields(logfields).WithField("port", port)

	// Create and host a new gRPC service on the requested port.
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logger.Errorf("TCP net listener initialization failed for port %v, got %s", port, err.Error())
		return err
	}

	logger.Infof("MMF gRPC Server starting on TCP port %v", port)
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(grpcMessageSoftLimitBytes),
		grpc.MaxSendMsgSize(grpcMessageSoftLimitBytes),
	)
	pb.RegisterMatchMakingFunctionServiceServer(server, mmfServer)

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			logger.Infof("Shutting down MMF gRPC Server gracefully (up to %v)", gracefulStopTimeout)
			stopped := make(chan struct{})
			go func() {
				server.GracefulStop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(gracefulStopTimeout):
				logger.Warnf("MMF gRPC Server did not stop within %v, closing remaining streams", gracefulStopTimeout)
				server.Stop()
			}
		case <-done:
		}
	}()

	// Serve returns nil once GracefulStop or Stop has been called.
	if err = server.Serve(ln); err != nil {
		logger.Errorf("gRPC serve failed, got %s", err.Error())
		return err
	}

	return nil
}

// BackfillProfilePrefix starts the name of every backfill profile an MMF
// creates. Backfill profile names have the form
// "backfill.<base profile name>.<unique suffix>", and the game server
// director recognizes backfill profiles by this prefix.
const BackfillProfilePrefix = "backfill."

// BaseProfileName returns the stable part of a profile name, suitable for
// building match IDs and backfill profile names whose prefix (everything
// before the last dot) has bounded cardinality.
//
// By convention profile names use reverse-DNS notation with the unique part
// (timestamp, counter, ...) after the last dot, e.g. the director names its
// profiles "dev.open-match.<fleet>.<mode>.<unixnano>". This strips that last
// segment, and one leading BackfillProfilePrefix, so a backfill of a backfill doesn't
// keep growing its name:
//
//	"dev.open-match.fleet.mode.1730000000"         -> "dev.open-match.fleet.mode"
//	"backfill.dev.open-match.fleet.mode.1730000-3" -> "dev.open-match.fleet.mode"
//	"my-profile" (no dot)                          -> "my-profile"
func BaseProfileName(name string) string {
	name = strings.TrimPrefix(name, BackfillProfilePrefix)
	if i := strings.LastIndex(name, "."); i > 0 {
		name = name[:i]
	}
	return name
}

// GetChunkedRequest receives a MMF Server request stream sending partial
// 'chunks' of a profile and re-assembles it.
//
// Why chunks exist: every gRPC message in Open Match must stay under a 4 MiB
// soft limit (grpcMessageSoftLimitBytes). That's gRPC's default maximum
// message size, and some middleware layers that gRPC traffic passes through
// silently discard larger messages, so the limit can't simply be raised. A
// profile whose pools hold many tickets can be far larger than 4 MiB, so
// om-core splits it into several ChunkedMmfRunRequest messages, each sized
// (from the actual serialized protobuf) to fit under the limit, and streams
// them to the MMF. Each 'chunk' contains the complete profile except for the
// pools' tickets, plus a portion of those tickets; this function concatenates
// the tickets from every chunk to rebuild the full pools.
//
// It returns once all chunks have arrived. If the stream fails before then,
// it returns the error and discards any chunks already received, so the MMF
// never runs against an incomplete set of tickets.
//
// The caller passes in its own logger (typically the MMF's logger) so this
// helper's debug output carries the MMF's log fields.
func GetChunkedRequest(stream pb.MatchMakingFunctionService_RunServer, logger *logrus.Entry) (*pb.Profile, error) {
	// Loop until we've received the number of chunks specified in the
	// pb.ChunkedMmfRunRequest message, or the stream returns an error
	// (including io.EOF if om-core closes it early).
	pools := make(map[string]*pb.Pool)
	for i := int32(1); ; i++ {

		// Recv() returns a pb.ChunkedMmfRunRequest, which contains:
		// - A full copy of the profile /except/ the pools.
		//   - A portion of the pools with some of the participating tickets
		// - The total number of chunks for this request. Chunks are unordered.
		in, err := stream.Recv()
		if err != nil {
			return nil, err
		}

		logger.Debugf("Processing chunk %02d/%02d", i, in.GetNumChunks())
		for name, pool := range in.GetProfile().GetPools() {
			logger.Debugf("concatenating pool %v", name)
			if _, ok := pools[name]; !ok {
				rosterName := pool.GetParticipants().GetName()
				if rosterName == "" {
					rosterName = name
				}
				srcTickets := pool.GetParticipants().GetTickets()
				copiedTickets := make([]*pb.Ticket, len(srcTickets))
				copy(copiedTickets, srcTickets)

				// First chunk containing this pool; initialize the local copy.
				pools[name] = &pb.Pool{
					Name:                    pool.GetName(),
					TagPresentFilters:       pool.GetTagPresentFilters(),
					StringEqualsFilters:     pool.GetStringEqualsFilters(),
					DoubleRangeFilters:      pool.GetDoubleRangeFilters(),
					CreationTimeRangeFilter: pool.GetCreationTimeRangeFilter(),
					Extensions:              pool.GetExtensions(),
					Participants: &pb.Roster{
						Name:       rosterName,
						Tickets:    copiedTickets,
						Extensions: pool.GetParticipants().GetExtensions(),
					},
				}
			} else {
				// concatenate pools split among multiple chunks
				if pools[name].Participants == nil {
					rosterName := pool.GetParticipants().GetName()
					if rosterName == "" {
						rosterName = name
					}
					pools[name].Participants = &pb.Roster{
						Name:       rosterName,
						Extensions: pool.GetParticipants().GetExtensions(),
					}
				}
				pools[name].Participants.Tickets = append(pools[name].Participants.Tickets, pool.GetParticipants().GetTickets()...)
			}
		}

		// NOTE: om-core sends every chunk before it starts reading matches,
		// so in practice this always receives them all. If you'd rather run
		// with a partial profile than fail (e.g. after a timeout), this is
		// where you'd add that condition.
		if i >= in.GetNumChunks() {
			// Read the rest of the request profile from the last chunk.
			// (All chunks contain only a portion of the pools, but a
			// complete copy of everything else in the profile.)
			logger.Debugf("Finished receiving %v chunks of MMF profile %v", i, in.GetProfile().GetName())
			return &pb.Profile{
				Name:       in.GetProfile().GetName(),
				Pools:      pools,
				Extensions: in.GetProfile().GetExtensions(),
			}, nil
		}
	}
}
