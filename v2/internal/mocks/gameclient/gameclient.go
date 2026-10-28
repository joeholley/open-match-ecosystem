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
//
// In this game client mocking module, the 'game client' sends an Open match
// protobuf `ticket` message directly. However, in a real game client, you'd
// probably use whatever communication protocol/format (JSON string, binary
// encoding, custom format, etc) your game engine or dev kit encourages, and
// parse it into the Open Match protobuf `ticket` protobuf in your matchmaker
// queue.

package gameclient

import (
	"context"
	"math"
	"math/rand/v2"
	// Register pprof HTTP handlers for profiling (revive: blank-imports).
	// revive:disable-next-line:blank-imports
	_ "net/http/pprof"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
)

type weightedZone struct {
	zone          string
	cumWeight     int
	neighborZones []string
	minOffsetMs   int
	offsetSpanMs  int
}

// realWorldZones models global multiplayer population distribution across the 11
// datacenter zones in gsdirector.FleetConfig (APAC 44%, EMEA 28%, NA 17%, SA 8%, Africa 3%)
// and intra-continental fiber latency offsets to physically proximate neighbor zones.
var realWorldZones = []weightedZone{
	{zone: "asia-northeast3-b", cumWeight: 18, neighborZones: []string{"asia-northeast1-a"}, minOffsetMs: 74, offsetSpanMs: 60},           // KR_Seoul (18%)
	{zone: "europe-west3-a", cumWeight: 34, neighborZones: []string{"europe-central2-b"}, minOffsetMs: 72, offsetSpanMs: 60},              // DE_Frankfurt (16%)
	{zone: "asia-northeast1-a", cumWeight: 49, neighborZones: []string{"asia-northeast3-b"}, minOffsetMs: 74, offsetSpanMs: 60},           // JP_Tokyo (15%)
	{zone: "us-east4-b", cumWeight: 60, neighborZones: []string{"us-west2-c"}, minOffsetMs: 76, offsetSpanMs: 60},                         // US_Ashburn (11%)
	{zone: "europe-central2-b", cumWeight: 69, neighborZones: []string{"europe-west3-a"}, minOffsetMs: 72, offsetSpanMs: 60},              // PL_Warsaw (9%)
	{zone: "southamerica-east1-c", cumWeight: 77, neighborZones: nil, minOffsetMs: 0, offsetSpanMs: 0},                                    // BR_SaoPaulo (8%)
	{zone: "asia-south2-c", cumWeight: 84, neighborZones: []string{"me-central2-a"}, minOffsetMs: 76, offsetSpanMs: 60},                   // IN_Delhi (7%)
	{zone: "us-west2-c", cumWeight: 90, neighborZones: []string{"us-east4-b"}, minOffsetMs: 76, offsetSpanMs: 60},                         // US_LosAngeles (6%)
	{zone: "australia-southeast1-c", cumWeight: 94, neighborZones: nil, minOffsetMs: 0, offsetSpanMs: 0},                                  // AU_Sydney (4%)
	{zone: "me-central2-a", cumWeight: 97, neighborZones: []string{"europe-west3-a", "asia-south2-c"}, minOffsetMs: 76, offsetSpanMs: 60}, // SA_Dammam (3%)
	{zone: "africa-south1-b", cumWeight: 100, neighborZones: nil, minOffsetMs: 0, offsetSpanMs: 0},                                        // ZA_Johannesburg (3%)
}

// Simple generates a mock game client matchmaking request with realistic regional
// latency and Gaussian MMR skill attributes.
func Simple(ctx context.Context) *pb.Ticket {

	// Select home zone using weighted regional population distribution (0..99).
	roll := rand.IntN(100)
	selected := realWorldZones[0]
	for _, wz := range realWorldZones {
		if roll < wz.cumWeight {
			selected = wz
			break
		}
	}

	// Home zone ping between 20ms and 85ms (always within the <= 100ms ZonePool filter).
	homePing := float64(20 + rand.IntN(66))
	doubleArgs := map[string]float64{
		"ping." + selected.zone: homePing,
	}

	// Populate intra-continental neighbor zone ping(s) so border players (~20-35% in
	// connected regions) have <= 100ms ping to an adjacent regional datacenter.
	for _, nz := range selected.neighborZones {
		neighborPing := homePing + float64(selected.minOffsetMs+rand.IntN(selected.offsetSpanMs))
		doubleArgs["ping."+nz] = neighborPing
	}

	// Generate Gaussian Elo/MMR skill rating: N(mean=1500, sigma=250), clamped to [500, 2750].
	mmr := math.Round(rand.NormFloat64()*250.0 + 1500.0)
	if mmr < 500 {
		mmr = 500
	} else if mmr > 2750 {
		mmr = 2750
	}
	doubleArgs["mmr"] = mmr

	// No CreationTime or Tags: the matchmaking queue sets the creation time
	// (the platform decides when a player entered matchmaking), and none of
	// the example pools filter on tags.
	ticket := &pb.Ticket{
		Attributes: &pb.Ticket_FilterableData{
			// Make mock game client attributes.
			DoubleArgs: doubleArgs,
		},
	}

	// Matching Attribute example: this fake game type needs to match based
	// on what character the player has chosen.
	//
	// For this mock, randomly select a character archetype this fake player
	// wants to play
	var selectedClass string
	switch rand.IntN(5) + 1 { // 1d5
	case 1:
		selectedClass = "tank"
	case 2:
		selectedClass = "healer"
	default: // 60% of players will select dps
		selectedClass = "dps"
	}
	ticket.Attributes.StringArgs = map[string]string{"class": selectedClass}

	return ticket
}
