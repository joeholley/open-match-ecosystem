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
	"testing"
	"time"

	pb "github.com/googleforgames/open-match2/v2/pkg/pb"
)

func TestBuildSegmentedFleetConfig(t *testing.T) {
	buildSegmentedFleetConfig([]*pb.MatchmakingFunctionSpec{mmfFifo})

	if len(gameModesInZone) == 0 || len(zonePools) != len(gameModesInZone) {
		t.Fatalf("expected one zone pool per zone/tier game mode, got %d pools and %d game modes", len(zonePools), len(gameModesInZone))
	}
	for key, modes := range gameModesInZone {
		if len(modes) != 1 {
			t.Fatalf("%s: expected 1 game mode, got %d", key, len(modes))
		}
		gm := modes[0]
		if len(gm.MMFs) != 1 || gm.MMFs[0] != mmfFifo {
			t.Errorf("%s: expected only the FIFO MMF, got %v", key, gm.MMFs)
		}
		pool := gm.Pools["mmr_tier"]
		if pool == nil {
			t.Fatalf("%s: missing mmr_tier pool", key)
		}
		// Tier pools must not carry a creation time window: a window
		// captured at startup eventually stops matching new tickets.
		if pool.GetCreationTimeRangeFilter() != nil {
			t.Errorf("%s: tier pool has a CreationTimeRangeFilter: %v", key, pool.GetCreationTimeRangeFilter())
		}
	}
}

func TestWaitWithTimeout(t *testing.T) {
	if !waitWithTimeout(time.Second, func() {}) {
		t.Error("expected a function that returns immediately to finish in time")
	}
	block := make(chan struct{})
	defer close(block)
	if waitWithTimeout(10*time.Millisecond, func() { <-block }) {
		t.Error("expected a blocked function to time out")
	}
}
