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

package gameclient

import (
	"context"
	"testing"
)

func TestSimple(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		ticket := Simple(ctx)
		if ticket == nil {
			t.Fatalf("iteration %d: expected non-nil ticket", i)
		}
		attrs := ticket.GetAttributes()
		if attrs == nil {
			t.Fatalf("iteration %d: expected non-nil ticket attributes", i)
		}

		// The queue, not the client, sets CreationTime; no pool uses tags.
		if attrs.GetCreationTime() != nil {
			t.Fatalf("iteration %d: got CreationTime %v, want unset (set by the queue)", i, attrs.GetCreationTime())
		}
		if tags := attrs.GetTags(); len(tags) != 0 {
			t.Fatalf("iteration %d: got tags %v, want none", i, tags)
		}

		mmr, ok := attrs.GetDoubleArgs()["mmr"]
		if !ok {
			t.Fatalf("iteration %d: missing mmr in DoubleArgs", i)
		}
		if mmr < 500 || mmr > 2750 {
			t.Fatalf("iteration %d: got mmr %v, want in [500, 2750]", i, mmr)
		}
		hasZonePing := false
		for k, v := range attrs.GetDoubleArgs() {
			if len(k) > 5 && k[:5] == "ping." && v >= 20 && v <= 100 {
				hasZonePing = true
				break
			}
		}
		if !hasZonePing {
			t.Fatalf("iteration %d: expected at least one ping.<zone> in [20, 100], got %v", i, attrs.GetDoubleArgs())
		}

		class, ok := attrs.GetStringArgs()["class"]
		if !ok {
			t.Fatalf("iteration %d: missing class in StringArgs", i)
		}
		switch class {
		case "tank", "healer", "dps":
		default:
			t.Fatalf("iteration %d: unexpected class %q, want tank, healer, or dps", i, class)
		}
	}
}
