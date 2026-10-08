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

package extensions

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	knownpb "google.golang.org/protobuf/types/known/wrapperspb"
)

func TestBool(t *testing.T) {
	t.Run("round-trip true and false with MatchDeactivationTimeoutKey", func(t *testing.T) {
		for _, want := range []bool{true, false} {
			valAny, err := anypb.New(&knownpb.BoolValue{Value: want})
			if err != nil {
				t.Fatalf("anypb.New(BoolValue(%v)) failed: %v", want, err)
			}
			ex := map[string]*anypb.Any{
				MatchDeactivationTimeoutKey: valAny,
			}

			got, err := Bool(ex, MatchDeactivationTimeoutKey)
			if err != nil {
				t.Fatalf("Bool(%q) returned unexpected error: %v", MatchDeactivationTimeoutKey, err)
			}
			if got != want {
				t.Errorf("Bool(%q) = %v, want %v", MatchDeactivationTimeoutKey, got, want)
			}
		}
	})

	t.Run("missing key returns NoSuchKeyError", func(t *testing.T) {
		got, err := Bool(map[string]*anypb.Any{}, MatchDeactivationTimeoutKey)
		if !errors.Is(err, NoSuchKeyError) {
			t.Errorf("Bool(missing key) error = %v, want %v", err, NoSuchKeyError)
		}
		if got {
			t.Errorf("Bool(missing key) = %v, want false", got)
		}

		gotNil, errNil := Bool(nil, MatchDeactivationTimeoutKey)
		if !errors.Is(errNil, NoSuchKeyError) {
			t.Errorf("Bool(nil map) error = %v, want %v", errNil, NoSuchKeyError)
		}
		if gotNil {
			t.Errorf("Bool(nil map) = %v, want false", gotNil)
		}
	})

	t.Run("wrong Any payload type returns unmarshal error", func(t *testing.T) {
		wrongAny, err := anypb.New(&knownpb.StringValue{Value: "not-a-bool"})
		if err != nil {
			t.Fatalf("anypb.New(StringValue) failed: %v", err)
		}
		ex := map[string]*anypb.Any{
			MatchDeactivationTimeoutKey: wrongAny,
		}

		got, err := Bool(ex, MatchDeactivationTimeoutKey)
		if err == nil {
			t.Fatalf("Bool(wrong payload type) error = nil, want non-nil unmarshal error")
		}
		if errors.Is(err, NoSuchKeyError) {
			t.Errorf("Bool(wrong payload type) error = %v, want unmarshal error (not NoSuchKeyError)", err)
		}
		if got {
			t.Errorf("Bool(wrong payload type) = %v, want false", got)
		}
	})
}
