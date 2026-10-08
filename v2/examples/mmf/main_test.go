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
	"io"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/functions/debug"
	"open-match.dev/open-match-ecosystem/v2/examples/mmf/functions/fifo"
)

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
