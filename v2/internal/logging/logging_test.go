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

package logging

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// The shared logger writes at Info level even before NewSharedLogger runs.
func TestSharedLoggerUsableBeforeInit(t *testing.T) {
	l := GetSharedLogger()
	if l.Level != logrus.InfoLevel {
		t.Fatalf("default level = %v, want info", l.Level)
	}
	if _, ok := l.Formatter.(*logrus.JSONFormatter); !ok {
		t.Fatalf("default formatter = %T, want *logrus.JSONFormatter", l.Formatter)
	}

	var buf bytes.Buffer
	orig := l.Out
	l.SetOutput(&buf)
	defer l.SetOutput(orig)
	l.Info("early message")
	if !strings.Contains(buf.String(), "early message") {
		t.Fatalf("expected early Info message to be written, got %q", buf.String())
	}
}

func TestNewSharedLoggerAppliesConfig(t *testing.T) {
	l := GetSharedLogger()
	var buf bytes.Buffer
	orig := l.Out
	l.SetOutput(&buf)
	defer func() {
		l.SetOutput(orig)
		l.SetFormatter(cloudRunJSONFormatter())
		l.SetLevel(logrus.InfoLevel)
	}()

	cfg := viper.New()
	cfg.Set("LOGGING_FORMAT", "text")
	cfg.Set("LOGGING_LEVEL", "debug")
	if got := NewSharedLogger(cfg); got != l {
		t.Fatal("NewSharedLogger should configure and return the shared logger")
	}
	if l.Level != logrus.DebugLevel {
		t.Fatalf("level = %v, want debug", l.Level)
	}
	if _, ok := l.Formatter.(*logrus.TextFormatter); !ok {
		t.Fatalf("formatter = %T, want *logrus.TextFormatter", l.Formatter)
	}
	if !strings.Contains(buf.String(), "format text") {
		t.Fatalf("expected the configuration message to be logged, got %q", buf.String())
	}
}
