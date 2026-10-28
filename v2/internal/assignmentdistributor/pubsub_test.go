// Copyright 2025 Google LLC
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
package assignmentdistributor

import (
	"context"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/googleforgames/open-match2/v2/pkg/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// A message that isn't a Roster is acked (discarded) rather than nacked, so
// Pub/Sub doesn't redeliver it in a loop; valid rosters still reach the
// handler. Uses the in-process Pub/Sub fake via PUBSUB_EMULATOR_HOST.
func TestPubSubSubscriber_AcksUnparseableMessages(t *testing.T) {
	const project, topic = "test-project", "assignments"
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", srv.Addr)

	topicName := "projects/" + project + "/topics/" + topic
	if _, err := srv.GServer.CreateTopic(context.Background(), &pubsubpb.Topic{Name: topicName}); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	log := logrus.New()
	log.SetOutput(io.Discard)
	sub, err := NewPubSubSubscriber(project, topic, log)
	if err != nil {
		t.Fatalf("NewPubSubSubscriber: %v", err)
	}
	defer sub.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan string, 10)
	recvDone := make(chan error, 1)
	go func() {
		recvDone <- sub.Receive(ctx, func(_ context.Context, roster *pb.Roster) {
			received <- roster.GetName()
		})
	}()

	// Invalid protobuf wire data: field 1 claims more bytes than follow.
	poisonID := srv.Publish(topicName, []byte{0x0a, 0x05, 'x'}, nil)
	valid, err := proto.Marshal(&pb.Roster{Name: "valid-roster"})
	if err != nil {
		t.Fatalf("proto.Marshal failed: %v", err)
	}
	srv.Publish(topicName, valid, nil)

	select {
	case name := <-received:
		if name != "valid-roster" {
			t.Fatalf("handler got roster %q, want valid-roster", name)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the valid roster")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		m := srv.Message(poisonID)
		if m != nil && m.Acks > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unparseable message was not acked: %+v", m)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-recvDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Receive did not return after ctx was cancelled")
	}
}

// Missing configuration is reported as an error (and logged), not by exiting
// the process, so callers can shut down cleanly.
func TestPubSubConstructors_ReturnErrorOnMissingConfig(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	if pub, err := NewPubSubPublisher("", "topic", log); err == nil || pub != nil {
		t.Fatalf("NewPubSubPublisher with no project = (%v, %v), want (nil, error)", pub, err)
	}
	if sub, err := NewPubSubSubscriber("project", "", log); err == nil || sub != nil {
		t.Fatalf("NewPubSubSubscriber with no topic = (%v, %v), want (nil, error)", sub, err)
	}
}
