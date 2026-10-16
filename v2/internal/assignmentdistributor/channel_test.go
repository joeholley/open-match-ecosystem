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
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/googleforgames/open-match2/v2/pkg/pb"
)

// TestChannelSenderAndReceiver_BlockingReceiveAndDelivery verifies that
// ChannelReceiver.Receive blocks on the caller's goroutine (matching
// PubSubSubscriber.Receive), invokes the handler synchronously for each roster
// sent by ChannelSender, and returns cleanly when sender.Stop(), receiver.Stop(),
// or ctx cancellation occurs.
func TestChannelSenderAndReceiver_BlockingReceiveAndDelivery(t *testing.T) {
	t.Run("Receive blocks until sender.Stop closes channel after delivering rosters", func(t *testing.T) {
		ch := make(chan *pb.Roster, 4)
		sender := NewChannelSender(ch)
		receiver := NewChannelReceiver(ch)

		var (
			mu       sync.Mutex
			gotNames []string
		)
		recvDone := make(chan error, 1)
		go func() {
			recvDone <- receiver.Receive(context.Background(), func(_ context.Context, roster *pb.Roster) {
				mu.Lock()
				gotNames = append(gotNames, roster.GetName())
				mu.Unlock()
			})
		}()

		// Confirm Receive is blocking while channel is open.
		select {
		case err := <-recvDone:
			t.Fatalf("Receive returned prematurely before Stop/cancel: %v", err)
		case <-time.After(15 * time.Millisecond):
		}

		for _, name := range []string{"roster-1", "roster-2", "roster-3"} {
			if err := sender.Send(context.Background(), &pb.Roster{Name: name}); err != nil {
				t.Fatalf("sender.Send(%q) failed: %v", name, err)
			}
		}

		// Stop the sender (closing ch) and verify Receive drains all 3 rosters and returns nil.
		sender.Stop()

		select {
		case err := <-recvDone:
			if err != nil {
				t.Fatalf("Receive returned unexpected error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Receive to return after sender.Stop()")
		}

		mu.Lock()
		defer mu.Unlock()
		wantNames := []string{"roster-1", "roster-2", "roster-3"}
		if !slices.Equal(gotNames, wantNames) {
			t.Errorf("received rosters = %v, want %v", gotNames, wantNames)
		}
	})

	t.Run("Receive unblocks on receiver.Stop() and multiple Stop() calls are idempotent", func(t *testing.T) {
		ch := make(chan *pb.Roster)
		receiver := NewChannelReceiver(ch)

		recvDone := make(chan error, 1)
		go func() {
			recvDone <- receiver.Receive(context.Background(), func(context.Context, *pb.Roster) {})
		}()

		receiver.Stop()
		receiver.Stop() // Verify idempotency.

		select {
		case err := <-recvDone:
			if err != nil {
				t.Fatalf("Receive returned error on Stop: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for Receive to return after receiver.Stop()")
		}
	})
}

// TestChannelSender_ContextCancellation verifies that ChannelSender.Send checks
// ctx.Err() before sending and selects on ctx.Done() when blocked on a full or
// unbuffered channel.
func TestChannelSender_ContextCancellation(t *testing.T) {
	t.Run("pre-cancelled context returns context.Canceled without sending", func(t *testing.T) {
		ch := make(chan *pb.Roster, 4)
		sender := NewChannelSender(ch)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := sender.Send(ctx, &pb.Roster{Name: "should-not-send"})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send with pre-cancelled ctx error = %v, want context.Canceled", err)
		}
		if len(ch) != 0 {
			t.Errorf("expected 0 items in channel after cancelled Send, got %d", len(ch))
		}
	})

	t.Run("in-flight Send blocked on unbuffered channel unblocks on ctx cancel", func(t *testing.T) {
		ch := make(chan *pb.Roster) // Unbuffered with no reader.
		sender := NewChannelSender(ch)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- sender.Send(ctx, &pb.Roster{Name: "blocked-roster"})
		}()

		time.Sleep(15 * time.Millisecond)
		cancel()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Send after in-flight ctx cancel error = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for blocked Send to unblock on ctx cancel")
		}
	})
}

// TestChannelSender_StopUnblocksAndPreventsSendOnClosedChannel verifies that:
//  1. Calling Stop() while a Send() goroutine is blocked on a full/unbuffered
//     channel unblocks Send() immediately with ErrSenderStopped (without
//     deadlocking on RWMutex).
//  2. Concurrent Send() and Stop() calls across many goroutines never panic
//     with "send on closed channel" or "close of closed channel".
func TestChannelSender_StopUnblocksAndPreventsSendOnClosedChannel(t *testing.T) {
	t.Run("Stop unblocks in-flight Send blocked on full channel without deadlock", func(t *testing.T) {
		ch := make(chan *pb.Roster) // Unbuffered with no reader.
		sender := NewChannelSender(ch)

		errCh := make(chan error, 1)
		go func() {
			errCh <- sender.Send(context.Background(), &pb.Roster{Name: "blocked-on-full"})
		}()

		time.Sleep(15 * time.Millisecond)

		stopDone := make(chan struct{})
		go func() {
			defer close(stopDone)
			sender.Stop()
			sender.Stop() // Verify idempotency.
		}()

		select {
		case <-stopDone:
		case <-time.After(2 * time.Second):
			t.Fatal("sender.Stop() deadlocked waiting for blocked Send()")
		}

		select {
		case err := <-errCh:
			if !errors.Is(err, ErrSenderStopped) {
				t.Fatalf("blocked Send error after Stop = %v, want ErrSenderStopped", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for blocked Send to return after Stop()")
		}

		// Subsequent Send after Stop must also return ErrSenderStopped cleanly.
		if err := sender.Send(context.Background(), &pb.Roster{Name: "post-stop"}); !errors.Is(err, ErrSenderStopped) {
			t.Fatalf("post-Stop Send error = %v, want ErrSenderStopped", err)
		}
	})

	t.Run("concurrent Send and Stop stress test never panics", func(t *testing.T) {
		for range 25 {
			ch := make(chan *pb.Roster, 2)
			sender := NewChannelSender(ch)
			receiver := NewChannelReceiver(ch)

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup

			// Start receiver goroutine.
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = receiver.Receive(ctx, func(context.Context, *pb.Roster) {})
			}()

			// Start 16 concurrent sender goroutines racing against Stop().
			for range 16 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 20 {
						err := sender.Send(ctx, &pb.Roster{Name: "stress"})
						if err != nil && !errors.Is(err, ErrSenderStopped) && !errors.Is(err, context.Canceled) {
							t.Errorf("unexpected Send error: %v", err)
							return
						}
					}
				}()
			}

			// Trigger Stop() and cancel() concurrently with active senders.
			wg.Add(2)
			go func() {
				defer wg.Done()
				sender.Stop()
			}()
			go func() {
				defer wg.Done()
				receiver.Stop()
				cancel()
			}()

			wg.Wait()
		}
	})
}
