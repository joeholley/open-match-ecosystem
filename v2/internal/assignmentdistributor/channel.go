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
	"sync"

	"github.com/googleforgames/open-match2/v2/pkg/pb"
)

// ErrSenderStopped is returned by ChannelSender.Send when the sender has been stopped.
var ErrSenderStopped = errors.New("assignment channel sender is stopped")

// ChannelSender sends assignments to a Go channel and coordinates safe shutdown
// so concurrent Send calls never panic with "send on closed channel".
type ChannelSender struct {
	mu              sync.RWMutex
	initOnce        sync.Once
	stopOnce        sync.Once
	done            chan struct{}
	stopped         bool
	assignmentsChan chan<- *pb.Roster
}

func NewChannelSender(ch chan<- *pb.Roster) *ChannelSender {
	p := &ChannelSender{assignmentsChan: ch}
	p.ensureInit()
	return p
}

func (p *ChannelSender) ensureInit() {
	p.initOnce.Do(func() {
		p.done = make(chan struct{})
	})
}

// Send delivers roster to assignmentsChan, respecting ctx cancellation and Stop().
// Holding RLock across the channel send guarantees Stop() cannot close
// assignmentsChan while a send is in flight, while selecting on p.done ensures
// a Send blocked on a full channel unblocks immediately when Stop() is called.
func (p *ChannelSender) Send(ctx context.Context, roster *pb.Roster) error {
	p.ensureInit()
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.stopped {
		return ErrSenderStopped
	}
	select {
	case <-p.done:
		return ErrSenderStopped
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return ErrSenderStopped
	case p.assignmentsChan <- roster:
		return nil
	}
}

// Stop marks the sender as stopped and closes assignmentsChan once all in-flight
// Send calls have released RLock. Safe to call multiple times.
func (p *ChannelSender) Stop() {
	p.ensureInit()
	p.stopOnce.Do(func() {
		// Close p.done before acquiring the write lock so any concurrent Send()
		// blocked on a full assignmentsChan while holding RLock unblocks
		// immediately and releases RLock without deadlocking.
		close(p.done)

		p.mu.Lock()
		defer p.mu.Unlock()
		p.stopped = true
		if p.assignmentsChan != nil {
			close(p.assignmentsChan)
		}
	})
}

// ChannelReceiver receives assignments from a Go channel.
type ChannelReceiver struct {
	initOnce        sync.Once
	stopOnce        sync.Once
	done            chan struct{}
	assignmentsChan <-chan *pb.Roster
}

func NewChannelReceiver(ch <-chan *pb.Roster) *ChannelReceiver {
	r := &ChannelReceiver{assignmentsChan: ch}
	r.ensureInit()
	return r
}

func (r *ChannelReceiver) ensureInit() {
	r.initOnce.Do(func() {
		r.done = make(chan struct{})
	})
}

// Receive blocks on the caller's goroutine (matching PubSubSubscriber.Receive)
// and invokes handler for each incoming roster until ctx is cancelled, Stop()
// is called, or assignmentsChan is closed. Blocking on the caller's goroutine
// ensures MatchmakerQueue.Run's WaitGroup waits for in-flight assignment
// handling to finish before Run returns.
func (r *ChannelReceiver) Receive(ctx context.Context, handler func(ctx context.Context, roster *pb.Roster)) error {
	r.ensureInit()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.done:
			return nil
		case roster, ok := <-r.assignmentsChan:
			if !ok {
				return nil
			}
			handler(ctx, roster)
		}
	}
}

// Stop signals Receive to exit cleanly. Safe to call multiple times.
func (r *ChannelReceiver) Stop() {
	r.ensureInit()
	r.stopOnce.Do(func() {
		close(r.done)
	})
}
