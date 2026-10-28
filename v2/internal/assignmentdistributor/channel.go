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

// ErrSenderStopped is returned when attempting to send an assignment on a stopped ChannelSender.
var ErrSenderStopped = errors.New("assignment channel sender is stopped")

// ChannelSender sends assignments to a Go channel. It's only useful when the
// director and the matchmaking queue run in the same process.
//
// The caller owns the channel: Stop doesn't close it, because the channel
// is shared with a ChannelReceiver (and closing a channel that something
// else might still send on, or close, panics). Receivers stop on their own
// Stop or context instead.
type ChannelSender struct {
	assignmentsChan chan<- *pb.Roster
	stopOnce        sync.Once
	stopChan        chan struct{}
}

// NewChannelSender returns a ChannelSender that sends on ch.
func NewChannelSender(ch chan<- *pb.Roster) *ChannelSender {
	return &ChannelSender{
		assignmentsChan: ch,
		stopChan:        make(chan struct{}),
	}
}

// Send blocks until roster is sent, ctx is done, or the sender is stopped.
func (p *ChannelSender) Send(ctx context.Context, roster *pb.Roster) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Checked first so a stopped sender never sends, even if the channel
	// has buffer space (select picks among ready cases at random).
	select {
	case <-p.stopChan:
		return ErrSenderStopped
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.stopChan:
		return ErrSenderStopped
	case p.assignmentsChan <- roster:
		return nil
	}
}

// Stop makes current and future Sends return ErrSenderStopped. It is safe
// to call more than once.
func (p *ChannelSender) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopChan)
	})
}

// ChannelReceiver receives assignments from a Go channel.
type ChannelReceiver struct {
	assignmentsChan <-chan *pb.Roster
	stopOnce        sync.Once
	stopChan        chan struct{}
}

// NewChannelReceiver returns a ChannelReceiver that receives from ch.
func NewChannelReceiver(ch <-chan *pb.Roster) *ChannelReceiver {
	return &ChannelReceiver{
		assignmentsChan: ch,
		stopChan:        make(chan struct{}),
	}
}

// Receive calls handler for each roster until ctx is done, the receiver is
// stopped, or the channel is closed.
func (r *ChannelReceiver) Receive(ctx context.Context, handler func(ctx context.Context, roster *pb.Roster)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.stopChan:
			return nil
		case roster, ok := <-r.assignmentsChan:
			if !ok {
				return nil
			}
			handler(ctx, roster)
		}
	}
}

// Stop makes Receive return. It is safe to call more than once.
func (r *ChannelReceiver) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}
