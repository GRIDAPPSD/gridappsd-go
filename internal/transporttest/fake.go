// Package transporttest provides in-process fakes for transport.Conn and
// transport.Subscription. Tests drive inbound messages by pushing onto the fake
// subscription channel and inspect outbound sends by reading the recorded tuples.
// No live broker is required.
package transporttest

import (
	"context"
	"fmt"
	"sync"

	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// SendRecord captures one outbound SEND frame for later assertion.
type SendRecord struct {
	Destination string
	ContentType string
	Body        []byte
	Headers     map[string]string
}

// FakeConn is a test double for transport.Conn.
// Subscribe returns a *FakeSub backed by a buffered channel the test controls.
// Send records the outbound frame tuple.
// Disconnect records the call.
type FakeConn struct {
	mu            sync.Mutex
	Sends         []SendRecord
	Subs          []*FakeSub
	subsByDest    map[string]*FakeSub
	disconnected  bool
	DisconnectErr error // optional: returned by Disconnect
	SubscribeErr  error // optional: all Subscribe calls return this when set

	// SendNotify is closed (if non-nil) after the first Send is recorded.
	// A test may set this before calling any method that triggers a Send.
	SendNotify chan struct{}
	sendOnce   sync.Once
}

// NewFakeConn returns a ready FakeConn.
func NewFakeConn() *FakeConn {
	return &FakeConn{subsByDest: make(map[string]*FakeSub)}
}

// Send records the frame. Implements transport.Conn.
func (c *FakeConn) Send(_ context.Context, destination, contentType string, body []byte, headers map[string]string) error {
	hCopy := make(map[string]string, len(headers))
	for k, v := range headers {
		hCopy[k] = v
	}
	bCopy := make([]byte, len(body))
	copy(bCopy, body)
	c.mu.Lock()
	c.Sends = append(c.Sends, SendRecord{
		Destination: destination,
		ContentType: contentType,
		Body:        bCopy,
		Headers:     hCopy,
	})
	notify := c.SendNotify
	c.mu.Unlock()

	// Signal tests waiting for the first Send without time.Sleep.
	if notify != nil {
		c.sendOnce.Do(func() { close(notify) })
	}
	return nil
}

// Subscribe returns a new FakeSub for destination. Implements transport.Conn.
func (c *FakeConn) Subscribe(_ context.Context, destination string) (transport.Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.SubscribeErr != nil {
		return nil, c.SubscribeErr
	}
	sub := newFakeSub(destination)
	c.Subs = append(c.Subs, sub)
	c.subsByDest[destination] = sub
	return sub, nil
}

// Disconnect records the call. Implements transport.Conn.
func (c *FakeConn) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnected = true
	return c.DisconnectErr
}

// WasDisconnected returns true if Disconnect was called.
func (c *FakeConn) WasDisconnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.disconnected
}

// SubForDest returns the FakeSub for dest, or nil if none exists.
func (c *FakeConn) SubForDest(dest string) *FakeSub {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subsByDest[dest]
}

// LastSend returns the most recent SendRecord, or an error if none recorded.
func (c *FakeConn) LastSend() (SendRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.Sends) == 0 {
		return SendRecord{}, fmt.Errorf("no sends recorded")
	}
	return c.Sends[len(c.Sends)-1], nil
}

// AllSends returns a copy of all recorded sends.
func (c *FakeConn) AllSends() []SendRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]SendRecord, len(c.Sends))
	copy(out, c.Sends)
	return out
}

// FakeSub is a test double for transport.Subscription.
// Push inbound messages with Push; close the channel with Unsubscribe or Close.
type FakeSub struct {
	dest      string
	ch        chan transport.Msg
	closeOnce sync.Once

	mu          sync.Mutex
	unsubCalled bool
}

func newFakeSub(dest string) *FakeSub {
	return &FakeSub{
		dest: dest,
		ch:   make(chan transport.Msg, 64),
	}
}

// C returns the inbound message channel. Implements transport.Subscription.
func (s *FakeSub) C() <-chan transport.Msg { return s.ch }

// Unsubscribe closes the channel (reader goroutine's exit signal) and records the call.
// Implements transport.Subscription.
func (s *FakeSub) Unsubscribe() error {
	s.mu.Lock()
	s.unsubCalled = true
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.ch) })
	return nil
}

// WasUnsubscribed returns true if Unsubscribe was called.
func (s *FakeSub) WasUnsubscribed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unsubCalled
}

// Push sends a message into the subscription channel.
func (s *FakeSub) Push(msg transport.Msg) {
	s.ch <- msg
}

// Close closes the underlying channel without marking it as Unsubscribed.
// Use to simulate the broker closing the subscription.
func (s *FakeSub) Close() {
	s.closeOnce.Do(func() { close(s.ch) })
}

// Dest returns the destination this sub was created for.
func (s *FakeSub) Dest() string { return s.dest }
