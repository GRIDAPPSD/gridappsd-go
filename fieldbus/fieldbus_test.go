// Package fieldbus_test tests the public fieldbus surface against in-process fakes.
// No live broker is required.
package fieldbus_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/fieldbus"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/gridappsd"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/message"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// Compile-time assertion lives in fieldbus.go; verifying it compiles is sufficient.
func TestGridAPPSDMessageBus_ImplementsInterface(t *testing.T) {
	t.Parallel()
	// This line causes a compile error if GridAPPSDMessageBus stops satisfying MessageBus.
	var _ fieldbus.MessageBus = (*fieldbus.GridAPPSDMessageBus)(nil)
}

// TestGridAPPSDMessageBus_NotConnectedBehavior checks the unconnected guard on every
// method that requires an active session.
func TestGridAPPSDMessageBus_NotConnectedBehavior(t *testing.T) {
	t.Parallel()

	bus := fieldbus.New(dummyConfig())
	ctx := context.Background()

	if bus.IsConnected() {
		t.Error("IsConnected: want false on new bus, got true")
	}
	if err := bus.Disconnect(); err != nil {
		t.Errorf("Disconnect on unconnected bus: want nil, got %v", err)
	}

	h := fieldbus.Handler(func(_ map[string]string, _ []byte) {})
	if _, err := bus.Subscribe(ctx, "/queue/x", h); err == nil {
		t.Error("Subscribe on unconnected bus: want error, got nil")
	}
	if err := bus.Send(ctx, "/queue/x", "text/plain", []byte("body")); err == nil {
		t.Error("Send on unconnected bus: want error, got nil")
	}
	if _, err := bus.GetResponse(ctx, "/queue/x", "text/plain", []byte("q")); err == nil {
		t.Error("GetResponse on unconnected bus: want error, got nil")
	}
}

// dummyConfig returns a zero-value gridappsd.Config sufficient to construct a bus
// without dialing (Connect is not called in NotConnectedBehavior tests).
func dummyConfig() gridappsd.Config { return gridappsd.Config{} }

// ---------------------------------------------------------------------------
// Connected-state tests via fieldbus.NewForTest (test-only injection hook).
// ---------------------------------------------------------------------------

// connFake is a minimal in-process transport.Conn for fieldbus tests.
type connFake struct {
	mu         sync.Mutex
	sent       []sentFrame
	subs       map[string]*subFake
	disc       bool
	sendNotify chan struct{} // closed after the first Send is recorded; nil = unused
	sendOnce   sync.Once
}

type sentFrame struct {
	dest    string
	ct      string
	body    []byte
	headers map[string]string
}

type subFake struct {
	ch        chan transport.Msg
	closeOnce sync.Once
	mu        sync.Mutex
	unsubbed  bool
}

func (s *subFake) C() <-chan transport.Msg { return s.ch }
func (s *subFake) Unsubscribe() error {
	s.mu.Lock()
	s.unsubbed = true
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.ch) })
	return nil
}

func newConnFake() *connFake {
	return &connFake{subs: make(map[string]*subFake)}
}

// newConnFakeWithNotify returns a connFake that closes sendNotify after the first Send.
func newConnFakeWithNotify() (*connFake, chan struct{}) {
	notify := make(chan struct{})
	return &connFake{subs: make(map[string]*subFake), sendNotify: notify}, notify
}

func (c *connFake) Send(_ context.Context, dest, ct string, body []byte, headers map[string]string) error {
	hCopy := make(map[string]string, len(headers))
	for k, v := range headers {
		hCopy[k] = v
	}
	bCopy := make([]byte, len(body))
	copy(bCopy, body)
	c.mu.Lock()
	c.sent = append(c.sent, sentFrame{dest, ct, bCopy, hCopy})
	notify := c.sendNotify
	c.mu.Unlock()

	// Signal tests waiting for the first Send without time.Sleep.
	if notify != nil {
		c.sendOnce.Do(func() { close(notify) })
	}
	return nil
}

func (c *connFake) Subscribe(_ context.Context, dest string) (transport.Subscription, error) {
	sub := &subFake{ch: make(chan transport.Msg, 64)}
	c.mu.Lock()
	c.subs[dest] = sub
	c.mu.Unlock()
	return sub, nil
}

func (c *connFake) Disconnect() error {
	c.mu.Lock()
	c.disc = true
	c.mu.Unlock()
	return nil
}

func (c *connFake) lastSent() (sentFrame, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sent) == 0 {
		return sentFrame{}, false
	}
	return c.sent[len(c.sent)-1], true
}

func (c *connFake) subFor(dest string) *subFake {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subs[dest]
}

// TestConnectedBus_SendStampsGossHeaders asserts that Send stamps
// GOSS_HAS_SUBJECT and GOSS_SUBJECT headers on the outbound frame.
// Open-item resolution: goss.py stamps both headers even on token-auth sessions
// (goss.py:174-176); we mirror that behavior.
func TestConnectedBus_SendStampsGossHeaders(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "test-user")
	ctx := context.Background()

	if err := bus.Send(ctx, "/queue/some.dest", "application/json", []byte(`{}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	rec, ok := conn.lastSent()
	if !ok {
		t.Fatal("no send recorded")
	}
	if rec.headers[message.HeaderGossHasSubject] != "true" {
		t.Errorf("GOSS_HAS_SUBJECT: got %q, want %q", rec.headers[message.HeaderGossHasSubject], "true")
	}
	if rec.headers[message.HeaderGossSubject] != "test-user" {
		t.Errorf("GOSS_SUBJECT: got %q, want %q", rec.headers[message.HeaderGossSubject], "test-user")
	}
}

// TestConnectedBus_SendNormalizesDestination asserts bare destinations get /queue/ prefix.
func TestConnectedBus_SendNormalizesDestination(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "user")
	ctx := context.Background()

	if err := bus.Send(ctx, "bare.dest", "text/plain", []byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	rec, ok := conn.lastSent()
	if !ok {
		t.Fatal("no send recorded")
	}
	if rec.dest != "/queue/bare.dest" {
		t.Errorf("Send dest: got %q, want /queue/bare.dest", rec.dest)
	}
}

// TestConnectedBus_SubscribeQueueNormalizes asserts Subscribe normalizes bare destinations.
func TestConnectedBus_SubscribeQueueNormalizes(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "user")
	ctx := context.Background()

	if _, err := bus.Subscribe(ctx, "bare.dest", fieldbus.Handler(func(_ map[string]string, _ []byte) {})); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if sub := conn.subFor("/queue/bare.dest"); sub == nil {
		t.Error("Subscribe did not normalize bare dest to /queue/bare.dest")
	}
	if sub := conn.subFor("bare.dest"); sub != nil {
		t.Error("Subscribe created sub with un-normalized bare dest")
	}
}

// TestConnectedBus_DisconnectClosesConnAndRouter asserts that Disconnect
// unsubscribes all destinations, closes the transport conn, and marks the bus
// as not connected. Run under -race to prove no goroutine leak.
func TestConnectedBus_DisconnectClosesConnAndRouter(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "user")
	ctx := context.Background()

	const dest = "/queue/test.dest"
	if _, err := bus.Subscribe(ctx, dest, fieldbus.Handler(func(_ map[string]string, _ []byte) {})); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	done := make(chan struct{})
	go func() {
		if err := bus.Disconnect(); err != nil {
			t.Errorf("Disconnect: %v", err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnect timed out (router goroutine likely leaked)")
	}

	conn.mu.Lock()
	disconnected := conn.disc
	conn.mu.Unlock()
	if !disconnected {
		t.Error("transport.Conn.Disconnect was not called")
	}
	if bus.IsConnected() {
		t.Error("IsConnected: want false after Disconnect, got true")
	}
}

// TestConnectedBus_GetResponse_DestNormalized asserts GetResponse normalizes the
// request destination to /queue/ and the reply-to header is bare. Uses SendNotify
// to synchronize deterministically instead of a sleep-poll loop (GAG-002).
func TestConnectedBus_GetResponse_DestNormalized(t *testing.T) {
	t.Parallel()

	conn, sendNotify := newConnFakeWithNotify()
	bus := fieldbus.NewForTest(conn, "user")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wantReply := []byte("the-reply")

	go func() {
		// Wait deterministically for the first Send to be recorded.
		select {
		case <-sendNotify:
		case <-ctx.Done():
			return
		}
		conn.mu.Lock()
		last := conn.sent[len(conn.sent)-1]
		conn.mu.Unlock()
		replyTo := last.headers[message.HeaderReplyTo]
		sub := conn.subFor("/queue/" + replyTo)
		if sub != nil {
			sub.ch <- transport.Msg{Body: wantReply}
		}
	}()

	got, err := bus.GetResponse(ctx, "bare.svc", "application/json", []byte("request"))
	if err != nil {
		t.Fatalf("GetResponse: %v", err)
	}
	if !bytes.Equal(got, wantReply) {
		t.Errorf("GetResponse reply: got %q, want %q", got, wantReply)
	}

	conn.mu.Lock()
	last := conn.sent[len(conn.sent)-1]
	conn.mu.Unlock()

	if last.dest != "/queue/bare.svc" {
		t.Errorf("request dest: got %q, want /queue/bare.svc", last.dest)
	}

	// reply-to must be bare (no /queue/ prefix).
	replyTo := last.headers[message.HeaderReplyTo]
	if len(replyTo) == 0 {
		t.Fatal("reply-to header missing from GetResponse SEND")
	}
	if len(replyTo) > 7 && replyTo[:7] == "/queue/" {
		t.Errorf("reply-to must be bare name, got %q", replyTo)
	}
}

// TestConnectedBus_IsConnectedIdempotentDisconnect asserts that Disconnect can
// be called twice without error.
func TestConnectedBus_IsConnectedIdempotentDisconnect(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "user")

	if err := bus.Disconnect(); err != nil {
		t.Fatalf("first Disconnect: %v", err)
	}
	if err := bus.Disconnect(); err != nil {
		t.Fatalf("second Disconnect: %v", err)
	}
	if bus.IsConnected() {
		t.Error("IsConnected: want false after double Disconnect")
	}
}

// TestConnectedBus_Unsubscribe asserts Unsubscribe normalizes the destination and
// removes the subscription from the router when the last token is removed.
func TestConnectedBus_Unsubscribe(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "user")
	ctx := context.Background()

	const raw = "bare.dest"
	const normalized = "/queue/bare.dest"

	tok, err := bus.Subscribe(ctx, raw, fieldbus.Handler(func(_ map[string]string, _ []byte) {}))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub := conn.subFor(normalized)
	if sub == nil {
		t.Fatalf("sub not created for %s", normalized)
	}

	if err := bus.Unsubscribe(ctx, raw, tok); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	// Router closed the subscription (channel should now be closed).
	sub.mu.Lock()
	unsubbed := sub.unsubbed
	sub.mu.Unlock()
	if !unsubbed {
		t.Error("transport subscription was not unsubscribed after Unsubscribe of last token")
	}
}

// TestConnectedBus_UnsubscribeIdempotent asserts Unsubscribe with an unknown token
// returns nil (idempotent).
func TestConnectedBus_UnsubscribeIdempotent(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "user")
	ctx := context.Background()

	if err := bus.Unsubscribe(ctx, "/queue/nonexistent", fieldbus.Token(0)); err != nil {
		t.Errorf("Unsubscribe on nonexistent dest: want nil, got %v", err)
	}
}

// TestConnectedBus_ConnectIdempotent asserts that calling Connect on an
// already-connected bus is a no-op that returns nil (exercises the idempotent
// guard in Connect without dialing a real broker).
func TestConnectedBus_ConnectIdempotent(t *testing.T) {
	t.Parallel()

	conn := newConnFake()
	bus := fieldbus.NewForTest(conn, "user")
	ctx := context.Background()

	// bus is already "connected" via NewForTest; a second Connect must be a no-op.
	if err := bus.Connect(ctx); err != nil {
		t.Errorf("Connect on already-connected bus: want nil, got %v", err)
	}
	if !bus.IsConnected() {
		t.Error("IsConnected: want true after idempotent Connect")
	}
}
