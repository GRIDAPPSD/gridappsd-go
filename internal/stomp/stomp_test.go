package stomp

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// fakeStompSub implements goStompSub for whitebox bridge tests without a live broker.
type fakeStompSub struct {
	ch          chan *gostomp.Message
	unsubCalled atomic.Bool
}

func (f *fakeStompSub) Unsubscribe(opts ...func(*frame.Frame) error) error {
	f.unsubCalled.Store(true)
	return nil
}

// newBridgeSub wires a sub to a fakeStompSub and starts the bridge goroutine.
func newBridgeSub(ctx context.Context, f *fakeStompSub, outBuf int) *sub {
	s := &sub{
		gossub: f,
		src:    f.ch,
		ch:     make(chan transport.Msg, outBuf),
	}
	go s.bridge(ctx)
	return s
}

// TestBridge_ForwardsMessages verifies that normal messages are forwarded with
// correct body content.
func TestBridge_ForwardsMessages(t *testing.T) {
	t.Parallel()
	f := &fakeStompSub{ch: make(chan *gostomp.Message, 2)}
	s := newBridgeSub(context.Background(), f, subChanBuf)

	f.ch <- &gostomp.Message{Body: []byte("hello")}
	f.ch <- &gostomp.Message{Body: []byte("world")}
	close(f.ch)

	var got []string
	for m := range s.ch {
		if m.Err != nil {
			t.Fatalf("unexpected error message: %v", m.Err)
		}
		got = append(got, string(m.Body))
	}
	if len(got) != 2 || got[0] != "hello" || got[1] != "world" {
		t.Errorf("body mismatch: want [hello world], got %v", got)
	}
}

// TestBridge_SourceCloseExits verifies exit path (b): when the go-stomp source
// channel closes, the bridge exits and closes the output channel.
func TestBridge_SourceCloseExits(t *testing.T) {
	t.Parallel()
	f := &fakeStompSub{ch: make(chan *gostomp.Message, 1)}
	s := newBridgeSub(context.Background(), f, subChanBuf)

	f.ch <- &gostomp.Message{Body: []byte("last")}
	close(f.ch)

	var got []transport.Msg
	for m := range s.ch {
		got = append(got, m)
	}
	if len(got) != 1 || string(got[0].Body) != "last" {
		t.Errorf("want 1 message 'last', got %v", got)
	}
}

// TestBridge_CtxCancelExitsGoroutine verifies exit path (a): ctx cancellation
// unblocks a bridge goroutine that is stuck trying to send on a full output channel.
//
// Without the nested-select fix (finding 2), the goroutine blocks on the outbound
// send indefinitely and this test fails at the 500 ms timeout. A WaitGroup tracks
// goroutine exit independently of s.ch so the test does not accidentally act as a
// receiver and unblock the bridge's stuck send.
func TestBridge_CtxCancelExitsGoroutine(t *testing.T) {
	t.Parallel()

	f := &fakeStompSub{ch: make(chan *gostomp.Message, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Deliberately unbuffered output channel: the bridge blocks immediately on
	// the first outbound send because nobody is reading from s.ch.
	s := &sub{
		gossub: f,
		src:    f.ch,
		ch:     make(chan transport.Msg, 0),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.bridge(ctx)
	}()

	// Queue a message; the bridge dequeues it and blocks on s.ch (unbuffered, no reader).
	f.ch <- &gostomp.Message{Body: []byte("stuck")}

	// Small delay so the bridge goroutine proceeds past the source receive and
	// reaches the blocking outbound send before we cancel.
	time.Sleep(10 * time.Millisecond)

	// Cancelling ctx must unblock the bridge via the nested-select ctx.Done case.
	cancel()

	// Wait for the goroutine to exit via WaitGroup rather than draining s.ch,
	// which would accidentally unblock the bridge's stuck send and give a false pass.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		// Goroutine exited: bridge has an exit path when the outbound channel is full.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("bridge goroutine leaked: did not exit within 500 ms after ctx cancel")
	}
}

// TestBridge_CtxCancelCallsUnsubscribe verifies that ctx cancellation triggers
// a best-effort Unsubscribe on the upstream subscription.
func TestBridge_CtxCancelCallsUnsubscribe(t *testing.T) {
	t.Parallel()
	f := &fakeStompSub{ch: make(chan *gostomp.Message)}
	ctx, cancel := context.WithCancel(context.Background())
	s := newBridgeSub(ctx, f, subChanBuf)
	_ = s

	cancel()
	// Allow the goroutine to observe ctx.Done and call Unsubscribe.
	time.Sleep(20 * time.Millisecond)

	if !f.unsubCalled.Load() {
		t.Error("expected Unsubscribe to be called on ctx cancel, but it was not")
	}
}

// TestBridge_ErrorMessageForwarded verifies that error messages are forwarded
// and that the bridge exits after delivery (error messages are terminal).
func TestBridge_ErrorMessageForwarded(t *testing.T) {
	t.Parallel()
	f := &fakeStompSub{ch: make(chan *gostomp.Message, 1)}
	s := newBridgeSub(context.Background(), f, subChanBuf)

	brokerErr := errors.New("broker error")
	f.ch <- &gostomp.Message{Err: brokerErr}
	// Do not close f.ch; bridge must exit on its own after the error.

	m, ok := <-s.ch
	if !ok {
		t.Fatal("output channel closed before receiving error message")
	}
	if m.Err == nil || m.Err.Error() != brokerErr.Error() {
		t.Errorf("error mismatch: want %q, got %v", brokerErr.Error(), m.Err)
	}
	// After an error message the bridge exits and closes s.ch.
	_, ok = <-s.ch
	if ok {
		t.Error("expected output channel to close after error message, but it remained open")
	}
}

// --- Fake STOMP server helpers ---

// pipeRWC is a bidirectional io.ReadWriteCloser backed by two io.Pipes.
// Used to simulate a network connection for the Dialer tests.
type pipeRWC struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (p *pipeRWC) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *pipeRWC) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *pipeRWC) Close() error {
	_ = p.r.Close()
	_ = p.w.Close()
	return nil
}

// startFakeSTOMPServer starts a goroutine serving a minimal STOMP protocol
// exchange and returns the client-side ReadWriteCloser. The server handles
// CONNECT/SEND/SUBSCRIBE/UNSUBSCRIBE/DISCONNECT frames. For SUBSCRIBE it
// delivers exactly one MESSAGE so the caller can verify receipt. Cleanup
// is registered via t.Cleanup.
func startFakeSTOMPServer(t *testing.T) *pipeRWC {
	t.Helper()
	cr, sw := io.Pipe() // client reads, server writes
	sr, cw := io.Pipe() // server reads, client writes

	t.Cleanup(func() {
		_ = cr.Close()
		_ = sw.Close()
		_ = sr.Close()
		_ = cw.Close()
	})

	go func() {
		r := frame.NewReader(sr)
		w := frame.NewWriter(sw)

		// CONNECT -> CONNECTED handshake.
		f, err := r.Read()
		if err != nil || f == nil || f.Command != frame.CONNECT {
			return
		}
		_ = w.Write(frame.New(frame.CONNECTED, frame.Version, "1.2", frame.HeartBeat, "0,0"))

		for {
			f, err := r.Read()
			if err != nil || f == nil {
				return
			}
			switch f.Command {
			case frame.SEND:
				if id, ok := f.Header.Contains(frame.Receipt); ok {
					_ = w.Write(frame.New(frame.RECEIPT, frame.ReceiptId, id))
				}
			case frame.SUBSCRIBE:
				// Deliver one message to exercise the Subscribe -> newSub -> bridge path.
				subID, _ := f.Header.Contains(frame.Id)
				dest, _ := f.Header.Contains(frame.Destination)
				msg := frame.New(frame.MESSAGE,
					frame.Subscription, subID,
					frame.Destination, dest,
					frame.MessageId, "m1",
				)
				msg.Body = []byte("hello")
				_ = w.Write(msg)
			case frame.UNSUBSCRIBE:
				if id, ok := f.Header.Contains(frame.Receipt); ok {
					_ = w.Write(frame.New(frame.RECEIPT, frame.ReceiptId, id))
				}
			case frame.DISCONNECT:
				if id, ok := f.Header.Contains(frame.Receipt); ok {
					_ = w.Write(frame.New(frame.RECEIPT, frame.ReceiptId, id))
				}
				return
			}
		}
	}()

	return &pipeRWC{r: cr, w: cw}
}

// --- Dialer and conn tests using the fake STOMP server ---

// TestDial_ConnectsSuccessfully verifies that Dial performs the STOMP handshake
// and returns a non-nil Conn. Disconnect is exercised as the teardown.
func TestDial_ConnectsSuccessfully(t *testing.T) {
	t.Parallel()
	d := &Dialer{}
	rwc := startFakeSTOMPServer(t)

	c, err := d.Dial(context.Background(), rwc, transport.ConnConfig{
		Login:    "user",
		Passcode: "pass",
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if c == nil {
		t.Fatal("Dial returned nil conn")
	}
	if err := c.Disconnect(); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
}

// TestConn_Send verifies that Send transmits a frame without error.
func TestConn_Send(t *testing.T) {
	t.Parallel()
	d := &Dialer{}
	rwc := startFakeSTOMPServer(t)

	c, err := d.Dial(context.Background(), rwc, transport.ConnConfig{Login: "u", Passcode: "p"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	if err := c.Send(context.Background(), "/topic/test", "text/plain", []byte("body"), nil); err != nil {
		t.Errorf("Send: %v", err)
	}
	if err := c.Disconnect(); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
}

// TestConn_Subscribe verifies that Subscribe returns a working Subscription
// and that the bridge forwards the message delivered by the fake server.
// Teardown uses Disconnect only: calling cancel() concurrent with Disconnect
// triggers a race in go-stomp v3 where Unsubscribe() sends on an already-closed
// channel after the read loop finishes closeChannel. Let Disconnect close the
// connection so the read loop closes gossub.C, and the bridge exits via the !ok
// path without calling Unsubscribe() at all.
func TestConn_Subscribe(t *testing.T) {
	t.Parallel()
	d := &Dialer{}
	rwc := startFakeSTOMPServer(t)

	c, err := d.Dial(context.Background(), rwc, transport.ConnConfig{Login: "u", Passcode: "p"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	sub, err := c.Subscribe(context.Background(), "/queue/test")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Read the message the fake server delivers on subscribe.
	m := <-sub.C()
	if m.Err != nil {
		t.Fatalf("received error: %v", m.Err)
	}
	if string(m.Body) != "hello" {
		t.Errorf("body: want 'hello', got %q", string(m.Body))
	}

	// Disconnect closes the connection; the read loop closes gossub.C; bridge
	// exits via !ok; sub.C() closes. Drain sub.C() to let the bridge goroutine
	// finish before the test returns.
	if err := c.Disconnect(); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
	for range sub.C() {
	}
}

// TestNewSub_AndCAccessor exercises newSub and C() without a live broker.
// A pre-closed source channel triggers the bridge's !ok exit path, which
// avoids calling Unsubscribe on the stub gostomp.Subscription (which has
// no live connection and would panic if Unsubscribe were called).
func TestNewSub_AndCAccessor(t *testing.T) {
	t.Parallel()
	src := make(chan *gostomp.Message)
	close(src) // bridge exits on !ok immediately

	s := newSub(context.Background(), &gostomp.Subscription{C: src})
	if s.C() == nil {
		t.Fatal("C() returned nil channel")
	}
	// Drain until closed; bridge exits quickly via the !ok case.
	for range s.C() {
	}
}

// TestSub_UnsubscribeDelegates verifies that sub.Unsubscribe delegates to
// goStompSub.Unsubscribe without requiring a live broker.
func TestSub_UnsubscribeDelegates(t *testing.T) {
	t.Parallel()
	f := &fakeStompSub{ch: make(chan *gostomp.Message)}
	s := &sub{gossub: f, src: f.ch, ch: make(chan transport.Msg, subChanBuf)}

	if err := s.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe: %v", err)
	}
	if !f.unsubCalled.Load() {
		t.Error("Unsubscribe did not delegate to goStompSub.Unsubscribe")
	}
}

// --- Error-path and deadline-path tests for Dialer and conn ---

// deadlineRWC wraps pipeRWC and records whether SetDeadline was called with a
// non-zero time. Dial applies a deadline before Connect and clears it after via
// a deferred SetDeadline(time.Time{}), so recording the zero-vs-nonzero call is
// the reliable signal; the final value is always zero.
type deadlineRWC struct {
	*pipeRWC
	calledWithNonZero bool
}

func (d *deadlineRWC) SetDeadline(t time.Time) error {
	if !t.IsZero() {
		d.calledWithNonZero = true
	}
	return nil
}

// startClosingFakeServer starts a goroutine that closes the connection
// immediately without sending a CONNECTED frame, causing gostomp.Connect to fail.
func startClosingFakeServer(t *testing.T) *pipeRWC {
	t.Helper()
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()

	t.Cleanup(func() {
		_ = cr.Close()
		_ = cw.Close()
	})

	go func() {
		// Close server side immediately; client gets EOF on the CONNECT attempt.
		_ = sr.Close()
		_ = sw.Close()
	}()

	return &pipeRWC{r: cr, w: cw}
}

// TestDial_ReturnsErrorOnFailedHandshake exercises the error-return path in Dial
// when the server closes the connection before sending CONNECTED.
func TestDial_ReturnsErrorOnFailedHandshake(t *testing.T) {
	t.Parallel()
	d := &Dialer{}
	rwc := startClosingFakeServer(t)

	_, err := d.Dial(context.Background(), rwc, transport.ConnConfig{Login: "u", Passcode: "p"})
	if err == nil {
		t.Fatal("expected error when server closes connection before CONNECTED, got nil")
	}
}

// TestDial_WithContextDeadline exercises the outer deadline branch in Dial when
// ctx carries a deadline. pipeRWC does not implement the deadliner interface,
// so the inner SetDeadline block is not entered; the outer branch is covered.
func TestDial_WithContextDeadline(t *testing.T) {
	t.Parallel()
	d := &Dialer{}
	rwc := startClosingFakeServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := d.Dial(ctx, rwc, transport.ConnConfig{Login: "u", Passcode: "p"})
	if err == nil {
		t.Fatal("expected error from failed handshake, got nil")
	}
}

// TestDial_DeadlineAppliedToDeadliner exercises the inner SetDeadline call in Dial
// when the rwc implements the deadliner interface. A no-op SetDeadline verifies
// Dial propagates the ctx deadline without breaking the subsequent handshake attempt.
func TestDial_DeadlineAppliedToDeadliner(t *testing.T) {
	t.Parallel()
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	t.Cleanup(func() {
		_ = cr.Close()
		_ = cw.Close()
	})

	// Server closes immediately so the connect fails; we just need Dial to
	// exercise the SetDeadline path before failing.
	go func() {
		_ = sr.Close()
		_ = sw.Close()
	}()

	d := &Dialer{}
	rwc := &deadlineRWC{pipeRWC: &pipeRWC{r: cr, w: cw}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := d.Dial(ctx, rwc, transport.ConnConfig{Login: "u", Passcode: "p"})
	if err == nil {
		t.Fatal("expected error from failed handshake, got nil")
	}
	if !rwc.calledWithNonZero {
		t.Error("Dial did not call SetDeadline with a non-zero deadline on the underlying conn")
	}
}

// TestConn_SendReturnsErrorAfterDisconnect exercises the error-return path in Send
// by calling Send on a connection that has been gracefully disconnected.
func TestConn_SendReturnsErrorAfterDisconnect(t *testing.T) {
	t.Parallel()
	d := &Dialer{}
	rwc := startFakeSTOMPServer(t)

	c, err := d.Dial(context.Background(), rwc, transport.ConnConfig{Login: "u", Passcode: "p"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// go-stomp marks the connection closed after Disconnect; Send returns an error.
	if err := c.Send(context.Background(), "/topic/x", "text/plain", []byte("hi"), nil); err == nil {
		t.Error("expected error when sending on closed conn, got nil")
	}
}

// TestConn_SubscribeReturnsErrorAfterDisconnect exercises the error-return path
// in Subscribe by calling it on a disconnected connection.
func TestConn_SubscribeReturnsErrorAfterDisconnect(t *testing.T) {
	t.Parallel()
	d := &Dialer{}
	rwc := startFakeSTOMPServer(t)

	c, err := d.Dial(context.Background(), rwc, transport.ConnConfig{Login: "u", Passcode: "p"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	// go-stomp marks the connection closed after Disconnect; Subscribe returns an error.
	if _, err := c.Subscribe(context.Background(), "/queue/x"); err == nil {
		t.Error("expected error when subscribing on closed conn, got nil")
	}
}
