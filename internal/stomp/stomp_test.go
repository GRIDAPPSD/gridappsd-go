package stomp

import (
	"context"
	"errors"
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
