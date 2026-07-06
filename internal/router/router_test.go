package router_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/internal/router"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/internal/transporttest"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/message"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// TestRouter_TwoHandlersSameDestination asserts that two handlers registered on
// the same destination both receive the exact body bytes and the correct
// destination header. Data invariant: dispatch isolation per destination.
func TestRouter_TwoHandlersSameDestination(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()
	dest := "/queue/test.dest"

	var (
		mu     sync.Mutex
		gotA   []byte
		gotB   []byte
		hdrsA  map[string]string
		hdrsB  map[string]string
		readyA = make(chan struct{})
		readyB = make(chan struct{})
	)

	handlerA := router.Handler(func(headers map[string]string, body []byte) {
		mu.Lock()
		gotA = append([]byte(nil), body...)
		hdrsA = headers
		mu.Unlock()
		close(readyA)
	})
	handlerB := router.Handler(func(headers map[string]string, body []byte) {
		mu.Lock()
		gotB = append([]byte(nil), body...)
		hdrsB = headers
		mu.Unlock()
		close(readyB)
	})

	if err := r.Subscribe(ctx, dest, handlerA); err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	if err := r.Subscribe(ctx, dest, handlerB); err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}

	want := []byte("hello-both-handlers")
	fc.SubForDest(dest).Push(transport.Msg{Body: want})

	// Wait for both handlers with a deadline rather than time.Sleep.
	timeout := time.After(5 * time.Second)
	for _, ch := range []chan struct{}{readyA, readyB} {
		select {
		case <-ch:
		case <-timeout:
			t.Fatal("timed out waiting for handlers")
		}
	}

	// Assert exact body bytes received by both handlers.
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(gotA, want) {
		t.Errorf("handler A body: got %q, want %q", gotA, want)
	}
	if !bytes.Equal(gotB, want) {
		t.Errorf("handler B body: got %q, want %q", gotB, want)
	}

	// Assert the destination header is correct (router owns this; transport.Msg has none).
	if hdrsA[message.HeaderDestination] != dest {
		t.Errorf("handler A destination header: got %q, want %q", hdrsA[message.HeaderDestination], dest)
	}
	if hdrsB[message.HeaderDestination] != dest {
		t.Errorf("handler B destination header: got %q, want %q", hdrsB[message.HeaderDestination], dest)
	}
}

// TestRouter_DispatchIsolation asserts a message on destA does NOT fire destB's handler.
func TestRouter_DispatchIsolation(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()

	destA := "/queue/dest.a"
	destB := "/queue/dest.b"

	firedB := make(chan struct{}, 1)
	firedA := make(chan struct{})

	if err := r.Subscribe(ctx, destA, router.Handler(func(_ map[string]string, _ []byte) {
		close(firedA)
	})); err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	if err := r.Subscribe(ctx, destB, router.Handler(func(_ map[string]string, _ []byte) {
		firedB <- struct{}{}
	})); err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}

	// Push a message onto destA only.
	fc.SubForDest(destA).Push(transport.Msg{Body: []byte("msg-a")})

	// Wait for destA's handler to fire.
	select {
	case <-firedA:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for destA handler")
	}

	// Give destB a brief window to fire spuriously (it must not).
	select {
	case <-firedB:
		t.Error("destB handler fired when message was on destA (dispatch isolation violation)")
	case <-time.After(20 * time.Millisecond):
		// Correct: destB did not fire.
	}
}

// TestRouter_DuplicateHandlerGuard asserts that registering the same handler
// value twice on one destination returns an error and does not double-register.
func TestRouter_DuplicateHandlerGuard(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()
	dest := "/queue/dup.dest"

	fireCount := 0
	var mu sync.Mutex
	ready := make(chan struct{})

	h := router.Handler(func(_ map[string]string, _ []byte) {
		mu.Lock()
		fireCount++
		if fireCount == 1 {
			close(ready)
		}
		mu.Unlock()
	})

	if err := r.Subscribe(ctx, dest, h); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	if err := r.Subscribe(ctx, dest, h); err == nil {
		t.Fatal("second Subscribe with same handler should return error, got nil")
	}

	fc.SubForDest(dest).Push(transport.Msg{Body: []byte("trigger")})

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for handler")
	}

	// Small window to detect a spurious second fire.
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	got := fireCount
	mu.Unlock()
	if got != 1 {
		t.Errorf("handler fired %d times, want exactly 1", got)
	}
}

// TestRouter_UnsubscribeRemovesHandlersAndGoroutineExits asserts that Unsubscribe
// drops all handlers, calls the fake sub's Unsubscribe, and that the reader
// goroutine exits cleanly (proved via WaitGroup in router.Close which we call
// after unsubscribing all). Tested under -race.
func TestRouter_UnsubscribeRemovesHandlersAndGoroutineExits(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()
	dest := "/queue/unsub.dest"

	fired := make(chan struct{}, 1)
	h := router.Handler(func(_ map[string]string, _ []byte) {
		fired <- struct{}{}
	})

	if err := r.Subscribe(ctx, dest, h); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Confirm the sub exists.
	sub := fc.SubForDest(dest)
	if sub == nil {
		t.Fatal("no sub created for dest")
	}

	// Unsubscribe should close the channel (exit signal for reader goroutine).
	if err := r.Unsubscribe(ctx, dest); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	// Close waits for all reader goroutines to exit (WaitGroup).
	// If the goroutine leaked this would hang; the -race detector catches data races.
	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("router.Close timed out: reader goroutine likely leaked")
	}

	// The transport subscription should have been unsubscribed.
	if !sub.WasUnsubscribed() {
		t.Error("transport subscription was not unsubscribed")
	}

	// No handler should fire after Unsubscribe.
	select {
	case <-fired:
		t.Error("handler fired after Unsubscribe")
	default:
	}
}

// TestRouter_ErrorMsgIsTerminal asserts that a Msg with Err set is terminal:
// the reader goroutine exits and no handler is invoked with a nil body.
func TestRouter_ErrorMsgIsTerminal(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()
	dest := "/queue/err.dest"

	handlerFired := make(chan struct{}, 1)
	h := router.Handler(func(_ map[string]string, body []byte) {
		handlerFired <- struct{}{}
		if body == nil {
			// Should never happen: terminal error msgs must not invoke handlers with nil body.
			panic("handler called with nil body on error msg")
		}
	})

	if err := r.Subscribe(ctx, dest, h); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	sub := fc.SubForDest(dest)
	// Push a terminal error message.
	sub.Push(transport.Msg{Err: errors.New("broker error")})

	// Handler must NOT fire for an error message.
	select {
	case <-handlerFired:
		t.Error("handler invoked for error Msg: must not be called")
	case <-time.After(50 * time.Millisecond):
		// Correct: handler did not fire.
	}

	// Router.Close should return immediately (reader goroutine already exited).
	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("router.Close timed out after terminal error (goroutine likely leaked)")
	}
}

// TestRouter_CloseWaitsForAllGoroutines asserts that Close waits for every
// reader goroutine across multiple destinations. Run under -race.
func TestRouter_CloseWaitsForAllGoroutines(t *testing.T) {
	t.Parallel()

	const numDests = 5
	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()

	for i := 0; i < numDests; i++ {
		dest := "/queue/multi." + string(rune('a'+i))
		if err := r.Subscribe(ctx, dest, router.Handler(func(_ map[string]string, _ []byte) {})); err != nil {
			t.Fatalf("Subscribe %s: %v", dest, err)
		}
	}

	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("router.Close timed out with multiple subscriptions (goroutine leak?)")
	}
}

// TestRouter_UnsubscribeIdempotent asserts that unsubscribing a destination
// with no registered handlers returns nil.
func TestRouter_UnsubscribeIdempotent(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()

	if err := r.Unsubscribe(ctx, "/queue/nonexistent"); err != nil {
		t.Errorf("Unsubscribe on nonexistent dest: want nil, got %v", err)
	}
}
