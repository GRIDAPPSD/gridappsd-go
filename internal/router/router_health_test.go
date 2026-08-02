package router_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/router"
	"github.com/GRIDAPPSD/gridappsd-go/internal/transporttest"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// waitFor polls cond until it reports true or the deadline expires. Polling is
// used rather than a fixed sleep so the test is not tuned to a machine's speed;
// the failure message names the invariant that was never reached.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition never held within %v: %s", d, msg)
}

// recvErr reads one error from ch within d, or fails the test.
func recvErr(t *testing.T, ch <-chan error, d time.Duration, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		if err == nil {
			t.Fatalf("%s: received a nil error on the Errors channel", what)
		}
		return err
	case <-time.After(d):
		t.Fatalf("%s: no error delivered on the Errors channel within %v", what, d)
		return nil
	}
}

// TestRouter_DeadDestinationIsDeregisteredAndResubscribes is the GAG-009
// regression test for the worse half of the defect: a destination whose reader
// goroutine has died was left in the registry, so a later Subscribe attached to
// the dead reader, returned a Token and a nil error, and delivered nothing. The
// caller saw a subscription that looked healthy and was not.
func TestRouter_DeadDestinationIsDeregisteredAndResubscribes(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	t.Cleanup(r.Close)
	ctx := context.Background()
	dest := "/queue/dead.dest"

	if _, err := r.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	deadSub := fc.SubForDest(dest)
	if deadSub == nil {
		t.Fatal("no transport subscription was created for the destination")
	}

	// Kill the subscription the way a broker does: a terminal error message.
	deadSub.Push(transport.Msg{Err: errors.New("broker closed subscription")})

	// The reader goroutine must deregister the destination on its way out.
	waitFor(t, 5*time.Second, func() bool { return r.LiveDestinationCount() == 0 },
		"destination is still registered after its reader goroutine exited, so a later Subscribe attaches to a dead reader")

	// A later Subscribe must yield a WORKING subscription: a fresh transport
	// subscription that actually delivers to the newly registered handler.
	got := make(chan []byte, 1)
	if _, err := r.Subscribe(ctx, dest, func(_ map[string]string, body []byte) {
		got <- append([]byte(nil), body...)
	}); err != nil {
		t.Fatalf("resubscribe after the destination died: %v", err)
	}
	fresh := fc.SubForDest(dest)
	if fresh == nil {
		t.Fatal("resubscribe created no transport subscription")
	}
	if fresh == deadSub {
		t.Fatal("resubscribe reattached to the dead transport subscription")
	}

	want := []byte("after-recovery")
	fresh.Push(transport.Msg{Body: want})
	select {
	case body := <-got:
		if !bytes.Equal(body, want) {
			t.Errorf("body after resubscribe: got %q, want %q", body, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler registered after the destination died never received a message")
	}
}

// TestRouter_SubscriptionErrorReachesCaller is the GAG-009 regression test for
// the first half of the defect: a terminal subscription error was pushed into an
// unread sink, so the caller never learned the subscription had stopped.
func TestRouter_SubscriptionErrorReachesCaller(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	t.Cleanup(r.Close)
	ctx := context.Background()
	dest := "/queue/err.reaches.caller"

	if _, err := r.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	brokerErr := errors.New("broker rejected the subscription")
	fc.SubForDest(dest).Push(transport.Msg{Err: brokerErr})

	err := recvErr(t, r.Errors(), 5*time.Second, "terminal subscription error")
	if !errors.Is(err, brokerErr) {
		t.Errorf("delivered error does not wrap the broker error: got %v", err)
	}
	if !strings.Contains(err.Error(), dest) {
		t.Errorf("delivered error does not name the destination %q: got %v", dest, err)
	}
}

// TestRouter_UnexpectedCloseReachesCaller asserts the other death path: the
// broker (or a dropped connection) closes the subscription channel without an
// error message. That also leaves the caller with a channel that will never
// deliver again, so it is reported with a matchable sentinel.
func TestRouter_UnexpectedCloseReachesCaller(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	t.Cleanup(r.Close)
	ctx := context.Background()
	dest := "/queue/silent.close"

	if _, err := r.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Close, not Unsubscribe: the peer went away without our asking.
	fc.SubForDest(dest).Close()

	err := recvErr(t, r.Errors(), 5*time.Second, "unexpected subscription close")
	if !errors.Is(err, router.ErrSubscriptionClosed) {
		t.Errorf("error does not match ErrSubscriptionClosed: got %v", err)
	}
	if !strings.Contains(err.Error(), dest) {
		t.Errorf("error does not name the destination %q: got %v", dest, err)
	}
	waitFor(t, 5*time.Second, func() bool { return r.LiveDestinationCount() == 0 },
		"destination is still registered after the broker closed its subscription")
}

// TestRouter_DeliberateTeardownReportsNoError asserts that Unsubscribe and Close
// are not reported as failures. A health monitor that treats its own teardown as
// a fault is as useless as one that reports nothing.
func TestRouter_DeliberateTeardownReportsNoError(t *testing.T) {
	t.Parallel()

	t.Run("unsubscribe", func(t *testing.T) {
		t.Parallel()
		fc := transporttest.NewFakeConn()
		r := router.New(fc)
		ctx := context.Background()
		dest := "/queue/deliberate.unsub"

		tok, err := r.Subscribe(ctx, dest, func(map[string]string, []byte) {})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if err := r.Unsubscribe(ctx, dest, tok); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		r.Close()

		select {
		case err := <-r.Errors():
			t.Errorf("deliberate Unsubscribe reported as an error: %v", err)
		case <-time.After(200 * time.Millisecond):
			// Correct: no error published for our own teardown.
		}
	})

	t.Run("close", func(t *testing.T) {
		t.Parallel()
		fc := transporttest.NewFakeConn()
		r := router.New(fc)
		ctx := context.Background()

		for _, dest := range []string{"/queue/close.a", "/queue/close.b"} {
			if _, err := r.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
				t.Fatalf("Subscribe %s: %v", dest, err)
			}
		}
		r.Close()

		select {
		case err := <-r.Errors():
			t.Errorf("router Close reported as an error: %v", err)
		case <-time.After(200 * time.Millisecond):
			// Correct: no error published for our own teardown.
		}
	})
}

// TestRouter_ErrorsChannelDoesNotBlockReaders asserts the drop policy: a caller
// that never drains Errors must not wedge the reader goroutines. More
// destinations die than the channel can hold, and every reader still exits.
func TestRouter_ErrorsChannelDoesNotBlockReaders(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	r := router.New(fc)
	ctx := context.Background()

	const numDests = router.ErrorChanBuffer * 2
	dests := make([]string, 0, numDests)
	for i := 0; i < numDests; i++ {
		dest := "/queue/flood." + strconv.Itoa(i)
		dests = append(dests, dest)
		if _, err := r.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
			t.Fatalf("Subscribe %s: %v", dest, err)
		}
	}
	for _, dest := range dests {
		fc.SubForDest(dest).Push(transport.Msg{Err: errors.New("broker went away")})
	}

	// Every reader must deregister and exit even though nobody drains Errors.
	waitFor(t, 5*time.Second, func() bool { return r.LiveDestinationCount() == 0 },
		"reader goroutines did not all exit while the Errors channel was full")

	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("router.Close timed out: a reader goroutine is wedged on the Errors channel")
	}
}
