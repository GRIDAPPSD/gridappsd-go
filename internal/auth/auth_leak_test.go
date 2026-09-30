package auth_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/auth"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// countOpenFDs reports the number of open file descriptors for this process,
// read from /proc/self/fd so a leaked dialed socket is visible directly
// rather than inferred from a mock's own bookkeeping.
func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd on this host: %v", err)
	}
	return len(entries)
}

// realFDListener returns a TCP listener that accepts and immediately closes
// each connection, so this test's own accept loop does not itself accumulate
// server-side descriptors and confound the count under measurement (the
// dialed, client-side descriptor).
func realFDListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot open a TCP listener on this host: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	return ln
}

// failingDialer always fails the STOMP CONNECT handshake, mirroring a broker
// that refuses the login. It never touches rwc: this models go-stomp's own
// documented behavior of returning an error without closing the connection
// it was handed.
type failingDialer struct {
	err error
}

func (d *failingDialer) Dial(_ context.Context, _ io.ReadWriteCloser, _ transport.ConnConfig) (transport.Conn, error) {
	return nil, d.err
}

// TestFetchToken_ClosesCredentialConnOnDialError proves the credential leg's
// dialed socket does not leak when the STOMP CONNECT handshake fails.
func TestFetchToken_ClosesCredentialConnOnDialError(t *testing.T) {
	// Not t.Parallel: disables the GC process-wide for its duration so a
	// spontaneous collection cannot reclaim a leaked descriptor through the
	// net.Conn finalizer and mask the defect under test.
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)

	ln := realFDListener(t)
	defer ln.Close()

	netDial := func(ctx context.Context) (io.ReadWriteCloser, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	}
	dialer := &failingDialer{err: errors.New("stomp connect: login refused")}

	const attempts = 40
	before := countOpenFDs(t)
	for i := 0; i < attempts; i++ {
		if _, err := auth.Exchange(context.Background(), netDial, dialer, "u", "p", time.Second, nil); err == nil {
			t.Fatalf("attempt %d: expected error from a failing dialer, got nil", i)
		}
	}
	after := countOpenFDs(t)

	if got := after - before; got != 0 {
		t.Errorf("descriptor count grew by %d across %d refused logins, want 0 (before=%d after=%d)", got, attempts, before, after)
	}
}

// twoCallDialer succeeds on the first Dial call and fails on the second,
// modelling a token-exchange second leg whose STOMP CONNECT is refused after
// the credential leg already produced a token.
type twoCallDialer struct {
	mu        sync.Mutex
	calls     int
	first     transport.Conn
	secondErr error
}

func (d *twoCallDialer) Dial(_ context.Context, _ io.ReadWriteCloser, _ transport.ConnConfig) (transport.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.calls == 1 {
		return d.first, nil
	}
	return nil, d.secondErr
}

// TestExchange_ClosesSecondConnOnDialError proves the durable (token) leg's
// dialed socket does not leak when its STOMP CONNECT handshake fails, even
// though the credential leg ahead of it succeeded.
func TestExchange_ClosesSecondConnOnDialError(t *testing.T) {
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)

	ln := realFDListener(t)
	defer ln.Close()

	// The credential leg is fully mocked (no real socket): its own
	// close-on-error path is covered by TestFetchToken_ClosesCredentialConnOnDialError,
	// and giving it a real fd here would fold an unrelated fake's cleanup
	// behavior into the count this test asserts on. Only the durable leg,
	// the one under test, dials a real, fd-backed connection.
	callN := 0
	netDial := func(ctx context.Context) (io.ReadWriteCloser, error) {
		callN++
		if callN%2 == 1 {
			return nopRWC{}, nil
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	}

	const attempts = 40
	before := countOpenFDs(t)
	for i := 0; i < attempts; i++ {
		dialer := &twoCallDialer{first: newFakeConn(fakeToken), secondErr: errors.New("stomp connect: login refused")}
		if _, err := auth.Exchange(context.Background(), netDial, dialer, "u", "p", time.Second, nil); err == nil {
			t.Fatalf("attempt %d: expected error from a failing second dial, got nil", i)
		}
	}
	after := countOpenFDs(t)

	if got := after - before; got != 0 {
		t.Errorf("descriptor count grew by %d across %d refused second-leg logins, want 0 (before=%d after=%d)", got, attempts, before, after)
	}
}

// slowFakeSub wraps fakeSub so Unsubscribe blocks for a configured delay,
// modelling go-stomp's own receipt wait against a broker that accepted the
// login and then went silent.
type slowFakeSub struct {
	*fakeSub
	delay time.Duration
}

func (s *slowFakeSub) Unsubscribe() error {
	time.Sleep(s.delay)
	return s.fakeSub.Unsubscribe()
}

// slowFakeConn wraps fakeConn so Disconnect, and the Subscription it hands
// back, each block for a configured delay before completing.
type slowFakeConn struct {
	*fakeConn
	delay time.Duration
}

func (c *slowFakeConn) Disconnect() error {
	time.Sleep(c.delay)
	return c.fakeConn.Disconnect()
}

func (c *slowFakeConn) Subscribe(ctx context.Context, dest string) (transport.Subscription, error) {
	sub, err := c.fakeConn.Subscribe(ctx, dest)
	if err != nil {
		return nil, err
	}
	return &slowFakeSub{fakeSub: sub.(*fakeSub), delay: c.delay}, nil
}

// TestFetchToken_TeardownBoundedByContext proves that a credential connection
// whose Disconnect and Unsubscribe each hang, as a silent broker's unanswered
// receipt does, cannot hold Exchange open past the caller's context deadline
// by more than a small margin. Without the bound, the two LIFO-ordered
// deferred calls run one after the other and together take roughly 2*delay.
func TestFetchToken_TeardownBoundedByContext(t *testing.T) {
	const (
		teardownDelay = 3 * time.Second
		ctxBudget     = 200 * time.Millisecond
		// Headroom for scheduling noise on this host, not a re-derivation of
		// go-stomp's own receipt timeout.
		margin = 400 * time.Millisecond
	)

	conn1 := &slowFakeConn{fakeConn: newFakeConn(fakeToken), delay: teardownDelay}
	conn2 := newFakeConn("")
	dialer := newFakeDialer(conn1, conn2)

	ctx, cancel := context.WithTimeout(context.Background(), ctxBudget)
	defer cancel()

	start := time.Now()
	_, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Exchange returned error: %v", err)
	}
	if elapsed > ctxBudget+margin {
		t.Errorf("Exchange took %v, want at most %v (ctx budget %v + %v margin); teardown is not bounded by ctx", elapsed, ctxBudget+margin, ctxBudget, margin)
	}
	if elapsed < ctxBudget {
		t.Errorf("Exchange returned in %v, before the %v ctx budget elapsed; this run cannot tell a bounded teardown from one that never blocked", elapsed, ctxBudget)
	}
}

// TestFetchToken_TeardownGoroutineExits proves the goroutine started to bound
// a hanging teardown call still exits on its own once that call returns,
// rather than leaking for the life of the process.
func TestFetchToken_TeardownGoroutineExits(t *testing.T) {
	const teardownDelay = 150 * time.Millisecond

	conn1 := &slowFakeConn{fakeConn: newFakeConn(fakeToken), delay: teardownDelay}
	conn2 := newFakeConn("")
	dialer := newFakeDialer(conn1, conn2)

	// A ctx that expires well before teardownDelay: Exchange returns once
	// ctx.Done() fires, but the underlying Disconnect/Unsubscribe goroutines
	// are still running in the background at that point.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	before := runtime.NumGoroutine()
	if _, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil); err != nil {
		t.Fatalf("Exchange returned error: %v", err)
	}

	deadline := time.Now().Add(2 * teardownDelay)
	for {
		if runtime.NumGoroutine() <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("goroutine count still %d (started at %d) after %v, want it back at or below the start count", runtime.NumGoroutine(), before, 2*teardownDelay)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
