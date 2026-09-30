package auth_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
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

// realFDListener returns a TCP listener that accepts and immediately resets
// each connection, and a channel that receives once per connection after its
// server-side descriptor is closed. Counting only after every accepted
// connection is closed keeps this test's own descriptors out of the count
// under measurement (the dialed, client-side descriptor).
func realFDListener(t *testing.T, conns int) (net.Listener, <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot open a TCP listener on this host: %v", err)
	}
	closed := make(chan struct{}, conns)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Reset rather than close, so no socket lingers in TIME_WAIT and
			// repeated runs do not exhaust the local port range.
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			_ = c.Close()
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}()
	return ln, closed
}

// assertNoFDLeak runs attempt fdLeakAttempts times, each expected to fail
// after dialing the listener once through dialReal, and fails the test if the
// process holds more descriptors afterwards than before.
func assertNoFDLeak(t *testing.T, what string, attempt func(dialReal auth.NetDialer) error) {
	t.Helper()
	// Not parallel: disables the GC process-wide for its duration so a
	// spontaneous collection cannot reclaim a leaked descriptor through the
	// net.Conn finalizer and mask the defect under test.
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)

	ln, closed := realFDListener(t, fdLeakAttempts)
	defer ln.Close()
	dialReal := func(ctx context.Context) (io.ReadWriteCloser, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	}

	before := countOpenFDs(t)
	for i := 0; i < fdLeakAttempts; i++ {
		if err := attempt(dialReal); err == nil {
			t.Fatalf("attempt %d: expected an error, got nil", i)
		}
	}
	for i := 0; i < fdLeakAttempts; i++ {
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatalf("listener closed %d of %d accepted connections", i, fdLeakAttempts)
		}
	}
	after := countOpenFDs(t)

	if got := after - before; got != 0 {
		t.Errorf("descriptor count grew by %d across %d %s, want 0 (before=%d after=%d)", got, fdLeakAttempts, what, before, after)
	}
}

const fdLeakAttempts = 40

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
	dialer := &failingDialer{err: errors.New("stomp connect: login refused")}
	assertNoFDLeak(t, "refused logins", func(dialReal auth.NetDialer) error {
		_, err := auth.Exchange(context.Background(), dialReal, dialer, "u", "p", time.Second, nil)
		return err
	})
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
	// The credential leg is fully mocked (no real socket): its own
	// close-on-error path is covered by TestFetchToken_ClosesCredentialConnOnDialError,
	// and giving it a real fd here would fold an unrelated fake's cleanup
	// behavior into the count this test asserts on. Only the durable leg,
	// the one under test, dials a real, fd-backed connection.
	assertNoFDLeak(t, "refused second-leg logins", func(dialReal auth.NetDialer) error {
		callN := 0
		netDial := func(ctx context.Context) (io.ReadWriteCloser, error) {
			callN++
			if callN == 1 {
				return nopRWC{}, nil
			}
			return dialReal(ctx)
		}
		dialer := &twoCallDialer{first: newFakeConn(fakeToken), secondErr: errors.New("stomp connect: login refused")}
		_, err := auth.Exchange(context.Background(), netDial, dialer, "u", "p", time.Second, nil)
		return err
	})
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
// by more than a small margin. Without the bound, Unsubscribe and Disconnect
// run one after the other and together take roughly 2*delay.
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

	// The token arrived, but the credential connection was still being torn
	// down when ctx ran out, so that is what the error names.
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "credential connection teardown") {
		t.Fatalf("Exchange error = %v, want credential connection teardown wrapping %v", err, context.DeadlineExceeded)
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
	// ctx.Done() fires, but the teardown goroutine is still waiting on the
	// slow Unsubscribe at that point.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	before := runtime.NumGoroutine()
	if _, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exchange error = %v, want it to wrap %v", err, context.DeadlineExceeded)
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

// panickingSub is a fakeSub whose Unsubscribe fails the way go-stomp's does
// when its receipt wait times out after the channel was closed.
type panickingSub struct{ *fakeSub }

func (s *panickingSub) Unsubscribe() error {
	s.fakeSub.closeC()
	s.fakeSub.ch <- transport.Msg{} // send on closed channel
	return nil
}

type panickingSubConn struct{ *fakeConn }

func (c *panickingSubConn) Subscribe(ctx context.Context, dest string) (transport.Subscription, error) {
	sub, err := c.fakeConn.Subscribe(ctx, dest)
	if err != nil {
		return nil, err
	}
	return &panickingSub{fakeSub: sub.(*fakeSub)}, nil
}

// TestFetchToken_UnsubscribePanicDoesNotEscape proves a runtime panic inside
// Unsubscribe is contained to the teardown, which still disconnects the
// credential connection and reports the panic, instead of crashing the process.
func TestFetchToken_UnsubscribePanicDoesNotEscape(t *testing.T) {
	conn1 := &panickingSubConn{fakeConn: newFakeConn(fakeToken)}
	dialer := newFakeDialer(conn1, newFakeConn(""))

	conn, err := auth.Exchange(context.Background(), netDialStub, dialer, "u", "p", 10*time.Second, nil)
	var re runtime.Error
	if !errors.As(err, &re) || !strings.Contains(err.Error(), "credential connection teardown") {
		t.Fatalf("Exchange error = %v, want credential connection teardown wrapping the recovered runtime.Error", err)
	}
	if conn != nil {
		t.Errorf("Exchange returned a connection alongside error %v", err)
	}
	if !conn1.disconnected {
		t.Error("credential connection was not disconnected after Unsubscribe panicked")
	}
}

var errUnsubscribeFailed = errors.New("unsubscribe receipt timeout")

// failingSub is a fakeSub whose Unsubscribe ends the subscription and then
// reports an error, as go-stomp's does after its receipt wait times out.
type failingSub struct{ *fakeSub }

func (s *failingSub) Unsubscribe() error {
	s.fakeSub.closeC()
	return errUnsubscribeFailed
}

type failingSubConn struct{ *fakeConn }

func (c *failingSubConn) Subscribe(ctx context.Context, dest string) (transport.Subscription, error) {
	sub, err := c.fakeConn.Subscribe(ctx, dest)
	if err != nil {
		return nil, err
	}
	return &failingSub{fakeSub: sub.(*fakeSub)}, nil
}

// TestFetchToken_UnsubscribeErrorReported proves an Unsubscribe error on the
// credential connection is returned from Exchange rather than discarded, and
// that the connection is still disconnected.
func TestFetchToken_UnsubscribeErrorReported(t *testing.T) {
	conn1 := &failingSubConn{fakeConn: newFakeConn(fakeToken)}
	dialer := newFakeDialer(conn1, newFakeConn(""))

	_, err := auth.Exchange(context.Background(), netDialStub, dialer, "u", "p", 10*time.Second, nil)
	if !errors.Is(err, errUnsubscribeFailed) || !strings.Contains(err.Error(), "credential connection teardown") {
		t.Fatalf("Exchange error = %v, want credential connection teardown wrapping %v", err, errUnsubscribeFailed)
	}
	if !conn1.disconnected {
		t.Error("credential connection was not disconnected after Unsubscribe failed")
	}
	if len(dialer.calls) != 1 {
		t.Errorf("dialer called %d times, want 1: the durable leg must not be dialed after a failed teardown", len(dialer.calls))
	}
}

// cancellingSub is a fakeSub whose Unsubscribe cancels the caller's context
// and then fails, as go-stomp's does when the transport closes under it.
type cancellingSub struct {
	*fakeSub
	cancel context.CancelFunc
}

func (s *cancellingSub) Unsubscribe() error {
	s.cancel()
	s.fakeSub.closeC()
	return errUnsubscribeFailed
}

type cancellingSubConn struct {
	*fakeConn
	cancel context.CancelFunc
}

func (c *cancellingSubConn) Subscribe(ctx context.Context, dest string) (transport.Subscription, error) {
	sub, err := c.fakeConn.Subscribe(ctx, dest)
	if err != nil {
		return nil, err
	}
	return &cancellingSub{fakeSub: sub.(*fakeSub), cancel: c.cancel}, nil
}

// TestFetchToken_UnsubscribeErrorAfterCancelKeepsContextError proves that an
// Unsubscribe failing once the context has ended still leaves the context
// error matchable, whichever of the teardown and the context the exchange
// observes first. The order is up to the scheduler, so it runs many times.
func TestFetchToken_UnsubscribeErrorAfterCancelKeepsContextError(t *testing.T) {
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		conn1 := &cancellingSubConn{fakeConn: newFakeConn(fakeToken), cancel: cancel}
		dialer := newFakeDialer(conn1, newFakeConn(""))

		_, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil)
		cancel()
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "credential connection teardown") {
			t.Fatalf("attempt %d: Exchange error = %v, want credential connection teardown wrapping %v", i, err, context.Canceled)
		}
	}
}

// cancellingDisconnectConn is a fakeConn whose Disconnect cancels the
// caller's context and then succeeds, as go-stomp's does when the transport
// closes under a pending DISCONNECT.
type cancellingDisconnectConn struct {
	*fakeConn
	cancel context.CancelFunc
}

func (c *cancellingDisconnectConn) Disconnect() error {
	c.cancel()
	return c.fakeConn.Disconnect()
}

// TestFetchToken_ContextEndingDuringDisconnectIsReported proves that a
// Disconnect cut short by the end of the context fails the exchange with the
// context error, whichever of the teardown and the context the exchange
// observes first, rather than going on to the durable leg.
func TestFetchToken_ContextEndingDuringDisconnectIsReported(t *testing.T) {
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		conn1 := &cancellingDisconnectConn{fakeConn: newFakeConn(fakeToken), cancel: cancel}
		dialer := newFakeDialer(conn1, newFakeConn(""))

		_, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil)
		cancel()
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "credential connection teardown") {
			t.Fatalf("attempt %d: Exchange error = %v, want credential connection teardown wrapping %v", i, err, context.Canceled)
		}
		if len(dialer.calls) != 1 {
			t.Fatalf("attempt %d: dialer called %d times, want 1", i, len(dialer.calls))
		}
	}
}
