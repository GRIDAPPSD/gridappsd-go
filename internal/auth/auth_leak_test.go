package auth_test

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"log/slog"
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
		_, err := auth.Exchange(context.Background(), dialReal, dialer, "u", "p", time.Second, nil, nil)
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
		_, err := auth.Exchange(context.Background(), netDial, dialer, "u", "p", time.Second, nil, nil)
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
	_, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil, nil)
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
	if _, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
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

// logRecord is one record as a caller's slog.Handler receives it.
type logRecord struct {
	level slog.Level
	msg   string
	attrs map[string]slog.Value
}

// text renders the record the way a text handler would expose it, for
// asserting that no secret reaches a log sink.
func (r logRecord) text() string {
	var b strings.Builder
	b.WriteString(r.msg)
	for k, v := range r.attrs {
		b.WriteString(" " + k + "=" + v.String())
	}
	return b.String()
}

// recordingHandler is a slog.Handler that keeps every record it receives.
type recordingHandler struct {
	mu      sync.Mutex
	records []logRecord
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{level: r.Level, msg: r.Message, attrs: map[string]slog.Value{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, rec)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) snapshot() []logRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]logRecord(nil), h.records...)
}

const (
	secretUser = "grid-operator"
	secretPass = "s3cret-passcode-xyz"
)

// requireTeardownLogged runs Exchange over conn1 with a recording logger and
// requires the token to be used for the durable leg, the credential
// connection to be disconnected when disconnected is non-nil, and exactly one
// Warn record whose "error" attribute satisfies match and which carries no
// credential or token.
func requireTeardownLogged(t *testing.T, conn1 transport.Conn, disconnected func() bool, match func(error) bool) {
	t.Helper()
	h := &recordingHandler{}
	dialer := newFakeDialer(conn1, newFakeConn(""))

	conn, err := auth.Exchange(context.Background(), netDialStub, dialer, secretUser, secretPass, 10*time.Second, nil, slog.New(h))
	if err != nil {
		t.Fatalf("Exchange error = %v, want nil: a teardown failure after the token arrived must not fail the exchange", err)
	}
	if conn == nil {
		t.Fatal("Exchange returned a nil connection with a nil error")
	}
	if len(dialer.calls) != 2 || dialer.calls[1].Login != fakeToken {
		t.Fatalf("durable leg dial configs = %+v, want a second dial with login %q", dialer.calls, fakeToken)
	}
	if disconnected != nil && !disconnected() {
		t.Error("credential connection was not disconnected")
	}

	recs := h.snapshot()
	if len(recs) != 1 {
		t.Fatalf("got %d log records, want 1: %+v", len(recs), recs)
	}
	rec := recs[0]
	if rec.level != slog.LevelWarn {
		t.Errorf("record level = %v, want %v", rec.level, slog.LevelWarn)
	}
	if !strings.Contains(rec.msg, "credential connection teardown") {
		t.Errorf("record message = %q, want it to name the credential connection teardown", rec.msg)
	}
	if v, ok := rec.attrs["token_received"]; !ok || v.Kind() != slog.KindBool || !v.Bool() {
		t.Errorf("token_received attr = %v (present %v), want true", v, ok)
	}
	v, ok := rec.attrs["error"]
	if !ok {
		t.Fatalf("record has no error attr: %+v", rec.attrs)
	}
	logged, isErr := v.Any().(error)
	if !isErr || !match(logged) {
		t.Errorf("error attr = %v, does not match the teardown failure", v)
	}
	payload := base64.StdEncoding.EncodeToString([]byte(secretUser + ":" + secretPass))
	for _, secret := range []string{secretPass, fakeToken, payload} {
		if strings.Contains(rec.text(), secret) {
			t.Errorf("log record %q carries a secret %q", rec.text(), secret)
		}
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
// Unsubscribe after the token arrived is contained to the teardown, which
// still disconnects, logs the panic and keeps the token.
func TestFetchToken_UnsubscribePanicDoesNotEscape(t *testing.T) {
	conn1 := &panickingSubConn{fakeConn: newFakeConn(fakeToken)}
	requireTeardownLogged(t, conn1, func() bool { return conn1.disconnected }, func(err error) bool {
		var re runtime.Error
		return errors.As(err, &re)
	})
}

var (
	errUnsubscribeFailed = errors.New("unsubscribe receipt timeout")
	errDisconnectFailed  = errors.New("disconnect receipt timeout")
)

// failingSub is a fakeSub whose Unsubscribe ends the subscription and then
// reports an error, as go-stomp's does after its receipt wait times out.
type failingSub struct{ *fakeSub }

func (s *failingSub) Unsubscribe() error {
	s.fakeSub.closeC()
	return errUnsubscribeFailed
}

// failingConn is a fakeConn whose subscription's Unsubscribe fails when
// failUnsub is set, and whose Disconnect fails when failDisconnect is set.
type failingConn struct {
	*fakeConn
	failUnsub, failDisconnect bool
}

func (c *failingConn) Subscribe(ctx context.Context, dest string) (transport.Subscription, error) {
	sub, err := c.fakeConn.Subscribe(ctx, dest)
	if err != nil || !c.failUnsub {
		return sub, err
	}
	return &failingSub{fakeSub: sub.(*fakeSub)}, nil
}

func (c *failingConn) Disconnect() error {
	err := c.fakeConn.Disconnect()
	if c.failDisconnect {
		return errDisconnectFailed
	}
	return err
}

// TestFetchToken_TeardownErrorLoggedTokenKept proves that an Unsubscribe or
// Disconnect failure on the credential connection, once the token arrived,
// reaches the caller's logger and leaves the exchange successful.
func TestFetchToken_TeardownErrorLoggedTokenKept(t *testing.T) {
	cases := []struct {
		name                      string
		failUnsub, failDisconnect bool
	}{
		{"unsubscribe fails", true, false},
		{"disconnect fails", false, true},
		{"both fail", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn1 := &failingConn{fakeConn: newFakeConn(fakeToken), failUnsub: tc.failUnsub, failDisconnect: tc.failDisconnect}
			requireTeardownLogged(t, conn1, func() bool { return conn1.disconnected }, func(err error) bool {
				return errors.Is(err, errUnsubscribeFailed) == tc.failUnsub &&
					errors.Is(err, errDisconnectFailed) == tc.failDisconnect
			})
		})
	}
}

// TestExchange_NilLoggerUsesSlogDefault proves a nil logger falls back to
// slog.Default rather than dropping the teardown failure.
func TestExchange_NilLoggerUsesSlogDefault(t *testing.T) {
	// Not parallel: it swaps the process-wide default logger. slog.SetDefault
	// also redirects the log package, so its writer and flags are restored too.
	oldLogger, oldWriter, oldFlags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})
	h := &recordingHandler{}
	slog.SetDefault(slog.New(h))

	conn1 := &failingConn{fakeConn: newFakeConn(fakeToken), failUnsub: true}
	dialer := newFakeDialer(conn1, newFakeConn(""))
	if _, err := auth.Exchange(context.Background(), netDialStub, dialer, "u", "p", 10*time.Second, nil, nil); err != nil {
		t.Fatalf("Exchange error = %v, want nil", err)
	}
	recs := h.snapshot()
	if len(recs) != 1 || recs[0].level != slog.LevelWarn {
		t.Fatalf("default logger records = %+v, want one Warn record", recs)
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

// requireCancelDuringTeardownFails runs Exchange many times over a
// credential connection built by newConn, which cancels the context during
// the teardown, and requires every run to fail with the context error, never
// dial the durable leg, and log nothing: the context path is reported through
// the error, the teardown-error path through the logger. Which of the
// teardown and the context the exchange observes first is up to the
// scheduler, hence the repetition.
func requireCancelDuringTeardownFails(t *testing.T, newConn func(cancel context.CancelFunc) transport.Conn) {
	t.Helper()
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		h := &recordingHandler{}
		dialer := newFakeDialer(newConn(cancel), newFakeConn(""))

		conn, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil, slog.New(h))
		cancel()
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "credential connection teardown") {
			t.Fatalf("attempt %d: Exchange error = %v, want credential connection teardown wrapping %v", i, err, context.Canceled)
		}
		if conn != nil {
			t.Fatalf("attempt %d: Exchange returned a connection alongside error %v", i, err)
		}
		if len(dialer.calls) != 1 {
			t.Fatalf("attempt %d: dialer called %d times, want 1", i, len(dialer.calls))
		}
		if recs := h.snapshot(); len(recs) != 0 {
			t.Fatalf("attempt %d: got log records %+v, want none on the context path", i, recs)
		}
	}
}

// TestFetchToken_UnsubscribeErrorAfterCancelKeepsContextError proves that an
// Unsubscribe failing once the context has ended fails the exchange with the
// context error rather than logging it and keeping the token.
func TestFetchToken_UnsubscribeErrorAfterCancelKeepsContextError(t *testing.T) {
	requireCancelDuringTeardownFails(t, func(cancel context.CancelFunc) transport.Conn {
		return &cancellingSubConn{fakeConn: newFakeConn(fakeToken), cancel: cancel}
	})
}

// TestFetchToken_ContextEndingDuringDisconnectIsReported proves that a
// Disconnect cut short by the end of the context fails the exchange with the
// context error rather than going on to the durable leg.
func TestFetchToken_ContextEndingDuringDisconnectIsReported(t *testing.T) {
	requireCancelDuringTeardownFails(t, func(cancel context.CancelFunc) transport.Conn {
		return &cancellingDisconnectConn{fakeConn: newFakeConn(fakeToken), cancel: cancel}
	})
}

// lateDoneCtx reports Canceled from Err as soon as cancel is called but
// closes Done only on release. That holds open, deterministically, the window
// in which the teardown has seen the context end and the caller has not, so
// the teardown's own result is what the caller receives.
type lateDoneCtx struct {
	context.Context
	mu   sync.Mutex
	err  error
	done chan struct{}
}

func newLateDoneCtx() *lateDoneCtx {
	return &lateDoneCtx{Context: context.Background(), done: make(chan struct{})}
}

func (c *lateDoneCtx) Done() <-chan struct{} { return c.done }

func (c *lateDoneCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *lateDoneCtx) cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = context.Canceled
}

// TestFetchToken_CancelDuringUnsubscribeWrapsBoth proves that when the
// context ends during a failing Unsubscribe, the exchange error wraps both
// the context error and the Unsubscribe error, and nothing is logged.
func TestFetchToken_CancelDuringUnsubscribeWrapsBoth(t *testing.T) {
	ctx := newLateDoneCtx()
	t.Cleanup(func() { close(ctx.done) })
	h := &recordingHandler{}
	conn1 := &cancellingSubConn{fakeConn: newFakeConn(fakeToken), cancel: ctx.cancel}
	dialer := newFakeDialer(conn1, newFakeConn(""))

	conn, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil, slog.New(h))
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errUnsubscribeFailed) || !strings.Contains(err.Error(), "credential connection teardown") {
		t.Fatalf("Exchange error = %v, want credential connection teardown wrapping both %v and %v", err, context.Canceled, errUnsubscribeFailed)
	}
	if conn != nil {
		t.Errorf("Exchange returned a connection alongside error %v", err)
	}
	if len(dialer.calls) != 1 {
		t.Errorf("dialer called %d times, want 1", len(dialer.calls))
	}
	if conn1.disconnected {
		t.Error("Disconnect ran after the context ended; the closed transport ends the connection instead")
	}
	if recs := h.snapshot(); len(recs) != 0 {
		t.Errorf("got log records %+v, want none on the context path", recs)
	}
}
