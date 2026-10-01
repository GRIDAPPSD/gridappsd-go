package auth_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	"github.com/GRIDAPPSD/gridappsd-go/internal/auth"
	"github.com/GRIDAPPSD/gridappsd-go/internal/stomp"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// brokerMode selects how stompBroker answers after CONNECTED.
type brokerMode int

const (
	// brokerSilent answers CONNECT and then never writes again: no MESSAGE,
	// no RECEIPT. This is a broker that accepted the login and then stalled.
	brokerSilent brokerMode = iota
	// brokerAnswering routes the token reply and answers every receipt.
	brokerAnswering
	// brokerNoDisconnectReceipt is brokerAnswering except that DISCONNECT is
	// never answered: the broker stalls after handing out the token.
	brokerNoDisconnectReceipt
	// brokerFlood is brokerAnswering except that the token reply is followed
	// by more messages on the reply queue than the client buffers hold.
	brokerFlood
	// brokerNoUnsubscribeReceipt is brokerAnswering except that UNSUBSCRIBE
	// is never answered: the broker stalls after handing out the token.
	brokerNoUnsubscribeReceipt
	// brokerCloseAfterToken is brokerAnswering except that the credential
	// connection is closed as soon as the token reply is written, before
	// UNSUBSCRIBE or DISCONNECT can be answered.
	brokerCloseAfterToken
	// brokerErrorOnDisconnect is brokerAnswering except that DISCONNECT is
	// answered with an ERROR frame whose message header is oversizedErrLen
	// bytes long.
	brokerErrorOnDisconnect
)

// oversizedErrLen is the length of brokerErrorOnDisconnect's ERROR message.
const oversizedErrLen = 4 << 20

// floodExtra is how many messages brokerFlood sends after the token: more
// than go-stomp's and the bridge's channel buffers (16 each) can absorb.
const floodExtra = 64

// stompBroker is an in-process TCP STOMP 1.2 broker, just enough of one for
// the real go-stomp client behind internal/stomp to run the token exchange
// against. It records the commands each connection sent, in order.
type stompBroker struct {
	t    *testing.T
	ln   net.Listener
	mode brokerMode
	// onFrame, when set, is called with each frame's command as it is read,
	// before any reply is written.
	onFrame func(cmd string)
	wg      sync.WaitGroup

	mu      sync.Mutex
	conns   [][]*frame.Frame
	sockets []net.Conn
}

func startStompBroker(t *testing.T, mode brokerMode) *stompBroker {
	t.Helper()
	return startStompBrokerHook(t, mode, nil)
}

func startStompBrokerHook(t *testing.T, mode brokerMode, onFrame func(cmd string)) *stompBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot open a TCP listener on this host: %v", err)
	}
	b := &stompBroker{t: t, ln: ln, mode: mode, onFrame: onFrame}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			idx := len(b.conns)
			b.conns = append(b.conns, nil)
			b.sockets = append(b.sockets, c)
			b.mu.Unlock()
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				b.serve(idx, c)
			}()
		}
	}()
	t.Cleanup(func() {
		// Close the server side too, so a client left hanging by a failed
		// test cannot hold cleanup open.
		_ = ln.Close()
		b.mu.Lock()
		for _, c := range b.sockets {
			_ = c.Close()
		}
		b.mu.Unlock()
		b.wg.Wait()
	})
	return b
}

// commands returns the commands connection idx has sent so far, in order.
func (b *stompBroker) commands(idx int) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	if idx < len(b.conns) {
		for _, f := range b.conns[idx] {
			out = append(out, f.Command)
		}
	}
	return out
}

// frameOf returns the first frame connection idx sent with command cmd, or
// nil if there is none.
func (b *stompBroker) frameOf(idx int, cmd string) *frame.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()
	if idx >= len(b.conns) {
		return nil
	}
	for _, f := range b.conns[idx] {
		if f.Command == cmd {
			return f
		}
	}
	return nil
}

func (b *stompBroker) dial(ctx context.Context) (io.ReadWriteCloser, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", b.ln.Addr().String())
}

func (b *stompBroker) serve(idx int, c net.Conn) {
	defer c.Close()
	r := frame.NewReader(c)
	w := frame.NewWriter(c)
	subIDs := make(map[string]string) // destination -> subscription id
	for {
		f, err := r.Read()
		if err != nil {
			return
		}
		if f == nil {
			continue // heart-beat
		}
		b.mu.Lock()
		b.conns[idx] = append(b.conns[idx], f)
		b.mu.Unlock()
		if b.onFrame != nil {
			b.onFrame(f.Command)
		}

		var out []*frame.Frame
		switch f.Command {
		case frame.CONNECT, frame.STOMP:
			out = append(out, frame.New(frame.CONNECTED, frame.Version, "1.2", frame.HeartBeat, "0,0"))
		case frame.SUBSCRIBE:
			subIDs[f.Header.Get(frame.Destination)] = f.Header.Get(frame.Id)
		case frame.SEND:
			if replyTo, ok := f.Header.Contains("reply-to"); ok && b.mode != brokerSilent {
				dest := "/queue/" + replyTo
				n := 1
				if b.mode == brokerFlood {
					n += floodExtra
				}
				for i := 0; i < n; i++ {
					msg := frame.New(frame.MESSAGE,
						frame.Destination, dest,
						frame.Subscription, subIDs[dest],
						frame.MessageId, strconv.Itoa(i))
					msg.Body = []byte(fakeToken)
					out = append(out, msg)
				}
			}
		}
		if b.mode == brokerErrorOnDisconnect && f.Command == frame.DISCONNECT {
			out = append(out, frame.New(frame.ERROR, frame.Message, strings.Repeat("E", oversizedErrLen)))
		}
		receipt, ok := f.Header.Contains(frame.Receipt)
		answer := b.mode != brokerSilent &&
			!(b.mode == brokerErrorOnDisconnect && f.Command == frame.DISCONNECT) &&
			!(b.mode == brokerNoDisconnectReceipt && f.Command == frame.DISCONNECT) &&
			!(b.mode == brokerNoUnsubscribeReceipt && f.Command == frame.UNSUBSCRIBE)
		if ok && answer {
			out = append(out, frame.New(frame.RECEIPT, frame.ReceiptId, receipt))
		}
		for _, o := range out {
			if err := w.Write(o); err != nil {
				return
			}
		}
		if b.mode == brokerCloseAfterToken && f.Command == frame.SEND {
			return
		}
	}
}

func waitForGoroutines(t *testing.T, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for runtime.NumGoroutine() > want {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count still %d after %v, want at most %d", runtime.NumGoroutine(), within, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// silentExchange runs one Exchange through the real go-stomp stack against a
// silent broker, with a caller deadline far shorter than go-stomp's 30 s
// receipt timeouts, and requires it to fail.
func silentExchange(t *testing.T, b *stompBroker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, nil); err == nil {
		t.Fatal("Exchange against a silent broker returned nil error")
	}
}

// TestExchange_SilentBrokerTeardownDoesNotPanic drives the token exchange
// through real go-stomp against a broker that stalls after CONNECTED. go-stomp
// can panic on a closed connection when an UNSUBSCRIBE receipt timeout fires
// (30 s), so the wait below outlasts it and an escaped panic fails the run; a
// clean teardown leaves no goroutine behind and returns at once.
func TestExchange_SilentBrokerTeardownDoesNotPanic(t *testing.T) {
	b := startStompBroker(t, brokerSilent)
	before := runtime.NumGoroutine()
	for i := 0; i < 40; i++ {
		silentExchange(t, b)
	}
	waitForGoroutines(t, before, 40*time.Second)
}

// TestExchange_SilentBrokerRetryLoopKeepsGoroutinesBounded retries the token
// exchange against a broker that stalls after CONNECTED and requires the
// goroutine count to be back at its starting value after every attempt, so a
// per-attempt residue fails on the first attempt that leaves one. Once the
// caller's deadline passes the transport is closed rather than left waiting on
// go-stomp's 30 s receipt timeouts, so the in-flight bound is one attempt's
// goroutines.
func TestExchange_SilentBrokerRetryLoopKeepsGoroutinesBounded(t *testing.T) {
	b := startStompBroker(t, brokerSilent)
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		silentExchange(t, b)
		waitForGoroutines(t, before, 2*time.Second)
	}
}

// TestExchange_AnsweringBrokerFrameOrder checks, on the wire of the real
// go-stomp stack, the frames each leg sends and that the credential leg ends
// with UNSUBSCRIBE then DISCONNECT while the caller's deadline is live.
func TestExchange_AnsweringBrokerFrameOrder(t *testing.T) {
	b := startStompBroker(t, brokerAnswering)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, nil)
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if err := conn.Disconnect(); err != nil {
		t.Fatalf("durable Disconnect: %v", err)
	}

	want := []string{frame.CONNECT, frame.SUBSCRIBE, frame.SEND, frame.UNSUBSCRIBE, frame.DISCONNECT}
	if got := b.commands(0); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("credential leg frames = %v, want %v", got, want)
	}
	want = []string{frame.CONNECT, frame.DISCONNECT}
	if got := b.commands(1); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("durable leg frames = %v, want %v", got, want)
	}
	credConnect, durableConnect := b.frameOf(0, frame.CONNECT), b.frameOf(1, frame.CONNECT)
	subFrame, sendFrame := b.frameOf(0, frame.SUBSCRIBE), b.frameOf(0, frame.SEND)
	if credConnect == nil || durableConnect == nil || subFrame == nil || sendFrame == nil {
		t.Fatalf("broker recorded credential CONNECT %v, durable CONNECT %v, SUBSCRIBE %v, SEND %v; want all four", credConnect != nil, durableConnect != nil, subFrame != nil, sendFrame != nil)
	}
	if got := credConnect.Header.Get(frame.Login); got != "u" {
		t.Errorf("credential CONNECT login = %q, want %q", got, "u")
	}
	if got := durableConnect.Header.Get(frame.Login); got != fakeToken {
		t.Errorf("durable CONNECT login = %q, want the token %q", got, fakeToken)
	}
	if _, ok := subFrame.Header.Contains("reply-to"); ok {
		t.Error("SUBSCRIBE carries a reply-to header; it belongs on SEND only")
	}
	if got, want := "/queue/"+sendFrame.Header.Get("reply-to"), subFrame.Header.Get(frame.Destination); got != want {
		t.Errorf("SEND reply-to resolves to %q, want the subscribed destination %q", got, want)
	}
	waitForGoroutines(t, before, 5*time.Second)
}

// TestExchange_BrokerClosingCredentialConnKeepsToken runs the real stack
// against a broker that closes the credential connection right after the
// token reply, so UNSUBSCRIBE fails on an already ended session. The token
// that arrived must still be used for the durable leg, and the failure must
// reach the caller's logger without the credentials or the token.
func TestExchange_BrokerClosingCredentialConnKeepsToken(t *testing.T) {
	const attempts = 50
	b := startStompBroker(t, brokerCloseAfterToken)
	h := &recordingHandler{}
	ok, stalled, byErr := 0, 0, map[string]int{}
	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, secretUser, secretPass, 0, &transport.HeartBeatIntervals{}, slog.New(h))
		cancel()
		switch {
		case err == nil:
			ok++
			_ = conn.Disconnect()
		case errors.Is(err, context.DeadlineExceeded):
			// go-stomp's Unsubscribe can miss the wakeup from the closed
			// connection and wait out its receipt timeout (#24); the
			// deadline then fails the exchange, as the context rule requires.
			stalled++
		default:
			byErr[err.Error()]++
		}
	}
	if len(byErr) > 0 || ok < attempts/2 {
		t.Fatalf("%d of %d exchanges kept the token, %d hit the deadline, other failures: %v", ok, attempts, stalled, byErr)
	}
	recs := h.snapshot()
	if len(recs) == 0 {
		t.Fatal("no teardown failure reached the logger")
	}
	payload := base64.StdEncoding.EncodeToString([]byte(secretUser + ":" + secretPass))
	for _, rec := range recs {
		if rec.level != slog.LevelWarn {
			t.Errorf("record level = %v, want %v", rec.level, slog.LevelWarn)
		}
		for _, secret := range []string{secretPass, fakeToken, payload} {
			if strings.Contains(rec.text(), secret) {
				t.Errorf("log record %q carries a secret %q", rec.text(), secret)
			}
		}
	}
	t.Logf("%d kept the token, %d hit the deadline, %d teardown failures logged", ok, stalled, len(recs))
}

// TestExchange_StalledDisconnectReceiptBoundedByContext runs the real stack
// against a broker that hands out the token and then never answers
// DISCONNECT: Exchange must return by the caller's deadline with an error
// naming the credential connection, and closing the transport at that
// deadline must end go-stomp's goroutines rather than its 30 s timeout.
func TestExchange_StalledDisconnectReceiptBoundedByContext(t *testing.T) {
	const (
		ctxBudget = 300 * time.Millisecond
		margin    = 400 * time.Millisecond
	)
	b := startStompBroker(t, brokerNoDisconnectReceipt)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), ctxBudget)
	defer cancel()
	start := time.Now()
	_, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, nil)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "credential connection teardown") {
		t.Fatalf("Exchange error = %v, want credential connection teardown wrapping %v", err, context.DeadlineExceeded)
	}
	if elapsed > ctxBudget+margin {
		t.Errorf("Exchange took %v, want at most %v", elapsed, ctxBudget+margin)
	}
	waitForGoroutines(t, before, 2*time.Second)
}

// TestExchange_StalledUnsubscribeReceiptBoundedByContext runs the real stack
// against a broker that hands out the token and then never answers
// UNSUBSCRIBE. The UNSUBSCRIBE is already pending when the caller's deadline
// passes, and closing the transport then must end go-stomp's receipt wait and
// release the sockets, rather than holding them for its 30 s receipt timeout.
func TestExchange_StalledUnsubscribeReceiptBoundedByContext(t *testing.T) {
	const (
		ctxBudget = 300 * time.Millisecond
		margin    = 400 * time.Millisecond
		settle    = 2 * time.Second
	)
	b := startStompBroker(t, brokerNoUnsubscribeReceipt)
	beforeG, beforeFD := runtime.NumGoroutine(), countOpenFDs(t)

	ctx, cancel := context.WithTimeout(context.Background(), ctxBudget)
	defer cancel()
	start := time.Now()
	_, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, nil)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "credential connection teardown") {
		t.Fatalf("Exchange error = %v, want credential connection teardown wrapping %v", err, context.DeadlineExceeded)
	}
	if got := b.commands(0); len(got) == 0 || got[len(got)-1] != frame.UNSUBSCRIBE {
		t.Fatalf("credential leg frames = %v, want the last one UNSUBSCRIBE; the deadline did not find it pending", got)
	}
	if elapsed > ctxBudget+margin {
		t.Errorf("Exchange took %v, want at most %v", elapsed, ctxBudget+margin)
	}
	deadline := time.Now().Add(settle)
	for (runtime.NumGoroutine() > beforeG || countOpenFDs(t) > beforeFD) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > beforeG {
		t.Errorf("goroutine count %d, %v after Exchange returned, want at most %d", got, settle, beforeG)
	}
	if got := countOpenFDs(t); got > beforeFD {
		t.Errorf("descriptor count %d, %v after Exchange returned, want at most %d", got, settle, beforeFD)
	}
}

// TestExchange_ContextEndsAtEachTeardownStep cancels the caller's context at
// the moment the broker reads each step of the credential leg, with the
// step's receipt withheld or answered, so context expiry lands before, during
// and racing each receipt wait. Every order must fail the exchange without a
// panic, return promptly after the cancel, and leave no goroutine or socket.
func TestExchange_ContextEndsAtEachTeardownStep(t *testing.T) {
	const (
		margin = 400 * time.Millisecond
		settle = 2 * time.Second
	)
	cases := []struct {
		name string
		mode brokerMode
		at   string
	}{
		{"before the token, at SEND", brokerAnswering, frame.SEND},
		{"UNSUBSCRIBE pending, receipt withheld", brokerNoUnsubscribeReceipt, frame.UNSUBSCRIBE},
		{"UNSUBSCRIBE pending, receipt answered", brokerAnswering, frame.UNSUBSCRIBE},
		{"DISCONNECT pending, receipt withheld", brokerNoDisconnectReceipt, frame.DISCONNECT},
		{"DISCONNECT pending, receipt answered", brokerAnswering, frame.DISCONNECT},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var (
				once     sync.Once
				cancelMu sync.Mutex
				cancelAt time.Time
			)
			b := startStompBrokerHook(t, tc.mode, func(cmd string) {
				if cmd == tc.at {
					once.Do(func() {
						cancelMu.Lock()
						cancelAt = time.Now()
						cancelMu.Unlock()
						cancel()
					})
				}
			})
			beforeG, beforeFD := runtime.NumGoroutine(), countOpenFDs(t)

			conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, nil)
			returned := time.Now()
			if err == nil {
				_ = conn.Disconnect()
				t.Fatal("Exchange returned nil error after its context was cancelled")
			}
			cancelMu.Lock()
			at := cancelAt
			cancelMu.Unlock()
			if at.IsZero() {
				t.Fatalf("broker never read %s; Exchange error = %v", tc.at, err)
			}
			if d := returned.Sub(at); d > margin {
				t.Errorf("Exchange returned %v after the cancel, want at most %v", d, margin)
			}
			deadline := time.Now().Add(settle)
			for (runtime.NumGoroutine() > beforeG || countOpenFDs(t) > beforeFD) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if got := runtime.NumGoroutine(); got > beforeG {
				t.Errorf("goroutine count %d, %v after Exchange returned, want at most %d", got, settle, beforeG)
			}
			if got := countOpenFDs(t); got > beforeFD {
				t.Errorf("descriptor count %d, %v after Exchange returned, want at most %d", got, settle, beforeFD)
			}
		})
	}
}

// TestExchange_FloodedReplyQueueDoesNotStallTeardown runs the real stack
// against a broker that follows the token with more messages than the client
// buffers hold. Unread, they would stall go-stomp's I/O loop so UNSUBSCRIBE is
// never written and teardown waits on go-stomp's 30 s receipt timeout.
func TestExchange_FloodedReplyQueueDoesNotStallTeardown(t *testing.T) {
	b := startStompBroker(t, brokerFlood)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, nil)
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if err := conn.Disconnect(); err != nil {
		t.Fatalf("durable Disconnect: %v", err)
	}
	waitForGoroutines(t, before, 5*time.Second)
}

// TestExchange_OversizedBrokerErrorIsBoundedInLog runs the real stack against
// a broker that answers DISCONNECT with a 4 MiB ERROR message. The teardown
// failure must still be logged with the token kept, but as one bounded line
// under each standard handler rather than a copy of the broker's text.
func TestExchange_OversizedBrokerErrorIsBoundedInLog(t *testing.T) {
	const maxLine = 1024
	handlers := map[string]func(io.Writer) slog.Handler{
		"text": func(w io.Writer) slog.Handler { return slog.NewTextHandler(w, nil) },
		"json": func(w io.Writer) slog.Handler { return slog.NewJSONHandler(w, nil) },
	}
	for name, newHandler := range handlers {
		t.Run(name, func(t *testing.T) {
			b := startStompBroker(t, brokerErrorOnDisconnect)
			var buf bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, secretUser, secretPass, 0, &transport.HeartBeatIntervals{}, slog.New(newHandler(&buf)))
			if err != nil {
				t.Fatalf("Exchange error = %v, want nil: a DISCONNECT ERROR after the token arrived must not fail the exchange", err)
			}
			_ = conn.Disconnect()

			out := buf.String()
			if n := strings.Count(out, "\n"); n != 1 {
				t.Fatalf("handler wrote %d lines (%d bytes), want 1", n, len(out))
			}
			if len(out) > maxLine {
				t.Errorf("log line is %d bytes, want at most %d", len(out), maxLine)
			}
			for _, want := range []string{"credential connection teardown", "EEEE", "truncated"} {
				if !strings.Contains(out, want) {
					t.Errorf("log line %q does not contain %q", out, want)
				}
			}
			payload := base64.StdEncoding.EncodeToString([]byte(secretUser + ":" + secretPass))
			for _, secret := range []string{secretPass, fakeToken, payload} {
				if strings.Contains(out, secret) {
					t.Errorf("log line carries a secret %q", secret)
				}
			}
		})
	}
}
