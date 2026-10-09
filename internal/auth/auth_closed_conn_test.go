package auth_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/auth"
	"github.com/GRIDAPPSD/gridappsd-go/internal/stomp"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// allStacks returns the stacks of every goroutine.
func allStacks() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

const (
	disconnectWait  = "go-stomp Disconnect receipt wait"
	unsubscribeWait = "go-stomp Unsubscribe receipt wait"
)

// stalledIn names the go-stomp wait a stalled teardown is parked in, read from
// the stacks of every goroutine.
func stalledIn() string {
	stacks := allStacks()
	switch {
	case strings.Contains(stacks, "stomp/v3.(*Conn).Disconnect"):
		return disconnectWait
	case strings.Contains(stacks, "stomp/v3.waitWithTimeout"):
		return unsubscribeWait
	default:
		return "unidentified wait"
	}
}

// stompGoroutines returns the stack of each goroutine running go-stomp, or
// this module's stomp transport or auth teardown, keyed by goroutine id.
func stompGoroutines() map[string]string {
	out := map[string]string{}
	for _, g := range strings.Split(allStacks(), "\n\n") {
		if !strings.Contains(g, "go-stomp/stomp/v3") &&
			!strings.Contains(g, "gridappsd-go/internal/stomp.") &&
			!strings.Contains(g, "gridappsd-go/internal/auth.") {
			continue
		}
		id, _, _ := strings.Cut(strings.TrimPrefix(g, "goroutine "), " ")
		out[id] = g
	}
	return out
}

// waitNoNewStompGoroutines fails the test unless every goroutine that
// stompGoroutines reports, and that was not in base, has exited within d.
// Comparing ids rather than counts keeps a goroutine an earlier test left
// parked, and that exits meanwhile, from hiding a new one. ignore, when
// non-nil, excludes the goroutines it reports true for.
func waitNoNewStompGoroutines(t *testing.T, base map[string]string, d time.Duration, ignore func(stack string) bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		var left []string
		for id, g := range stompGoroutines() {
			if _, old := base[id]; !old && (ignore == nil || !ignore(g)) {
				left = append(left, g)
			}
		}
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d go-stomp or transport goroutines remain %v after the last attempt:\n%s",
				len(left), d, strings.Join(left, "\n\n"))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestExchange_BrokerClosingOnTokenRequestDoesNotStallTeardown runs the real
// stack against a broker that closes the credential connection on reading the
// token request. go-stomp v3.1.2 ends its I/O loop on the close and then needs
// the connection's close lock; a graceful Disconnect that takes the lock first
// waits for a receipt nothing can deliver, for go-stomp's 30 s receipt
// timeout. Every attempt must fail on the closed subscription well inside the
// caller's deadline and leave no goroutine behind. The race is narrow, so the
// attempts are many.
func TestExchange_BrokerClosingOnTokenRequestDoesNotStallTeardown(t *testing.T) {
	const (
		attempts   = 300
		budget     = 2 * time.Second
		stallBound = time.Second
		settle     = 2 * time.Second
	)
	b := startStompBroker(t, brokerCloseOnTokenRequest)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	base := stompGoroutines()

	stalls := map[string]int{}
	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		start := time.Now()
		conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, logger)
		elapsed := time.Since(start)
		cancel()
		if err == nil {
			_ = conn.Disconnect()
			t.Fatalf("attempt %d: Exchange returned nil error from a broker that closed before the token", i)
		}
		if !strings.Contains(err.Error(), "token subscription") {
			t.Fatalf("attempt %d: Exchange error = %v, want the token subscription to report the closed connection", i, err)
		}
		// The exchange already failed, so a teardown cut short by the deadline
		// keeps the subscription error; only the time shows the stall.
		if elapsed > stallBound {
			stalls[stalledIn()]++
		}
	}
	if len(stalls) > 0 {
		t.Errorf("%d attempts: teardown took over %v, by wait: %v", attempts, stallBound, stalls)
	}
	waitNoNewStompGoroutines(t, base, settle, nil)
}

// TestExchange_BrokerClosingAfterTokenDoesNotStallDisconnect runs the real
// stack against a broker that writes the token and then closes the credential
// connection. A close that lands after Disconnect has queued its DISCONNECT
// leaves go-stomp waiting for a receipt its ended I/O loop cannot deliver,
// until the credential connection's bound; no teardown may stall there.
//
// The same close can also land as go-stomp's Unsubscribe starts waiting, and
// go-stomp v3.1.2 then misses the wakeup, waits out its 30 s unsubscribe
// receipt timeout and keeps the waiting goroutine for good (#24). That wait is
// counted and logged here, and its goroutines are left out of the leak check.
func TestExchange_BrokerClosingAfterTokenDoesNotStallDisconnect(t *testing.T) {
	const (
		attempts   = 50
		budget     = 2 * time.Second
		stallBound = 1500 * time.Millisecond
		settle     = 2 * time.Second
	)
	b := startStompBroker(t, brokerCloseAfterToken)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	base := stompGoroutines()

	stalls, failed := map[string]int{}, map[string]int{}
	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		start := time.Now()
		conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, logger)
		elapsed := time.Since(start)
		cancel()
		if elapsed > stallBound {
			stalls[stalledIn()]++
			continue
		}
		if err != nil {
			failed[err.Error()]++
			continue
		}
		if err := conn.Disconnect(); err != nil {
			t.Errorf("attempt %d: durable Disconnect: %v", i, err)
		}
	}
	missed := stalls[unsubscribeWait]
	delete(stalls, unsubscribeWait)
	if len(stalls) > 0 || len(failed) > 0 {
		t.Errorf("%d attempts: teardown took over %v, by wait: %v; failed exchanges: %v", attempts, stallBound, stalls, failed)
	}
	if missed > 0 {
		t.Logf("%d of %d teardowns parked in the %s (#24)", missed, attempts, unsubscribeWait)
	}
	waitNoNewStompGoroutines(t, base, settle, func(stack string) bool {
		return strings.Contains(stack, "stomp/v3.waitWithTimeout")
	})
}

// TestExchange_UnansweredDisconnectBoundedWithoutLeak runs the real stack
// against a broker that hands out the token and then never answers
// DISCONNECT, under a caller deadline longer than the receipt bound. The bound
// must end the teardown, keep the token, log the failure, and leave go-stomp's
// I/O loop no abandoned receipt channel to block on.
func TestExchange_UnansweredDisconnectBoundedWithoutLeak(t *testing.T) {
	const (
		budget = 10 * time.Second
		within = 2 * time.Second
		settle = 2 * time.Second
	)
	b := startStompBroker(t, brokerNoCredentialDisconnectReceipt)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	base := stompGoroutines()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, logger)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Exchange error = %v, want nil: the token arrived before DISCONNECT went unanswered", err)
	}
	if elapsed > within {
		t.Errorf("Exchange took %v, want at most %v", elapsed, within)
	}
	if err := conn.Disconnect(); err != nil {
		t.Errorf("durable Disconnect: %v", err)
	}
	if got := b.commands(0); len(got) == 0 || got[len(got)-1] != "DISCONNECT" {
		t.Errorf("credential leg frames = %v, want the last one DISCONNECT", got)
	}
	if !strings.Contains(logs.String(), "credential connection teardown failed") {
		t.Errorf("log = %q, want the teardown failure", logs.String())
	}
	waitNoNewStompGoroutines(t, base, settle, nil)
}
