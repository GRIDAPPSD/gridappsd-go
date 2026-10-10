package auth_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
// the stacks of the goroutines that attempt started. A goroutine left parked
// by an earlier attempt says nothing about this one.
func stalledIn(fresh map[string]string) string {
	var inUnsubscribe bool
	for _, g := range fresh {
		if strings.Contains(g, "stomp/v3.(*Conn).Disconnect") {
			return disconnectWait
		}
		inUnsubscribe = inUnsubscribe || strings.Contains(g, "stomp/v3.waitWithTimeout")
	}
	if inUnsubscribe {
		return unsubscribeWait
	}
	return "unidentified wait"
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

// newStompGoroutines returns the goroutines stompGoroutines reports that are
// not in base.
func newStompGoroutines(base map[string]string) map[string]string {
	out := map[string]string{}
	for id, g := range stompGoroutines() {
		if _, old := base[id]; !old {
			out[id] = g
		}
	}
	return out
}

// waitNoNewStompGoroutines fails the test unless every goroutine that
// stompGoroutines reports, and that was not in base or ignore, has exited
// within d. Comparing ids rather than counts keeps a goroutine an earlier test
// left parked, and that exits meanwhile, from hiding a new one.
func waitNoNewStompGoroutines(t *testing.T, base map[string]string, d time.Duration, ignore map[string]bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		var left []string
		for id, g := range newStompGoroutines(base) {
			if !ignore[id] {
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
		before := stompGoroutines()
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
			stalls[stalledIn(newStompGoroutines(before))]++
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
// receipt timeout and keeps the waiting goroutine for good (#24). Only an
// attempt that fails at the deadline with a new goroutine parked in that wait
// is set aside as this case, at most maxMissed times, and only those
// goroutines are left out of the leak check.
func TestExchange_BrokerClosingAfterTokenDoesNotStallDisconnect(t *testing.T) {
	const (
		attempts   = 50
		budget     = 2 * time.Second
		stallBound = 1500 * time.Millisecond
		settle     = 2 * time.Second
		// maxMissed is four times the lost wakeup's measured rate of about 2
		// in 50 attempts, so a change that makes it common still fails.
		maxMissed = 8
	)
	b := startStompBroker(t, brokerCloseAfterToken)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	base := stompGoroutines()

	stalls, failed := map[string]int{}, map[string]int{}
	missed, parked := 0, map[string]bool{}
	for i := 0; i < attempts; i++ {
		before := stompGoroutines()
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		start := time.Now()
		conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, logger)
		elapsed := time.Since(start)
		cancel()
		if elapsed > stallBound {
			fresh := newStompGoroutines(before)
			wait := stalledIn(fresh)
			if wait == unsubscribeWait && errors.Is(err, context.DeadlineExceeded) {
				missed++
				for id, g := range fresh {
					if strings.Contains(g, "stomp/v3.waitWithTimeout") {
						parked[id] = true
					}
				}
				continue
			}
			stalls[fmt.Sprintf("%s, err %v", wait, err)]++
			if err == nil {
				_ = conn.Disconnect()
			}
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
	if len(stalls) > 0 || len(failed) > 0 {
		t.Errorf("%d attempts: teardown took over %v, by wait: %v; failed exchanges: %v", attempts, stallBound, stalls, failed)
	}
	t.Logf("%d of %d teardowns parked in the %s (#24), at most %d allowed", missed, attempts, unsubscribeWait, maxMissed)
	if missed > maxMissed {
		t.Errorf("%d of %d teardowns parked in the %s, want at most %d", missed, attempts, unsubscribeWait, maxMissed)
	}
	waitNoNewStompGoroutines(t, base, settle, parked)
}

// TestExchange_UnansweredDisconnectBoundedWithoutLeak runs the real stack
// against a broker that hands out the token and then never answers
// DISCONNECT, under a caller deadline longer than the receipt bound. The bound
// must end the teardown, keep the token, log the failure, and leave go-stomp's
// I/O loop no abandoned receipt channel to block on. With the I/O loop alive,
// the transport close at half the 1 s bound is what ends the wait.
func TestExchange_UnansweredDisconnectBoundedWithoutLeak(t *testing.T) {
	const (
		budget = 10 * time.Second
		within = 800 * time.Millisecond
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
	if !strings.Contains(logs.String(), "credential connection teardown failed") ||
		!strings.Contains(logs.String(), "half the 1s bound") {
		t.Errorf("log = %q, want the teardown failure naming the bound", logs.String())
	}
	waitNoNewStompGoroutines(t, base, settle, nil)
}

// TestExchange_DurableDisconnectKeepsDefaultReceiptWait checks that the
// credential connection's disconnect bound does not reach the durable
// connection: a durable DISCONNECT answered after that bound still ends on the
// broker's RECEIPT, with no error.
func TestExchange_DurableDisconnectKeepsDefaultReceiptWait(t *testing.T) {
	b := startStompBroker(t, brokerSlowDurableDisconnectReceipt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}, nil)
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	start := time.Now()
	if err := conn.Disconnect(); err != nil {
		t.Fatalf("durable Disconnect after %v: %v, want nil from the broker's late RECEIPT", time.Since(start), err)
	}
	if elapsed := time.Since(start); elapsed < slowReceiptDelay {
		t.Errorf("durable Disconnect returned after %v, before the broker's %v receipt delay", elapsed, slowReceiptDelay)
	}
	if got := b.commands(1); len(got) == 0 || got[len(got)-1] != "DISCONNECT" {
		t.Errorf("durable leg frames = %v, want the last one DISCONNECT", got)
	}
}
