package auth_test

import (
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

// stalledIn names the go-stomp wait a stalled teardown is parked in, read from
// the stacks of every goroutine.
func stalledIn() string {
	buf := make([]byte, 1<<20)
	stacks := string(buf[:runtime.Stack(buf, true)])
	switch {
	case strings.Contains(stacks, "stomp/v3.(*Conn).Disconnect"):
		return "go-stomp Disconnect receipt wait"
	case strings.Contains(stacks, "stomp/v3.waitWithTimeout"):
		return "go-stomp Unsubscribe receipt wait"
	default:
		return "unidentified wait"
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
	before := runtime.NumGoroutine()

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
	deadline := time.Now().Add(settle)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		buf := make([]byte, 1<<20)
		t.Fatalf("goroutine count %d, %v after the last attempt, want at most %d; parked in %s:\n%s",
			got, settle, before, stalledIn(), buf[:runtime.Stack(buf, true)])
	}
}
