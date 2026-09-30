package auth_test

import (
	"context"
	"io"
	"net"
	"runtime"
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
)

// stompBroker is an in-process TCP STOMP 1.2 broker, just enough of one for
// the real go-stomp client behind internal/stomp to run the token exchange
// against. It records the commands each connection sent, in order.
type stompBroker struct {
	t    *testing.T
	ln   net.Listener
	mode brokerMode
	wg   sync.WaitGroup

	mu    sync.Mutex
	conns [][]*frame.Frame
}

func startStompBroker(t *testing.T, mode brokerMode) *stompBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot open a TCP listener on this host: %v", err)
	}
	b := &stompBroker{t: t, ln: ln, mode: mode}
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
			b.mu.Unlock()
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				b.serve(idx, c)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		b.wg.Wait()
	})
	return b
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

		var out []*frame.Frame
		switch f.Command {
		case frame.CONNECT, frame.STOMP:
			out = append(out, frame.New(frame.CONNECTED, frame.Version, "1.2", frame.HeartBeat, "0,0"))
		case frame.SUBSCRIBE:
			subIDs[f.Header.Get(frame.Destination)] = f.Header.Get(frame.Id)
		case frame.SEND:
			if replyTo, ok := f.Header.Contains("reply-to"); ok && b.mode == brokerAnswering {
				dest := "/queue/" + replyTo
				msg := frame.New(frame.MESSAGE,
					frame.Destination, dest,
					frame.Subscription, subIDs[dest],
					frame.MessageId, "1")
				msg.Body = []byte(fakeToken)
				out = append(out, msg)
			}
		}
		if receipt, ok := f.Header.Contains(frame.Receipt); ok && b.mode == brokerAnswering {
			out = append(out, frame.New(frame.RECEIPT, frame.ReceiptId, receipt))
		}
		for _, o := range out {
			if err := w.Write(o); err != nil {
				return
			}
		}
	}
}

// waitForGoroutines polls until the process goroutine count is back at or
// below want, and fails the test if it is not by the deadline.
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
	if _, err := auth.Exchange(ctx, b.dial, &stomp.Dialer{}, "u", "p", 0, &transport.HeartBeatIntervals{}); err == nil {
		t.Fatal("Exchange against a silent broker returned nil error")
	}
}

// TestExchange_SilentBrokerTeardownDoesNotPanic drives the token exchange
// through real go-stomp against a broker that stalls after CONNECTED. A
// teardown left waiting on an UNSUBSCRIBE receipt when its connection closes
// panics the process inside go-stomp when that receipt timeout fires (30 s),
// so the wait below outlasts it; a clean teardown leaves no goroutine behind
// and returns at once.
func TestExchange_SilentBrokerTeardownDoesNotPanic(t *testing.T) {
	b := startStompBroker(t, brokerSilent)
	before := runtime.NumGoroutine()
	for i := 0; i < 40; i++ {
		silentExchange(t, b)
	}
	waitForGoroutines(t, before, 40*time.Second)
}
