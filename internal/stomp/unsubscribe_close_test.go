package stomp

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// tcpBroker is a fake STOMP broker on a loopback TCP listener. It answers
// CONNECT and DISCONNECT, delivers msgsOnSubscribe MESSAGE frames per
// SUBSCRIBE, and never answers UNSUBSCRIBE, so an unsubscribe waits out its
// receipt timeout.
type tcpBroker struct {
	ln              net.Listener
	msgsOnSubscribe int

	// unsubAt and discAt receive the arrival time of each UNSUBSCRIBE and
	// DISCONNECT frame.
	unsubAt chan time.Time
	discAt  chan time.Time

	mu    sync.Mutex
	conns []net.Conn
	wg    sync.WaitGroup
}

func startTCPBroker(t *testing.T, msgsOnSubscribe int) *tcpBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	b := &tcpBroker{
		ln:              ln,
		msgsOnSubscribe: msgsOnSubscribe,
		unsubAt:         make(chan time.Time, 16),
		discAt:          make(chan time.Time, 16),
	}
	b.wg.Add(1)
	go b.accept()
	t.Cleanup(b.close)
	return b
}

func (b *tcpBroker) accept() {
	defer b.wg.Done()
	for {
		nc, err := b.ln.Accept()
		if err != nil {
			return
		}
		b.mu.Lock()
		b.conns = append(b.conns, nc)
		b.mu.Unlock()
		b.wg.Add(1)
		go b.serve(nc)
	}
}

func (b *tcpBroker) serve(nc net.Conn) {
	defer b.wg.Done()
	defer nc.Close()
	r := frame.NewReader(nc)
	w := frame.NewWriter(nc)
	for {
		f, err := r.Read()
		if err != nil {
			return
		}
		if f == nil {
			continue
		}
		switch f.Command {
		case frame.CONNECT, frame.STOMP:
			_ = w.Write(frame.New(frame.CONNECTED, frame.Version, "1.2", frame.HeartBeat, "0,0"))
		case frame.SUBSCRIBE:
			id, _ := f.Header.Contains(frame.Id)
			dest, _ := f.Header.Contains(frame.Destination)
			for i := 0; i < b.msgsOnSubscribe; i++ {
				m := frame.New(frame.MESSAGE,
					frame.Subscription, id,
					frame.Destination, dest,
					frame.MessageId, "m",
				)
				m.Body = []byte("x")
				_ = w.Write(m)
			}
		case frame.UNSUBSCRIBE:
			b.unsubAt <- time.Now()
		case frame.DISCONNECT:
			b.discAt <- time.Now()
			if id, ok := f.Header.Contains(frame.Receipt); ok {
				_ = w.Write(frame.New(frame.RECEIPT, frame.ReceiptId, id))
			}
			return
		}
	}
}

func (b *tcpBroker) close() {
	_ = b.ln.Close()
	b.mu.Lock()
	for _, nc := range b.conns {
		_ = nc.Close()
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// dialTCPBroker returns a conn over real go-stomp. It builds the go-stomp
// connection itself rather than through Dialer only to shorten go-stomp's 30 s
// receipt timeouts; Subscribe, the bridge and Disconnect are the production code.
func dialTCPBroker(t *testing.T, b *tcpBroker, unsubTimeout time.Duration) *conn {
	t.Helper()
	nc, err := net.Dial("tcp", b.ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	gc, err := gostomp.Connect(nc,
		gostomp.ConnOpt.HeartBeat(0, 0),
		gostomp.ConnOpt.UnsubscribeReceiptTimeout(unsubTimeout),
		gostomp.ConnOpt.DisconnectReceiptTimeout(2*time.Second),
	)
	if err != nil {
		_ = nc.Close()
		t.Fatalf("stomp connect: %v", err)
	}
	return &conn{c: gc}
}

// waitGoroutines waits for the goroutine count to fall back to baseline, which
// shows no bridge or go-stomp goroutine outlived the connection.
func waitGoroutines(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines: %d, baseline %d\n%s", runtime.NumGoroutine(), baseline, buf[:n])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitTime(t *testing.T, ch <-chan time.Time, what string) time.Time {
	t.Helper()
	select {
	case at := <-ch:
		return at
	case <-time.After(3 * time.Second):
		t.Fatalf("broker did not receive %s", what)
		return time.Time{}
	}
}

// TestSubscribe_CtxUnsubscribeWithUnreadMessages covers the stalled path of
// #23: a consumer that stops reading, then cancels the Subscribe context while
// go-stomp's own channel is full. The unsubscribe must still finish, and
// nothing may outlive Disconnect.
func TestSubscribe_CtxUnsubscribeWithUnreadMessages(t *testing.T) {
	baseline := runtime.NumGoroutine()
	// Enough frames to fill the bridge output buffer and go-stomp's channel,
	// with more behind them, so go-stomp blocks unless someone drains it.
	b := startTCPBroker(t, 3*subChanBuf)
	c := dialTCPBroker(t, b, 200*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	s, err := c.Subscribe(ctx, "/queue/unread")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	waitTime(t, b.unsubAt, "UNSUBSCRIBE")

	if err := s.Unsubscribe(); !errors.Is(err, &gostomp.ErrUnsubscribeReceiptTimeout) {
		t.Errorf("Unsubscribe: got %v, want the unsubscribe receipt timeout", err)
	}
	if err := c.Disconnect(); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
	b.close()
	waitGoroutines(t, baseline)
}

// TestDisconnect_WaitsForPendingUnsubscribe pins the ordering that keeps
// go-stomp from panicking: the connection is not closed while an UNSUBSCRIBE is
// waiting for its receipt (#23).
func TestDisconnect_WaitsForPendingUnsubscribe(t *testing.T) {
	baseline := runtime.NumGoroutine()
	const unsubTimeout = 300 * time.Millisecond
	b := startTCPBroker(t, 0)
	c := dialTCPBroker(t, b, unsubTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	s, err := c.Subscribe(ctx, "/queue/pending")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	unsubAt := waitTime(t, b.unsubAt, "UNSUBSCRIBE")

	if err := c.Disconnect(); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
	discAt := waitTime(t, b.discAt, "DISCONNECT")
	if gap := discAt.Sub(unsubAt); gap < unsubTimeout/2 {
		t.Errorf("DISCONNECT arrived %v after UNSUBSCRIBE; want it held until the unsubscribe ended (%v)", gap, unsubTimeout)
	}
	if err := s.Unsubscribe(); !errors.Is(err, &gostomp.ErrUnsubscribeReceiptTimeout) {
		t.Errorf("Unsubscribe: got %v, want the unsubscribe receipt timeout", err)
	}
	for range s.C() {
	}
	b.close()
	waitGoroutines(t, baseline)
}

// panickingStompSub fails the way go-stomp v3.1.2's Unsubscribe does when its
// connection closes under it: a send on a closed channel.
type panickingStompSub struct{ fakeStompSub }

func (p *panickingStompSub) Unsubscribe(...func(*frame.Frame) error) error {
	ch := make(chan struct{})
	close(ch)
	ch <- struct{}{}
	return nil
}

func newTestSub(ctx context.Context, gs goStompSub, src chan *gostomp.Message) *sub {
	s := &sub{gossub: gs, src: src, ch: make(chan transport.Msg, subChanBuf)}
	go s.bridge(ctx)
	return s
}

// TestUnsubscribe_RecoversAndReportsPanic proves the go-stomp panic is
// contained to Unsubscribe and returned to the caller, not discarded.
func TestUnsubscribe_RecoversAndReportsPanic(t *testing.T) {
	t.Parallel()
	src := make(chan *gostomp.Message)
	defer close(src)
	s := newTestSub(context.Background(), &panickingStompSub{}, src)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped Unsubscribe: %v", r)
			}
		}()
		err = s.Unsubscribe()
	}()
	var re runtime.Error
	if !errors.As(err, &re) {
		t.Fatalf("Unsubscribe: got %v, want the recovered runtime error", err)
	}
}
