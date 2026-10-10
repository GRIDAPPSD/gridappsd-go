// Package stomp provides a go-stomp-backed implementation of transport.Dialer and transport.Conn.
//
// TLS is the caller's responsibility. Pass a *tls.Conn to Dial; the package
// does not fall back to plain TCP.
package stomp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// DefaultHeartBeat matches GRIDAPPSD_HEARTBEAT (10 s) used by the GridAPPS-D broker.
const DefaultHeartBeat = 10 * time.Second

// subChanBuf is the buffer depth for the bridge output channel.
// A buffer of this size absorbs bursts of consecutive broker frames without
// stalling the go-stomp read loop. Must be non-zero to decouple the bridge
// pace from the consumer; the bridge's nested-select exit prevents the goroutine
// from leaking when the consumer is slow and the buffer is full.
const subChanBuf = 16

// Dialer is the go-stomp-backed transport.Dialer.
// Wrap an already-dialed TLS net.Conn and this type handles the STOMP handshake.
type Dialer struct {
	disconnectBound time.Duration
}

// WithDisconnectBound returns a copy of d whose connections give up waiting
// for the DISCONNECT receipt after bound, instead of go-stomp's 30 s default.
// Halfway through the wait the transport is closed, which ends the wait at
// once while go-stomp's I/O loop still runs, so a live broker has bound/2 to
// answer; the full bound only ends a wait whose loop has already exited (#24).
func (d *Dialer) WithDisconnectBound(bound time.Duration) transport.Dialer {
	nd := *d
	nd.disconnectBound = bound
	return &nd
}

// resolveHeartBeat returns the outgoing and incoming heart-beat intervals to
// offer in the CONNECT frame.
//
// cfg.HeartBeats, when non-nil, is authoritative and is passed through
// unchanged, zeros included: STOMP 1.2 defines a zero in a direction as
// "disabled", so a caller who asks for no incoming heart-beat must not have a
// default silently substituted for it.
//
// When cfg.HeartBeats is nil the v0.1.0 symmetric path applies exactly:
// cfg.HeartBeat drives both directions, and a zero cfg.HeartBeat falls back to
// DefaultHeartBeat.
func resolveHeartBeat(cfg transport.ConnConfig) (send, recv time.Duration) {
	if cfg.HeartBeats != nil {
		return cfg.HeartBeats.Send, cfg.HeartBeats.Recv
	}
	hb := cfg.HeartBeat
	if hb == 0 {
		hb = DefaultHeartBeat
	}
	return hb, hb
}

// Dial performs the STOMP CONNECT handshake over rwc.
// Heart-beat intervals come from cfg per resolveHeartBeat: symmetric from
// cfg.HeartBeat by default, or independent per direction when cfg.HeartBeats
// is set.
// ctx deadline, if set, is applied to rwc before the handshake and cleared
// after; go-stomp's Connect has no built-in context support, so a cancelled ctx
// without a deadline does not interrupt the handshake.
func (d *Dialer) Dial(ctx context.Context, rwc io.ReadWriteCloser, cfg transport.ConnConfig) (transport.Conn, error) {
	sendHB, recvHB := resolveHeartBeat(cfg)
	// Bound the synchronous STOMP CONNECT handshake using any deadline in ctx.
	// go-stomp's Connect is not context-aware; applying a deadline on the
	// underlying connection is the only mechanism to unblock a hung handshake.
	// The deadline is cleared after Connect so subsequent I/O is not bounded.
	if dl, ok := ctx.Deadline(); ok {
		type deadliner interface {
			SetDeadline(time.Time) error
		}
		if dc, ok2 := rwc.(deadliner); ok2 {
			// Error discarded: SetDeadline failure here is non-actionable; if rwc
			// cannot accept a deadline the handshake may still succeed or will fail
			// on its own, and either outcome is handled by the gostomp.Connect error
			// path below.
			_ = dc.SetDeadline(dl)
			defer func() {
				// Clear the deadline so post-handshake I/O is not bounded by the
				// ctx deadline. Error discarded: on the failure path (Connect returned
				// an error) the caller owns rwc and will close it; clearing the
				// deadline on a failed or closed connection is a no-op with no
				// recovery path.
				_ = dc.SetDeadline(time.Time{})
			}()
		}
	}
	opts := []func(*gostomp.Conn) error{
		gostomp.ConnOpt.Login(cfg.Login, cfg.Passcode),
		gostomp.ConnOpt.HeartBeat(sendHB, recvHB),
	}
	if d.disconnectBound > 0 {
		opts = append(opts, gostomp.ConnOpt.DisconnectReceiptTimeout(d.disconnectBound))
	}
	c, err := gostomp.Connect(rwc, opts...)
	if err != nil {
		return nil, fmt.Errorf("stomp connect: %w", err)
	}
	return &conn{c: c, rwc: rwc, bound: d.disconnectBound, closeAfter: d.disconnectBound / 2}, nil
}

// errDisconnected reports an Unsubscribe that began after Disconnect. The
// connection teardown ends the subscription instead.
var errDisconnected = errors.New("stomp unsubscribe: connection disconnected")

// conn wraps a *gostomp.Conn to satisfy transport.Conn.
//
// go-stomp v3.1.2 can panic the process when its connection closes under an
// Unsubscribe (#23), so Disconnect refuses new unsubscribes and waits for
// pending ones before it closes anything.
//
// ended is set once a subscription has received go-stomp's ERROR frame, which
// go-stomp sends as its I/O loop ends. go-stomp's graceful Disconnect cannot
// get a receipt after that (#24).
//
// closeAfter, when non-zero, is how long Disconnect waits for its receipt
// before closing rwc, half of bound; see Dialer.WithDisconnectBound.
type conn struct {
	c          *gostomp.Conn
	rwc        io.Closer
	bound      time.Duration
	closeAfter time.Duration

	mu      sync.Mutex
	closing bool
	unsubs  sync.WaitGroup

	ended atomic.Bool
}

// The "reply-to" key in headers sets the reply-to header on the SEND frame;
// it is NOT passed to Subscribe and does NOT trigger go-stomp's RabbitMQ-style
// temp-queue path (which intercepts reply-to on SUBSCRIBE, not SEND).
func (c *conn) Send(_ context.Context, destination, contentType string, body []byte, headers map[string]string) error {
	// ctx unused: go-stomp's Conn.Send is synchronous with no context support.
	opts := make([]func(*frame.Frame) error, 0, len(headers))
	for k, v := range headers {
		opts = append(opts, gostomp.SendOpt.Header(k, v))
	}
	if err := c.c.Send(destination, contentType, body, opts...); err != nil {
		return fmt.Errorf("stomp send %q: %w", destination, err)
	}
	return nil
}

// Cancelling ctx triggers an Unsubscribe so the broker stops delivering messages.
// Its result is what a later Unsubscribe call returns.
func (c *conn) Subscribe(ctx context.Context, destination string) (transport.Subscription, error) {
	// AckAuto: GOSS does not require explicit message acknowledgement.
	// reply-to is deliberately NOT set on the SUBSCRIBE frame; go-stomp at
	// conn.go:407 suppresses SUBSCRIBE frames that carry reply-to and routes
	// them via the RabbitMQ temp-queue path, which is incompatible with GOSS.
	gossub, err := c.c.Subscribe(destination, gostomp.AckAuto)
	if err != nil {
		return nil, fmt.Errorf("stomp subscribe %q: %w", destination, err)
	}
	return newSub(ctx, c, gossub), nil
}

// Disconnect sends a STOMP DISCONNECT frame and closes the underlying connection.
// Pending unsubscribes finish first; each is bounded by go-stomp's
// unsubscribe receipt timeout. A connection whose end a subscription has
// already received is closed without a DISCONNECT frame; one that ends without
// a subscription seeing it waits for the receipt up to the Dialer's
// disconnect bound, or go-stomp's 30 s default.
func (c *conn) Disconnect() error {
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	c.unsubs.Wait()
	if c.ended.Load() {
		// go-stomp v3.1.2's Disconnect would hold the close lock its ended I/O
		// loop needs while it waits for the receipt that loop would have
		// delivered, until its 30 s receipt timeout (#24).
		if err := c.c.MustDisconnect(); err != nil {
			return fmt.Errorf("stomp disconnect: %w", err)
		}
		return nil
	}
	var closed atomic.Bool
	if c.closeAfter > 0 {
		// Closing the transport while the receipt wait is still live lets
		// go-stomp's I/O loop deliver its close error to that wait. Once
		// go-stomp's own timeout gives up, the loop would block forever
		// sending to the abandoned receipt channel (#24).
		//
		// INFERRED (#24): on a *tls.Conn, Close first sends close_notify
		// under a 5 s write deadline unless a write is in flight, so a broker
		// that stopped reading could delay the real close past the bound.
		t := time.AfterFunc(c.closeAfter, func() {
			closed.Store(true)
			_ = c.rwc.Close()
		})
		defer t.Stop()
	}
	err := c.c.Disconnect()
	switch {
	case err == nil:
		return nil
	case c.bound > 0 && errors.Is(err, gostomp.ErrDisconnectReceiptTimeout):
		return fmt.Errorf("stomp disconnect: no receipt within the %v bound: %w", c.bound, err)
	case closed.Load():
		return fmt.Errorf("stomp disconnect: no receipt within %v, half the %v bound, so the transport was closed: %w",
			c.closeAfter, c.bound, err)
	}
	return fmt.Errorf("stomp disconnect: %w", err)
}

func (c *conn) markEnded() { c.ended.Store(true) }

// beginUnsubscribe registers a pending unsubscribe, or reports false once
// Disconnect has begun.
func (c *conn) beginUnsubscribe() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return false
	}
	c.unsubs.Add(1)
	return true
}

func (c *conn) endUnsubscribe() { c.unsubs.Done() }

// unsubGate orders unsubscribes against the connection's Disconnect, and
// takes the subscription's report that the connection has ended.
type unsubGate interface {
	beginUnsubscribe() bool
	endUnsubscribe()
	markEnded()
}

// goStompSub is the subset of *gostomp.Subscription used by the bridge.
// Defined as an interface to allow whitebox testing without a live broker.
type goStompSub interface {
	Unsubscribe(opts ...func(*frame.Frame) error) error
}

// sub bridges a *gostomp.Subscription into transport.Subscription.
// A goroutine forwards messages from the gostomp channel to s.ch, and keeps
// draining the gostomp channel until go-stomp closes it: go-stomp's read loop,
// and with it every receipt on the connection, blocks while that channel is
// full.
//
// readyForSend is a test-only hook: when non-nil, the bridge sends on it
// immediately before attempting the outbound send to s.ch, giving the test
// a deterministic signal that the goroutine has reached the blocking point.
// It is nil in all production code paths.
type sub struct {
	gate         unsubGate
	gossub       goStompSub
	src          <-chan *gostomp.Message
	ch           chan transport.Msg
	stop         chan struct{} // closed when Unsubscribe begins
	readyForSend chan struct{} // test hook; nil in production

	once sync.Once
	err  error
}

func newSub(ctx context.Context, gate unsubGate, gossub *gostomp.Subscription) *sub {
	s := &sub{
		gate:   gate,
		gossub: gossub,
		src:    gossub.C,
		ch:     make(chan transport.Msg, subChanBuf),
		stop:   make(chan struct{}),
	}
	go s.bridge(ctx)
	return s
}

func (s *sub) C() <-chan transport.Msg { return s.ch }

// Unsubscribe unsubscribes once; every call returns that attempt's result.
func (s *sub) Unsubscribe() error {
	s.once.Do(func() {
		close(s.stop)
		s.err = s.unsubscribe()
	})
	return s.err
}

func (s *sub) unsubscribe() (err error) {
	if !s.gate.beginUnsubscribe() {
		return errDisconnected
	}
	defer s.gate.endUnsubscribe()
	// go-stomp's receipt-timeout path sends on a channel a concurrent close may
	// have closed (#23). It runs on this goroutine, so it is recoverable here.
	defer func() {
		if r := recover(); r != nil {
			re, ok := r.(runtime.Error)
			if !ok {
				panic(r)
			}
			err = fmt.Errorf("stomp unsubscribe: %w", re)
		}
	}()
	if err := s.gossub.Unsubscribe(); err != nil {
		return fmt.Errorf("stomp unsubscribe: %w", err)
	}
	return nil
}

// bridge forwards from gostomp.Subscription.C to s.ch until the subscription
// ends, is unsubscribed, or ctx is done; it then closes s.ch and discards
// whatever go-stomp still delivers until go-stomp closes its channel.
func (s *sub) bridge(ctx context.Context) {
	out, done, stop := s.ch, ctx.Done(), s.stop
	endOut := func() {
		if out != nil {
			close(out)
			out, done, stop = nil, nil, nil
		}
	}
	defer endOut()
	unsubscribeOnCtx := func() {
		// The result is kept for later Unsubscribe calls; go-stomp's channel
		// must be drained meanwhile, so it cannot run on this goroutine.
		go func() { _ = s.Unsubscribe() }()
		endOut()
	}
	for {
		select {
		case msg, ok := <-s.src:
			if !ok {
				return
			}
			if endsConn(msg.Err) {
				s.gate.markEnded()
			}
			if out == nil {
				continue
			}
			m := transport.Msg{Body: msg.Body}
			if msg.Err != nil {
				m = transport.Msg{Err: msg.Err}
			}
			// Signal the test hook (nil in production) that we are about to
			// attempt the outbound send; this gives tests a deterministic
			// synchronization point before they cancel ctx.
			if s.readyForSend != nil {
				s.readyForSend <- struct{}{}
			}
			select {
			case out <- m:
				if msg.Err != nil {
					// Error messages are terminal.
					endOut()
				}
			case <-done:
				unsubscribeOnCtx()
			case <-stop:
				endOut()
			}
		case <-done:
			unsubscribeOnCtx()
		case <-stop:
			endOut()
		}
	}
}

// endsConn reports whether err is an ERROR frame go-stomp delivered to the
// subscription. go-stomp sends one when its I/O loop ends, and on a broker
// ERROR, after which it closes the connection. Errors raised by Unsubscribe
// itself carry no frame.
func endsConn(err error) bool {
	var e *gostomp.Error
	return errors.As(err, &e) && e.Frame != nil
}
