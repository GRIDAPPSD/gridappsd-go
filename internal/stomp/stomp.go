// Package stomp provides a go-stomp-backed implementation of transport.Dialer and transport.Conn.
//
// TLS is the caller's responsibility. Pass a *tls.Conn to Dial; the package
// does not fall back to plain TCP.
package stomp

import (
	"context"
	"fmt"
	"io"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
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
type Dialer struct{}

// Dial performs the STOMP CONNECT handshake over rwc.
// cfg.HeartBeat defaults to DefaultHeartBeat when zero.
// ctx is stored but go-stomp's synchronous handshake is not context-aware;
// network-level cancellation must be handled by the caller before invoking Dial.
func (d *Dialer) Dial(_ context.Context, rwc io.ReadWriteCloser, cfg transport.ConnConfig) (transport.Conn, error) {
	hb := cfg.HeartBeat
	if hb == 0 {
		hb = DefaultHeartBeat
	}
	c, err := gostomp.Connect(rwc,
		gostomp.ConnOpt.Login(cfg.Login, cfg.Passcode),
		gostomp.ConnOpt.HeartBeat(hb, hb),
	)
	if err != nil {
		return nil, fmt.Errorf("stomp connect (login=%q): %w", cfg.Login, err)
	}
	return &conn{c: c}, nil
}

// conn wraps a *gostomp.Conn to satisfy transport.Conn.
type conn struct {
	c *gostomp.Conn
}

// Send transmits a STOMP SEND frame to destination.
// Each entry in headers becomes an additional STOMP frame header.
// The "reply-to" key in headers sets the reply-to header on the SEND frame;
// it is NOT passed to Subscribe and does NOT trigger go-stomp's RabbitMQ-style
// temp-queue path (which intercepts reply-to on SUBSCRIBE, not SEND).
func (c *conn) Send(_ context.Context, destination, contentType string, body []byte, headers map[string]string) error {
	opts := make([]func(*frame.Frame) error, 0, len(headers))
	for k, v := range headers {
		opts = append(opts, gostomp.SendOpt.Header(k, v))
	}
	if err := c.c.Send(destination, contentType, body, opts...); err != nil {
		return fmt.Errorf("stomp send %q: %w", destination, err)
	}
	return nil
}

// Subscribe creates a subscription on destination with auto-ack.
// Cancelling ctx triggers an Unsubscribe so the broker stops delivering messages.
func (c *conn) Subscribe(ctx context.Context, destination string) (transport.Subscription, error) {
	// AckAuto: GOSS does not require explicit message acknowledgement.
	// reply-to is deliberately NOT set on the SUBSCRIBE frame; go-stomp at
	// conn.go:407 suppresses SUBSCRIBE frames that carry reply-to and routes
	// them via the RabbitMQ temp-queue path, which is incompatible with GOSS.
	gossub, err := c.c.Subscribe(destination, gostomp.AckAuto)
	if err != nil {
		return nil, fmt.Errorf("stomp subscribe %q: %w", destination, err)
	}
	return newSub(ctx, gossub), nil
}

// Disconnect sends a STOMP DISCONNECT frame and closes the underlying connection.
func (c *conn) Disconnect() error {
	if err := c.c.Disconnect(); err != nil {
		return fmt.Errorf("stomp disconnect: %w", err)
	}
	return nil
}

// goStompSub is the subset of *gostomp.Subscription used by the bridge.
// Defined as an interface to allow whitebox testing without a live broker.
type goStompSub interface {
	Unsubscribe(opts ...func(*frame.Frame) error) error
}

// sub bridges a *gostomp.Subscription into transport.Subscription.
// A goroutine forwards messages from the gostomp channel to s.ch.
type sub struct {
	gossub goStompSub
	src    <-chan *gostomp.Message
	ch     chan transport.Msg
}

func newSub(ctx context.Context, gossub *gostomp.Subscription) *sub {
	s := &sub{
		gossub: gossub,
		src:    gossub.C,
		ch:     make(chan transport.Msg, subChanBuf),
	}
	go s.bridge(ctx)
	return s
}

func (s *sub) C() <-chan transport.Msg { return s.ch }

func (s *sub) Unsubscribe() error {
	return s.gossub.Unsubscribe()
}

// bridge runs in a goroutine, forwarding from gostomp.Subscription.C to s.ch.
// Each outbound send is wrapped in a nested select so a full s.ch cannot trap
// the goroutine when ctx is cancelled; the goroutine always has an exit path.
func (s *sub) bridge(ctx context.Context) {
	defer close(s.ch)
	for {
		select {
		case msg, ok := <-s.src:
			if !ok {
				// go-stomp closed its channel: subscription ended.
				return
			}
			m := transport.Msg{Body: msg.Body}
			if msg.Err != nil {
				m = transport.Msg{Err: msg.Err}
			}
			select {
			case s.ch <- m:
			case <-ctx.Done():
				// Best-effort unsubscribe; ignore error since context is already done.
				_ = s.gossub.Unsubscribe()
				return
			}
			if msg.Err != nil {
				// Error messages are terminal: exit after forwarding.
				return
			}
		case <-ctx.Done():
			// Best-effort unsubscribe; ignore error since context is already done.
			_ = s.gossub.Unsubscribe()
			return
		}
	}
}
