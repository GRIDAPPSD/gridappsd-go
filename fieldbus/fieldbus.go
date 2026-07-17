// Package fieldbus defines the public messaging boundary for the GridAPPS-D Go client.
//
// MessageBus is the Go equivalent of the Python FieldMessageBus abstract from
// gridappsd-field-bus-lib/interfaces.py:120-192. GridAPPSDMessageBus is the
// concrete GOSS/STOMP implementation built on the phase-1 transport.Conn,
// the internal/router callback router, and internal/reqresp correlated request/reply.
//
// Deliberate divergences from the Python surface:
//   - GetResponse accepts a ctx deadline rather than a timeout int. Go idiom is
//     ctx-bounded waits (go.md context discipline); callers who want the Python
//     default wrap the call with context.WithTimeout(ctx, 5*time.Second).
//   - Handler is func(headers, body), not a listener object with an on_message
//     method. The object-with-on_message affordance (goss.py:281-298) has no
//     idiomatic Go analog and is dropped.
//   - Body is []byte, not auto-JSON-decoded. The Python router best-effort
//     json.loads every message (goss.py:405-409); that couples transport to
//     serialization. The bus stays byte-clean; JSON decode is the caller's concern.
//   - Subscribe returns an opaque Token; Unsubscribe takes (dest, token) to
//     remove one specific handler. This replaces the Python all-handlers-at-once
//     unsubscribe shape with per-handler removal, and eliminates the unsound
//     %p func-pointer duplicate guard from the initial implementation.
//   - Subscribe/Unsubscribe operate over one transport.Subscription per destination
//     (one reader goroutine per destination) rather than a single global listener,
//     because the phase-1 transport.Msg carries no destination header. The
//     behavioral result is the same: per-destination callback dispatch.
package fieldbus

import (
	"context"
	"fmt"
	"sync"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
	"github.com/GRIDAPPSD/gridappsd-go/internal/reqresp"
	"github.com/GRIDAPPSD/gridappsd-go/internal/router"
	"github.com/GRIDAPPSD/gridappsd-go/message"
	"github.com/GRIDAPPSD/gridappsd-go/topics"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// Handler is the canonical callback type for message delivery.
// It is invoked for each message delivered to a subscribed destination.
// headers carries the STOMP frame headers as received; body is the raw frame body.
// A Handler must not block the dispatch goroutine for long: offload slow work to
// its own goroutine. A Handler is safe to call after its destination's Subscribe returns.
//
// router.Handler is defined identically; fieldbus.Handler is the canonical declaration
// and router.Handler is a type alias pointing here.
type Handler = router.Handler

// Token is an opaque handle returned by Subscribe. Pass it to Unsubscribe to
// remove exactly that registered handler, leaving other handlers on the same
// destination unaffected.
type Token = router.Token

// MessageBus is the Go equivalent of the Python FieldMessageBus abstract.
// It is the public boundary the go-2664-gridappsd gateway codes against.
type MessageBus interface {
	// Connect establishes the authenticated session (two-step GOSS token auth,
	// TLS both legs) and starts the dispatch machinery. Idempotent: a second
	// Connect on an already-connected bus is a no-op that returns nil.
	Connect(ctx context.Context) error

	// Disconnect stops dispatch, unsubscribes every registered destination, and
	// closes the underlying transport.Conn. After Disconnect the bus may be
	// reconnected with Connect. Idempotent.
	Disconnect() error

	// IsConnected reports whether the bus currently holds a live session.
	IsConnected() bool

	// Subscribe registers h to receive every message delivered to destination.
	// The destination is normalized by the queue-prepend rule before use
	// (see topics.NormalizeDestination). Multiple Subscribe calls on the same
	// destination register additional handlers; each is invoked per message.
	// Returns an opaque Token the caller passes to Unsubscribe to remove this
	// specific handler. Each Subscribe call returns a distinct Token.
	Subscribe(ctx context.Context, destination string, h Handler) (Token, error)

	// Unsubscribe removes the single handler identified by tok on destination.
	// When tok is the last handler for destination, the underlying transport
	// subscription is cancelled. Unsubscribing an unknown token is a no-op
	// that returns nil.
	Unsubscribe(ctx context.Context, destination string, tok Token) error

	// Send publishes body to destination as a fire-and-forget SEND. contentType
	// sets the STOMP content-type header. The GOSS auth-subject headers are
	// stamped by the concrete impl.
	Send(ctx context.Context, destination, contentType string, body []byte) error

	// GetResponse performs a correlated request/reply: it sends body to
	// destination with a reply-to temp destination, waits for the first reply
	// on that destination, and returns the reply body. The wait is bounded by
	// ctx (the caller sets the deadline; there is no separate timeout arg,
	// unlike the Python timeout=5). destination is normalized by the
	// queue-prepend rule before send.
	GetResponse(ctx context.Context, destination, contentType string, body []byte) ([]byte, error)
}

// Compile-time assertion: GridAPPSDMessageBus must satisfy MessageBus.
var _ MessageBus = (*GridAPPSDMessageBus)(nil)

// GridAPPSDMessageBus is the GOSS/STOMP concrete MessageBus. Construct with New;
// it is not connected until Connect is called.
type GridAPPSDMessageBus struct {
	cfg gridappsd.Config

	mu        sync.Mutex
	conn      transport.Conn // nil until Connect; nilled on Disconnect
	rtr       *router.Router // nil until Connect; stopped on Disconnect
	connected bool
	// subject carries the username (Config.User) stamped into GOSS_SUBJECT on
	// every outbound SEND. It is NOT a secret: it identifies the authenticated
	// session subject for the GOSS broker's ACL check. The password is never
	// stored here; it is consumed by gridappsd.Connect and discarded.
	subject string
}

// New returns an unconnected GridAPPSDMessageBus for the given config.
func New(cfg gridappsd.Config) *GridAPPSDMessageBus {
	return &GridAPPSDMessageBus{cfg: cfg}
}

// Connect establishes the authenticated session and starts the dispatch machinery.
// Idempotent: a second Connect on an already-connected bus returns nil immediately.
func (b *GridAPPSDMessageBus) Connect(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.connected {
		return nil
	}
	conn, err := gridappsd.Connect(ctx, b.cfg)
	if err != nil {
		return fmt.Errorf("fieldbus connect: %w", err)
	}
	b.conn = conn
	b.rtr = router.New(conn)
	// subject carries the username for GOSS_SUBJECT. The GOSS application layer
	// requires the subject headers for ACL checks even on token-authenticated STOMP
	// sessions (goss.py:174-176, 230-234). We stamp Config.User because that is the
	// identity the broker's ACL checks on a token-authed session: the token IS the
	// user identity. If a later integration test shows otherwise, this is the
	// narrowly-scoped change site. See GAG-004 open-item resolution in the phase-2 report.
	b.subject = b.cfg.User
	b.connected = true
	return nil
}

// Disconnect stops dispatch, unsubscribes all destinations, and closes the transport.
// Idempotent.
func (b *GridAPPSDMessageBus) Disconnect() error {
	b.mu.Lock()
	rtr := b.rtr
	conn := b.conn
	b.rtr = nil
	b.conn = nil
	b.connected = false
	b.mu.Unlock()

	if rtr != nil {
		rtr.Close()
	}
	if conn != nil {
		if err := conn.Disconnect(); err != nil {
			return fmt.Errorf("fieldbus disconnect: %w", err)
		}
	}
	return nil
}

// IsConnected reports whether the bus currently holds a live session.
func (b *GridAPPSDMessageBus) IsConnected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.connected
}

// Subscribe registers h to receive messages on destination (queue-normalized).
// Returns an opaque Token identifying this registration.
func (b *GridAPPSDMessageBus) Subscribe(ctx context.Context, destination string, h Handler) (Token, error) {
	b.mu.Lock()
	rtr := b.rtr
	b.mu.Unlock()
	if rtr == nil {
		return 0, fmt.Errorf("fieldbus subscribe: not connected")
	}
	dest := topics.NormalizeDestination(destination)
	return rtr.Subscribe(ctx, dest, h)
}

// Unsubscribe removes the handler identified by tok on destination.
func (b *GridAPPSDMessageBus) Unsubscribe(ctx context.Context, destination string, tok Token) error {
	b.mu.Lock()
	rtr := b.rtr
	b.mu.Unlock()
	if rtr == nil {
		return nil
	}
	dest := topics.NormalizeDestination(destination)
	return rtr.Unsubscribe(ctx, dest, tok)
}

// Send publishes body to destination as fire-and-forget, stamping GOSS auth-subject headers.
//
// GOSS_SUBJECT open-item resolution (GAG-004): goss.py stamps both
// GOSS_HAS_SUBJECT=true and GOSS_SUBJECT=<token> on every send, even when using
// a token-authenticated STOMP connection (see goss.py:174-176, 230-234). The
// token IS the STOMP login on the durable leg, but the GOSS application layer
// still requires the subject headers for its own ACL checks independent of the
// STOMP-level authentication. Phase-2 mirrors that behavior: we stamp the user
// identity (b.subject) in both headers on every outbound Send.
func (b *GridAPPSDMessageBus) Send(ctx context.Context, destination, contentType string, body []byte) error {
	b.mu.Lock()
	conn := b.conn
	subject := b.subject
	b.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("fieldbus send: not connected")
	}
	dest := topics.NormalizeDestination(destination)
	headers := map[string]string{
		message.HeaderGossHasSubject: "true",
		message.HeaderGossSubject:    subject,
	}
	return conn.Send(ctx, dest, contentType, body, headers)
}

// GetResponse performs a correlated request/reply bounded by ctx.
func (b *GridAPPSDMessageBus) GetResponse(ctx context.Context, destination, contentType string, body []byte) ([]byte, error) {
	b.mu.Lock()
	conn := b.conn
	subject := b.subject
	b.mu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("fieldbus get_response: not connected")
	}
	baseHeaders := map[string]string{
		message.HeaderGossHasSubject: "true",
		message.HeaderGossSubject:    subject,
	}
	return reqresp.GetResponse(ctx, conn, destination, contentType, body, baseHeaders)
}
