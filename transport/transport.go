// Package transport defines the consumer-side interfaces for a STOMP connection.
// The real implementation lives in internal/stomp; tests use in-process fakes.
//
// Design note: the Dialer accepts an io.ReadWriteCloser rather than a network
// address so that TLS is the caller's responsibility. Callers must pass a
// *tls.Conn; plain TCP is not the default or easy path. This keeps TLS out of
// the STOMP layer and makes the interface testable without a real broker.
package transport

import (
	"context"
	"io"
	"time"
)

// HeartBeatIntervals configures the two STOMP heart-beat directions
// independently.
//
// STOMP 1.2 section 3.3.1 gives each direction its own value and defines zero
// in a direction as "heart-beating disabled in that direction". A caller that
// wants to emit heart-beats without requiring the broker to send any, so that
// no read deadline is derived and a quiet broker cannot tear down the whole
// connection, sets Send to the interval and leaves Recv zero.
type HeartBeatIntervals struct {
	// Send is the interval at which this client guarantees to emit heart-beats.
	// Zero disables outgoing heart-beats.
	Send time.Duration
	// Recv is the interval at which this client requires heart-beats from the
	// broker. Zero disables the incoming requirement, so no read deadline is
	// derived from it.
	Recv time.Duration
}

// ConnConfig holds the credentials and heartbeat settings for a STOMP CONNECT handshake.
type ConnConfig struct {
	// Login is the STOMP login header value.
	Login string
	// Passcode is the STOMP passcode header value. Empty on the token-authenticated leg.
	Passcode string

	// HeartBeat is the symmetric heartbeat interval offered to the broker.
	// The same value is used for both send and receive, and zero selects the
	// transport implementation's default.
	//
	// It is superseded by HeartBeats when that field is non-nil. It is retained
	// unchanged for callers written against v0.1.0, which had no other option.
	HeartBeat time.Duration

	// HeartBeats, when non-nil, supersedes HeartBeat and sets the outgoing and
	// incoming intervals independently with STOMP 1.2 semantics: the values are
	// offered exactly as given, and a zero in a direction disables that
	// direction rather than selecting a default. A nil HeartBeats keeps the
	// symmetric HeartBeat behavior, so a caller that does not opt in is
	// unaffected.
	HeartBeats *HeartBeatIntervals
}

// Msg is a single message received on a Subscription.
type Msg struct {
	Body []byte
	// Err is non-nil when the broker reported an error or the subscription
	// was closed unexpectedly before a message was delivered.
	Err error
}

// Subscription delivers messages received from a STOMP destination.
type Subscription interface {
	// C returns the channel on which incoming Msg values arrive.
	// The channel is closed when the subscription ends.
	C() <-chan Msg
	// Unsubscribe cancels the subscription and closes C.
	Unsubscribe() error
}

// Conn is an active STOMP session. Obtained from Dialer.Dial.
type Conn interface {
	// Send transmits a STOMP SEND frame to destination. headers carries
	// extra STOMP frame headers (e.g. "reply-to"). contentType is set on
	// the STOMP content-type header.
	Send(ctx context.Context, destination, contentType string, body []byte, headers map[string]string) error
	// Subscribe creates a subscription on destination. The returned
	// Subscription's C() channel delivers incoming messages until the
	// subscription is cancelled or the connection is closed.
	Subscribe(ctx context.Context, destination string) (Subscription, error)
	// Disconnect sends a STOMP DISCONNECT frame and closes the connection.
	Disconnect() error
}

// Dialer creates a STOMP Conn over an already-established io.ReadWriteCloser.
//
// The caller is responsible for TLS: pass a *tls.Conn (or any TLS-wrapped
// io.ReadWriteCloser). Accepting io.ReadWriteCloser rather than net.Conn
// makes the Dialer testable with in-process pipes and fakes.
type Dialer interface {
	Dial(ctx context.Context, rwc io.ReadWriteCloser, cfg ConnConfig) (Conn, error)
}
