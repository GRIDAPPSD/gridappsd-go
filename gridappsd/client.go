// Package gridappsd provides the entry point for connecting to a GridAPPS-D / GOSS broker.
//
// Connect dials the broker (plain TCP or TLS, per Config.TLSConfig and
// Config.AllowPlaintext) and performs the two-step token authentication,
// then returns a durable transport.Conn ready for messaging (phases 2-4).
// The zero-value Config is fail-closed: it dials TLS.
//
// Re-auth: call Connect again with the same Config to refresh an expired session.
// The Config carries all required state; there is no mutable singleton.
package gridappsd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/auth"
	istormp "github.com/GRIDAPPSD/gridappsd-go/internal/stomp"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// DefaultAddress is the default GOSS STOMP endpoint. Whether the dial uses
// plain TCP or TLS is controlled by Config.TLSConfig and
// Config.AllowPlaintext, not by this address.
const DefaultAddress = "localhost:61613"

// DefaultHandshakeTimeout bounds a TLS handshake when the caller's context
// carries no deadline of its own. It applies only to the handshake step,
// after the TCP connection is already established: the TCP connect step is
// unaffected and keeps whatever bound the caller's context already gave it.
//
// 5 seconds is sized for a handshake specifically, not for a whole connect:
// by the time HandshakeContext runs, DNS resolution and routing are already
// done, and what remains is a small, fixed number of round trips over an
// already-open socket (ClientHello, ServerHello/Certificate, Finished). A
// value sized for a whole connect would let the exact hang this bounds run
// that much longer before failing legibly.
const DefaultHandshakeTimeout = 5 * time.Second

// handshakeTimeout is DefaultHandshakeTimeout, indirected through a
// variable so a test can shrink it rather than waiting out the real
// default.
var handshakeTimeout = DefaultHandshakeTimeout

// ErrHandshakeTimeout is returned, wrapped, when a TLS handshake does not
// complete within the caller's deadline or, absent one,
// DefaultHandshakeTimeout. Compare with errors.Is. The peer having accepted
// the TCP connection but never completing the handshake is the fingerprint
// of a plaintext broker on what the caller believes is a TLS port.
var ErrHandshakeTimeout = errors.New("tls handshake did not complete: the peer accepted the TCP connection but may not be speaking TLS")

// Config holds all parameters required to establish an authenticated GridAPPS-D connection.
type Config struct {
	// Address is the GOSS broker endpoint in host:port form.
	// Defaults to DefaultAddress when empty.
	Address string

	// TLSConfig controls whether the connection uses TLS and, when it does,
	// how.
	//
	// A non-nil TLSConfig always dials TLS using that config, for both the
	// credential and durable legs, regardless of AllowPlaintext: the caller
	// may supply client certificates (mTLS), a custom CA pool, or specific
	// cipher/curve constraints. InsecureSkipVerify is not set here: the
	// caller who wants that must set it on their own TLSConfig and accepts
	// the responsibility. A zero MinVersion on the supplied config is raised
	// to tls.VersionTLS12; an explicit MinVersion is never lowered.
	//
	// A nil TLSConfig (the zero value) defers to AllowPlaintext: see its
	// doc comment for the full transport-selection contract.
	TLSConfig *tls.Config

	// AllowPlaintext opts into a plain TCP dial when TLSConfig is nil. It
	// has no effect when TLSConfig is non-nil: TLSConfig always wins.
	//
	// The zero value (false) is fail-closed: a nil TLSConfig with
	// AllowPlaintext false dials TLS using the system trust store
	// (ServerName derived from Address, minimum TLS 1.2). Set
	// AllowPlaintext true to reach a plaintext broker, such as the dev
	// gridappsd-docker stack on 61613.
	//
	// Transport-selection matrix:
	//
	//	TLSConfig  AllowPlaintext  Result
	//	non-nil    (either)        TLS with TLSConfig
	//	nil        false (zero)    TLS with system trust store (fail-closed)
	//	nil        true            plain TCP
	AllowPlaintext bool

	// User is the GOSS username used on the credential leg.
	User string

	// Password is the GOSS password used on the credential leg.
	Password string

	// HeartBeat is the symmetric STOMP heartbeat interval offered on both
	// connection legs: the same value in both directions. Zero uses
	// internal/stomp.DefaultHeartBeat (10 s). Superseded by HeartBeats when
	// that field is non-nil.
	HeartBeat time.Duration

	// HeartBeats, when non-nil, supersedes HeartBeat and sets the outgoing and
	// incoming heartbeat intervals independently on both legs, with STOMP 1.2
	// semantics: a zero in a direction disables that direction rather than
	// selecting a default.
	//
	// The case this exists for is send-only heart-beating
	// (HeartBeatIntervals{Send: d}): the client keeps proving liveness to the
	// broker, but requires nothing back, so no read deadline is derived. That
	// matters because go-stomp treats an expired read deadline as fatal to the
	// whole connection, fanning an error to every subscription and
	// disconnecting, which takes the publish path down with the receive path.
	//
	// Correctness note: requesting zero incoming does not guarantee go-stomp
	// leaves its read timer disarmed. go-stomp raises the negotiated incoming
	// interval to whatever the broker advertises it can send, ignoring the
	// STOMP 1.2 rule that a zero from either party disables that direction. A
	// broker that answers with a non-zero send interval therefore still arms
	// the timer. See the characterization tests in internal/stomp.
	HeartBeats *transport.HeartBeatIntervals
}

// Connect dials the GridAPPS-D broker (plain TCP or TLS, per cfg.TLSConfig
// and cfg.AllowPlaintext: see Config.AllowPlaintext for the full
// transport-selection contract), performs the two-step GOSS token
// authentication, and returns a durable transport.Conn.
//
// Both the credential leg and the durable leg use the same transport (both
// plain, or both TLS): the credential leg carries base64-encoded
// credentials, so a caller connecting to a broker that requires
// confidentiality must not set AllowPlaintext (or must set TLSConfig).
//
// The returned transport.Conn is ready for messaging. Call Connect again to
// re-authenticate when the token expires.
func Connect(ctx context.Context, cfg Config) (transport.Conn, error) {
	if cfg.Address == "" {
		cfg.Address = DefaultAddress
	}

	netDial := func(ctx context.Context) (io.ReadWriteCloser, error) {
		return dial(ctx, cfg.Address, cfg.TLSConfig, cfg.AllowPlaintext)
	}

	conn, err := auth.Exchange(ctx, netDial, &istormp.Dialer{}, cfg.User, cfg.Password, cfg.HeartBeat, cfg.HeartBeats)
	if err != nil {
		return nil, fmt.Errorf("gridappsd connect to %s: %w", cfg.Address, err)
	}
	return conn, nil
}

// dial opens a connection to addr per the transport-selection contract
// documented on Config.AllowPlaintext:
//
//   - tlsCfg non-nil: TLS using that config (allowPlaintext is ignored).
//   - tlsCfg nil, allowPlaintext false (the zero value): TLS using the
//     system trust store. Fail-closed default.
//   - tlsCfg nil, allowPlaintext true: plain TCP.
//
// Returns an io.ReadWriteCloser so it satisfies auth.NetDialer without
// importing crypto/tls in the auth package.
func dial(ctx context.Context, addr string, tlsCfg *tls.Config, allowPlaintext bool) (io.ReadWriteCloser, error) {
	if tlsCfg == nil {
		if allowPlaintext {
			return dialPlain(ctx, addr)
		}
		tlsCfg = &tls.Config{}
	}
	return dialTLS(ctx, addr, tlsCfg)
}

// dialPlain opens a plain TCP connection to addr, honoring ctx cancellation
// and deadlines. This is the dev-broker path: no TLS terminator in front of
// the local gridappsd-docker STOMP port.
func dialPlain(ctx context.Context, addr string) (io.ReadWriteCloser, error) {
	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %q: %w", addr, err)
	}
	return netConn, nil
}

// dialTLS opens a TLS connection to addr. tlsCfg must be non-nil: this is
// a defensive check, not a restatement of the call-site contract, so a
// future caller that skips dial()'s nil handling gets a clear wrapped
// error instead of a nil-pointer panic on cfg.ServerName below.
func dialTLS(ctx context.Context, addr string, tlsCfg *tls.Config) (io.ReadWriteCloser, error) {
	if tlsCfg == nil {
		return nil, fmt.Errorf("tls dial %q: nil TLS config", addr)
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", addr, err)
	}

	cfg := tlsCfg
	if cfg.ServerName == "" || cfg.MinVersion == 0 {
		// Clone to avoid mutating the caller's config. An explicit
		// MinVersion is never lowered; a zero MinVersion is raised to
		// TLS 1.2, the workspace security floor.
		clone := cfg.Clone()
		if clone.ServerName == "" {
			clone.ServerName = host
		}
		if clone.MinVersion == 0 {
			clone.MinVersion = tls.VersionTLS12
		}
		cfg = clone
	}

	var d net.Dialer
	rawConn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tls dial %q: tcp connect: %w", addr, err)
	}

	// The handshake gets its own bound, applied only when the caller did
	// not already give ctx a deadline: an explicit caller deadline is used
	// as-is and is never lengthened by the default.
	hsCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		hsCtx, cancel = context.WithTimeout(ctx, handshakeTimeout)
		defer cancel()
	}

	tlsConn := tls.Client(rawConn, cfg)
	if err := tlsConn.HandshakeContext(hsCtx); err != nil {
		_ = rawConn.Close()
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("tls dial %q: %w: %w", addr, ErrHandshakeTimeout, err)
		}
		return nil, fmt.Errorf("tls dial %q: handshake: %w", addr, err)
	}
	return tlsConn, nil
}
