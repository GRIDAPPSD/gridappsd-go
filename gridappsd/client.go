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

	// HeartBeat is the STOMP heartbeat interval offered on both connection legs.
	// Zero uses internal/stomp.DefaultHeartBeat (10 s).
	HeartBeat time.Duration
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

	conn, err := auth.Exchange(ctx, netDial, &istormp.Dialer{}, cfg.User, cfg.Password, cfg.HeartBeat)
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

	dialer := &tls.Dialer{Config: cfg}
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tls dial %q: %w", addr, err)
	}
	return netConn, nil
}
