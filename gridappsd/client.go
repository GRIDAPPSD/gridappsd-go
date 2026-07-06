// Package gridappsd provides the entry point for connecting to a GridAPPS-D / GOSS broker.
//
// Connect performs TLS dialing and the two-step token authentication,
// then returns a durable transport.Conn ready for messaging (phases 2-4).
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

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/internal/auth"
	istormp "tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/internal/stomp"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// DefaultAddress is the default GOSS STOMP+TLS endpoint.
const DefaultAddress = "localhost:61613"

// Config holds all parameters required to establish an authenticated GridAPPS-D connection.
type Config struct {
	// Address is the GOSS broker endpoint in host:port form.
	// Defaults to DefaultAddress when empty.
	Address string

	// TLSConfig controls TLS settings for both the credential and durable legs.
	// The caller may supply a *tls.Config with client certificates (mTLS), a
	// custom CA pool, or specific cipher/curve constraints. Nil is allowed when
	// the system trust store is sufficient, but InsecureSkipVerify is not set
	// here: the caller who wants that must set it on their own TLSConfig and
	// accepts the responsibility.
	TLSConfig *tls.Config

	// User is the GOSS username used on the credential leg.
	User string

	// Password is the GOSS password used on the credential leg.
	Password string

	// HeartBeat is the STOMP heartbeat interval offered on both connection legs.
	// Zero uses internal/stomp.DefaultHeartBeat (10 s).
	HeartBeat time.Duration
}

// Connect dials a TLS connection to the GridAPPS-D broker, performs the
// two-step GOSS token authentication, and returns a durable transport.Conn.
//
// Both the credential leg and the durable leg use TLS. The TLS dial is done
// here so that the credential leg (which carries base64-encoded credentials)
// is never sent over plain TCP.
//
// The returned transport.Conn is ready for messaging. Call Connect again to
// re-authenticate when the token expires.
func Connect(ctx context.Context, cfg Config) (transport.Conn, error) {
	if cfg.Address == "" {
		cfg.Address = DefaultAddress
	}

	netDial := func(ctx context.Context) (io.ReadWriteCloser, error) {
		return dialTLS(ctx, cfg.Address, cfg.TLSConfig)
	}

	conn, err := auth.Exchange(ctx, netDial, &istormp.Dialer{}, cfg.User, cfg.Password, cfg.HeartBeat)
	if err != nil {
		return nil, fmt.Errorf("gridappsd connect to %s: %w", cfg.Address, err)
	}
	return conn, nil
}

// dialTLS opens a TLS connection to addr. tlsCfg may be nil (system trust store).
// Returns an io.ReadWriteCloser so it satisfies auth.NetDialer without importing
// crypto/tls in the auth package.
func dialTLS(ctx context.Context, addr string, tlsCfg *tls.Config) (io.ReadWriteCloser, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", addr, err)
	}
	cfg := tlsCfg
	if cfg == nil {
		cfg = &tls.Config{ServerName: host}
	} else if cfg.ServerName == "" {
		// Clone to avoid mutating the caller's config.
		clone := cfg.Clone()
		clone.ServerName = host
		cfg = clone
	}

	dialer := &tls.Dialer{Config: cfg}
	netConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tls dial %q: %w", addr, err)
	}
	return netConn, nil
}
