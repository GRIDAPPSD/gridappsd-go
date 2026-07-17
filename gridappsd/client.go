// Package gridappsd provides the entry point for connecting to a GridAPPS-D / GOSS broker.
//
// Connect dials the broker (plain TCP or TLS, depending on Config.TLSConfig)
// and performs the two-step token authentication, then returns a durable
// transport.Conn ready for messaging (phases 2-4).
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
// plain TCP or TLS is controlled by Config.TLSConfig, not by this address.
const DefaultAddress = "localhost:61613"

// Config holds all parameters required to establish an authenticated GridAPPS-D connection.
type Config struct {
	// Address is the GOSS broker endpoint in host:port form.
	// Defaults to DefaultAddress when empty.
	Address string

	// TLSConfig controls whether the connection uses TLS and, when it does,
	// how.
	//
	// A nil TLSConfig (the zero value) dials a plain TCP connection: this is
	// the default, matching the plaintext dev broker most local
	// gridappsd-docker stacks expose on 61613. A non-nil TLSConfig opts into
	// a TLS dial for both the credential and durable legs, using that config:
	// the caller may supply client certificates (mTLS), a custom CA pool, or
	// specific cipher/curve constraints. InsecureSkipVerify is not set here:
	// the caller who wants that must set it on their own TLSConfig and
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

// Connect dials the GridAPPS-D broker (plain TCP or TLS, per
// cfg.TLSConfig), performs the two-step GOSS token authentication, and
// returns a durable transport.Conn.
//
// Both the credential leg and the durable leg use the same transport (both
// plain, or both TLS): the credential leg carries base64-encoded
// credentials, so a caller connecting to a broker that requires
// confidentiality must set cfg.TLSConfig.
//
// The returned transport.Conn is ready for messaging. Call Connect again to
// re-authenticate when the token expires.
func Connect(ctx context.Context, cfg Config) (transport.Conn, error) {
	if cfg.Address == "" {
		cfg.Address = DefaultAddress
	}

	netDial := func(ctx context.Context) (io.ReadWriteCloser, error) {
		return dial(ctx, cfg.Address, cfg.TLSConfig)
	}

	conn, err := auth.Exchange(ctx, netDial, &istormp.Dialer{}, cfg.User, cfg.Password, cfg.HeartBeat)
	if err != nil {
		return nil, fmt.Errorf("gridappsd connect to %s: %w", cfg.Address, err)
	}
	return conn, nil
}

// dial opens a connection to addr: plain TCP when tlsCfg is nil, TLS when
// tlsCfg is non-nil. Returns an io.ReadWriteCloser so it satisfies
// auth.NetDialer without importing crypto/tls in the auth package.
func dial(ctx context.Context, addr string, tlsCfg *tls.Config) (io.ReadWriteCloser, error) {
	if tlsCfg == nil {
		return dialPlain(ctx, addr)
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

// dialTLS opens a TLS connection to addr. tlsCfg must be non-nil.
func dialTLS(ctx context.Context, addr string, tlsCfg *tls.Config) (io.ReadWriteCloser, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", addr, err)
	}
	cfg := tlsCfg
	if cfg.ServerName == "" {
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
