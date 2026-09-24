package gridappsd

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// hangingListener accepts TCP connections and never writes or reads on
// them: it is the "peer speaks plaintext STOMP, not TLS" case from issue
// #14, reduced to its essential shape. It never answers a TLS ClientHello.
func hangingListener(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Held open deliberately, never written to nor read from: the
			// peer accepted TCP and then said nothing.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	return ln
}

// withHandshakeTimeout overrides the package's handshake bound for the
// duration of one test, since the real 5s default would make every case
// here slow. Not run in parallel with tests that read the same var.
func withHandshakeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := handshakeTimeout
	handshakeTimeout = d
	t.Cleanup(func() { handshakeTimeout = prev })
}

// TestDialTLS_DefaultBoundsHandshake is acceptance criterion 1 and 5: a
// context with no deadline must not wait forever against a peer that
// accepts TCP and never completes a TLS handshake.
func TestDialTLS_DefaultBoundsHandshake(t *testing.T) {
	withHandshakeTimeout(t, 100*time.Millisecond)
	ln := hangingListener(t)

	start := time.Now()
	_, err := dialTLS(context.Background(), ln.Addr().String(), &tls.Config{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from a handshake that never completes, got nil")
	}
	if elapsed > time.Second {
		t.Fatalf("dialTLS with no caller deadline took %s, want bounded by the default (100ms)", elapsed)
	}
	if !errors.Is(err, ErrHandshakeTimeout) {
		t.Fatalf("expected errors.Is(err, ErrHandshakeTimeout), got %v", err)
	}
}

// TestDialTLS_HandshakeTimeoutNamesLikelyCause is acceptance criterion 3:
// the error text points the reader at the transport setting, not the
// network.
func TestDialTLS_HandshakeTimeoutNamesLikelyCause(t *testing.T) {
	withHandshakeTimeout(t, 100*time.Millisecond)
	ln := hangingListener(t)

	_, err := dialTLS(context.Background(), ln.Addr().String(), &tls.Config{})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	const want = "may not be speaking TLS"
	if got := err.Error(); !strings.Contains(got, want) {
		t.Fatalf("error %q does not name the likely cause (%q)", got, want)
	}
}

// TestDialTLS_ExplicitDeadlineNeverLengthened is acceptance criterion 2:
// a caller-supplied deadline shorter than the package default is honoured
// as-is, never extended to the default.
func TestDialTLS_ExplicitDeadlineNeverLengthened(t *testing.T) {
	withHandshakeTimeout(t, 10*time.Second)
	ln := hangingListener(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := dialTLS(ctx, ln.Addr().String(), &tls.Config{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from a handshake that never completes, got nil")
	}
	if elapsed > time.Second {
		t.Fatalf("dialTLS with a 100ms caller deadline took %s, want bounded by the caller's deadline, not the 10s default", elapsed)
	}
	if !errors.Is(err, ErrHandshakeTimeout) {
		t.Fatalf("expected errors.Is(err, ErrHandshakeTimeout), got %v", err)
	}
}
