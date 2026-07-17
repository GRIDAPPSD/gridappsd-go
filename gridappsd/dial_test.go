package gridappsd

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// TestDial_AllowPlaintext_DialsPlainTCP verifies that dial with a nil
// TLSConfig and AllowPlaintext true opens a plain TCP connection and that
// bytes flow over it unmodified: this is the dev-broker path
// (gridappsd-docker on 61613, no TLS terminator in front of it).
func TestDial_AllowPlaintext_DialsPlainTCP(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	// Error discarded: test teardown, no recovery path.
	defer func() { _ = ln.Close() }()

	const frame = "PLAIN-FRAME\n"
	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_, err = conn.Write([]byte(frame))
		serverDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := dial(ctx, ln.Addr().String(), nil, true)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	if _, ok := clientConn.(*tls.Conn); ok {
		t.Fatal("dial with nil TLSConfig and AllowPlaintext true returned a *tls.Conn, want plain net.Conn")
	}

	got, err := bufio.NewReader(clientConn).ReadString('\n')
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if got != frame {
		t.Fatalf("got frame %q, want %q", got, frame)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server write: %v", err)
	}
}

// TestDial_ZeroValue_FailsClosedToTLS verifies that the zero-value
// combination (TLSConfig nil, AllowPlaintext false) attempts a TLS dial
// using the system trust store rather than silently falling back to plain
// TCP. The listener presents a self-signed certificate the system trust
// store does not know, so a genuine TLS attempt must fail the handshake;
// a plain-TCP fallback would instead succeed at the transport level. The
// wrapped error text proves the failure came from the TLS dial path.
func TestDial_ZeroValue_FailsClosedToTLS(t *testing.T) {
	t.Parallel()

	cert, _ := newSelfSignedCert(t)

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	// Error discarded: test teardown, no recovery path.
	defer func() { _ = ln.Close() }()

	go func() {
		conn, err := ln.Accept()
		if err == nil {
			// Error discarded: best-effort cleanup of a connection whose
			// handshake the client is expected to reject.
			_ = conn.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = dial(ctx, ln.Addr().String(), nil, false)
	if err == nil {
		t.Fatal("expected TLS handshake failure against an untrusted self-signed cert, got nil error")
	}
	if !strings.Contains(err.Error(), "tls dial") {
		t.Fatalf("expected the error to come from the TLS dial path (\"tls dial\" in message), got %v", err)
	}
}

// TestDial_TLSConfigWinsOverAllowPlaintext verifies the documented
// precedence: a non-nil TLSConfig dials TLS even when AllowPlaintext is
// true.
func TestDial_TLSConfigWinsOverAllowPlaintext(t *testing.T) {
	t.Parallel()

	cert, pool := newSelfSignedCert(t)

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	// Error discarded: test teardown, no recovery path.
	defer func() { _ = ln.Close() }()

	const frame = "WINS-FRAME\n"
	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_, err = conn.Write([]byte(frame))
		serverDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := dial(ctx, ln.Addr().String(), &tls.Config{RootCAs: pool}, true)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	if _, ok := clientConn.(*tls.Conn); !ok {
		t.Fatalf("dial with non-nil TLSConfig and AllowPlaintext true returned %T, want *tls.Conn", clientConn)
	}

	got, err := bufio.NewReader(clientConn).ReadString('\n')
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if got != frame {
		t.Fatalf("got frame %q, want %q", got, frame)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server write: %v", err)
	}
}

// TestDialTLS_HandshakeAndFrameFlow spins up a local TLS listener with a
// runtime-generated self-signed certificate, dials through dial() with a
// non-nil TLSConfig, and asserts the handshake completes and a frame
// written by the server is read back unmodified by the client. This is
// the wire-level proof that TLS wraps the transport transparently; the
// certificate is generated at test runtime and never committed as a
// fixture (no private key on disk).
func TestDialTLS_HandshakeAndFrameFlow(t *testing.T) {
	t.Parallel()

	cert, pool := newSelfSignedCert(t)

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	// Error discarded: test teardown, no recovery path.
	defer func() { _ = ln.Close() }()

	const frame = "TLS-FRAME\n"
	serverDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_, err = conn.Write([]byte(frame))
		serverDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := dial(ctx, ln.Addr().String(), &tls.Config{RootCAs: pool}, false)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	tlsConn, ok := clientConn.(*tls.Conn)
	if !ok {
		t.Fatalf("dial with non-nil TLSConfig returned %T, want *tls.Conn", clientConn)
	}
	if v := tlsConn.ConnectionState().Version; v < tls.VersionTLS12 {
		t.Fatalf("negotiated TLS version 0x%04x, want at least TLS 1.2 (0x%04x)", v, tls.VersionTLS12)
	}

	got, err := bufio.NewReader(clientConn).ReadString('\n')
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if got != frame {
		t.Fatalf("got frame %q, want %q", got, frame)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server write: %v", err)
	}
}

// TestDialTLS_RespectsContextCancellation verifies that dial() with a
// non-nil TLSConfig honors context cancellation on the TLS leg: a
// pre-cancelled context must fail the dial rather than block on (or
// silently complete) the TLS handshake.
func TestDialTLS_RespectsContextCancellation(t *testing.T) {
	t.Parallel()

	// A plain (non-TLS) listener is enough here: a cancelled context must
	// short-circuit the dial before any handshake bytes are exchanged, so
	// the listener never needs to complete a TLS handshake for this test
	// to be meaningful.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	// Error discarded: test teardown, no recovery path.
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err = dial(ctx, ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}, false)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from dial with a pre-cancelled context, got nil")
	}
	// A cancelled context must fail fast, not fall through to a real
	// handshake attempt or a connect timeout. 1s is generous headroom over
	// a local-loopback dial.
	if elapsed > time.Second {
		t.Fatalf("dial with cancelled context took %s, want fast failure", elapsed)
	}
}

// TestDial_PlainTCP_RespectsContextCancellation mirrors
// TestDialTLS_RespectsContextCancellation for the plain-TCP path
// (TLSConfig nil, AllowPlaintext true): a pre-cancelled context must fail
// dialPlain fast rather than block on or silently complete the TCP dial.
func TestDial_PlainTCP_RespectsContextCancellation(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	// Error discarded: test teardown, no recovery path.
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err = dial(ctx, ln.Addr().String(), nil, true)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from dial with a pre-cancelled context, got nil")
	}
	if elapsed > time.Second {
		t.Fatalf("dial with cancelled context took %s, want fast failure", elapsed)
	}
}

// TestDialTLS_NilConfigReturnsError verifies dialTLS's defensive nil
// check directly: called with a nil tlsCfg (bypassing dial()'s own nil
// handling), it must return a wrapped error rather than panic on a nil
// pointer dereference.
func TestDialTLS_NilConfigReturnsError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := dialTLS(ctx, "127.0.0.1:0", nil)
	if err == nil {
		t.Fatal("expected error from dialTLS with a nil TLS config, got nil")
	}
	if !strings.Contains(err.Error(), "nil TLS config") {
		t.Fatalf("expected error to name the nil config, got %v", err)
	}
}

// newSelfSignedCert generates a throwaway self-signed certificate for
// 127.0.0.1, valid for the duration of the test process. The private key
// never touches disk.
func newSelfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating test key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("generating test serial: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "gridappsd-go dial test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("creating test certificate: %v", err)
	}

	leaf, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatalf("parsing test certificate: %v", err)
	}

	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
		Leaf:        leaf,
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return cert, pool
}
