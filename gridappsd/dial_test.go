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
	"testing"
	"time"
)

// TestDial_PlainTCP verifies that dial with a nil TLSConfig (the zero-value
// default) opens a plain TCP connection and that bytes flow over it
// unmodified: this is the dev-broker path (gridappsd-docker on 61613,
// no TLS terminator in front of it).
func TestDial_PlainTCP(t *testing.T) {
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

	clientConn, err := dial(ctx, ln.Addr().String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	if _, ok := clientConn.(*tls.Conn); ok {
		t.Fatal("dial with nil TLSConfig returned a *tls.Conn, want plain net.Conn")
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

	clientConn, err := dial(ctx, ln.Addr().String(), &tls.Config{RootCAs: pool})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	if _, ok := clientConn.(*tls.Conn); !ok {
		t.Fatalf("dial with non-nil TLSConfig returned %T, want *tls.Conn", clientConn)
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
	_, err = dial(ctx, ln.Addr().String(), &tls.Config{InsecureSkipVerify: true})
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
