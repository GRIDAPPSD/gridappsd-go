package gridappsd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"
)

// selfSignedCert issues a short-lived certificate signed by its own key,
// valid for the given DNS names or IP addresses. A caller that does not add
// it to a trust pool rejects it as an untrusted root; a caller that does
// trust it still rejects any host name not listed here.
func selfSignedCert(t *testing.T, names ...string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "gridappsd-go test certificate"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, name := range names {
		if ip := net.ParseIP(name); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, name)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// verifyingListener runs a real TLS server handshake with serverCert
// against each accepted connection. It reports, per connection, how long
// after the server's own handshake attempt ended the client closed its
// side of the raw socket: this is how the tests below observe closure from
// the peer rather than from dialTLS's own return value. A connection whose
// client never closes within the bound below reports nothing, which is
// what a removed rawConn.Close() in dialTLS produces.
func verifyingListener(t *testing.T, serverCert tls.Certificate) (addr string, closedWithin <-chan time.Duration) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	ch := make(chan time.Duration, 1)
	go func() {
		rawConn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { _ = rawConn.Close() })

		serverConn := tls.Server(rawConn, &tls.Config{Certificates: []tls.Certificate{serverCert}})
		start := time.Now()
		_ = serverConn.Handshake() // expected to fail: the client rejects this certificate

		_ = rawConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, readErr := rawConn.Read(make([]byte, 1))
		if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
			return // the client never closed its side within the bound
		}
		ch <- time.Since(start)
	}()

	return ln.Addr().String(), ch
}

// assertClosedPromptly fails the test if the peer did not observe the raw
// socket close within 3 seconds of the server's handshake attempt ending.
func assertClosedPromptly(t *testing.T, closedWithin <-chan time.Duration) {
	t.Helper()
	select {
	case <-closedWithin:
	case <-time.After(3 * time.Second):
		t.Fatal("peer did not observe the connection close after the handshake failed; dialTLS may have left the raw socket open")
	}
}

// TestDialTLS_UntrustedRootFails covers certificate verification (an
// untrusted root) and, via assertClosedPromptly, the same raw-socket-close
// property as TestDialTLS_HostnameMismatchFails below: a handshake failure
// that is not a timeout must still close the raw connection. Together the
// two tests fail red under either of two independent mutants: forcing
// InsecureSkipVerify true (the error assertions), or removing dialTLS's
// rawConn.Close() on a handshake failure (the close assertion).
func TestDialTLS_UntrustedRootFails(t *testing.T) {
	cert := selfSignedCert(t, "127.0.0.1")
	addr, closedWithin := verifyingListener(t, cert)

	_, err := dialTLS(context.Background(), addr, &tls.Config{})
	if err == nil {
		t.Fatal("expected an error dialing a peer with an untrusted certificate, got nil")
	}
	if errors.Is(err, ErrHandshakeTimeout) {
		t.Fatalf("expected an x509 verification error, got the handshake-timeout sentinel: %v", err)
	}
	var unknownAuth x509.UnknownAuthorityError
	if !errors.As(err, &unknownAuth) {
		t.Fatalf("expected an x509.UnknownAuthorityError, got %v (%T)", err, err)
	}
	assertClosedPromptly(t, closedWithin)
}

// TestDialTLS_HostnameMismatchFails is the SAN-mismatch half of the
// certificate-verification acceptance criterion: a certificate signed by a
// trusted root but issued for a different name must still fail
// verification with an x509 error, not the handshake-timeout sentinel, and
// close the raw connection. See TestDialTLS_UntrustedRootFails for the
// mutants this pair proves.
func TestDialTLS_HostnameMismatchFails(t *testing.T) {
	cert := selfSignedCert(t, "wrong.example.invalid")
	addr, closedWithin := verifyingListener(t, cert)

	pool := x509.NewCertPool()
	pool.AddCert(cert.Leaf)

	_, err := dialTLS(context.Background(), addr, &tls.Config{RootCAs: pool})
	if err == nil {
		t.Fatal("expected an error dialing a peer whose certificate does not name the address, got nil")
	}
	if errors.Is(err, ErrHandshakeTimeout) {
		t.Fatalf("expected an x509 hostname error, got the handshake-timeout sentinel: %v", err)
	}
	var hostErr x509.HostnameError
	if !errors.As(err, &hostErr) {
		t.Fatalf("expected an x509.HostnameError, got %v (%T)", err, err)
	}
	assertClosedPromptly(t, closedWithin)
}
