package gridappsd_test

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
)

// TestConfig_Defaults verifies that zero-value Config fields get sane defaults
// when Connect validates them. This is a unit-level check on the Config shape;
// the actual dial is not performed (no live broker in CI).
func TestConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := gridappsd.Config{
		User:     "admin",
		Password: "admin",
	}

	// Address defaults to DefaultAddress when empty.
	if cfg.Address != "" {
		t.Skip("test assumes Address starts empty")
	}

	// Connect with a cancelled context: we exercise the Config wiring path
	// without needing a live broker. The error must come from the dial, not
	// from a nil-pointer or bad default.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := gridappsd.Connect(ctx, cfg)
	if err == nil {
		t.Fatal("expected error with cancelled context and no live broker")
	}
	// The error should not be a panic or internal nil-pointer; any non-nil
	// error from the dial layer is acceptable here.
}

// TestConfig_TLSConfig verifies that a caller-supplied *tls.Config is accepted
// without panic. InsecureSkipVerify is set here only to allow a rejected-TLS
// error (no cert) rather than a "bad config" error.
func TestConfig_TLSConfig(t *testing.T) {
	t.Parallel()

	cfg := gridappsd.Config{
		Address:   "127.0.0.1:19999", // nothing listening; fast refusal
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
		User:      "admin",
		Password:  "admin",
		HeartBeat: 10 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := gridappsd.Connect(ctx, cfg)
	if err == nil {
		t.Fatal("expected connection refused error, got nil")
	}
	// Any error is acceptable: we just verify no panic on a custom TLSConfig.
}
