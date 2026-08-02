package fieldbus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/fieldbus"
	"github.com/GRIDAPPSD/gridappsd-go/internal/router"
	"github.com/GRIDAPPSD/gridappsd-go/internal/transporttest"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// v0Bus implements exactly the MessageBus method set as it shipped in v0.1.0.
// The compile-time assertion below is the backward-compatibility gate for
// GAG-009: subscription-failure reporting was added as the separate
// ErrorReporter interface precisely so that a v0.1.0 implementation of
// MessageBus keeps satisfying MessageBus without change.
type v0Bus struct{}

func (v0Bus) Connect(context.Context) error { return nil }
func (v0Bus) Disconnect() error             { return nil }
func (v0Bus) IsConnected() bool             { return false }
func (v0Bus) Send(context.Context, string, string, []byte) error {
	return nil
}
func (v0Bus) Subscribe(context.Context, string, fieldbus.Handler) (fieldbus.Token, error) {
	return 0, nil
}
func (v0Bus) Unsubscribe(context.Context, string, fieldbus.Token) error { return nil }
func (v0Bus) GetResponse(context.Context, string, string, []byte) ([]byte, error) {
	return nil, nil
}

var _ fieldbus.MessageBus = v0Bus{}

// TestBus_SubscriptionErrorReachesCaller asserts the consumer-facing half of
// GAG-009: a subscription that dies is reported to whoever holds the bus, not
// swallowed. Before the fix the bus offered no way at all to learn this.
func TestBus_SubscriptionErrorReachesCaller(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	bus := fieldbus.NewForTest(fc, "subject")
	t.Cleanup(func() { _ = bus.Disconnect() })
	ctx := context.Background()

	const dest = "/queue/goss.gridappsd.simulation.output"
	if _, err := bus.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	brokerErr := errors.New("broker dropped the connection")
	fc.SubForDest(dest).Push(transport.Msg{Err: brokerErr})

	select {
	case err := <-bus.Errors():
		if !errors.Is(err, brokerErr) {
			t.Errorf("bus error does not wrap the broker error: got %v", err)
		}
		if !strings.Contains(err.Error(), dest) {
			t.Errorf("bus error does not name the destination %q: got %v", dest, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no subscription error delivered on bus.Errors within 5s")
	}
}

// TestBus_UnexpectedSubscriptionCloseReported asserts the connection-drop shape:
// the broker closes the subscription with no error frame. That is the failure
// that produced zero bus frames while the bus still looked connected.
func TestBus_UnexpectedSubscriptionCloseReported(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	bus := fieldbus.NewForTest(fc, "subject")
	t.Cleanup(func() { _ = bus.Disconnect() })
	ctx := context.Background()

	const dest = "/queue/goss.gridappsd.simulation.log"
	if _, err := bus.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fc.SubForDest(dest).Close()

	select {
	case err := <-bus.Errors():
		if !errors.Is(err, router.ErrSubscriptionClosed) {
			t.Errorf("bus error does not match router.ErrSubscriptionClosed: got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no close notification delivered on bus.Errors within 5s")
	}
}

// TestBus_ErrorsChannelIsStableAcrossSessions asserts that the channel a health
// monitor captured once keeps delivering after a Disconnect/Connect cycle. A
// per-session channel would leave a monitor holding a channel that silently
// stops carrying anything, which is the same failure shape in a new place.
func TestBus_ErrorsChannelIsStableAcrossSessions(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	bus := fieldbus.NewForTest(fc, "subject")
	ctx := context.Background()

	// A monitor captures the channel up front, before any failure.
	errCh := bus.Errors()

	if err := bus.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	// Re-attach a new session the way Connect does, without dialing a broker.
	fc2 := transporttest.NewFakeConn()
	bus2 := fieldbus.ReconnectForTest(bus, fc2)
	t.Cleanup(func() { _ = bus2.Disconnect() })

	const dest = "/queue/second.session"
	if _, err := bus2.Subscribe(ctx, dest, func(map[string]string, []byte) {}); err != nil {
		t.Fatalf("Subscribe on second session: %v", err)
	}
	fc2.SubForDest(dest).Push(transport.Msg{Err: errors.New("second session failure")})

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), dest) {
			t.Errorf("error from the second session does not name %q: got %v", dest, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel captured before reconnect stopped delivering after reconnect")
	}
}

// TestBus_DeliberateDisconnectReportsNoError asserts that the bus's own teardown
// is not reported as a fault.
func TestBus_DeliberateDisconnectReportsNoError(t *testing.T) {
	t.Parallel()

	fc := transporttest.NewFakeConn()
	bus := fieldbus.NewForTest(fc, "subject")
	ctx := context.Background()

	if _, err := bus.Subscribe(ctx, "/queue/quiet.teardown", func(map[string]string, []byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := bus.Disconnect(); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}

	select {
	case err := <-bus.Errors():
		t.Errorf("deliberate Disconnect reported as a subscription failure: %v", err)
	case <-time.After(200 * time.Millisecond):
		// Correct: our own teardown is not a fault.
	}
}
