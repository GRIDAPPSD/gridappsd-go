package fieldbus

import (
	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
	"github.com/GRIDAPPSD/gridappsd-go/internal/router"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// NewForTest constructs a GridAPPSDMessageBus in the already-connected state,
// using conn as the transport instead of dialing a real broker. subject is
// stamped into GOSS_SUBJECT on every outbound frame as the username identity.
//
// This constructor exists for unit tests only. It is not part of the public API.
// Production code must use New followed by Connect.
func NewForTest(conn transport.Conn, subject string) *GridAPPSDMessageBus {
	// Built through New so the bus-owned Errors channel is wired exactly as it
	// is in production; only the dial is bypassed.
	b := New(gridappsd.Config{})
	b.conn = conn
	b.rtr = router.NewWithErrorChan(conn, b.errs)
	b.subject = subject
	b.connected = true
	return b
}

// ReconnectForTest re-attaches b to a new transport the way Connect does,
// without dialing a broker, and returns b. Used to assert that the bus-owned
// Errors channel keeps delivering across a Disconnect/Connect cycle.
//
// This helper exists for unit tests only. It is not part of the public API.
func ReconnectForTest(b *GridAPPSDMessageBus, conn transport.Conn) *GridAPPSDMessageBus {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.conn = conn
	b.rtr = router.NewWithErrorChan(conn, b.errs)
	b.connected = true
	return b
}
