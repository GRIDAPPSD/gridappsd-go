package fieldbus

import (
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/internal/router"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// NewForTest constructs a GridAPPSDMessageBus in the already-connected state,
// using conn as the transport instead of dialing a real broker. subject is
// stamped into GOSS_SUBJECT on every outbound frame as the username identity.
//
// This constructor exists for unit tests only. It is not part of the public API.
// Production code must use New followed by Connect.
func NewForTest(conn transport.Conn, subject string) *GridAPPSDMessageBus {
	b := &GridAPPSDMessageBus{}
	b.conn = conn
	b.rtr = router.New(conn)
	b.subject = subject
	b.connected = true
	return b
}
