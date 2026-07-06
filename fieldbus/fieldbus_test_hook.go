package fieldbus

import (
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/internal/router"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// NewForTest constructs a GridAPPSDMessageBus in the already-connected state,
// using conn as the transport instead of dialing a real broker. token is used
// as the GOSS auth subject stamped on outbound frames.
//
// This constructor exists for unit tests only. It is not part of the public API.
// Production code must use New followed by Connect.
func NewForTest(conn transport.Conn, token string) *GridAPPSDMessageBus {
	b := &GridAPPSDMessageBus{}
	b.conn = conn
	b.rtr = router.New(conn)
	b.token = token
	b.connected = true
	return b
}
