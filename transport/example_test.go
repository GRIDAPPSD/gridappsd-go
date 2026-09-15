package transport_test

import (
	"fmt"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// ExampleHeartBeatIntervals shows the STOMP 1.2 zero-disables-direction
// semantics: a caller that wants to keep proving liveness without requiring
// the broker to send anything back leaves Recv at zero.
func ExampleHeartBeatIntervals() {
	hb := transport.HeartBeatIntervals{Send: 10 * time.Second}
	fmt.Println("send interval:", hb.Send)
	fmt.Println("recv disabled:", hb.Recv == 0)

	// Output:
	// send interval: 10s
	// recv disabled: true
}
