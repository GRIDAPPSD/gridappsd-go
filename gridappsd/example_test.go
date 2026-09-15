package gridappsd_test

import (
	"context"
	"fmt"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
)

// ExampleConnect shows the call shape. There is no live broker to dial here,
// so the context is cancelled up front: dial() checks context cancellation
// before touching the network, so this fails fast and deterministically
// instead of needing a broker.
func ExampleConnect() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := gridappsd.Connect(ctx, gridappsd.Config{User: "system", Password: "manager"})
	fmt.Println("connect failed:", err != nil)

	// Output:
	// connect failed: true
}
