package fieldbus_test

import (
	"context"
	"fmt"

	"github.com/GRIDAPPSD/gridappsd-go/fieldbus"
	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
	"github.com/GRIDAPPSD/gridappsd-go/topics"
)

// ExampleNew shows the guard that Subscribe, Send, and GetResponse raise
// before Connect succeeds (Unsubscribe on an unconnected bus is a no-op that
// returns nil instead). This example has no live GridAPPS-D broker to
// connect to, so it only demonstrates the pre-Connect state.
func ExampleNew() {
	bus := fieldbus.New(gridappsd.Config{User: "system", Password: "example-password"})
	fmt.Println("connected:", bus.IsConnected())

	h := fieldbus.Handler(func(_ map[string]string, _ []byte) {})
	_, err := bus.Subscribe(context.Background(), topics.FieldBusOutput, h)
	fmt.Println("subscribe before Connect fails:", err != nil)

	// Output:
	// connected: false
	// subscribe before Connect fails: true
}
