package topics_test

import (
	"fmt"

	"github.com/GRIDAPPSD/gridappsd-go/topics"
)

// ExampleNormalizeDestination shows the queue-prepend rule: a destination
// with no /topic/, /queue/, or /temp-queue/ prefix is treated as a queue.
func ExampleNormalizeDestination() {
	fmt.Println(topics.NormalizeDestination(topics.FieldBusOutput))
	fmt.Println(topics.NormalizeDestination("my.service.request"))

	// Output:
	// /topic/goss.gridappsd.field.output
	// /queue/my.service.request
}
