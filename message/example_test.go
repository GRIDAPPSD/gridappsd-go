package message_test

import (
	"fmt"

	"github.com/GRIDAPPSD/gridappsd-go/message"
)

// Example lists the STOMP header names the client stamps on an
// authenticated SEND frame.
func Example() {
	fmt.Println(message.HeaderGossHasSubject)
	fmt.Println(message.HeaderGossSubject)
	fmt.Println(message.HeaderReplyTo)

	// Output:
	// GOSS_HAS_SUBJECT
	// GOSS_SUBJECT
	// reply-to
}
