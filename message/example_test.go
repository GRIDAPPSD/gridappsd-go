package message_test

import (
	"fmt"

	"github.com/GRIDAPPSD/gridappsd-go/message"
)

// Example lists the STOMP header names the client uses. The two GOSS subject
// headers are stamped on every authenticated SEND; HeaderReplyTo is added
// only on the request send of GetResponse, not on every SEND.
func Example() {
	fmt.Println(message.HeaderGossHasSubject)
	fmt.Println(message.HeaderGossSubject)
	fmt.Println(message.HeaderReplyTo)

	// Output:
	// GOSS_HAS_SUBJECT
	// GOSS_SUBJECT
	// reply-to
}
