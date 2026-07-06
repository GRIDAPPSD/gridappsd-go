// Package message provides STOMP frame header constants for the GOSS protocol.
//
// The full CIM request/response envelope with typed payloads is deferred to
// phase 3 (query). Phase 2 ships only the header constants the messaging core
// itself needs to stamp on outbound frames.
package message

// STOMP frame header name constants used by the GOSS protocol.
const (
	// HeaderGossHasSubject is the GOSS header indicating a subject is present.
	// Value must be the string "true". Present on every authenticated SEND.
	// Mirrors goss.py:175 ("GOSS_HAS_SUBJECT": True).
	HeaderGossHasSubject = "GOSS_HAS_SUBJECT"

	// HeaderGossSubject carries the GOSS auth token (the subject) on every SEND.
	// The value is the token returned by the two-step GOSS auth flow.
	// Mirrors goss.py:175 ("GOSS_SUBJECT": self.__token).
	HeaderGossSubject = "GOSS_SUBJECT"

	// HeaderReplyTo is the STOMP reply-to header key.
	// Must be set on the SEND frame only, never on SUBSCRIBE.
	// See internal/stomp/stomp.go:98-108 and auth.go:141.
	HeaderReplyTo = "reply-to"

	// HeaderContentType is the STOMP content-type header key.
	HeaderContentType = "content-type"

	// HeaderDestination is the STOMP destination header key.
	HeaderDestination = "destination"
)
