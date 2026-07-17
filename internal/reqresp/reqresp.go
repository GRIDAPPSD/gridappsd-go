// Package reqresp implements correlated request/reply over a transport.Conn.
//
// GetResponse mirrors goss.py:178-247 but replaces the sleep-poll wait with a
// ctx-bounded channel receive, and replaces the timestamp-based reply-id with a
// crypto/rand nonce to avoid collisions under concurrent calls (same discipline
// as auth.go:117-121).
//
// Reply-destination discipline (reused from auth.go:111-143):
//   - Subscribe uses the /queue/ prefix: /queue/reply.<nonce>
//   - The reply-to header on the SEND frame carries the BARE name: reply.<nonce>
//   - reply-to is NEVER set on the SUBSCRIBE frame (go-stomp would intercept it)
//
// Correlation: each GetResponse owns a unique reply destination and its own
// subscription, so the first message on that subscription is the correlated reply.
// No correlation-id matching across a shared channel is needed.
package reqresp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/GRIDAPPSD/gridappsd-go/message"
	"github.com/GRIDAPPSD/gridappsd-go/topics"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// GetResponse sends body to dest and waits for the first reply, bounded by ctx.
//
// dest is queue-normalized by topics.NormalizeDestination before the SEND.
// baseHeaders carries caller-provided headers (e.g. GOSS auth-subject headers);
// the reply-to header is added by this function. baseHeaders must not contain
// a reply-to key (it will be overwritten without error; the caller must omit it).
//
// Returns the reply body on success. On ctx expiry returns a wrapped ctx.Err()
// distinguishable via errors.Is. On a closed subscription before any reply
// returns a distinct closed-subscription error.
func GetResponse(
	ctx context.Context,
	conn transport.Conn,
	dest, contentType string,
	body []byte,
	baseHeaders map[string]string,
) ([]byte, error) {
	// Generate a unique reply destination using crypto/rand to avoid collisions
	// under concurrent GetResponse calls (auth.go:117-121 uses the same approach).
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("reqresp: generating reply nonce: %w", err)
	}
	bareReply := "reply." + hex.EncodeToString(nonce[:])
	queueReply := "/queue/" + bareReply

	// Subscribe BEFORE sending so no reply is missed (same ordering as auth.go:124-128).
	sub, err := conn.Subscribe(ctx, queueReply)
	if err != nil {
		return nil, fmt.Errorf("reqresp: subscribe reply destination %q: %w", queueReply, err)
	}
	// Always unsubscribe the reply destination on return (success or error).
	defer func() { _ = sub.Unsubscribe() }()

	// Merge caller-provided headers with the reply-to header.
	// reply-to carries the BARE name, not the /queue/ prefix (see package doc).
	headers := make(map[string]string, len(baseHeaders)+1)
	for k, v := range baseHeaders {
		headers[k] = v
	}
	headers[message.HeaderReplyTo] = bareReply

	// Send to the queue-normalized request destination.
	normDest := topics.NormalizeDestination(dest)
	if err := conn.Send(ctx, normDest, contentType, body, headers); err != nil {
		return nil, fmt.Errorf("reqresp: send to %q: %w", normDest, err)
	}

	// Wait for the first reply, bounded by ctx. No sleep-poll.
	select {
	case msg, ok := <-sub.C():
		if !ok {
			return nil, fmt.Errorf("reqresp: reply subscription closed before response arrived")
		}
		if msg.Err != nil {
			return nil, fmt.Errorf("reqresp: reply subscription error: %w", msg.Err)
		}
		return msg.Body, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("reqresp: context cancelled waiting for reply: %w", ctx.Err())
	}
}
