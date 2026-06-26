// Package auth implements the GridAPPS-D / GOSS two-step token authentication flow.
//
// Flow (mirrors goss.py _make_connection, lines 308-389):
//
//  1. Open a FIRST STOMP connection authenticating with plain credentials.
//  2. Subscribe to a named temp queue: /queue/temp.token_resp.<user>-<nonce>.
//  3. SEND base64(user:password) to /topic/pnnl.goss.token.topic with the
//     reply-to header set to the temp queue name (without the /queue/ prefix).
//     The reply-to header goes on the SEND frame, never on SUBSCRIBE (go-stomp
//     intercepts reply-to on SUBSCRIBE for a RabbitMQ-specific path that is
//     incompatible with GOSS).
//  4. Read the token from the subscription channel, bounded by ctx.
//  5. DISCONNECT the first connection. (goss.py leaks this connection; we do not.)
//  6. Open a SECOND STOMP connection: token as login, empty passcode.
//
// Re-auth: Exchange is stateless and re-entrant. Call it again with fresh
// connections from netDial to refresh an expired token.
package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

const (
	// TokenTopic is the GOSS destination that issues authentication tokens.
	TokenTopic = "/topic/pnnl.goss.token.topic"

	// replyDestPrefix is the prefix for the named temp reply queue.
	// The full destination is replyDestPrefix + user + "-" + nonce.
	replyDestPrefix = "temp.token_resp."
)

// NetDialer dials a raw network connection that the caller has already wrapped
// with TLS. It is called twice: once for the credential leg, once for the
// durable authenticated leg.
type NetDialer func(ctx context.Context) (io.ReadWriteCloser, error)

// Exchange performs the two-step token auth flow and returns the durable,
// token-authenticated transport.Conn.
//
// netDial is called twice: both calls must return a TLS-wrapped connection
// to the same GOSS broker. The credential leg is disconnected before the
// durable leg is established.
//
// heartBeat is the STOMP heartbeat interval offered on both connections.
// When zero, transport.Dialer implementations apply their own default.
func Exchange(
	ctx context.Context,
	netDial NetDialer,
	d transport.Dialer,
	user, pass string,
	heartBeat time.Duration,
) (transport.Conn, error) {
	token, err := fetchToken(ctx, netDial, d, user, pass, heartBeat)
	if err != nil {
		return nil, fmt.Errorf("auth exchange: %w", err)
	}

	// Second leg: token as login, empty passcode.
	rwc2, err := netDial(ctx)
	if err != nil {
		return nil, fmt.Errorf("auth exchange second dial: %w", err)
	}
	conn2, err := d.Dial(ctx, rwc2, transport.ConnConfig{
		Login:     token,
		Passcode:  "",
		HeartBeat: heartBeat,
	})
	if err != nil {
		return nil, fmt.Errorf("auth exchange second connect: %w", err)
	}
	return conn2, nil
}

// fetchToken performs the credential leg and returns the raw token string.
// The credential connection is disconnected before fetchToken returns.
func fetchToken(
	ctx context.Context,
	netDial NetDialer,
	d transport.Dialer,
	user, pass string,
	heartBeat time.Duration,
) (string, error) {
	// Dial and STOMP-connect with real credentials.
	rwc1, err := netDial(ctx)
	if err != nil {
		return "", fmt.Errorf("credential dial: %w", err)
	}
	conn1, err := d.Dial(ctx, rwc1, transport.ConnConfig{
		Login:     user,
		Passcode:  pass,
		HeartBeat: heartBeat,
	})
	if err != nil {
		return "", fmt.Errorf("credential connect: %w", err)
	}
	// Always disconnect the credential connection; goss.py leaks it, we do not.
	defer func() { _ = conn1.Disconnect() }()

	// Named temp reply destination. The subscribe uses the /queue/ prefix;
	// the reply-to header on SEND uses the bare name (no prefix). GOSS routes
	// the reply to /queue/<replyDest> based on the reply-to value.
	//
	// Use crypto/rand for the nonce to avoid collisions when the same user
	// performs concurrent re-auth. UnixNano is collision-prone under parallel calls.
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("generating reply-queue nonce: %w", err)
	}
	replyDest := fmt.Sprintf("%s%s-%s", replyDestPrefix, user, hex.EncodeToString(nonce[:]))
	queueDest := "/queue/" + replyDest

	// Subscribe BEFORE sending the request so no message is missed.
	sub, err := conn1.Subscribe(ctx, queueDest)
	if err != nil {
		return "", fmt.Errorf("token subscribe %q: %w", queueDest, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	// The payload is base64(user + ":" + password), exactly as goss.py does:
	//   base64Str = base64.b64encode(userAuthStr.encode())  where userAuthStr = f"{user}:{pass}"
	payload := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	err = conn1.Send(ctx, TokenTopic, "text/plain", []byte(payload), map[string]string{
		// reply-to goes on the SEND frame only, never on SUBSCRIBE.
		"reply-to": replyDest,
	})
	if err != nil {
		return "", fmt.Errorf("token send: %w", err)
	}

	// Read the token, bounded by ctx. No busy-wait or sleep loop.
	select {
	case msg, ok := <-sub.C():
		if !ok {
			return "", fmt.Errorf("token subscription closed before response arrived")
		}
		if msg.Err != nil {
			return "", fmt.Errorf("token subscription error: %w", msg.Err)
		}
		token := strings.TrimSpace(string(msg.Body))
		if token == "" {
			return "", fmt.Errorf("broker returned empty token")
		}
		return token, nil
	case <-ctx.Done():
		return "", fmt.Errorf("token exchange cancelled: %w", ctx.Err())
	}
}
