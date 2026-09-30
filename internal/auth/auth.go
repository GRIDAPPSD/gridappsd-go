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
//  5. End the first connection. (goss.py leaks this connection; we do not.)
//     While ctx is live this is UNSUBSCRIBE then DISCONNECT, each waiting for
//     its receipt; once ctx is done the transport is closed instead, and that
//     teardown finishes in the background after Exchange has returned.
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
	"runtime"
	"strings"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/transport"
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
// heartBeat and heartBeats carry the STOMP heart-beat settings for both
// connections; they are passed through to transport.ConnConfig unchanged, so
// the resolution rule (heartBeats supersedes heartBeat when non-nil) lives in
// exactly one place, the Dialer.
func Exchange(
	ctx context.Context,
	netDial NetDialer,
	d transport.Dialer,
	user, pass string,
	heartBeat time.Duration,
	heartBeats *transport.HeartBeatIntervals,
) (transport.Conn, error) {
	token, err := fetchToken(ctx, netDial, d, user, pass, heartBeat, heartBeats)
	if err != nil {
		return nil, fmt.Errorf("auth exchange: %w", err)
	}

	// Second leg: token as login, empty passcode.
	rwc2, err := netDial(ctx)
	if err != nil {
		return nil, fmt.Errorf("auth exchange second dial: %w", err)
	}
	conn2, err := d.Dial(ctx, rwc2, transport.ConnConfig{
		Login:      token,
		Passcode:   "",
		HeartBeat:  heartBeat,
		HeartBeats: heartBeats,
	})
	if err != nil {
		// go-stomp's Connect does not close rwc on a failed handshake.
		_ = rwc2.Close()
		return nil, fmt.Errorf("auth exchange second connect: %w", err)
	}
	return conn2, nil
}

// endCredentialConn ends the credential connection and returns nil once that
// is done, or ctx.Err() if ctx is done first; the teardown then carries on in
// the background.
//
// go-stomp v3.1.2 panics the process (send on closed channel) when the
// connection closes under an Unsubscribe still waiting for its receipt and
// that wait then times out. So the steps run in order in one goroutine, rwc is
// never closed while Unsubscribe is pending, and once ctx is done nothing
// waits on the broker: rwc is closed, which ends go-stomp's goroutines at once
// rather than at its 30 s receipt timeout.
func endCredentialConn(ctx context.Context, rwc io.Closer, conn transport.Conn, sub transport.Subscription) error {
	if sub != nil {
		// The subscription's forwarding goroutine blocks once its buffer is
		// full, which would stall go-stomp's I/O loop and every receipt behind
		// it. Read until the transport closes the channel.
		go func() {
			for range sub.C() {
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if sub != nil && ctx.Err() == nil {
			_ = unsubscribe(sub)
		}
		if ctx.Err() != nil {
			_ = rwc.Close()
			return
		}
		stop := context.AfterFunc(ctx, func() { _ = rwc.Close() })
		defer stop()
		_ = conn.Disconnect()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		select {
		case <-done:
			return nil
		default:
			return ctx.Err()
		}
	}
}

// unsubscribe calls sub.Unsubscribe and reports a runtime panic from it as an
// error. go-stomp v3.1.2's Unsubscribe sends on its message channel after a
// receipt timeout without checking whether that channel is already closed;
// the call runs on a goroutine of ours, so the process can survive it.
func unsubscribe(sub transport.Subscription) (err error) {
	defer func() {
		if r := recover(); r != nil {
			re, ok := r.(runtime.Error)
			if !ok {
				panic(r)
			}
			err = fmt.Errorf("unsubscribe: %w", re)
		}
	}()
	return sub.Unsubscribe()
}

// fetchToken performs the credential leg and returns the raw token string.
// The credential connection is always ended, per endCredentialConn; if ctx is
// done before that finishes, fetchToken returns an error naming the credential
// connection even when a token arrived.
func fetchToken(
	ctx context.Context,
	netDial NetDialer,
	d transport.Dialer,
	user, pass string,
	heartBeat time.Duration,
	heartBeats *transport.HeartBeatIntervals,
) (token string, err error) {
	// Dial and STOMP-connect with real credentials.
	rwc1, err := netDial(ctx)
	if err != nil {
		return "", fmt.Errorf("credential dial: %w", err)
	}
	conn1, err := d.Dial(ctx, rwc1, transport.ConnConfig{
		Login:      user,
		Passcode:   pass,
		HeartBeat:  heartBeat,
		HeartBeats: heartBeats,
	})
	if err != nil {
		// go-stomp's Connect does not close rwc on a failed handshake.
		_ = rwc1.Close()
		return "", fmt.Errorf("credential connect: %w", err)
	}
	var sub transport.Subscription
	defer func() {
		if terr := endCredentialConn(ctx, rwc1, conn1, sub); terr != nil && err == nil {
			token, err = "", fmt.Errorf("credential connection teardown: %w", terr)
		}
	}()

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

	// Subscribe BEFORE sending the request so no message is missed. The
	// transport unsubscribes on its own when the Subscribe ctx is done, which
	// would race endCredentialConn, so it gets a ctx that is never cancelled.
	s, err := conn1.Subscribe(context.WithoutCancel(ctx), queueDest)
	if err != nil {
		return "", fmt.Errorf("token subscribe %q: %w", queueDest, err)
	}
	sub = s

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
		tok := strings.TrimSpace(string(msg.Body))
		if tok == "" {
			return "", fmt.Errorf("broker returned empty token")
		}
		return tok, nil
	case <-ctx.Done():
		return "", fmt.Errorf("token exchange cancelled: %w", ctx.Err())
	}
}
