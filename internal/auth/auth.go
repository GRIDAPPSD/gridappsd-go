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
//     its receipt, the DISCONNECT receipt for at most credentialDisconnectBound.
//     Once ctx is done the transport is closed, which ends a pending receipt
//     wait in the common case; go-stomp can still keep a goroutine past it
//     (#24). A ctx that ends during the teardown fails an exchange that had
//     a token. A failed teardown step is otherwise logged and the connection
//     closed, and a token that arrived is still used.
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
	"log/slog"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

const (
	// TokenTopic is the GOSS destination that issues authentication tokens.
	TokenTopic = "/topic/pnnl.goss.token.topic"

	// replyDestPrefix is the prefix for the named temp reply queue.
	// The full destination is replyDestPrefix + user + "-" + nonce.
	replyDestPrefix = "temp.token_resp."

	// maxStepErrLen caps the text of a teardown error. A broker's ERROR
	// message reaches it whole, and go-stomp puts no limit on header size.
	maxStepErrLen = 512

	// credentialDisconnectBound caps the credential connection's wait for its
	// DISCONNECT receipt. A broker that is up answers in a round trip; a
	// close that lands after the DISCONNECT is queued leaves a wait that
	// cannot succeed, and callers check for leaked goroutines within seconds
	// (#24). The token has already arrived, so giving up costs a log line.
	credentialDisconnectBound = time.Second
)

// disconnectBounder is a transport.Dialer that can bound its connections'
// wait for a DISCONNECT receipt.
type disconnectBounder interface {
	WithDisconnectBound(bound time.Duration) transport.Dialer
}

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
//
// logger receives a credential connection teardown failure that does not fail
// the exchange; nil means slog.Default().
func Exchange(
	ctx context.Context,
	netDial NetDialer,
	d transport.Dialer,
	user, pass string,
	heartBeat time.Duration,
	heartBeats *transport.HeartBeatIntervals,
	logger *slog.Logger,
) (transport.Conn, error) {
	if logger == nil {
		logger = slog.Default()
	}
	token, err := fetchToken(ctx, netDial, d, user, pass, heartBeat, heartBeats, logger)
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

// endCredentialConn ends the credential connection. stepErr joins the
// Unsubscribe and Disconnect errors, and ctxErr is ctx.Err() when ctx was done
// by the end. If ctx is done first it returns at once with a nil stepErr, and
// the teardown carries on in the background.
//
// Once ctx is done rwc is closed, also under a pending UNSUBSCRIBE or
// DISCONNECT, so go-stomp ends the subscription and wakes both receipt waits
// instead of waiting out its 30 s receipt timeout. go-stomp can miss that
// wakeup in Unsubscribe, keeping a goroutine past the close (#24); a
// Disconnect whose connection closes under it waits out
// credentialDisconnectBound. A go-stomp v3.1.2 panic from a receipt
// timeout racing that close is recovered by unsubscribe and reported like any
// other error.
func endCredentialConn(ctx context.Context, rwc io.Closer, conn transport.Conn, sub transport.Subscription) (stepErr, ctxErr error) {
	if sub != nil {
		// The subscription's forwarding goroutine blocks once its buffer is
		// full, which would stall go-stomp's I/O loop and every receipt behind
		// it. Read until the transport closes the channel.
		go func() {
			for range sub.C() {
			}
		}()
	}
	stop := context.AfterFunc(ctx, func() { _ = rwc.Close() })
	done := make(chan struct{})
	var gotStep, gotCtx error
	go func() {
		defer close(done)
		defer stop()
		var uerr, derr error
		if sub != nil && ctx.Err() == nil {
			uerr = unsubscribe(sub)
		}
		if ctx.Err() == nil {
			derr = conn.Disconnect()
		}
		gotStep = joinSteps(bound(uerr), bound(derr))
		// A step the close cut short reads as finished, so a done ctx is
		// reported whichever of done and ctx.Done the caller sees first.
		gotCtx = ctx.Err()
	}()
	select {
	case <-done:
	case <-ctx.Done():
		select {
		case <-done:
		default:
			return nil, ctx.Err()
		}
	}
	return gotStep, gotCtx
}

// joinSteps joins the teardown step errors on one line, since the result is
// logged and errors.Join would put a newline between them. Each step error is
// bounded before the join so neither can hide the other. The "; " join and a
// literal "(truncated, N bytes)" inside broker text are ambiguous with the
// real separator and note.
func joinSteps(uerr, derr error) error {
	if uerr != nil && derr != nil {
		return fmt.Errorf("%w; %w", uerr, derr)
	}
	if uerr != nil {
		return uerr
	}
	return derr
}

// boundedError renders err as one line of printable text, capped at
// maxStepErrLen bytes, and still unwraps to err. The text can come from a
// broker, so control characters are escaped before the cap: a line break would
// split a log line, and an escape sequence would reach whoever reads it.
type boundedError struct{ err error }

func (e boundedError) Error() string {
	raw := e.err.Error()
	s, truncated := escapeBounded(raw, maxStepErrLen)
	if !truncated {
		return s
	}
	return fmt.Sprintf("%s... (truncated, %d bytes)", s, len(raw))
}

// escapeBounded writes s as printable text of at most limit bytes and reports
// whether anything was left out. It stops before the first token that does not
// fit, so an escape is never cut in half, and it reads only as much of s as the
// limit keeps. The escapes keep the text from splitting a log line, reaching a
// terminal as a control sequence, or reading like an escape the broker did not
// send: backslash becomes \\; each ASCII control byte, DEL included, and each
// byte of invalid UTF-8 becomes \xNN; each C1 control, U+2028, U+2029 and
// format character (see isFormat) becomes \uNNNN, or \UNNNNNNNN for the tag
// range. Other runes are kept.
func escapeBounded(s string, limit int) (string, bool) {
	var b strings.Builder
	b.Grow(min(len(s), limit))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		tok := s[i : i+size]
		switch {
		case r == utf8.RuneError && size == 1:
			tok = fmt.Sprintf("\\x%02x", s[i])
		case r == '\\':
			tok = `\\`
		case r < 0x20 || r == 0x7f:
			tok = fmt.Sprintf("\\x%02x", r)
		case r >= 0x80 && r <= 0x9f, r == 0x2028, r == 0x2029:
			tok = fmt.Sprintf("\\u%04x", r)
		case isFormat(r):
			if r > 0xffff {
				tok = fmt.Sprintf("\\U%08x", r)
			} else {
				tok = fmt.Sprintf("\\u%04x", r)
			}
		}
		if b.Len()+len(tok) > limit {
			return b.String(), true
		}
		b.WriteString(tok)
		i += size
	}
	return b.String(), false
}

// isFormat reports the invisible format characters that reorder or hide text
// in a log viewer: bidirectional controls, zero-width characters and tags.
func isFormat(r rune) bool {
	switch {
	case r == 0x061c, r == 0x200e, r == 0x200f:
	case r >= 0x200b && r <= 0x200d:
	case r >= 0x202a && r <= 0x202e:
	case r == 0x2060, r >= 0x2066 && r <= 0x2069:
	case r == 0xfeff:
	case r >= 0xe0000 && r <= 0xe007f:
	default:
		return false
	}
	return true
}

// bound wraps a non-nil err in a boundedError.
func bound(err error) error {
	if err == nil {
		return nil
	}
	return boundedError{err}
}

func (e boundedError) Unwrap() error { return e.err }

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
// The credential connection is always ended, per endCredentialConn. If a
// token arrived and ctx is done before that finishes, fetchToken returns an
// error naming the credential connection and wrapping ctx.Err() and any step
// error. Otherwise a failed step goes to logger only, with token_received
// saying whether a token arrived: an exchange that already failed keeps its
// own error, and a token that arrived is still valid.
func fetchToken(
	ctx context.Context,
	netDial NetDialer,
	d transport.Dialer,
	user, pass string,
	heartBeat time.Duration,
	heartBeats *transport.HeartBeatIntervals,
	logger *slog.Logger,
) (token string, err error) {
	// Dial and STOMP-connect with real credentials.
	rwc1, err := netDial(ctx)
	if err != nil {
		return "", fmt.Errorf("credential dial: %w", err)
	}
	if b, ok := d.(disconnectBounder); ok {
		d = b.WithDisconnectBound(credentialDisconnectBound)
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
		stepErr, ctxErr := endCredentialConn(ctx, rwc1, conn1, sub)
		if stepErr != nil {
			if ctxErr == nil {
				// The context close is the only other closer, and a failed
				// step does not show that the transport closed rwc1.
				_ = rwc1.Close()
			}
		}
		switch {
		case ctxErr != nil && err == nil:
			if stepErr != nil {
				ctxErr = fmt.Errorf("%w: %w", ctxErr, stepErr)
			}
			token, err = "", fmt.Errorf("credential connection teardown: %w", ctxErr)
		case stepErr != nil:
			logger.LogAttrs(ctx, slog.LevelWarn, "gridappsd auth: credential connection teardown failed",
				slog.Bool("token_received", err == nil),
				slog.Any("error", stepErr))
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
