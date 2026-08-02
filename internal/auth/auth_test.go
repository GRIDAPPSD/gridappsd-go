package auth_test

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/auth"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// fakeToken is the synthetic token returned by the fakeConn subscription.
const fakeToken = "fake-goss-token-abc123"

// --- fake transport ---

// sentMsg records a single STOMP SEND call for assertions.
type sentMsg struct {
	destination string
	body        []byte
	headers     map[string]string
}

// fakeConn is an in-process transport.Conn used to assert wire-level invariants
// without a real broker.
type fakeConn struct {
	sends        []sentMsg
	subs         map[string]*fakeSub
	disconnected bool
	// tokenToDeliver is the token body placed on the subscription channel
	// when a SEND carrying a "reply-to" header is received.
	tokenToDeliver string
	// deliverToken gates auto-delivery in Send; false simulates a
	// slow/unresponsive broker that never routes a reply.
	deliverToken bool
}

func newFakeConn(token string) *fakeConn {
	return &fakeConn{
		subs:           make(map[string]*fakeSub),
		tokenToDeliver: token,
		deliverToken:   token != "",
	}
}

type fakeSub struct {
	ch chan transport.Msg
}

func (s *fakeSub) C() <-chan transport.Msg { return s.ch }
func (s *fakeSub) Unsubscribe() error      { return nil }

func (c *fakeConn) Send(_ context.Context, dest, _ string, body []byte, headers map[string]string) error {
	c.sends = append(c.sends, sentMsg{
		destination: dest,
		body:        append([]byte(nil), body...),
		headers:     copyMap(headers),
	})
	// Simulate GOSS server routing: deliver the token only when deliverToken is
	// true; a false value models a slow/unresponsive broker that never replies.
	if c.deliverToken {
		if replyTo, ok := headers["reply-to"]; ok {
			queueDest := "/queue/" + replyTo
			if sub, found := c.subs[queueDest]; found {
				sub.ch <- transport.Msg{Body: []byte(c.tokenToDeliver)}
			}
		}
	}
	return nil
}

func (c *fakeConn) Subscribe(_ context.Context, dest string) (transport.Subscription, error) {
	s := &fakeSub{ch: make(chan transport.Msg, 1)}
	c.subs[dest] = s
	return s, nil
}

func (c *fakeConn) Disconnect() error {
	c.disconnected = true
	return nil
}

// fakeDialer returns transport.Conns in order. Each call to Dial pops the next
// conn and records the ConnConfig used.
type fakeDialer struct {
	conns []transport.Conn
	calls []transport.ConnConfig
	idx   int
}

func newFakeDialer(conns ...transport.Conn) *fakeDialer {
	return &fakeDialer{conns: conns}
}

func (d *fakeDialer) Dial(_ context.Context, _ io.ReadWriteCloser, cfg transport.ConnConfig) (transport.Conn, error) {
	d.calls = append(d.calls, cfg)
	c := d.conns[d.idx]
	d.idx++
	return c, nil
}

// nopRWC is a no-op io.ReadWriteCloser; the fakeDialer ignores the rwc entirely.
type nopRWC struct{}

func (nopRWC) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (nopRWC) Write(p []byte) (int, error) { return len(p), nil }
func (nopRWC) Close() error                { return nil }

// netDialStub returns a nopRWC; the fakeDialer ignores the rwc.
func netDialStub(_ context.Context) (io.ReadWriteCloser, error) {
	return nopRWC{}, nil
}

// copyMap duplicates a string map so stored sentMsg is not mutated later.
func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// --- tests ---

func TestExchange_WireLevelInvariants(t *testing.T) {
	t.Parallel()

	const (
		user      = "testuser"
		pass      = "testpass"
		heartBeat = 10 * time.Second
	)

	conn1 := newFakeConn(fakeToken)
	conn2 := newFakeConn("")
	dialer := newFakeDialer(conn1, conn2)

	ctx := context.Background()
	durableConn, err := auth.Exchange(ctx, netDialStub, dialer, user, pass, heartBeat, nil)
	if err != nil {
		t.Fatalf("Exchange returned error: %v", err)
	}
	if durableConn == nil {
		t.Fatal("Exchange returned nil conn")
	}

	// --- invariant 1: two Dial calls total ---
	if got := len(dialer.calls); got != 2 {
		t.Errorf("expected 2 Dial calls, got %d", got)
	}

	// --- invariant 2: first Dial uses real credentials ---
	if got := dialer.calls[0].Login; got != user {
		t.Errorf("first Dial login: want %q, got %q", user, got)
	}
	if got := dialer.calls[0].Passcode; got != pass {
		t.Errorf("first Dial passcode: want %q, got %q", pass, got)
	}

	// --- invariant 3: heartbeat set on BOTH connections ---
	if got := dialer.calls[0].HeartBeat; got != heartBeat {
		t.Errorf("first Dial heartbeat: want %v, got %v", heartBeat, got)
	}
	if got := dialer.calls[1].HeartBeat; got != heartBeat {
		t.Errorf("second Dial heartbeat: want %v, got %v", heartBeat, got)
	}

	// --- invariant 4: second Dial uses token as login, empty passcode ---
	if got := dialer.calls[1].Login; got != fakeToken {
		t.Errorf("second Dial login: want %q, got %q", fakeToken, got)
	}
	if got := dialer.calls[1].Passcode; got != "" {
		t.Errorf("second Dial passcode: want empty, got %q", got)
	}

	// --- invariant 5: exactly one SEND on the credential connection ---
	if got := len(conn1.sends); got != 1 {
		t.Fatalf("expected 1 SEND on conn1, got %d", got)
	}
	send := conn1.sends[0]

	// --- invariant 6: SEND destination is the GOSS token topic ---
	if send.destination != auth.TokenTopic {
		t.Errorf("SEND destination: want %q, got %q", auth.TokenTopic, send.destination)
	}

	// --- invariant 7: SEND body is base64(user:password), exactly ---
	wantPayload := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	if got := string(send.body); got != wantPayload {
		t.Errorf("SEND body: want base64(%q), got %q", user+":"+pass, got)
	}
	// Round-trip decode to verify the colon-separated user:pass blob
	decoded, err := base64.StdEncoding.DecodeString(string(send.body))
	if err != nil {
		t.Fatalf("SEND body is not valid base64: %v", err)
	}
	if got := string(decoded); got != user+":"+pass {
		t.Errorf("decoded SEND body: want %q, got %q", user+":"+pass, got)
	}

	// --- invariant 8: reply-to header present on SEND, not empty ---
	replyTo, hasReplyTo := send.headers["reply-to"]
	if !hasReplyTo || replyTo == "" {
		t.Error("SEND frame missing reply-to header")
	}

	// --- invariant 9: reply-to value has expected prefix, no /queue/ prefix ---
	const wantPrefix = "temp.token_resp."
	if !strings.HasPrefix(replyTo, wantPrefix) {
		t.Errorf("reply-to %q does not start with %q", replyTo, wantPrefix)
	}
	if strings.HasPrefix(replyTo, "/queue/") {
		t.Errorf("reply-to %q must NOT start with /queue/ (prefix belongs on Subscribe, not reply-to)", replyTo)
	}
	if !strings.Contains(replyTo, user) {
		t.Errorf("reply-to %q should contain user %q", replyTo, user)
	}

	// --- invariant 10: subscribe destination is /queue/ + reply-to value ---
	wantSubDest := "/queue/" + replyTo
	if _, subbed := conn1.subs[wantSubDest]; !subbed {
		t.Errorf("expected Subscribe to %q, got subscriptions: %v", wantSubDest, subKeys(conn1.subs))
	}

	// --- invariant 11: reply-to is NOT set on any Subscribe call ---
	// go-stomp intercepts reply-to on SUBSCRIBE (conn.go:407) for the RabbitMQ
	// temp-queue path, which GOSS does not use. Verify none of the subscriptions
	// have reply-to in their destination path as a proxy (the fake records by dest).
	for dest := range conn1.subs {
		if strings.Contains(dest, "reply-to") {
			t.Errorf("subscription destination %q looks like reply-to leaked into Subscribe", dest)
		}
	}

	// --- invariant 12: credential connection is disconnected ---
	if !conn1.disconnected {
		t.Error("credential connection was not disconnected before returning durable conn")
	}

	// --- invariant 13: durable connection is NOT disconnected ---
	if conn2.disconnected {
		t.Error("durable connection was disconnected prematurely")
	}
}

// TestExchange_CtxCancelledBeforeToken verifies that context cancellation
// surfaces as an error rather than blocking indefinitely.
func TestExchange_CtxCancelledBeforeToken(t *testing.T) {
	t.Parallel()

	// conn1 never delivers a token: deliverToken is false when the token string is empty.
	conn1 := newFakeConn("")
	conn2 := newFakeConn("")
	dialer := newFakeDialer(conn1, conn2)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately; Exchange should not block.
	cancel()

	_, err := auth.Exchange(ctx, netDialStub, dialer, "u", "p", 10*time.Second, nil)
	if err == nil {
		t.Fatal("expected error when context is cancelled, got nil")
	}
	if !strings.Contains(err.Error(), "cancelled") && !strings.Contains(err.Error(), "context") {
		t.Errorf("error should mention cancellation, got: %v", err)
	}
}

// TestExchange_EmptyToken verifies that an empty token from the broker is rejected.
func TestExchange_EmptyToken(t *testing.T) {
	t.Parallel()

	// emptyConn delivers a zero-byte body to simulate a broker that replies with
	// an empty token. newFakeConn("") would suppress delivery entirely; this
	// variant sends the empty body explicitly.
	emptyConn := &emptyTokenConn{fakeConn: newFakeConn("")}
	conn2 := newFakeConn("")
	dialer := newFakeDialer(emptyConn, conn2)

	_, err := auth.Exchange(context.Background(), netDialStub, dialer, "u", "p", 10*time.Second, nil)
	if err == nil {
		t.Fatal("expected error for empty token, got nil")
	}
	if !strings.Contains(err.Error(), "empty token") {
		t.Errorf("error should mention empty token, got: %v", err)
	}
}

// emptyTokenConn is a fakeConn variant that delivers an empty body on token request.
type emptyTokenConn struct {
	*fakeConn
}

func (c *emptyTokenConn) Send(ctx context.Context, dest, ct string, body []byte, headers map[string]string) error {
	c.fakeConn.sends = append(c.fakeConn.sends, sentMsg{
		destination: dest,
		body:        append([]byte(nil), body...),
		headers:     copyMap(headers),
	})
	// Deliver empty body when reply-to is present.
	if replyTo, ok := headers["reply-to"]; ok {
		queueDest := "/queue/" + replyTo
		if sub, found := c.fakeConn.subs[queueDest]; found {
			sub.ch <- transport.Msg{Body: []byte("")} // empty token
		}
	}
	return nil
}

// subKeys returns subscription destination keys for error messages.
func subKeys(m map[string]*fakeSub) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// --- Additional error-path tests ---

// closedSubConn is a fakeConn variant whose Subscribe returns a pre-closed
// channel. The closed channel triggers the fetchToken !ok exit path, which
// surfaces as an error rather than blocking indefinitely.
type closedSubConn struct {
	*fakeConn
}

func (c *closedSubConn) Subscribe(_ context.Context, dest string) (transport.Subscription, error) {
	ch := make(chan transport.Msg)
	close(ch) // closed before any message is delivered
	sub := &fakeSub{ch: ch}
	c.fakeConn.subs[dest] = sub
	return sub, nil
}

// TestExchange_SubscriptionClosedBeforeToken verifies that Exchange returns an error
// when the subscription channel is closed before a token is delivered.
func TestExchange_SubscriptionClosedBeforeToken(t *testing.T) {
	t.Parallel()

	conn1 := &closedSubConn{fakeConn: newFakeConn("")}
	conn2 := newFakeConn("")
	dialer := newFakeDialer(conn1, conn2)

	_, err := auth.Exchange(context.Background(), netDialStub, dialer, "u", "p", 10*time.Second, nil)
	if err == nil {
		t.Fatal("expected error when subscription channel closes before token, got nil")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("error should mention subscription closed, got: %v", err)
	}
}

// errSubConn is a fakeConn variant whose Send delivers a transport.Msg with a
// non-nil Err instead of a token body. This triggers the msg.Err != nil exit path.
type errSubConn struct {
	*fakeConn
	subErr error
}

func (c *errSubConn) Send(_ context.Context, dest, _ string, body []byte, headers map[string]string) error {
	c.fakeConn.sends = append(c.fakeConn.sends, sentMsg{
		destination: dest,
		body:        append([]byte(nil), body...),
		headers:     copyMap(headers),
	})
	// Deliver an error message on the reply-to subscription instead of a token.
	if replyTo, ok := headers["reply-to"]; ok {
		queueDest := "/queue/" + replyTo
		if sub, found := c.fakeConn.subs[queueDest]; found {
			sub.ch <- transport.Msg{Err: c.subErr}
		}
	}
	return nil
}

// TestExchange_TokenSubscriptionError verifies that Exchange returns an error
// when the broker delivers an error message on the token subscription.
func TestExchange_TokenSubscriptionError(t *testing.T) {
	t.Parallel()

	brokerErr := errors.New("broker error on token subscription")
	conn1 := &errSubConn{fakeConn: newFakeConn(""), subErr: brokerErr}
	conn2 := newFakeConn("")
	dialer := newFakeDialer(conn1, conn2)

	_, err := auth.Exchange(context.Background(), netDialStub, dialer, "u", "p", 10*time.Second, nil)
	if err == nil {
		t.Fatal("expected error when broker delivers subscription error, got nil")
	}
	if !strings.Contains(err.Error(), brokerErr.Error()) {
		t.Errorf("error should wrap broker error: want %q in %q", brokerErr.Error(), err.Error())
	}
}

// TestExchange_HeartBeatSettingsReachBothLegs asserts that the heart-beat
// configuration is applied to the credential leg AND the durable leg (GAG-013).
// The credential leg is short-lived, but a read deadline armed on it can still
// abort the token exchange, so both legs must carry the caller's intent.
func TestExchange_HeartBeatSettingsReachBothLegs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		heartBeat  time.Duration
		heartBeats *transport.HeartBeatIntervals
	}{
		{
			// v0.1.0 callers: the symmetric field must still reach both legs.
			name:      "symmetric interval",
			heartBeat: 10 * time.Second,
		},
		{
			name:       "send-only intervals",
			heartBeat:  10 * time.Second,
			heartBeats: &transport.HeartBeatIntervals{Send: 10 * time.Second},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dialer := newFakeDialer(newFakeConn(fakeToken), newFakeConn(""))
			if _, err := auth.Exchange(context.Background(), netDialStub, dialer,
				"u", "p", tc.heartBeat, tc.heartBeats); err != nil {
				t.Fatalf("Exchange: %v", err)
			}
			if got := len(dialer.calls); got != 2 {
				t.Fatalf("expected 2 Dial calls, got %d", got)
			}
			for i, cfg := range dialer.calls {
				leg := []string{"credential", "durable"}[i]
				if cfg.HeartBeat != tc.heartBeat {
					t.Errorf("%s leg HeartBeat: got %v, want %v", leg, cfg.HeartBeat, tc.heartBeat)
				}
				if cfg.HeartBeats != tc.heartBeats {
					t.Errorf("%s leg HeartBeats: got %+v, want %+v", leg, cfg.HeartBeats, tc.heartBeats)
				}
			}
		})
	}
}
