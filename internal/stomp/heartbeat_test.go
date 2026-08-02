package stomp

import (
	"context"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"

	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// TestDial_HeartBeatNegotiation asserts the heart-beat header this client puts
// on the wire for each way of configuring it (GAG-013).
//
// The assertion is on the negotiated CONNECT header, "send,recv" in
// milliseconds, because that is the contract with the broker. Asserting only
// that Dial returned no error would pass for every one of these cases.
//
// The v0.1.0 rows are the backward-compatibility gate: a caller that leaves
// HeartBeats nil must produce byte-identical negotiation to v0.1.0, where the
// only control was the symmetric HeartBeat field with a zero-means-default rule.
func TestDial_HeartBeatNegotiation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  transport.ConnConfig
		want string
	}{
		{
			name: "v0.1.0 symmetric interval is offered in both directions",
			cfg:  transport.ConnConfig{HeartBeat: 10 * time.Second},
			want: "10000,10000",
		},
		{
			name: "v0.1.0 zero symmetric interval falls back to the default",
			cfg:  transport.ConnConfig{},
			want: "10000,10000",
		},
		{
			name: "v0.1.0 non-default symmetric interval is honored",
			cfg:  transport.ConnConfig{HeartBeat: 250 * time.Millisecond},
			want: "250,250",
		},
		{
			name: "send-only leaves the incoming direction disabled",
			cfg: transport.ConnConfig{
				HeartBeats: &transport.HeartBeatIntervals{Send: 10 * time.Second},
			},
			want: "10000,0",
		},
		{
			name: "recv-only leaves the outgoing direction disabled",
			cfg: transport.ConnConfig{
				HeartBeats: &transport.HeartBeatIntervals{Recv: 10 * time.Second},
			},
			want: "0,10000",
		},
		{
			name: "asymmetric intervals are offered independently",
			cfg: transport.ConnConfig{
				HeartBeats: &transport.HeartBeatIntervals{Send: 10 * time.Second, Recv: 30 * time.Second},
			},
			want: "10000,30000",
		},
		{
			name: "explicit zeros are offered as zeros, not replaced by the default",
			cfg: transport.ConnConfig{
				HeartBeats: &transport.HeartBeatIntervals{},
			},
			want: "0,0",
		},
		{
			name: "HeartBeats supersedes a set HeartBeat rather than merging with it",
			cfg: transport.ConnConfig{
				HeartBeat:  30 * time.Second,
				HeartBeats: &transport.HeartBeatIntervals{Send: 10 * time.Second},
			},
			want: "10000,0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hb := make(chan string, 1)
			rwc := startFakeSTOMPServerOpts(t, fakeServerOpts{clientHeartBeat: hb})

			cfg := tc.cfg
			cfg.Login, cfg.Passcode = "u", "p"
			c, err := (&Dialer{}).Dial(context.Background(), rwc, cfg)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			t.Cleanup(func() { _ = c.Disconnect() })

			select {
			case got := <-hb:
				if got != tc.want {
					t.Errorf("CONNECT heart-beat header: got %q, want %q", got, tc.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("broker never received a CONNECT frame")
			}
		})
	}
}

// TestGoStompReadTimerArming characterizes go-stomp v3's read-timer behavior
// (GAG-013 defect 3, third-party). It changes nothing in go-stomp; it pins what
// go-stomp actually does so the boundary of our own fix is documented and a
// future dependency bump that changes it fails loudly.
//
// go-stomp computes its read timeout at conn.go:184-204 as
// max(brokerAdvertisedSend, clientRequestedRecv) and arms a timer whenever that
// is above zero. STOMP 1.2 section 3.3.1 says a zero from EITHER party disables
// that direction, so the max() is wrong in both of the ways below: it raises the
// broker's zero to the client's request, and it raises the client's zero to the
// broker's advertisement. On expiry go-stomp fans an error to every subscription
// and drops the whole connection, taking the publish path down with it.
//
// The intervals are milliseconds and HeartBeatError is overridden so the test
// runs in well under a second. gostomp.Connect is called directly rather than
// through Dialer because Dialer deliberately does not expose HeartBeatError.
func TestGoStompReadTimerArming(t *testing.T) {
	t.Parallel()

	const (
		hbError    = 20 * time.Millisecond
		armedIn    = 3 * time.Second        // generous: the real timer is ~70ms
		quietFor   = 750 * time.Millisecond // >> 70ms, so an armed timer would fire
		clientSend = 50 * time.Millisecond
	)

	tests := []struct {
		name         string
		brokerOffers string
		clientSendHB time.Duration
		clientRecvHB time.Duration
		wantArmed    bool
	}{
		{
			// The v0.1.0 shape. The broker says "no heart-beating", and
			// go-stomp arms the timer anyway off the client's own request.
			name:         "symmetric request arms the timer even when the broker answers 0,0",
			brokerOffers: "0,0",
			clientSendHB: clientSend,
			clientRecvHB: clientSend,
			wantArmed:    true,
		},
		{
			// What GAG-013 buys us against a broker that agrees to stay quiet.
			name:         "send-only request leaves the timer disarmed when the broker answers 0,0",
			brokerOffers: "0,0",
			clientSendHB: clientSend,
			clientRecvHB: 0,
			wantArmed:    false,
		},
		{
			// The limit of what GAG-013 buys us: our zero is overridden by the
			// broker's advertisement, contrary to STOMP 1.2. Necessary but not
			// sufficient; an upstream fix is required to close this case.
			name:         "send-only request still arms the timer when the broker advertises a send interval",
			brokerOffers: "50,50",
			clientSendHB: clientSend,
			clientRecvHB: 0,
			wantArmed:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rwc := startFakeSTOMPServerOpts(t, fakeServerOpts{
				connectedHeartBeat: tc.brokerOffers,
				quietOnSubscribe:   true,
			})

			c, err := gostomp.Connect(rwc,
				gostomp.ConnOpt.Login("u", "p"),
				gostomp.ConnOpt.HeartBeat(tc.clientSendHB, tc.clientRecvHB),
				gostomp.ConnOpt.HeartBeatError(hbError),
			)
			if err != nil {
				t.Fatalf("gostomp.Connect: %v", err)
			}

			// The subscription is the observation point: on read-timeout
			// go-stomp fans the error to every subscription before dropping
			// the connection.
			sub, err := c.Subscribe("/queue/hb.probe", gostomp.AckAuto)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			if tc.wantArmed {
				select {
				case msg := <-sub.C:
					if msg == nil || msg.Err == nil {
						t.Fatalf("expected a read-timeout error on the subscription, got %+v", msg)
					}
					if msg.Err.Error() != "read timeout" {
						t.Errorf("subscription error: got %q, want %q", msg.Err.Error(), "read timeout")
					}
				case <-time.After(armedIn):
					t.Fatalf("go-stomp did not arm a read timer for broker %q with client recv %v; "+
						"this characterization is stale, recheck the fix's boundary",
						tc.brokerOffers, tc.clientRecvHB)
				}
				return
			}

			select {
			case msg := <-sub.C:
				t.Fatalf("read timer fired despite a zero incoming heart-beat request: %+v", msg)
			case <-time.After(quietFor):
				// Correct: no read deadline was derived, so a silent broker
				// cannot tear the connection down.
			}
			_ = c.Disconnect()
		})
	}
}
