package router_test

import (
	"context"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/router"
	"github.com/GRIDAPPSD/gridappsd-go/internal/transporttest"
	"github.com/GRIDAPPSD/gridappsd-go/message"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// ctxConn is a FakeConn whose subscriptions end when the Subscribe context is
// done, as the go-stomp transport's do.
type ctxConn struct {
	*transporttest.FakeConn
	unsubscribed chan string
}

func (c *ctxConn) Subscribe(ctx context.Context, dest string) (transport.Subscription, error) {
	sub, err := c.FakeConn.Subscribe(ctx, dest)
	if err != nil {
		return nil, err
	}
	context.AfterFunc(ctx, func() {
		_ = sub.Unsubscribe()
		c.unsubscribed <- dest
	})
	return sub, nil
}

// TestRouter_CancelledSubscribeCtxKeepsSubscription pins the documented
// contract: cancelling the context passed to Subscribe after it returns does
// not tear the subscription down (#23).
func TestRouter_CancelledSubscribeCtxKeepsSubscription(t *testing.T) {
	t.Parallel()
	fc := &ctxConn{FakeConn: transporttest.NewFakeConn(), unsubscribed: make(chan string, 1)}
	r := router.New(fc)
	t.Cleanup(r.Close)
	const dest = "/queue/ctx.contract"

	got := make(chan map[string]string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := r.Subscribe(ctx, dest, func(h map[string]string, body []byte) {
		if string(body) == "after-cancel" {
			got <- h
		}
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()

	select {
	case d := <-fc.unsubscribed:
		t.Fatalf("cancelling the Subscribe context unsubscribed %q", d)
	case <-time.After(200 * time.Millisecond):
	}

	fc.SubForDest(dest).Push(transport.Msg{Body: []byte("after-cancel")})
	select {
	case h := <-got:
		if h[message.HeaderDestination] != dest {
			t.Errorf("destination header: got %q, want %q", h[message.HeaderDestination], dest)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not receive a message sent after the context was cancelled")
	}
	if n := r.LiveDestinationCount(); n != 1 {
		t.Errorf("live destinations: got %d, want 1", n)
	}
}
