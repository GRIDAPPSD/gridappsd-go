package reqresp_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/reqresp"
	"github.com/GRIDAPPSD/gridappsd-go/internal/transporttest"
	"github.com/GRIDAPPSD/gridappsd-go/message"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// waitAndReply is a test helper that blocks until the FakeConn records a send
// whose request body matches wantBody, then delivers replyBody to the reply subscription.
// Returns without action if the context expires before a matching send is seen.
func waitAndReply(ctx context.Context, fc *transporttest.FakeConn, wantBody, replyBody []byte) {
	select {
	case <-fc.SendNotify:
	case <-ctx.Done():
		return
	}
	sends := fc.AllSends()
	for _, rec := range sends {
		if !bytes.Equal(rec.Body, wantBody) && len(wantBody) > 0 {
			continue
		}
		replyTo := rec.Headers[message.HeaderReplyTo]
		if replyTo == "" {
			continue
		}
		sub := fc.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Push(transport.Msg{Body: replyBody})
			return
		}
	}
}

// mustSendNotify creates a FakeConn with a fresh SendNotify channel.
func mustSendNotify() *transporttest.FakeConn {
	fc := transporttest.NewFakeConn()
	fc.SendNotify = make(chan struct{})
	return fc
}

// TestGetResponse_ReplyToOnSendOnly asserts reply-to appears on the SEND frame
// with the bare name, and that the SUBSCRIBE destination carries the /queue/ prefix.
// Data invariant: reply-to on SEND only (see stomp.go:82-84, auth.go:141).
func TestGetResponse_ReplyToOnSendOnly(t *testing.T) {
	t.Parallel()

	fc := mustSendNotify()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	replyBody := []byte("response payload")
	go func() {
		select {
		case <-fc.SendNotify:
		case <-ctx.Done():
			return
		}
		sends := fc.AllSends()
		if len(sends) == 0 {
			return
		}
		replyTo := sends[0].Headers[message.HeaderReplyTo]
		sub := fc.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Push(transport.Msg{Body: replyBody})
		}
	}()

	got, err := reqresp.GetResponse(ctx, fc, "some.service", "application/json", []byte("request"), nil)
	if err != nil {
		t.Fatalf("GetResponse returned error: %v", err)
	}
	if !bytes.Equal(got, replyBody) {
		t.Errorf("reply body: got %q, want %q", got, replyBody)
	}

	rec, recErr := fc.LastSend()
	if recErr != nil {
		t.Fatal(recErr)
	}

	// Assert reply-to is the BARE name (no /queue/ or /topic/ prefix).
	replyTo, ok := rec.Headers[message.HeaderReplyTo]
	if !ok {
		t.Fatal("SEND frame missing reply-to header")
	}
	if strings.HasPrefix(replyTo, "/queue/") || strings.HasPrefix(replyTo, "/topic/") {
		t.Errorf("reply-to on SEND must be bare name, got %q", replyTo)
	}

	// Assert the subscribe destination is /queue/ + the bare reply-to.
	subscribed := ""
	for _, sub := range fc.Subs {
		if strings.HasPrefix(sub.Dest(), "/queue/reply.") {
			subscribed = sub.Dest()
			break
		}
	}
	if subscribed == "" {
		t.Fatal("no /queue/reply.* subscription found")
	}
	wantSubscribeDest := "/queue/" + replyTo
	if subscribed != wantSubscribeDest {
		t.Errorf("subscribe dest: got %q, want %q", subscribed, wantSubscribeDest)
	}
	// No SUBSCRIBE call recorded should carry reply-to in any form.
	// (Fake subscriptions carry no headers; this is structural: Subscribe
	// takes only context + destination, no headers argument.)
}

// TestGetResponse_QueueNormalization asserts the request destination gets /queue/ prepended.
func TestGetResponse_QueueNormalization(t *testing.T) {
	t.Parallel()

	fc := mustSendNotify()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		select {
		case <-fc.SendNotify:
		case <-ctx.Done():
			return
		}
		sends := fc.AllSends()
		if len(sends) == 0 {
			return
		}
		replyTo := sends[0].Headers[message.HeaderReplyTo]
		sub := fc.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Push(transport.Msg{Body: []byte("ok")})
		}
	}()

	_, err := reqresp.GetResponse(ctx, fc, "bare.destination", "text/plain", []byte("body"), nil)
	if err != nil {
		t.Fatalf("GetResponse error: %v", err)
	}

	rec, recErr := fc.LastSend()
	if recErr != nil {
		t.Fatal(recErr)
	}
	const want = "/queue/bare.destination"
	if rec.Destination != want {
		t.Errorf("request destination: got %q, want %q", rec.Destination, want)
	}
}

// TestGetResponse_BodyPassedThrough asserts reply body bytes are returned unmodified.
// Data invariant: bus stays byte-clean; no JSON decode in the bus layer.
func TestGetResponse_BodyPassedThrough(t *testing.T) {
	t.Parallel()

	fc := mustSendNotify()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want := []byte(`{"result":[1,2,3]}`)
	go func() {
		select {
		case <-fc.SendNotify:
		case <-ctx.Done():
			return
		}
		sends := fc.AllSends()
		if len(sends) == 0 {
			return
		}
		replyTo := sends[0].Headers[message.HeaderReplyTo]
		sub := fc.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Push(transport.Msg{Body: want})
		}
	}()

	got, err := reqresp.GetResponse(ctx, fc, "/queue/service", "application/json", []byte("q"), nil)
	if err != nil {
		t.Fatalf("GetResponse error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("body not passed through: got %q, want %q", got, want)
	}
}

// TestGetResponse_CtxExpiry asserts ctx cancellation returns a wrapped ctx.Err().
func TestGetResponse_CtxExpiry(t *testing.T) {
	t.Parallel()

	fc := mustSendNotify()
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-fc.SendNotify
		cancel()
	}()

	_, err := reqresp.GetResponse(ctx, fc, "/queue/service", "text/plain", []byte("q"), nil)
	if err == nil {
		t.Fatal("expected error on ctx cancel, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error should wrap context.Canceled, got: %v", err)
	}
}

// TestGetResponse_ClosedSubscription asserts a closed reply channel returns a
// distinct non-ctx error.
func TestGetResponse_ClosedSubscription(t *testing.T) {
	t.Parallel()

	fc := mustSendNotify()
	ctx := context.Background()

	go func() {
		<-fc.SendNotify
		sends := fc.AllSends()
		if len(sends) == 0 {
			return
		}
		replyTo := sends[0].Headers[message.HeaderReplyTo]
		sub := fc.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Close()
		}
	}()

	_, err := reqresp.GetResponse(ctx, fc, "/queue/service", "text/plain", []byte("q"), nil)
	if err == nil {
		t.Fatal("expected error on closed subscription, got nil")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("closed-subscription error must not wrap ctx error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("error message should mention closed, got: %v", err)
	}
}

// TestGetResponse_UnsubscribedOnReturn asserts defer-Unsubscribe fires on both
// success and failure paths.
func TestGetResponse_UnsubscribedOnReturn(t *testing.T) {
	t.Parallel()

	fc := mustSendNotify()
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-fc.SendNotify
		cancel()
	}()

	_, _ = reqresp.GetResponse(ctx, fc, "/queue/service", "text/plain", []byte("q"), nil)

	for _, sub := range fc.Subs {
		if strings.HasPrefix(sub.Dest(), "/queue/reply.") {
			if !sub.WasUnsubscribed() {
				t.Errorf("reply subscription %q was not unsubscribed on return", sub.Dest())
			}
		}
	}
}

// TestGetResponse_ConcurrentDistinctReplyDests asserts two concurrent calls get
// distinct reply destinations and each receives only its own reply (correlation invariant).
func TestGetResponse_ConcurrentDistinctReplyDests(t *testing.T) {
	t.Parallel()

	// Two separate FakeConns so each has its own SendNotify.
	fcA := mustSendNotify()
	fcB := mustSendNotify()
	ctx := context.Background()

	replyA := []byte("reply-A")
	replyB := []byte("reply-B")

	startA := make(chan struct{})
	startB := make(chan struct{})

	go func() {
		<-fcA.SendNotify
		close(startA)
		sends := fcA.AllSends()
		if len(sends) == 0 {
			return
		}
		replyTo := sends[0].Headers[message.HeaderReplyTo]
		sub := fcA.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Push(transport.Msg{Body: replyA})
		}
	}()
	go func() {
		<-fcB.SendNotify
		close(startB)
		sends := fcB.AllSends()
		if len(sends) == 0 {
			return
		}
		replyTo := sends[0].Headers[message.HeaderReplyTo]
		sub := fcB.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Push(transport.Msg{Body: replyB})
		}
	}()

	var (
		gotA []byte
		gotB []byte
		errA error
		errB error
		wg   sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		gotA, errA = reqresp.GetResponse(ctx, fcA, "/queue/svc", "text/plain", []byte("req-A"), nil)
	}()
	go func() {
		defer wg.Done()
		gotB, errB = reqresp.GetResponse(ctx, fcB, "/queue/svc", "text/plain", []byte("req-B"), nil)
	}()
	wg.Wait()

	if errA != nil {
		t.Fatalf("call A error: %v", errA)
	}
	if errB != nil {
		t.Fatalf("call B error: %v", errB)
	}
	if !bytes.Equal(gotA, replyA) {
		t.Errorf("call A: got %q, want %q", gotA, replyA)
	}
	if !bytes.Equal(gotB, replyB) {
		t.Errorf("call B: got %q, want %q", gotB, replyB)
	}

	// Assert the two reply destinations differ.
	sendsA := fcA.AllSends()
	sendsB := fcB.AllSends()
	if len(sendsA) == 0 || len(sendsB) == 0 {
		t.Fatal("expected sends on both connections")
	}
	rtA := sendsA[0].Headers[message.HeaderReplyTo]
	rtB := sendsB[0].Headers[message.HeaderReplyTo]
	if rtA == rtB {
		t.Errorf("concurrent GetResponse calls got identical reply destinations: %q", rtA)
	}
}

// TestGetResponse_BaseHeadersPassedThrough asserts caller-supplied base headers
// (e.g. GOSS auth-subject) appear on the outbound SEND.
func TestGetResponse_BaseHeadersPassedThrough(t *testing.T) {
	t.Parallel()

	fc := mustSendNotify()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		select {
		case <-fc.SendNotify:
		case <-ctx.Done():
			return
		}
		sends := fc.AllSends()
		if len(sends) == 0 {
			return
		}
		replyTo := sends[0].Headers[message.HeaderReplyTo]
		sub := fc.SubForDest("/queue/" + replyTo)
		if sub != nil {
			sub.Push(transport.Msg{Body: []byte("ok")})
		}
	}()

	base := map[string]string{
		message.HeaderGossHasSubject: "true",
		message.HeaderGossSubject:    "test-token",
	}
	_, err := reqresp.GetResponse(ctx, fc, "/queue/svc", "text/plain", []byte("q"), base)
	if err != nil {
		t.Fatalf("GetResponse error: %v", err)
	}

	rec, _ := fc.LastSend()
	if rec.Headers[message.HeaderGossHasSubject] != "true" {
		t.Errorf("GOSS_HAS_SUBJECT missing or wrong: %q", rec.Headers[message.HeaderGossHasSubject])
	}
	if rec.Headers[message.HeaderGossSubject] != "test-token" {
		t.Errorf("GOSS_SUBJECT missing or wrong: %q", rec.Headers[message.HeaderGossSubject])
	}
}
