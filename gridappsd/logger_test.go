package gridappsd_test

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
)

const (
	loggerTestToken = "logger-test-token-7f3a"
	loggerTestPass  = "logger-test-passcode-91c2"
)

// errorOnCredentialDisconnectBroker is a plaintext STOMP broker that hands out
// loggerTestToken and answers the credential leg's DISCONNECT with an ERROR
// frame, which go-stomp returns from Disconnect. Every other receipt is
// answered.
func errorOnCredentialDisconnectBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot open a TCP listener on this host: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for idx := 0; ; idx++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(credential bool) {
				defer wg.Done()
				defer c.Close()
				serveErrorOnDisconnect(c, credential)
			}(idx == 0)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().String()
}

func serveErrorOnDisconnect(c net.Conn, credential bool) {
	r, w := frame.NewReader(c), frame.NewWriter(c)
	subIDs := map[string]string{}
	for {
		f, err := r.Read()
		if err != nil {
			return
		}
		if f == nil {
			continue
		}
		var out []*frame.Frame
		switch f.Command {
		case frame.CONNECT, frame.STOMP:
			out = append(out, frame.New(frame.CONNECTED, frame.Version, "1.2", frame.HeartBeat, "0,0"))
		case frame.SUBSCRIBE:
			subIDs[f.Header.Get(frame.Destination)] = f.Header.Get(frame.Id)
		case frame.SEND:
			if replyTo, ok := f.Header.Contains("reply-to"); ok {
				dest := "/queue/" + replyTo
				msg := frame.New(frame.MESSAGE, frame.Destination, dest, frame.Subscription, subIDs[dest], frame.MessageId, "1")
				msg.Body = []byte(loggerTestToken)
				out = append(out, msg)
			}
		}
		if receipt, ok := f.Header.Contains(frame.Receipt); ok {
			if credential && f.Command == frame.DISCONNECT {
				out = append(out, frame.New(frame.ERROR, frame.Message, "disconnect refused by test broker"))
			} else {
				out = append(out, frame.New(frame.RECEIPT, frame.ReceiptId, receipt))
			}
		}
		for _, o := range out {
			if err := w.Write(o); err != nil {
				return
			}
		}
	}
}

// capturingHandler keeps every record a caller's logger receives.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler            { return h }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

// TestConnect_TeardownFailureReachesConfigLogger proves Config.Logger is the
// logger the token exchange reports to: a credential connection whose
// Disconnect fails after the token arrived still yields a connection, and the
// failure arrives at the caller's handler as one Warn record carrying the
// cause and no credential or token.
func TestConnect_TeardownFailureReachesConfigLogger(t *testing.T) {
	addr := errorOnCredentialDisconnectBroker(t)
	h := &capturingHandler{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := gridappsd.Connect(ctx, gridappsd.Config{
		Address:        addr,
		AllowPlaintext: true,
		User:           "operator",
		Password:       loggerTestPass,
		Logger:         slog.New(h),
	})
	if err != nil {
		t.Fatalf("Connect error = %v, want nil: a teardown failure after the token arrived must not fail Connect", err)
	}
	defer func() { _ = conn.Disconnect() }()

	h.mu.Lock()
	recs := append([]slog.Record(nil), h.records...)
	h.mu.Unlock()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	rec := recs[0]
	if rec.Level != slog.LevelWarn {
		t.Errorf("record level = %v, want %v", rec.Level, slog.LevelWarn)
	}
	var text strings.Builder
	text.WriteString(rec.Message)
	var cause string
	rec.Attrs(func(a slog.Attr) bool {
		text.WriteString(" " + a.Key + "=" + a.Value.String())
		if a.Key == "error" {
			cause = a.Value.String()
		}
		return true
	})
	if !strings.Contains(cause, "disconnect refused by test broker") {
		t.Errorf("error attr = %q, want the broker's ERROR message", cause)
	}
	for _, secret := range []string{loggerTestPass, loggerTestToken} {
		if strings.Contains(text.String(), secret) {
			t.Errorf("record %q carries a secret %q", text.String(), secret)
		}
	}
}
