package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
	"github.com/GRIDAPPSD/gridappsd-go/query"
)

const (
	objectReply = "{\"responseComplete\": true,\n \"data\": {\"head\":{\"vars\":[\"a\"]},\"results\":{\"bindings\":[]}}}\n"
	stringReply = `{"data": "{\"head\":{\"vars\":[\"a\"]},\"results\":{\"bindings\":[]}}"}`
)

func TestQueryWritesReplyVerbatim(t *testing.T) {
	t.Parallel()
	for name, reply := range map[string]string{"data as object": objectReply, "data as string": stringReply} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(reply)
			h.deps.stdin = strings.NewReader("SELECT ?a WHERE { ?a ?b ?c }")
			out := filepath.Join(t.TempDir(), "reply.json")
			code, _, stderr := runCmd(t, h, "query", "--out", out)
			if code != 0 {
				t.Fatalf("exit %d: %s", code, stderr)
			}
			got, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != reply {
				t.Errorf("file = %q, want the exact reply %q", got, reply)
			}
		})
	}
}

func TestQueryReadsFileAndSendsItsText(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	qf := filepath.Join(dir, "q.rq")
	const text = "SELECT ?a WHERE { ?a ?b ?c }\n"
	if err := os.WriteFile(qf, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	h := newHarness(objectReply)
	h.deps.stdin = strings.NewReader("stdin text must not be used")
	var sent string
	dial := h.deps.dial
	h.deps.dial = func(c context.Context, cfg gridappsd.Config) (query.Requester, func(), error) {
		bus, closeFn, err := dial(c, cfg)
		return recordingBus{bus, &sent}, closeFn, err
	}
	code, _, stderr := runCmd(t, h, "query", "--query-file", qf, "--out", filepath.Join(dir, "r.json"))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !strings.Contains(sent, `"queryString":"SELECT ?a WHERE { ?a ?b ?c }\n"`) {
		t.Errorf("request body = %s, want the file text", sent)
	}
}

type recordingBus struct {
	query.Requester
	body *string
}

func (r recordingBus) GetResponse(ctx context.Context, dest, ct string, body []byte) ([]byte, error) {
	*r.body = string(body)
	return r.Requester.GetResponse(ctx, dest, ct, body)
}

func TestQueryFailureWritesNothingAndExitsNonZero(t *testing.T) {
	t.Parallel()
	const existing = "previous reply bytes\n"
	for name, tc := range map[string]struct {
		reply, stdin string
		missingFile  bool
		dialErr      bool
	}{
		"error member":      {reply: `{"error":"boom"}`, stdin: "SELECT 1"},
		"not JSON":          {reply: `nope`, stdin: "SELECT 1"},
		"no data":           {reply: `{}`, stdin: "SELECT 1"},
		"marked incomplete": {reply: `{"responseComplete":false,"data":{}}`, stdin: "SELECT 1"},
		"empty query":       {reply: objectReply, stdin: "  \n"},
		"dial fails":        {reply: objectReply, stdin: "SELECT 1", dialErr: true},
		"missing query file": {
			reply: objectReply, missingFile: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			out := filepath.Join(dir, "reply.json")
			if err := os.WriteFile(out, []byte(existing), 0o600); err != nil {
				t.Fatal(err)
			}
			h := newHarness(tc.reply)
			h.deps.stdin = strings.NewReader(tc.stdin)
			if tc.dialErr {
				h.deps.dial = func(context.Context, gridappsd.Config) (query.Requester, func(), error) {
					return nil, nil, errors.New("connection refused")
				}
			}
			args := []string{"query", "--out", out}
			if tc.missingFile {
				args = append(args, "--query-file", filepath.Join(dir, "absent.rq"))
			}
			code, stdout, _ := runCmd(t, h, args...)
			if code == 0 {
				t.Fatal("exit 0, want non-zero")
			}
			if stdout != "" {
				t.Errorf("stdout = %q on failure, want nothing", stdout)
			}
			if got, _ := os.ReadFile(out); string(got) != existing {
				t.Errorf("existing file changed to %q", got)
			}
			assertNoStrays(t, dir, "reply.json")
		})
	}
}

func TestQueryFailureCreatesNoFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	h := newHarness(`{"error":"boom"}`)
	h.deps.stdin = strings.NewReader("SELECT 1")
	if code, _, _ := runCmd(t, h, "query", "--out", filepath.Join(dir, "reply.json")); code == 0 {
		t.Fatal("exit 0, want non-zero")
	}
	assertNoStrays(t, dir)
}

func TestQueryPrintsEndpointAndTimestamp(t *testing.T) {
	t.Parallel()
	h := newHarness(objectReply)
	h.deps.stdin = strings.NewReader("SELECT 1")
	code, stdout, stderr := runCmd(t, h, "query", "--address", "broker.test:61613", "--out", filepath.Join(t.TempDir(), "r.json"))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	want := "endpoint: stomp://broker.test:61613 /queue/goss.gridappsd.process.request.data.powergridmodel\n" +
		"queried at: 2026-01-02T03:04:05Z\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
	if strings.Contains(stdout, testPassword) || strings.Contains(stdout, "test-user") {
		t.Errorf("stdout holds a credential: %q", stdout)
	}
}

func TestQueryRefusesCredentialFlag(t *testing.T) {
	t.Parallel()
	h := newHarness(objectReply)
	h.deps.stdin = strings.NewReader("SELECT 1")
	code, stdout, stderr := runCmd(t, h, "query", "--password="+testPassword, "--out", filepath.Join(t.TempDir(), "r.json"))
	if code != 2 || strings.Contains(stdout+stderr, testPassword) {
		t.Errorf("exit %d, output %q", code, stdout+stderr)
	}
}

func TestQueryPasswordNeverReachesOutput(t *testing.T) {
	t.Parallel()
	h := newHarness(`{"error":"denied ` + testPassword + `"}`)
	h.deps.stdin = strings.NewReader("SELECT 1")
	code, stdout, stderr := runCmd(t, h, "query", "--out", filepath.Join(t.TempDir(), "r.json"))
	if code == 0 || strings.Contains(stdout+stderr, testPassword) {
		t.Errorf("exit %d, output %q", code, stdout+stderr)
	}
}

func TestQueryRequiresOut(t *testing.T) {
	t.Parallel()
	if code, _, _ := runCmd(t, newHarness(objectReply), "query"); code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
}

func TestQueryUnwritableOutFails(t *testing.T) {
	t.Parallel()
	h := newHarness(objectReply)
	h.deps.stdin = strings.NewReader("SELECT 1")
	code, stdout, _ := runCmd(t, h, "query", "--out", filepath.Join(t.TempDir(), "absent-dir", "r.json"))
	if code == 0 || stdout != "" {
		t.Errorf("exit %d, stdout %q; want non-zero and empty stdout", code, stdout)
	}
}
