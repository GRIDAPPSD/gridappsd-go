package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
	"github.com/GRIDAPPSD/gridappsd-go/internal/transporttest"
	"github.com/GRIDAPPSD/gridappsd-go/query"
)

const testPassword = "test-password-9f3a"

// fixedNow is 03:04:05 UTC, expressed in another zone so a missing UTC
// conversion shows.
var fixedNow = time.Date(2026, 1, 2, 8, 4, 5, 0, time.FixedZone("test", 5*3600))

type harness struct {
	deps deps
	cfg  *gridappsd.Config
}

func newHarness(reply string) *harness {
	h := &harness{cfg: &gridappsd.Config{}}
	h.deps = deps{
		getenv: func(k string) string {
			return map[string]string{
				"GRIDAPPSD_USER":     "test-user",
				"GRIDAPPSD_PASSWORD": testPassword,
				"ALT_USER":           "alt-user",
			}[k]
		},
		dial: func(_ context.Context, cfg gridappsd.Config) (query.Requester, func(), error) {
			*h.cfg = cfg
			return transporttest.NewReplyBus([]byte(reply)), func() {}, nil
		},
		now:   func() time.Time { return fixedNow },
		stdin: strings.NewReader(""),
	}
	return h
}

func runCmd(t *testing.T, h *harness, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var o, e bytes.Buffer
	code = run(args, &o, &e, h.deps)
	return code, o.String(), e.String()
}

const goodReply = `{"data":{"modelNames":["Test-Feeder-B","test-feeder-a","TEST-FEEDER-B"]}}`

func TestNamesWritesSortedLowerCaseUniqueList(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "list.txt")
	code, _, stderr := runCmd(t, newHarness(goodReply), "names", "--address", "broker.test:61613", "--out", out)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	want := "# feeder-list v1\n" +
		"# source: endpoint: broker.test:61613; request: QUERY_MODEL_NAMES; queried at: 2026-01-02T03:04:05Z\n" +
		"test-feeder-a\ntest-feeder-b\n"
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("list =\n%q\nwant\n%q", got, want)
	}
}

func TestNamesKeepsNamesAbsentFromTheReply(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "list.txt")
	old := "# feeder-list v1\n# source: earlier\ntest-feeder-gone\ntest-feeder-a\n"
	if err := os.WriteFile(out, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runCmd(t, newHarness(goodReply), "names", "--out", out)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	got, _ := os.ReadFile(out)
	if !strings.HasSuffix(string(got), "\ntest-feeder-a\ntest-feeder-b\ntest-feeder-gone\n") {
		t.Errorf("list = %q, want the union with test-feeder-gone kept", got)
	}
	if strings.Contains(string(got), "# source: earlier") {
		t.Errorf("old source line survived: %q", got)
	}
}

func TestNamesFailureWritesNothing(t *testing.T) {
	t.Parallel()
	existing := "# feeder-list v1\n# source: earlier\ntest-feeder-a\n"
	for name, tc := range map[string]struct{ reply, existing string }{
		"error reply":        {`{"error":"boom"}`, existing},
		"not JSON":           {`nope`, existing},
		"no data":            {`{}`, existing},
		"empty list":         {`{"data":{"modelNames":[]}}`, existing},
		"existing no header": {goodReply, "test-feeder-a\n"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			out := filepath.Join(dir, "list.txt")
			if err := os.WriteFile(out, []byte(tc.existing), 0o600); err != nil {
				t.Fatal(err)
			}
			code, stdout, _ := runCmd(t, newHarness(tc.reply), "names", "--out", out)
			if code == 0 {
				t.Fatalf("exit 0, want non-zero; stdout %q", stdout)
			}
			if got, _ := os.ReadFile(out); string(got) != tc.existing {
				t.Errorf("existing file changed to %q", got)
			}
			assertNoStrays(t, dir, "list.txt")
		})
	}
}

func TestNamesFailureCreatesNoFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	code, _, _ := runCmd(t, newHarness(`{"error":"boom"}`), "names", "--out", filepath.Join(dir, "list.txt"))
	if code == 0 {
		t.Fatal("exit 0, want non-zero")
	}
	assertNoStrays(t, dir)
}

func assertNoStrays(t *testing.T, dir string, allowed ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		ok := false
		for _, a := range allowed {
			ok = ok || e.Name() == a
		}
		if !ok {
			t.Errorf("unexpected file %q left in %s", e.Name(), dir)
		}
	}
}

func TestNamesReadsCredentialsFromNamedEnvironment(t *testing.T) {
	t.Parallel()
	h := newHarness(goodReply)
	out := filepath.Join(t.TempDir(), "list.txt")
	if code, _, stderr := runCmd(t, h, "names", "--out", out); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if h.cfg.User != "test-user" || h.cfg.Password != testPassword {
		t.Errorf("default env: user %q, password matches %v", h.cfg.User, h.cfg.Password == testPassword)
	}
	if h.cfg.AllowPlaintext {
		t.Error("plaintext enabled without --allow-plaintext")
	}
	h = newHarness(goodReply)
	if code, _, stderr := runCmd(t, h, "names", "--user-env", "ALT_USER", "--allow-plaintext", "--out", out); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if h.cfg.User != "alt-user" || !h.cfg.AllowPlaintext {
		t.Errorf("--user-env/--allow-plaintext not applied: user %q plaintext %v", h.cfg.User, h.cfg.AllowPlaintext)
	}
}

func TestNoCredentialFlagIsAccepted(t *testing.T) {
	t.Parallel()
	for _, flagName := range []string{"--password", "--passcode", "--user", "--token"} {
		out := filepath.Join(t.TempDir(), "list.txt")
		code, stdout, stderr := runCmd(t, newHarness(goodReply), "names", flagName+"="+testPassword, "--out", out)
		if code != 2 {
			t.Errorf("%s: exit %d, want 2", flagName, code)
		}
		if strings.Contains(stdout+stderr, testPassword) {
			t.Errorf("%s: output echoes the secret: %q", flagName, stdout+stderr)
		}
		if _, err := os.Stat(out); err == nil {
			t.Errorf("%s: wrote a file", flagName)
		}
	}
}

func TestPasswordNeverReachesOutput(t *testing.T) {
	t.Parallel()
	leaky := newHarness(`{"error":"login failed for ` + testPassword + `"}`)
	dialFails := newHarness(goodReply)
	dialFails.deps.dial = func(context.Context, gridappsd.Config) (query.Requester, func(), error) {
		return nil, nil, errors.New("auth rejected, passcode " + testPassword)
	}
	for name, h := range map[string]*harness{"error member": leaky, "dial error": dialFails} {
		out := filepath.Join(t.TempDir(), "list.txt")
		code, stdout, stderr := runCmd(t, h, "names", "--out", out)
		if code == 0 {
			t.Errorf("%s: exit 0", name)
		}
		if all := stdout + stderr; strings.Contains(all, testPassword) {
			t.Errorf("%s: output holds the password: %q", name, all)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	t.Parallel()
	for name, args := range map[string][]string{
		"no command":      nil,
		"unknown command": {"bogus"},
		"names no --out":  {"names"},
		"names extra arg": {"names", "--out", "x", "extra"},
	} {
		if code, _, _ := runCmd(t, newHarness(goodReply), args...); code != 2 {
			t.Errorf("%s: exit %d, want 2", name, code)
		}
	}
}

func TestNamesUnwritableOutFails(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "absent-dir", "list.txt")
	code, stdout, _ := runCmd(t, newHarness(goodReply), "names", "--out", out)
	if code == 0 || stdout != "" {
		t.Errorf("exit %d, stdout %q; want non-zero and empty stdout", code, stdout)
	}
}
