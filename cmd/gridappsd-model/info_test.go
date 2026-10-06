package main

import (
	"strings"
	"testing"
)

const infoReply = `{"data":{"models":[` +
	`{"modelName":"test-feeder-b","modelId":"BBBB0000-0000-4000-8000-00000000000B"},` +
	`{"modelName":"Test-Feeder-A","modelId":"AAAA0000-0000-4000-8000-00000000000A"}]}}`

func TestInfoPrintsNameAndMRIDSortedByName(t *testing.T) {
	t.Parallel()
	code, stdout, stderr := runCmd(t, newHarness(infoReply), "info")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	want := "Test-Feeder-A\tAAAA0000-0000-4000-8000-00000000000A\n" +
		"test-feeder-b\tBBBB0000-0000-4000-8000-00000000000B\n"
	if stdout != want {
		t.Errorf("stdout = %q, want %q", stdout, want)
	}
}

func TestInfoFailurePrintsNothingAndFails(t *testing.T) {
	t.Parallel()
	for name, reply := range map[string]string{
		"error reply": `{"error":"boom"}`,
		"empty list":  `{"data":{"models":[]}}`,
		"not JSON":    `nope`,
	} {
		code, stdout, _ := runCmd(t, newHarness(reply), "info")
		if code == 0 || stdout != "" {
			t.Errorf("%s: exit %d, stdout %q; want non-zero and empty stdout", name, code, stdout)
		}
	}
}

func TestInfoRefusesCredentialFlagAndExtraArguments(t *testing.T) {
	t.Parallel()
	code, stdout, stderr := runCmd(t, newHarness(infoReply), "info", "--password="+testPassword)
	if code != 2 || strings.Contains(stdout+stderr, testPassword) {
		t.Errorf("--password: exit %d, output %q", code, stdout+stderr)
	}
	if code, _, _ := runCmd(t, newHarness(infoReply), "info", "extra"); code != 2 {
		t.Errorf("extra argument: exit %d, want 2", code)
	}
}

func TestInfoPasswordNeverReachesOutput(t *testing.T) {
	t.Parallel()
	code, stdout, stderr := runCmd(t, newHarness(`{"error":"denied `+testPassword+`"}`), "info")
	if code == 0 || strings.Contains(stdout+stderr, testPassword) {
		t.Errorf("exit %d, output %q", code, stdout+stderr)
	}
}
