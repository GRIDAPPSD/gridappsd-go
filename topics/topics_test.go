package topics_test

import (
	"testing"

	"github.com/GRIDAPPSD/gridappsd-go/topics"
)

// TestNormalizeDestination verifies the queue-prepend rule mirrors goss.py:415-417.
func TestNormalizeDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "/topic/ prefix unchanged",
			input: "/topic/goss.gridappsd.field.input",
			want:  "/topic/goss.gridappsd.field.input",
		},
		{
			name:  "/queue/ prefix unchanged",
			input: "/queue/some.queue",
			want:  "/queue/some.queue",
		},
		{
			name:  "/temp-queue/ prefix unchanged",
			input: "/temp-queue/response.abc123",
			want:  "/temp-queue/response.abc123",
		},
		{
			name:  "bare name gets /queue/ prepended",
			input: "goss.gridappsd.process.request.data.powergridmodel",
			want:  "/queue/goss.gridappsd.process.request.data.powergridmodel",
		},
		{
			name:  "single word gets /queue/ prepended",
			input: "logs",
			want:  "/queue/logs",
		},
		{
			name:  "dot-separated path without prefix gets /queue/ prepended",
			input: "pnnl.goss.token.topic",
			want:  "/queue/pnnl.goss.token.topic",
		},
		{
			name:  "empty string gets /queue/ prepended",
			input: "",
			want:  "/queue/",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := topics.NormalizeDestination(tc.input)
			if got != tc.want {
				t.Errorf("NormalizeDestination(%q): got %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
