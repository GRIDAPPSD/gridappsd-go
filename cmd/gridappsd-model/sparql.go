package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/query"
	"github.com/GRIDAPPSD/gridappsd-go/topics"
)

func runQuery(args []string, stdout, stderr io.Writer, d deps) int {
	fs := newFlagSet("query", stderr)
	var cf connFlags
	cf.register(fs)
	out := fs.String("out", "", "file to write the reply to (required)")
	queryFile := fs.String("query-file", "", "file holding the SPARQL text; standard input when empty")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "gridappsd-model query: --out is required and no arguments are accepted")
		return 2
	}

	redact := redactor(d.getenv(cf.passwordEnv))
	text, err := readQuery(*queryFile, d.stdin)
	if err != nil {
		return fail(stderr, redact, err)
	}

	ctx := context.Background()
	s, err := connect(ctx, &cf, d)
	if err != nil {
		return fail(stderr, redact, err)
	}
	defer s.close()

	rctx, cancel := s.requestContext(ctx)
	defer cancel()
	reply, err := query.SPARQL(rctx, s.bus, text)
	if err != nil {
		return fail(stderr, s.redact, err)
	}
	queriedAt := d.now()
	if err := writeAtomic(*out, reply); err != nil {
		return fail(stderr, s.redact, err)
	}
	// Printed after the write so a failed run prints nothing a script could
	// mistake for provenance of a file that was never written.
	fmt.Fprintf(stdout, "endpoint: stomp://%s %s\n", s.address, topics.NormalizeDestination(topics.Blazegraph))
	fmt.Fprintf(stdout, "queried at: %s\n", queriedAt.UTC().Format(time.RFC3339))
	return 0
}

func readQuery(path string, stdin io.Reader) (string, error) {
	var b []byte
	var err error
	if path != "" {
		b, err = os.ReadFile(path)
	} else {
		b, err = io.ReadAll(stdin)
	}
	if err != nil {
		return "", fmt.Errorf("reading query: %w", err)
	}
	if strings.TrimSpace(string(b)) == "" {
		return "", fmt.Errorf("the query is empty")
	}
	return string(b), nil
}
