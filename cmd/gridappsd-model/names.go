package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/query"
)

const listHeader = "# feeder-list v1"

func runNames(args []string, stdout, stderr io.Writer, d deps) int {
	fs := newFlagSet("names", stderr)
	var cf connFlags
	cf.register(fs)
	out := fs.String("out", "", "file to write the model name list to (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" || fs.NArg() != 0 {
		fmt.Fprintln(stderr, "gridappsd-model names: --out is required and no arguments are accepted")
		return 2
	}

	ctx := context.Background()
	s, err := connect(ctx, &cf, d)
	if err != nil {
		return fail(stderr, redactor(d.getenv(cf.passwordEnv)), err)
	}
	defer s.close()

	rctx, cancel := s.requestContext(ctx)
	defer cancel()
	names, err := query.ModelNames(rctx, s.bus)
	if err != nil {
		return fail(stderr, s.redact, err)
	}
	// Existing names are kept: a guard list that silently forgot a model the
	// platform unloaded would stop guarding it.
	existing, err := readList(*out)
	if err != nil {
		return fail(stderr, s.redact, err)
	}
	merged := normalize(append(existing, names...))
	content := renderList(s.address, d.now(), merged)
	if err := writeAtomic(*out, content); err != nil {
		return fail(stderr, s.redact, err)
	}
	fmt.Fprintf(stdout, "wrote %d model names to %s\n", len(merged), *out)
	return 0
}

func normalize(names []string) []string {
	seen := make(map[string]bool, len(names))
	var out []string
	for _, n := range names {
		n = strings.ToLower(n)
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func renderList(address string, now time.Time, names []string) []byte {
	var b bytes.Buffer
	fmt.Fprintln(&b, listHeader)
	fmt.Fprintf(&b, "# source: endpoint: %s; request: QUERY_MODEL_NAMES; queried at: %s\n",
		address, now.UTC().Format(time.RFC3339))
	for _, n := range names {
		fmt.Fprintln(&b, n)
	}
	return b.Bytes()
}

// readList returns the names in an existing list, or nil when the file does
// not exist. A file without the header is refused rather than overwritten,
// since it is not a list this command wrote.
func readList(path string) ([]string, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading existing list: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() || sc.Text() != listHeader {
		return nil, fmt.Errorf("existing file %s does not start with %q", path, listHeader)
	}
	var names []string
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		names = append(names, line)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading existing list: %w", err)
	}
	return names, nil
}

// writeAtomic writes content beside path and renames it into place, so a
// failure leaves any existing file untouched.
func writeAtomic(path string, content []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".gridappsd-model-*")
	if err != nil {
		return fmt.Errorf("creating temporary file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("writing %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return fmt.Errorf("replacing %s: %w", path, err)
	}
	return nil
}
