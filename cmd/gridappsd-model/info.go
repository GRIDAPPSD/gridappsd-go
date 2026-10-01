package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/GRIDAPPSD/gridappsd-go/query"
)

func runInfo(args []string, stdout, stderr io.Writer, d deps) int {
	fs := newFlagSet("info", stderr)
	var cf connFlags
	cf.register(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "gridappsd-model info: no arguments are accepted")
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
	models, err := query.ModelInfo(rctx, s.bus)
	if err != nil {
		return fail(stderr, s.redact, err)
	}
	sort.SliceStable(models, func(i, j int) bool { return models[i].Name < models[j].Name })
	// Built whole first so a failure above prints nothing to stdout.
	var b bytes.Buffer
	for _, m := range models {
		fmt.Fprintf(&b, "%s\t%s\n", m.Name, m.MRID)
	}
	if _, err := stdout.Write(b.Bytes()); err != nil {
		return fail(stderr, s.redact, err)
	}
	return 0
}
