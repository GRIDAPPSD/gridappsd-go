package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/fieldbus"
	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
	"github.com/GRIDAPPSD/gridappsd-go/query"
)

const usage = `usage: gridappsd-model <command> [flags]

commands:
  names   write the list of model names to a file (--out)
  info    print each model's name and mRID, tab separated
  query   run a SPARQL query (--query-file or stdin) and write the reply to --out

common flags: --address, --ca-file, --allow-plaintext, --user-env,
--password-env, --timeout. Credentials are read from the environment
variables those two flags name, never from flags.
`

// deps holds what a test replaces: the environment, the broker connection
// and the clock.
type deps struct {
	getenv func(string) string
	dial   func(ctx context.Context, cfg gridappsd.Config) (query.Requester, func(), error)
	now    func() time.Time
	stdin  io.Reader
}

func dialBroker(ctx context.Context, cfg gridappsd.Config) (query.Requester, func(), error) {
	bus := fieldbus.New(cfg)
	if err := bus.Connect(ctx); err != nil {
		return nil, nil, err
	}
	return bus, func() { _ = bus.Disconnect() }, nil
}

// session is one connected command run.
type session struct {
	bus     query.Requester
	timeout time.Duration
	address string
	redact  func(string) string
	closeFn func()
}

type connFlags struct {
	address        string
	caFile         string
	allowPlaintext bool
	userEnv        string
	passwordEnv    string
	timeout        time.Duration
}

func (c *connFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.address, "address", gridappsd.DefaultAddress, "broker address, host:port")
	fs.StringVar(&c.caFile, "ca-file", "", "PEM file of CA certificates to trust")
	fs.BoolVar(&c.allowPlaintext, "allow-plaintext", false, "connect without TLS; credentials cross the network unencrypted")
	fs.StringVar(&c.userEnv, "user-env", "GRIDAPPSD_USER", "name of the environment variable holding the user name")
	fs.StringVar(&c.passwordEnv, "password-env", "GRIDAPPSD_PASSWORD", "name of the environment variable holding the password")
	fs.DurationVar(&c.timeout, "timeout", 30*time.Second, "bound on connecting and on each request")
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func run(args []string, stdout, stderr io.Writer, d deps) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "names":
		return runNames(args[1:], stdout, stderr, d)
	case "info":
		return runInfo(args[1:], stdout, stderr, d)
	case "query":
		return runQuery(args[1:], stdout, stderr, d)
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "gridappsd-model: unknown command %q\n%s", args[0], usage)
		return 2
	}
}

// connect dials the broker with credentials from the environment. Its
// errors can quote the password, so callers print them through fail with a
// redactor.
func connect(ctx context.Context, cf *connFlags, d deps) (*session, error) {
	password := d.getenv(cf.passwordEnv)
	s := &session{timeout: cf.timeout, address: cf.address, redact: redactor(password)}
	cfg := gridappsd.Config{
		Address:        cf.address,
		User:           d.getenv(cf.userEnv),
		Password:       password,
		AllowPlaintext: cf.allowPlaintext,
	}
	if cf.caFile != "" {
		pem, err := os.ReadFile(cf.caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file holds no PEM certificate")
		}
		cfg.TLSConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	cctx, cancel := context.WithTimeout(ctx, cf.timeout)
	defer cancel()
	bus, closeFn, err := d.dial(cctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", cf.address, err)
	}
	s.bus, s.closeFn = bus, closeFn
	return s, nil
}

func (s *session) close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

func (s *session) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
}

func redactor(secret string) func(string) string {
	if secret == "" {
		return func(s string) string { return s }
	}
	return func(s string) string { return strings.ReplaceAll(s, secret, "[redacted]") }
}

func fail(stderr io.Writer, redact func(string) string, err error) int {
	fmt.Fprintf(stderr, "gridappsd-model: %s\n", redact(err.Error()))
	return 1
}
