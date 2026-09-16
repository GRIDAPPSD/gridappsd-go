# gridappsd-go

[![Build, vet, and test](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/ci.yml)
[![CodeQL](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/github-code-scanning/codeql)
[![Go 1.24](https://img.shields.io/badge/go-1.24-00ADD8?logo=go)](https://go.dev)

A Go client library for GridAPPS-D / GOSS. It mirrors the connection and
message-bus layer of the gridappsd-python client in idiomatic Go: connection
and two-step token authentication, STOMP transport, publish/subscribe
messaging, and correlated request/reply. The query API is not ported; see
Status below.

## Status

The connection, messaging, and transport layers described below are
implemented and covered by tests. The typed CIM query API (the
gridappsd-python `GridAPPSDClient` request helpers) is not yet ported; see
[CHANGELOG.md](CHANGELOG.md) for what has shipped so far.

## Installation

```sh
go get github.com/GRIDAPPSD/gridappsd-go
```

Requires Go 1.24 or later.

## Quick start

### Connecting

`gridappsd.Connect` dials the broker and performs GOSS's two-step token
authentication. The zero-value `Config` is fail-closed: it dials TLS using
the system trust store. Set `AllowPlaintext: true` to opt into a plain TCP
connection, for example against a local development broker that has no TLS
terminator in front of it. A plaintext connection sends the connect
credentials and the GOSS auth token unencrypted on the wire, so only opt in
against a broker and network path you trust. Connect and the examples below
read `GRIDAPPSD_USER` and `GRIDAPPSD_PASSWORD` from the environment; neither
is checked for emptiness before dialing, so an unset variable becomes an
empty credential rather than a local error. Check your own environment first
if a connection fails unexpectedly.

```go
import (
	"context"
	"os"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
)

// Connect blocks on the TLS dial and the GOSS token exchange; bound the
// context so a broker that accepts the connection but never answers cannot
// hang forever. Read credentials from the environment: the platform's
// default "manager" password must be changed on any broker beyond local
// development.
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Fail-closed default: dials TLS.
conn, err := gridappsd.Connect(ctx, gridappsd.Config{
	Address:  "gridappsd.example.org:61613",
	User:     os.Getenv("GRIDAPPSD_USER"),
	Password: os.Getenv("GRIDAPPSD_PASSWORD"),
})
if err != nil {
	// handle error
}
defer conn.Disconnect()

// Explicit opt-in for a plaintext dev broker.
devCtx, devCancel := context.WithTimeout(context.Background(), 5*time.Second)
defer devCancel()

devConn, err := gridappsd.Connect(devCtx, gridappsd.Config{
	Address:        "localhost:61613",
	User:           os.Getenv("GRIDAPPSD_USER"),
	Password:       os.Getenv("GRIDAPPSD_PASSWORD"),
	AllowPlaintext: true,
})
if err != nil {
	// handle error
}
defer devConn.Disconnect()
```

`Connect` returns a `transport.Conn`. Most callers instead construct a
`fieldbus.MessageBus`, which wraps `Connect` and adds subscription and
request/reply routing.

### Publish and subscribe

```go
import (
	"context"
	"log"
	"os"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/fieldbus"
	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
)

// No Address set: dials DefaultAddress ("localhost:61613") over TLS, the
// fail-closed default; that port is the plaintext dev broker's port, so set
// Address explicitly against a remote broker.
bus := fieldbus.New(gridappsd.Config{
	User:     os.Getenv("GRIDAPPSD_USER"),
	Password: os.Getenv("GRIDAPPSD_PASSWORD"),
})
connectCtx, connectCancel := context.WithTimeout(context.Background(), 5*time.Second)
defer connectCancel()
if err := bus.Connect(connectCtx); err != nil {
	// handle error
}
defer bus.Disconnect()

// Subscription failures (a broker error frame, or the peer closing the
// subscription) arrive here, not as a return from Subscribe or Send. The
// channel holds 16 entries and drops the oldest when full; it is never
// closed, so the drain goroutine needs its own stop signal. On shutdown,
// the select may pick done while errors are queued; drain the channel
// again after close(done) if all errors must be seen.
done := make(chan struct{})
go func() {
	for {
		select {
		case err := <-bus.Errors():
			log.Println("subscription error:", err)
		case <-done:
			return
		}
	}
}()
defer close(done)

token, err := bus.Subscribe(context.Background(), "/topic/goss.gridappsd.field.output",
	func(headers map[string]string, body []byte) {
		// handle the delivered frame
	})
if err != nil {
	// handle error
}
defer bus.Unsubscribe(context.Background(), "/topic/goss.gridappsd.field.output", token)

// Field output carries measurements and events out of the field or
// simulation; field input carries messages, including control commands,
// into it. Do not publish to field input against a live deployment unless
// that is what's intended.
if err := bus.Send(context.Background(), "/topic/goss.gridappsd.field.input", "application/json", []byte(`{}`)); err != nil {
	// handle error
}
```

A destination with no `/topic/`, `/queue/`, or `/temp-queue/` prefix is
treated as a queue automatically (`topics.NormalizeDestination`).

### Request and reply

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

reply, err := bus.GetResponse(ctx,
	"goss.gridappsd.process.request.status.platform", "application/json", []byte(`{}`))
if err != nil {
	// handle error
}
log.Println("platform status:", string(reply))
```

`GetResponse` sends `body` with a temporary reply-to destination and returns
the first reply, bounded by the context deadline: there is no separate
timeout argument. `context.Background()` never expires, so a request nobody
answers would block forever; bound it as shown above.

## Packages

- `gridappsd`: `Connect` and `Config`, the broker dial and GOSS
  authentication.
- `fieldbus`: `MessageBus`, the publish/subscribe and request/reply
  abstraction built on `gridappsd.Connect`.
- `message`: STOMP frame header name constants used by the GOSS protocol.
- `topics`: destination-naming helpers and well-known GridAPPS-D topic
  constants.
- `transport`: the STOMP connection interfaces (`Conn`, `Subscription`,
  `Dialer`) that `fieldbus` and `gridappsd` are built on. The concrete
  implementation is internal; tests and examples use in-process fakes.

Runnable examples for the packages above are in each package's
`example_test.go` and render on
[pkg.go.dev](https://pkg.go.dev/github.com/GRIDAPPSD/gridappsd-go).

## Versioning and releases

This module follows Go's semantic-import-versioning conventions: releases
are tagged `vMAJOR.MINOR.PATCH` on `main`. See
[CHANGELOG.md](CHANGELOG.md) for a running record of what changed between
releases, and [.github/release-notes/](.github/release-notes/) for the
per-release notes published alongside each tag.

## License and notice

Licensed under the [BSD 2-Clause "Simplified" License](LICENSE.md).
See [NOTICE.md](NOTICE.md) for the Battelle Memorial Institute attribution
and disclaimer that accompanies this license.
