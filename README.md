# gridappsd-go

[![Build, vet, and test](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/ci.yml)
[![CodeQL](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/codeql.yml)
[![Go 1.24](https://img.shields.io/badge/go-1.24-00ADD8?logo=go)](https://go.dev)

A Go client library for GridAPPS-D / GOSS. It mirrors the gridappsd-python
client API in idiomatic Go: connection and two-step token authentication,
STOMP transport, publish/subscribe messaging, and correlated request/reply.

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
terminator in front of it.

```go
import (
	"context"

	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
)

// Fail-closed default: dials TLS.
conn, err := gridappsd.Connect(context.Background(), gridappsd.Config{
	Address:  "gridappsd.example.org:61613",
	User:     "system",
	Password: "manager",
})

// Explicit opt-in for a plaintext dev broker.
devConn, err := gridappsd.Connect(context.Background(), gridappsd.Config{
	Address:        "localhost:61613",
	User:           "system",
	Password:       "manager",
	AllowPlaintext: true,
})
```

`Connect` returns a `transport.Conn`. Most callers instead construct a
`fieldbus.MessageBus`, which wraps `Connect` and adds subscription and
request/reply routing.

### Publish and subscribe

```go
import (
	"context"

	"github.com/GRIDAPPSD/gridappsd-go/fieldbus"
	"github.com/GRIDAPPSD/gridappsd-go/gridappsd"
)

bus := fieldbus.New(gridappsd.Config{User: "system", Password: "manager"})
if err := bus.Connect(context.Background()); err != nil {
	// handle error
}
defer bus.Disconnect()

token, err := bus.Subscribe(context.Background(), "/topic/goss.gridappsd.field.output",
	func(headers map[string]string, body []byte) {
		// handle the delivered frame
	})
if err != nil {
	// handle error
}
defer bus.Unsubscribe(context.Background(), "/topic/goss.gridappsd.field.output", token)

err = bus.Send(context.Background(), "/topic/goss.gridappsd.field.input", "application/json", []byte(`{}`))
```

A destination with no `/topic/`, `/queue/`, or `/temp-queue/` prefix is
treated as a queue automatically (`topics.NormalizeDestination`).

### Request and reply

```go
reply, err := bus.GetResponse(context.Background(),
	"goss.gridappsd.process.request.status.platform", "application/json", []byte(`{}`))
```

`GetResponse` sends `body` with a temporary reply-to destination and returns
the first reply, bounded by the context deadline: there is no separate
timeout argument.

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
