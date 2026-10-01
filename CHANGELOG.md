# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows Go's semantic-import-versioning conventions for
tags.

## [Unreleased]

## [0.3.2] - 2026-09-30

### Fixed

- Unicode format characters in teardown error text are escaped (#39): the
  bidirectional controls, zero-width characters and the byte order mark are
  written as `\uNNNN` and the tag characters as `\UNNNNNNNN`, so broker text
  can no longer reorder or hide part of a log line.
- Escaping of teardown error text in the token exchange (#36):
  - a backslash in a teardown error is written as `\\`, so a literal `\x0a`
    from the broker no longer reads like an escaped line break;
  - C1 control characters, U+2028 and U+2029 are written as `\uNNNN`, and
    each byte of invalid UTF-8 as `\xNN`;
  - the 512-byte cut never lands inside an escape, where it could leave a
    dangling `\x` before the truncation note;
  - `Error()` escapes only the text the bound keeps, so a very large broker
    message no longer costs several times its size in allocations.

## [0.3.1] - 2026-09-30

### Fixed

- Credential-connection teardown reporting in the token exchange (#31):
  - a teardown step failure after the exchange had already failed is
    logged with `token_received=false` only when a step error was produced
    before the context ends; if the context ends first, nothing is logged;
    the exchange still fails with its own error;
  - the credential connection is closed after a teardown step fails while
    the context is live, instead of being left to the transport;
  - the text a teardown error reports through `Error()` is cut to 512
    bytes, and the full original length is reported, so a broker ERROR
    message of any size no longer reaches a log that prints that text; a
    handler that unwraps the error still reaches the full message;
  - an UNSUBSCRIBE and a DISCONNECT failure are joined with `; ` rather
    than a newline.

- Teardown error text in the token exchange (#34):
  - the 512-byte bound applies to each of the UNSUBSCRIBE and DISCONNECT
    errors before they are joined, so a long UNSUBSCRIBE message no longer
    hides the DISCONNECT error;
  - ASCII control characters in a teardown error, including line breaks and
    terminal escapes, are written as `\xNN` escapes, so a broker message
    cannot split a log line on ASCII control bytes; non-ASCII control bytes
    (C1, U+2028, U+2029) and invalid UTF-8 pass through unescaped.

## [0.3.0] - 2026-09-30

### Added

- `Config.Logger`, a `*slog.Logger` that receives failures `Connect`
  reports without failing. Nil uses `slog.Default()`;
  `slog.New(slog.DiscardHandler)` silences it (#25).

### Fixed

- The token exchange's credential-connection teardown is now bounded by the
  caller's context also when an UNSUBSCRIBE or DISCONNECT is already waiting
  for its receipt: the connection is closed when the context ends, which
  ends go-stomp's receipt wait, instead of holding the broker session, its
  socket and go-stomp goroutines for up to go-stomp's 30s receipt timeout
  after the call returns. go-stomp can still keep a goroutine past that
  close (#24). An `Unsubscribe` or `Disconnect` error on the credential
  connection, including a runtime panic recovered from go-stomp, is no
  longer discarded: it is logged at warning level through `Config.Logger`,
  under the `error` key, with `token_received` saying whether the token
  arrived, and a token that arrived is still used. A context that ends while
  that teardown is still running fails the exchange with a credential
  connection teardown error that wraps the context error; before, the
  exchange could instead fail at the second dial with a timeout error that
  did not wrap the context error (#25).

## [0.2.1] - 2026-09-30

### Fixed

- An `Unsubscribe` can no longer outlive its connection and panic the
  process through go-stomp v3.1.2's send on a closed channel (#23).
  `Disconnect` now waits for pending unsubscribes before closing the
  connection, which can add up to go-stomp's 30s unsubscribe receipt
  timeout when the broker stops answering, and an unsubscribe that begins
  after `Disconnect` is refused with an error instead of reaching go-stomp.
  A runtime panic from go-stomp's `Unsubscribe` is recovered and returned
  as an error. The subscription bridge keeps draining go-stomp's channel
  until go-stomp closes it, so an unsubscribe on a subscription with unread
  messages no longer wedges the connection. The router no longer lets a
  cancelled `Subscribe` context tear its subscription down, as its
  documentation always stated. When the transport unsubscribes because the
  `Subscribe` context ended, a later `Unsubscribe` call returns that
  attempt's result.

## [0.2.0] - 2026-09-30

### Added

- `LICENSE.md` (BSD 2-Clause "Simplified" License) and `NOTICE.md` (Battelle
  Memorial Institute attribution and disclaimer).
- `gridappsd.DefaultHandshakeTimeout` (5s) bounds a TLS handshake when the
  caller's context carries no deadline of its own; an explicit caller
  deadline is honored as-is and never lengthened. On expiry, `Connect`
  returns an error matching the new `gridappsd.ErrHandshakeTimeout` via
  `errors.Is`, naming the likely cause: the peer accepted the TCP connection
  but may not be speaking TLS, such as a plaintext broker on what the caller
  believes is a TLS port.
- `transport.HeartBeatIntervals` and `gridappsd.Config.HeartBeats`: the send
  and receive STOMP heartbeat directions can now be configured
  independently, rather than only symmetrically via `HeartBeat`. This
  addresses part of the v0.1.0 known issue below: a caller can now request
  send-only heartbeats. The underlying `go-stomp` behavior noted in that
  issue is unchanged; see "Known issues" below.

### Fixed

- `internal/router` now surfaces subscription failures on a caller-readable
  `Errors()` channel and retires a destination whose reader goroutine has
  exited, instead of dropping the failure into an unread sink and leaving a
  later `Subscribe` call silently attach to a dead reader. This fixes the
  v0.1.0 known issue about dropped subscription errors and dead-destination
  reattachment. `fieldbus.GridAPPSDMessageBus` exposes this through the new
  `fieldbus.ErrorReporter` interface. Callers must drain `Errors()`
  themselves: the channel is buffered to 16 entries, and once full the
  oldest unread error is dropped to make room for the newest. The
  peer-side-close sentinel, `router.ErrSubscriptionClosed`, lives under
  `internal/`, so a caller outside this module cannot match it with
  `errors.Is`; only the broker-error-frame case is distinguishable from
  outside the module today.
- The token exchange (`internal/auth`) now closes the socket on every failed
  STOMP CONNECT, on both the credential leg and the token leg, instead of
  leaving it open until the garbage collector's finalizer eventually runs
  one. Teardown of the credential connection is bounded by the caller's
  context only when the context is already done before the deferred
  unsubscribe begins: it then closes the connection at once instead of
  waiting on the broker. An UNSUBSCRIBE already pending when the context
  ends is not interrupted and can still hold that broker session, and its
  socket, open for up to go-stomp's 30s receipt timeout after the call
  returns (#25).

### Known issues (new in this release)

- An `Unsubscribe` that outlives its connection can panic the process:
  go-stomp v3.1.2 sends on a channel the connection has already closed. It
  is reachable when a caller cancels the context it passed to `Subscribe`,
  contrary to the router's documented contract that a cancelled context does
  not tear down the subscription (#23).
- A connection whose receipts go unanswered can leak one `go-stomp`
  goroutine after it closes, with no public API on this module to close or
  drain the underlying connection (#24).
- A recovered panic in the token exchange's unsubscribe is discarded rather
  than reported, and the teardown bound described above is real: a pending
  `UNSUBSCRIBE` is not interrupted when the caller's context ends (#25).

### Known issues (carried from v0.1.0, not fixed)

- Requesting a zero receive interval via `HeartBeats` does not guarantee the
  underlying `go-stomp` client leaves its read timer disarmed: `go-stomp`
  raises the negotiated incoming interval to whatever the broker advertises
  it can send, regardless of what was requested. This is a third-party
  behavior, not tracked for a fix in this repository. See the correctness
  note on `transport.HeartBeatIntervals`.

## [0.1.0] - first tagged release

Seeded from `.github/release-notes/v0.1.0.md`.

### Added

- `gridappsd.Connect`: TLS dialing (fail-closed by default, plain-TCP
  opt-in via `AllowPlaintext`) and two-step token authentication, returning
  a durable `transport.Conn`.
- `fieldbus` package: publish/subscribe message bus abstraction over the
  durable transport.
- `internal/router`: subscription registry and per-destination read loop
  that dispatches frames to registered handlers.
- `internal/reqresp`: request/reply pattern over STOMP, matching correlated
  responses to pending requests.
- `topics` and `message` packages: GridAPPS-D topic naming helpers and the
  STOMP and GOSS header name constants.
- Module path corrected to `github.com/GRIDAPPSD/gridappsd-go`.

### Known issues (not fixed in this release)

- `transport.ConnConfig.HeartBeat` is symmetric: a consumer could not
  request independent send and receive heartbeat directions. Fixed in
  [0.2.0] above (`HeartBeatIntervals`); the underlying `go-stomp`
  limitation it was combined with remains, see [0.2.0] "Known issues".
- Subscription errors in `internal/router`'s read loop were dropped into an
  unread sink, and a dead destination was left registered so a later
  `Subscribe` call could silently attach to an already-exited reader. Fixed
  in [0.2.0] above.
- A third defect was in `go-stomp` itself and is third-party; not tracked in
  this repository. Still present; see [0.2.0] "Known issues".

[Unreleased]: https://github.com/GRIDAPPSD/gridappsd-go/compare/v0.3.2...main
[0.3.2]: https://github.com/GRIDAPPSD/gridappsd-go/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/GRIDAPPSD/gridappsd-go/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/GRIDAPPSD/gridappsd-go/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/GRIDAPPSD/gridappsd-go/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/GRIDAPPSD/gridappsd-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/GRIDAPPSD/gridappsd-go/releases/tag/v0.1.0
