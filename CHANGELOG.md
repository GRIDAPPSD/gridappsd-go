# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows Go's semantic-import-versioning conventions for
tags.

## [Unreleased]

### Added

- `LICENSE.md` (BSD 2-Clause "Simplified" License) and `NOTICE.md` (Battelle
  Memorial Institute attribution and disclaimer).
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
  Unreleased above (`HeartBeatIntervals`); the underlying `go-stomp`
  limitation it was combined with remains, see Unreleased "Known issues".
- Subscription errors in `internal/router`'s read loop were dropped into an
  unread sink, and a dead destination was left registered so a later
  `Subscribe` call could silently attach to an already-exited reader. Fixed
  in Unreleased above.
- A third defect was in `go-stomp` itself and is third-party; not tracked in
  this repository. Still present; see Unreleased "Known issues".

[Unreleased]: https://github.com/GRIDAPPSD/gridappsd-go/compare/v0.1.0...main
[0.1.0]: https://github.com/GRIDAPPSD/gridappsd-go/releases/tag/v0.1.0
