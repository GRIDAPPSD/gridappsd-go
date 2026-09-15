# Contributing

Thanks for your interest in gridappsd-go.

## Reporting a bug or requesting a feature

Open a GitHub issue describing the problem or the request. For a bug, include
the Go version, the module version (or commit), and a minimal reproduction.

For a suspected security vulnerability, see [SECURITY.md](SECURITY.md)
instead of opening a public issue.

## Development setup

Requires Go 1.24 or later (see `go.mod`). Clone the repository and run:

```sh
go build ./...
go vet ./...
go test -race ./...
```

These three commands are what CI runs on every pull request
(`.github/workflows/ci.yml`), plus a `gofmt -l .` formatting check and a
CodeQL static analysis pass (`.github/workflows/codeql.yml`).

## Before opening a pull request

- Run `gofmt -s -l .` and `goimports -l .` and fix anything they report.
- Run `go vet ./...` and `go test -race ./...`; both must be clean.
- `go mod tidy` should produce no diff.
- If you add or change exported API, update the package's `example_test.go`
  or add one: examples must compile and run under `go test ./...` without a
  live GridAPPS-D broker.
- Keep comments to the "why", not a restatement of the code; see the
  existing packages for the house style.

## Pull request process

- Keep pull requests focused on one change.
- Describe what changed and why in the pull request body.
- A maintainer reviews and merges; CI (build, vet, race test, gofmt, CodeQL)
  must be green first.

## License

By contributing, you agree that your contribution is licensed under the
[BSD 2-Clause "Simplified" License](LICENSE.md) that covers this repository.
