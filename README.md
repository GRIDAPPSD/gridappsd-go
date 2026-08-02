# gridappsd-go

[![Build, vet, and test](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/ci.yml)
[![CodeQL](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/GRIDAPPSD/gridappsd-go/actions/workflows/codeql.yml)
[![Go 1.24](https://img.shields.io/badge/go-1.24-00ADD8?logo=go)](https://go.dev)

This repo is private today, with a move to public visibility planned. Until
that flip happens, the workflow badges above render only for viewers with
repository access and show nothing for anonymous visitors; the CodeQL badge
in particular will report "no analysis" until then, because GitHub Advanced
Security (private-repo code scanning) is not purchased for this org, so the
codeql.yml workflow's analyze job stays skipped by design (see the
`check-visibility` job in that workflow) until the repo goes public. Once
public, badges render for everyone and CodeQL starts running with no further
changes needed here.

Go client library for GridAPPS-D. It mirrors the gridappsd-python client API in
idiomatic Go, providing connection, messaging, and query helpers for the
GridAPPS-D platform. The go-2664-gridappsd gateway consumes this library for the
bus side, talking to the GridAPPS-D message bus from Go services.

This repository currently holds a minimal, buildable module. The package layout
and implementation land in a later phase.
