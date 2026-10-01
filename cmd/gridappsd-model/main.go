// Command gridappsd-model queries a GridAPPS-D platform for its model data.
//
// Credentials are read only from environment variables, never from flags, so
// they cannot appear in a process listing or shell history.
package main

import (
	"os"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, defaultDeps()))
}

func defaultDeps() deps {
	return deps{getenv: os.Getenv, dial: dialBroker, now: time.Now, stdin: os.Stdin}
}
