package router

// LiveDestinationCount reports how many destinations are currently registered
// in the router's registry.
//
// Test-only accessor: export_test.go is compiled into the test binary and never
// into the package, so this does not widen the public surface. The registry is
// the invariant GAG-009 turns on (a destination present in the registry must
// have a live reader draining it), and a test needs to observe deregistration
// as a synchronization point before asserting resubscribe behavior.
func (r *Router) LiveDestinationCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.dests)
}
