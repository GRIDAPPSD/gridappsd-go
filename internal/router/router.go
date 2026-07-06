// Package router implements the per-destination callback dispatch layer.
//
// Design divergence from the Python CallbackRouter (goss.py:392-442):
//
// The Python router uses a single STOMP listener that receives all messages
// across all destinations. Each frame carries a "destination" header the router
// reads to look up the callback list (goss.py:437). The phase-1 Go transport
// does NOT expose this: transport.Subscribe returns a per-destination
// Subscription, and transport.Msg{Body, Err} carries no destination header.
// There is no global listener and no destination on the message.
//
// Chosen approach: the router owns one transport.Subscription and one reader
// goroutine PER destination. The subscription's identity IS the destination
// (the router holds the mapping), so the missing destination header is supplied
// by the router, not the message. This fits the phase-1 per-destination
// Subscribe exactly and needs no phase-1 change.
//
// Concurrency model:
//   - One sync.Mutex guards both the destination->handlers map and the
//     destination->subscription map.
//   - Registration and removal take the lock; dispatch snapshots handlers under
//     the lock then releases before calling them.
//   - A sync.WaitGroup tracks every live reader goroutine so Close can wait for
//     all of them to exit before returning.
//
// Duplicate-handler guard: Go cannot compare function values for equality
// (https://go.dev/ref/spec#Comparison_operators). The guard is implemented via
// an opaque *handlerEntry wrapper: Subscribe wraps the caller's func in a new
// *handlerEntry each call. The duplicate check compares the func value via
// an fmt.Sprintf("%p") pointer key stored at registration time. If two calls
// supply the exact same func literal (same address), the second is rejected.
// Closures that happen to share an address are unlikely in practice; this is
// the narrowest correct mechanism available in stdlib Go without reflection.
// The behavior mirrors goss.py:418-419 ("Callbacks can only be used one time
// per topic").
package router

import (
	"context"
	"fmt"
	"sync"

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/message"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// Handler is invoked for each message on the subscribed destination.
// Identical to fieldbus.Handler; redefined here to avoid a circular import.
type Handler func(headers map[string]string, body []byte)

// handlerEntry wraps a Handler with an identity pointer for duplicate detection.
type handlerEntry struct {
	h       Handler
	funcKey string // fmt.Sprintf("%p", h) captured at registration
}

// destState holds the subscription and handler list for one destination.
type destState struct {
	sub      transport.Subscription
	handlers []*handlerEntry
}

// Router dispatches inbound messages to per-destination handler lists.
type Router struct {
	conn transport.Conn

	mu    sync.Mutex
	dests map[string]*destState
	wg    sync.WaitGroup

	// errSink receives the first error from any reader goroutine. Buffered so
	// the goroutine never blocks on the send. The sink is not exposed in phase 2;
	// a phase-3 caller that needs error propagation can read it.
	errSink chan error
}

// New creates an idle Router attached to conn. No subscriptions are started
// until Subscribe is called.
func New(conn transport.Conn) *Router {
	return &Router{
		conn:    conn,
		dests:   make(map[string]*destState),
		errSink: make(chan error, 16),
	}
}

// Subscribe registers h on destination. On the first Subscribe for a destination
// a transport.Subscription is created and a reader goroutine is started. Subsequent
// calls for the same destination append the handler. Returns an error if h is
// already registered on destination.
func (r *Router) Subscribe(ctx context.Context, dest string, h Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	funcKey := funcPointerKey(h)

	if state, exists := r.dests[dest]; exists {
		// Destination already has a subscription: check for duplicate handler.
		for _, e := range state.handlers {
			if e.funcKey == funcKey {
				return fmt.Errorf("router: handler already registered on %q", dest)
			}
		}
		state.handlers = append(state.handlers, &handlerEntry{h: h, funcKey: funcKey})
		return nil
	}

	// First subscribe for this destination: create the transport subscription and
	// start the reader goroutine.
	sub, err := r.conn.Subscribe(ctx, dest)
	if err != nil {
		return fmt.Errorf("router: subscribe %q: %w", dest, err)
	}
	state := &destState{
		sub:      sub,
		handlers: []*handlerEntry{{h: h, funcKey: funcKey}},
	}
	r.dests[dest] = state
	r.wg.Add(1)
	go r.readLoop(dest, sub)
	return nil
}

// Unsubscribe removes all handlers for dest and cancels the transport subscription.
// Idempotent: unsubscribing a destination with no registered handlers returns nil.
func (r *Router) Unsubscribe(_ context.Context, dest string) error {
	r.mu.Lock()
	state, exists := r.dests[dest]
	if !exists {
		r.mu.Unlock()
		return nil
	}
	delete(r.dests, dest)
	sub := state.sub
	r.mu.Unlock()

	// Unsubscribing closes C(), which is the reader goroutine's normal exit signal.
	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("router: unsubscribe %q: %w", dest, err)
	}
	return nil
}

// Close unsubscribes all destinations and waits for every reader goroutine to exit.
// Must be called before the underlying transport.Conn is closed.
func (r *Router) Close() {
	r.mu.Lock()
	dests := make(map[string]transport.Subscription, len(r.dests))
	for dest, state := range r.dests {
		dests[dest] = state.sub
	}
	r.dests = make(map[string]*destState)
	r.mu.Unlock()

	for _, sub := range dests {
		// Best-effort; errors are ignored since we are tearing down.
		_ = sub.Unsubscribe()
	}
	// Wait for all reader goroutines to exit before returning. This guarantees
	// no goroutine outlives the router (go.md concurrency discipline).
	r.wg.Wait()
}

// readLoop drains the subscription channel for dest and dispatches to handlers.
// Runs in its own goroutine; exits when sub.C() is closed.
func (r *Router) readLoop(dest string, sub transport.Subscription) {
	defer r.wg.Done()
	for msg := range sub.C() {
		if msg.Err != nil {
			// Error is terminal for this subscription (matches stomp.go:183-186).
			select {
			case r.errSink <- fmt.Errorf("router: subscription error on %q: %w", dest, msg.Err):
			default:
			}
			return
		}
		// Snapshot handlers under the lock, then dispatch lock-free so a slow
		// handler does not block registrations on other destinations.
		r.mu.Lock()
		state := r.dests[dest]
		var snapshot []*handlerEntry
		if state != nil {
			snapshot = make([]*handlerEntry, len(state.handlers))
			copy(snapshot, state.handlers)
		}
		r.mu.Unlock()

		// Supply the destination header the router owns. Other headers are absent
		// until a later phase enriches transport.Msg; do NOT fabricate values we
		// do not have (data-invariants boundary).
		headers := map[string]string{
			message.HeaderDestination: dest,
		}
		for _, e := range snapshot {
			e.h(headers, msg.Body)
		}
	}
}

// funcPointerKey returns a string that identifies a function value by its
// code pointer. Two distinct func literals with the same implementation may
// share an address only if the compiler de-duplicates them (rare and
// implementation-specific). In the common case each func literal is unique.
// This is the narrowest correct duplicate-guard available without reflect.
func funcPointerKey(f Handler) string {
	return fmt.Sprintf("%p", f)
}
