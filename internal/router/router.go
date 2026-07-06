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
// Registration token model: Subscribe returns an opaque Token. Unsubscribe
// removes exactly the handler identified by that token; other handlers on the
// same destination remain live. Removing the last handler for a destination
// automatically cancels the transport subscription and exits the reader goroutine.
// This replaces the unsound %p func-pointer duplicate-guard from phase 2 v1.
package router

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/message"
	"tanuki.pnnl.gov/gpa-grid-improvements/gridappsd-go/transport"
)

// Handler is the canonical callback type invoked for each message on a subscribed destination.
// fieldbus.Handler is a type alias for this type; router is the leaf package so the canonical
// declaration lives here, avoiding a circular import.
type Handler func(headers map[string]string, body []byte)

// Token is an opaque handle returned by Subscribe. Pass it to Unsubscribe to
// remove exactly the registered handler, leaving other handlers on the same
// destination unaffected. Tokens are unique within a Router's lifetime.
type Token uint64

// destState holds the subscription and handler map for one destination.
type destState struct {
	sub      transport.Subscription
	handlers map[Token]Handler
}

// Router dispatches inbound messages to per-destination handler lists.
type Router struct {
	conn transport.Conn

	mu    sync.Mutex
	dests map[string]*destState
	wg    sync.WaitGroup

	// nextToken is a monotonic counter for generating unique registration tokens.
	// Accessed via atomic to avoid requiring the mu lock just for token generation;
	// the token is only inserted into the map under mu.
	nextToken atomic.Uint64

	// errSink receives errors from reader goroutines. Buffered to avoid blocking
	// the goroutine on a transient burst. Drops are observable: the drop path
	// logs via the logged-drop sentinel below rather than a bare default.
	// TODO(GAG-009): expose Errors() <-chan error for full error propagation.
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

// Subscribe registers h on destination and returns an opaque Token. Pass the
// Token to Unsubscribe to remove this specific handler later. On the first
// Subscribe for a destination a transport.Subscription is created and a reader
// goroutine is started. Subsequent calls for the same destination append the
// handler alongside existing ones. Each Subscribe call for the same handler
// function produces a distinct Token and registers an additional invocation.
func (r *Router) Subscribe(ctx context.Context, dest string, h Handler) (Token, error) {
	tok := Token(r.nextToken.Add(1))

	r.mu.Lock()
	defer r.mu.Unlock()

	if state, exists := r.dests[dest]; exists {
		// Destination already has a subscription: add another handler entry.
		state.handlers[tok] = h
		return tok, nil
	}

	// First subscribe for this destination: create the transport subscription and
	// start the reader goroutine.
	sub, err := r.conn.Subscribe(ctx, dest)
	if err != nil {
		return 0, fmt.Errorf("router: subscribe %q: %w", dest, err)
	}
	state := &destState{
		sub:      sub,
		handlers: map[Token]Handler{tok: h},
	}
	r.dests[dest] = state
	r.wg.Add(1)
	go r.readLoop(dest, sub)
	return tok, nil
}

// Unsubscribe removes the single handler identified by token on destination.
// If token is the last handler for destination, the transport subscription is
// cancelled and the reader goroutine exits. Unsubscribing an unknown token or
// an unknown destination is a no-op that returns nil.
func (r *Router) Unsubscribe(_ context.Context, dest string, tok Token) error {
	r.mu.Lock()
	state, exists := r.dests[dest]
	if !exists {
		r.mu.Unlock()
		return nil
	}
	delete(state.handlers, tok)
	if len(state.handlers) > 0 {
		// Other handlers remain: leave the subscription live.
		r.mu.Unlock()
		return nil
	}
	// Last handler removed: tear down the subscription.
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
			err := fmt.Errorf("router: subscription error on %q: %w", dest, msg.Err)
			select {
			case r.errSink <- err:
			default:
				// errSink full: write to stderr so the drop is observable, not silent.
				// TODO(GAG-009): expose Errors() <-chan error for full error propagation.
				fmt.Fprintf(os.Stderr, "router: errSink full, dropped error: %v\n", err)
			}
			return
		}
		// Snapshot handlers under the lock, then dispatch lock-free so a slow
		// handler does not block registrations on other destinations.
		r.mu.Lock()
		state := r.dests[dest]
		var snapshot []Handler
		if state != nil {
			snapshot = make([]Handler, 0, len(state.handlers))
			for _, h := range state.handlers {
				snapshot = append(snapshot, h)
			}
		}
		r.mu.Unlock()

		// Supply the destination header the router owns. Other headers are absent
		// until a later phase enriches transport.Msg; do NOT fabricate values we
		// do not have (data-invariants boundary).
		headers := map[string]string{
			message.HeaderDestination: dest,
		}
		for _, h := range snapshot {
			h(headers, msg.Body)
		}
	}
}
