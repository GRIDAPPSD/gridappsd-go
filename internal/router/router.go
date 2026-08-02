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
//
// Subscription health contract (GAG-009):
//
//	A destination present in the registry always has a live reader goroutine
//	draining its subscription. When a reader stops for any reason other than a
//	teardown the router itself performed, it deregisters the destination and
//	publishes the cause on Errors(). Two properties follow, and both are load
//	bearing for a long-running consumer:
//
//	  - A subscription failure is observable: the caller reads it from Errors()
//	    rather than losing it to a sink nobody drains.
//	  - A dead destination is never silently reattached to: the next Subscribe
//	    for it dials a fresh transport subscription instead of handing back a
//	    Token bound to a reader that has already exited.
package router

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/GRIDAPPSD/gridappsd-go/message"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// ErrorChanBuffer is the depth of the channel returned by Errors. Terminal
// errors are at most one per destination lifetime, so this holds the failures of
// a large fan-out losing its connection all at once without dropping any.
const ErrorChanBuffer = 16

// ErrSubscriptionClosed reports that a subscription ended without the router
// asking it to: the broker closed it, or the underlying connection dropped.
// Match it with errors.Is to distinguish a peer-side teardown from a broker
// error frame, which is delivered wrapped in the error the broker reported.
var ErrSubscriptionClosed = errors.New("subscription closed by peer")

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

	// done is closed by readLoop as its first action on the way out, before it
	// takes r.mu to deregister. Subscribe treats a destState whose done is
	// closed as absent, so a Subscribe that wins the lock in the window between
	// "reader decided to exit" and "reader deregistered" still creates a fresh
	// subscription instead of attaching a handler to a stopped reader.
	done chan struct{}
}

// dead reports whether this destination's reader goroutine has begun exiting.
func (s *destState) dead() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
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

	// errs carries terminal subscription errors to the caller. See Errors.
	errs chan error
}

// New creates an idle Router attached to conn. No subscriptions are started
// until Subscribe is called.
func New(conn transport.Conn) *Router {
	return NewWithErrorChan(conn, nil)
}

// NewWithErrorChan creates an idle Router that publishes terminal subscription
// errors on errs rather than on a channel of its own. Pass nil for the default.
//
// A caller that recreates its Router across reconnects injects one durable
// channel so its health monitor keeps reading the same channel instead of
// having to re-fetch one per session, which is itself a way to miss failures.
// errs must be buffered; the router never blocks a reader goroutine on it.
func NewWithErrorChan(conn transport.Conn, errs chan error) *Router {
	if errs == nil {
		errs = make(chan error, ErrorChanBuffer)
	}
	return &Router{
		conn:  conn,
		dests: make(map[string]*destState),
		errs:  errs,
	}
}

// Errors returns the channel on which terminal subscription failures are
// delivered: a broker error frame (wrapped, so errors.Is finds the broker's own
// error) or an unexpected close (matching ErrSubscriptionClosed). Teardown the
// router performed itself, via Unsubscribe or Close, is not reported.
//
// The channel is buffered and never closed: a Router publishes from its reader
// goroutines, so closing would turn a late publish into a panic. When the buffer
// is full the oldest queued error is discarded to make room for the newest,
// because a health monitor acts on the most recent failure; a caller that wants
// every error drains the channel.
func (r *Router) Errors() <-chan error { return r.errs }

// Subscribe registers h on destination and returns an opaque Token. Pass the
// Token to Unsubscribe to remove this specific handler later. On the first
// Subscribe for a destination a transport.Subscription is created and a reader
// goroutine is started. Subsequent calls for the same destination append the
// handler alongside existing ones. Each Subscribe call for the same handler
// function produces a distinct Token and registers an additional invocation.
//
// ctx lifetime decision: ctx governs the transport.Conn.Subscribe call only.
// The reader goroutine's lifetime is tied to the subscription channel (closed
// by Unsubscribe or Close), NOT to ctx. A caller that cancels ctx after
// Subscribe returns does not inadvertently tear down the subscription. This
// mirrors the expected messaging pattern: subscribe once, receive indefinitely,
// unsubscribe explicitly. If the caller wants a ctx-scoped subscription,
// they call Unsubscribe in a goroutine that selects on their ctx.Done().
func (r *Router) Subscribe(ctx context.Context, dest string, h Handler) (Token, error) {
	tok := Token(r.nextToken.Add(1))

	r.mu.Lock()
	defer r.mu.Unlock()

	if state, exists := r.dests[dest]; exists && !state.dead() {
		// Destination already has a live reader: add another handler entry.
		state.handlers[tok] = h
		return tok, nil
	}

	// No state for this destination, or the previous one's reader has stopped.
	// A dead state is replaced, never reused: reusing it would hand the caller a
	// Token bound to a subscription that can never deliver again, which is the
	// silent failure GAG-009 describes. The dead subscription is not
	// unsubscribed here: it ended peer-side, so its transport resources are
	// already released and calling Unsubscribe on it races go-stomp's teardown.
	sub, err := r.conn.Subscribe(ctx, dest)
	if err != nil {
		return 0, fmt.Errorf("router: subscribe %q: %w", dest, err)
	}
	state := &destState{
		sub:      sub,
		handlers: map[Token]Handler{tok: h},
		done:     make(chan struct{}),
	}
	r.dests[dest] = state
	r.wg.Add(1)
	go r.readLoop(dest, state)
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
	// Last handler removed: tear down the subscription. Deregistering BEFORE
	// calling Unsubscribe is what lets readLoop tell a teardown we performed
	// from a death we did not, so our own teardown is never reported as a fault.
	delete(r.dests, dest)
	dead := state.dead()
	sub := state.sub
	r.mu.Unlock()

	if dead {
		// The reader already stopped and the peer already closed the
		// subscription; unsubscribing it again has nothing to cancel.
		return nil
	}
	// Unsubscribing closes C(), which is the reader goroutine's normal exit signal.
	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("router: unsubscribe %q: %w", dest, err)
	}
	return nil
}

// Close unsubscribes all destinations and waits for every reader goroutine to exit.
// Must be called before the underlying transport.Conn is closed.
func (r *Router) Close() {
	// Clearing the registry before unsubscribing is what marks this as a
	// teardown we performed: readLoop finds its destination already gone and so
	// publishes nothing.
	r.mu.Lock()
	subs := make([]transport.Subscription, 0, len(r.dests))
	for _, state := range r.dests {
		if !state.dead() {
			// A dead state's subscription was already closed peer-side.
			subs = append(subs, state.sub)
		}
	}
	r.dests = make(map[string]*destState)
	r.mu.Unlock()

	for _, sub := range subs {
		// Best-effort; errors are ignored since we are tearing down.
		_ = sub.Unsubscribe()
	}
	// Wait for all reader goroutines to exit before returning. This guarantees
	// no goroutine outlives the router (go.md concurrency discipline).
	r.wg.Wait()
}

// readLoop drains the subscription channel for dest and dispatches to handlers.
// Runs in its own goroutine; exits when the subscription reports a terminal
// error or its channel closes. On the way out it always deregisters dest (unless
// the router already did, which is how a teardown we performed is recognized)
// and publishes the cause on Errors when the exit was not one we asked for.
func (r *Router) readLoop(dest string, state *destState) {
	defer r.wg.Done()

	var termErr error
	for msg := range state.sub.C() {
		if msg.Err != nil {
			// Error is terminal for this subscription (matches stomp.go:183-186).
			termErr = fmt.Errorf("router: subscription error on %q: %w", dest, msg.Err)
			break
		}
		// Snapshot this reader's own handlers under the lock, then dispatch
		// lock-free so a slow handler does not block registrations on other
		// destinations. The snapshot comes from the state this goroutine owns,
		// not from a fresh registry lookup: after a replacement the registry
		// entry belongs to a different reader, whose handlers are not ours.
		r.mu.Lock()
		snapshot := make([]Handler, 0, len(state.handlers))
		for _, h := range state.handlers {
			snapshot = append(snapshot, h)
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

	// Mark the state dead BEFORE taking the lock, so a Subscribe that wins the
	// lock in this window sees a dead state and creates a fresh subscription
	// rather than attaching to a reader that is on its way out.
	close(state.done)

	r.mu.Lock()
	// Still registered means the router did not tear this down: the exit was a
	// broker error or a peer-side close, and the caller needs to hear about it.
	unexpected := r.dests[dest] == state
	if unexpected {
		delete(r.dests, dest)
	}
	r.mu.Unlock()

	switch {
	case termErr != nil:
		r.publishErr(termErr)
	case unexpected:
		r.publishErr(fmt.Errorf("router: subscription on %q ended: %w", dest, ErrSubscriptionClosed))
	}
}

// publishErr delivers err on the Errors channel without ever blocking the
// calling reader goroutine. When the buffer is full the oldest queued error is
// discarded to make room, because a health monitor acts on the most recent
// failure. The attempt count is bounded so a concurrent burst of failing
// destinations cannot spin here.
func (r *Router) publishErr(err error) {
	for i := 0; i <= ErrorChanBuffer; i++ {
		select {
		case r.errs <- err:
			return
		default:
		}
		select {
		case <-r.errs: // discard the oldest to make room for the newest
		default:
		}
	}
}
