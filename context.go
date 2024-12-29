/*
   Copyright 2021 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

import (
	"context"
	"sync"
)

// ConflatedContext returns a new context that remains valid (i.e. not canceled)
// as long as *at least one* of the provided contexts is not canceled. Once
// *all* of the supplied contexts are canceled, the returned context is also
// canceled.
//
// ConflatedContext inherits only the key/value pairs from the *first* provided
// context. This includes any metadata such as logging fields, request-scoped
// values, etc., but not its cancellation. It uses [context.WithoutCancel] on
// the first context to avoid immediately propagating cancellation from that
// context alone. Instead, it performs its own logic to ensure the returned
// context is canceled only if *all* contexts are canceled.
//
// Typical use cases for ConflatedContext involve "de-duplicated" or "batched"
// operations, where multiple requests or function calls (each with its own
// context) can share a single ongoing computation or result. The operation
// should keep going as long as at least one caller remains interested.
//
// If ConflatedContext is called with no input contexts, it will panic, as
// there's no meaningful behavior in that scenario. If *all* of the contexts
// passed in are already canceled at the time of invocation, it will return
// immediately with a context whose Err() is [context.Canceled].
func ConflatedContext(contexts ...context.Context) (ctx context.Context, cancel context.CancelFunc) {
	if len(contexts) == 0 {
		panic("bigbuff.ConflatedContext requires at least one context")
	}

	// context values are inherited solely from the first context
	ctx, cancel = context.WithCancel(context.WithoutCancel(contexts[0]))

	var success bool
	defer func() {
		if !success {
			cancel()
		}
	}()

	var wg sync.WaitGroup // we wait for _all_ contexts to be canceled

	wg.Add(1) // so we can incrementally add w/o triggering wait (could panic)

	// guard against _all_ the job.ctx being canceled, and wire up cancel
	var ok bool
	for _, ctx2 := range contexts {
		if ctx2.Err() == nil {
			ok = true // indicate not to cancel prematurely

			wg.Add(1) // so we can call done

			// ensures single wg.Done call, ASAP, on either context cancel
			// (with cleanup hinging on ctx)
			ChainAfterFunc(ctx, ctx2, wg.Done)
		}
	}
	if !ok {
		return // no need to wait (will cancel immediately)
	}

	wg.Done() // decrement our first increment

	go func() {
		wg.Wait()
		cancel() // combined cancel
	}()

	success = true // don't cancel

	return
}

// ChainAfterFunc registers to call f on cancel of ctx OR other, where ctx is
// the primary context, and must be guaranteed to be cancelled, at some point.
// Uses [context.AfterFunc] to call f, exactly-once, assuming that either ctx
// or other will eventually be canceled (otherwise, f will never be called).
// Resource cleanup hinges on ctx being canceled.
// Does not return the stop function(s), as the exactly-once calling behavior
// relies on not calling said functions.
func ChainAfterFunc(ctx context.Context, other context.Context, f func()) {
	stop := context.AfterFunc(other, f)
	context.AfterFunc(ctx, func() {
		if stop() {
			// Stopped f from being run. Because this closure will only trigger
			// on ctx cancel, and we otherwise never stop either hooks, this is
			// guaranteed to indicate that we never ran f (vs possibly having
			// stopped f).
			f()
		}
	})
}

// CombineContext returns a context based on the ctx (first param), that will
// cancel when ANY of the other provided context values cancel.
//
// WARNING: This implementation relies on at least one of the contexts being
// cancelled, to facilitate cleanup. While the newer [context.AfterFunc] API
// is leveraged to reduce the risk of resource leaks, it still deserves
// significant care and attention.
func CombineContext(ctx context.Context, others ...context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	} else if ctx.Err() != nil {
		return ctx
	}

	// count number of non-nil contexts, and guard any cancels
	n := 0
	for _, other := range others {
		if other != nil {
			if other.Err() != nil {
				ctx, cancel := context.WithCancel(ctx)
				cancel()
				return ctx
			}
			n++
		}
	}
	if n == 0 {
		return ctx
	}

	// we will need to deregister all after funcs on cancel
	stops := make(stopCallbackSlice, 0, n)

	// this will be our result
	ctx, cancel := context.WithCancel(ctx)

	// wire up cancellation from any of the other contexts to our cancel
	for _, other := range others {
		if other != nil {
			stops = append(stops, context.AfterFunc(other, cancel))
		}
	}

	// deregister all after funcs on cancel (it's a one-way transition)
	context.AfterFunc(ctx, stops.Stop)

	_ = cancel // silence go vet for not using cancel on all code paths

	return ctx
}

type stopCallbackSlice []func() bool

func (s stopCallbackSlice) Stop() {
	for _, stop := range s {
		stop()
	}
}
