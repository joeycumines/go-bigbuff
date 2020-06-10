/*
   Copyright 2020 Joseph Cumines

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
)

// CombineContext returns a context based on the ctx (first param), that will cancel when ANY of the other provided
// context values cancel CAUTION this spawns one or more blocking goroutines, if you call this with contexts
// that don't cancel in the reasonable lifetime of your application you will have a leak
func CombineContext(ctx context.Context, others ...context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return ctx
	}
	done := make([]<-chan struct{}, 0, len(others))
	for _, other := range others {
		if other == nil {
			continue
		}
		if other.Err() != nil {
			combined, cancel := context.WithCancel(ctx)
			cancel()
			return combined
		}
		done = append(done, other.Done())
	}
	if len(done) == 0 {
		return ctx
	}
	combined, cancel := context.WithCancel(ctx)
	go combineContextWorker(combined.Done(), cancel, done...)
	return combined
}

// UNEXPORTED

// combineContextWorker handles the cancel aggregation logic for CombineContext, batching contexts in groups of up to
// five, meaning the number of goroutines can be reduced.
func combineContextWorker(ctx <-chan struct{}, cancel context.CancelFunc, others ...<-chan struct{}) {
	count := len(others)
	switch {
	case count >= 5:
		if count > 5 {
			go combineContextWorker(ctx, cancel, others[5:]...)
		}
		select {
		case <-ctx:
			// already cancelled
		case <-others[0]:
			cancel()
		case <-others[1]:
			cancel()
		case <-others[2]:
			cancel()
		case <-others[3]:
			cancel()
		case <-others[4]:
			cancel()
		}
	case count == 4:
		select {
		case <-ctx:
			// already cancelled
		case <-others[0]:
			cancel()
		case <-others[1]:
			cancel()
		case <-others[2]:
			cancel()
		case <-others[3]:
			cancel()
		}
	case count == 3:
		select {
		case <-ctx:
			// already cancelled
		case <-others[0]:
			cancel()
		case <-others[1]:
			cancel()
		case <-others[2]:
			cancel()
		}
	case count == 2:
		select {
		case <-ctx:
			// already cancelled
		case <-others[0]:
			cancel()
		case <-others[1]:
			cancel()
		}
	case count == 1:
		select {
		case <-ctx:
			// already cancelled
		case <-others[0]:
			cancel()
		}
	}
}
