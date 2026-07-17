/*
   Copyright 2026 Joseph Cumines

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
	"bytes"
	"context"
	"fmt"
	"runtime"
	"runtime/pprof"
	"time"
)

type (
	// valuelessContext is a test util to hide all values from a context.
	// Use case is making context implementations spawn goroutines. For
	// reference, the context package tries very hard to avoid unneeded
	// goroutines, which makes that quite difficult.
	valuelessContext struct {
		context.Context
	}
)

var (
	// compile time assertions

	_ context.Context = (*valuelessContext)(nil)
)

func waitNumGoroutines(maxWait time.Duration, fn func(n int) bool) (n int) {
	const minWait = time.Millisecond * 10
	if maxWait < minWait {
		maxWait = minWait
	}
	count := int(maxWait / minWait)
	maxWait /= time.Duration(count)
	n = runtime.NumGoroutine()
	for i := 0; i < count && !fn(n); i++ {
		time.Sleep(maxWait)
		runtime.GC()
		n = runtime.NumGoroutine()
	}
	return
}

// checkNumGoroutines should be called at the start of the test, like:
//
//	t.Cleanup(checkNumGoroutines(t))
func checkNumGoroutines(t interface {
	Helper()
	Errorf(format string, values ...any)
}) func() {
	before := runtime.NumGoroutine()
	return func() {
		if t != nil {
			t.Helper()
		}
		// Wait for the goroutine count to settle at or below the baseline.
		//
		// A single instantaneous reading (or a fixed 1s wait) is racy under
		// -race: the race detector, background goroutines, and in-flight worker
		// goroutines (whose cleanup happens only after an outcome is delivered)
		// may take longer than 1s to drain, producing a transient spike that
		// looks like a leak. Requiring the count to remain <= before for a short
		// stable window reduces transient false positives, while a genuine leak
		// (count never returning to baseline) will typically still fail once the
		// budget is exhausted.
		//
		// CAVEAT: runtime.NumGoroutine is process-global. This check cannot
		// eliminate all false positives (an unrelated goroutine may keep the
		// count above baseline) or all false negatives (an unrelated goroutine
		// may exit and mask a real leak). It is a pragmatic heuristic, not a
		// sound leak detector. For precise leak detection, use explicit
		// WaitGroup/completion-channel joins for goroutines owned by each test.
		const settleBudget = 5 * time.Second
		const stableWindow = 50 * time.Millisecond
		deadline := time.Now().Add(settleBudget)
		var stableSince time.Time
		for {
			runtime.GC() // help finalizer-based goroutine cleanup
			n := runtime.NumGoroutine()
			if n <= before {
				if stableSince.IsZero() {
					stableSince = time.Now()
				} else if time.Since(stableSince) >= stableWindow {
					return
				}
			} else {
				stableSince = time.Time{}
			}
			if time.Now().After(deadline) {
				var b bytes.Buffer
				_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
				testingErrorfOrPanic(t, "%s\n\nstarted with %d goroutines finished with %d", b.Bytes(), before, n)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func testingErrorfOrPanic(t interface {
	Helper()
	Errorf(format string, values ...any)
}, format string, values ...any) {
	if t == nil {
		panic(fmt.Errorf(format, values...))
	}
	t.Helper()
	t.Errorf(format, values...)
}

func (x *valuelessContext) Value(key any) any { return nil }
