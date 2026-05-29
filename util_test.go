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
		after := waitNumGoroutines(time.Second, func(n int) bool { return n <= before })
		if after > before {
			var b bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
			testingErrorfOrPanic(t, "%s\n\nstarted with %d goroutines finished with %d", b.Bytes(), before, after)
		}
	}
}

func testingErrorfOrPanic(t interface {
	Helper()
	Errorf(format string, values ...any)
}, format string, values ...interface{}) {
	if t == nil {
		panic(fmt.Errorf(format, values...))
	}
	t.Helper()
	t.Errorf(format, values...)
}

func (x *valuelessContext) Value(key any) any { return nil }
