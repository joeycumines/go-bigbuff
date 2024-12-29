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
