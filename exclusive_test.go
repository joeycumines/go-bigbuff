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
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForNoWork polls until the Exclusive has no pending work items, or fails
// the test if the bounded deadline elapses.
//
// A worker goroutine deletes the corresponding entry from e.work only AFTER it
// has delivered the outcome: the result is sent on the outcome channel (in
// resolve) before the delete. The outcome channel therefore establishes a
// happens-before edge for the *result*, but not for the subsequent cleanup —
// the delete is an asynchronous internal operation with no public completion
// signal. Reading e.work once (even under e.mutex) can therefore transiently
// observe a stale length of 1, which is a flaky failure, not a real bug.
//
// The worker goroutine exits shortly after the delete, so waiting for e.work
// to become empty is a necessary condition for worker completion, but not a
// sufficient one: a worker may still be between the delete and returning.
// This is acceptable for test purposes because the subsequent
// checkNumGoroutines cleanup (registered via t.Cleanup) catches any actual
// goroutine leak. A completion channel or sync.WaitGroup would be a true
// join, but would require modifying the production Exclusive type.
func waitForNoWork(t *testing.T, e *Exclusive) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		e.mutex.Lock()
		n := len(e.work)
		e.mutex.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected no work items remaining after calls completed, got %d", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestExclusive_Call_sequential(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	e := new(Exclusive)

	for range 5 {
		v, err := e.Call(
			1,
			func() (any, error) {
				return "one", errors.New("some_error")
			},
		)

		if v != "one" || err == nil || err.Error() != "some_error" {
			t.Fatal(v, err)
		}

		var failed bool
		var s string
		for range 5 {
			e.mutex.Lock()
			failed = len(e.work) != 0 || e.work == nil
			s = fmt.Sprint(e.work)
			e.mutex.Unlock()
			if !failed {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
		if failed {
			t.Fatal(s)
		}
	}
}

func TestExclusive_Call_blocking(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		e  = new(Exclusive)
		in = make(chan struct {
			V any
			E error
		})
		f = func() (any, error) {
			v := <-in
			return v.V, v.E
		}
		order      = make(chan int, 50)
		v1         = 7243
		e1         = errors.New("12451")
		v2         = 1471
		e2         = errors.New("6235")
		v3         = 781
		e3         = errors.New("623x")
		i1, i2, i3 *exclusiveItem
	)

	go func() {
		v, err := e.Call(22, f)
		if v != v1 || err != e1 {
			t.Error(v, err)
		}
		order <- 1
	}()

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work != nil || !i.running || i.count != 0 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		} else {
			i1 = i
		}
	}()

	v, err := e.Call(
		"a",
		func() (any, error) {
			return 88, e1
		},
	)
	if v != 88 || err != e1 {
		t.Fatal(v, err)
	}

	go func() {
		v, err := e.Call(22, f)
		if v != v2 || err != e2 {
			t.Error(v, err)
		}
		order <- 2
	}()

	go func() {
		v, err := e.Call(22, f)
		if v != v2 || err != e2 {
			t.Error(v, err)
		}
		order <- 2
	}()

	go func() {
		v, err := e.Call(22, f)
		if v != v2 || err != e2 {
			t.Error(v, err)
		}
		order <- 2
	}()

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work == nil || !i.running || i.count != 3 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		}
	}()

	in <- struct {
		V any
		E error
	}{
		V: v1,
		E: e1,
	}

	if v := <-order; v != 1 {
		t.Fatal(v)
	}

	go func() {
		v, err := e.Call(22, f)
		if v != v3 || err != e3 {
			t.Error(v, err)
		}
		order <- 3
	}()

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work == nil || !i.running || i.count != 1 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		} else {
			i2 = i
		}
	}()

	in <- struct {
		V any
		E error
	}{
		V: v2,
		E: e2,
	}

	if v := <-order; v != 2 {
		t.Fatal(v)
	}
	if v := <-order; v != 2 {
		t.Fatal(v)
	}
	if v := <-order; v != 2 {
		t.Fatal(v)
	}

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work != nil || !i.running || i.count != 0 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		} else {
			i3 = i
		}
	}()

	in <- struct {
		V any
		E error
	}{
		V: v3,
		E: e3,
	}

	if v := <-order; v != 3 {
		t.Fatal(v)
	}

	close(order)

	for range order {
	}

	// Wait for the worker goroutines to finish their asynchronous cleanup
	// (the delete from e.work happens only after each outcome is delivered).
	waitForNoWork(t, e)

	if i1 == i2 || i1 == i3 || i2 == i3 || i1.mutex != i2.mutex || i3.mutex != i1.mutex || i1.cond != i2.cond || i3.cond != i1.cond {
		t.Fatal(i1, i2, i3)
	}
	if !i1.complete || i1.running {
		t.Fatal(i1)
	}
	if !i2.complete || i2.running {
		t.Fatal(i2)
	}
	if i3.complete || i3.running {
		// i3 is never started
		t.Fatal(i3)
	}
}

// TestExclusive_Call_concurrent verifies that Exclusive debounces concurrent
// calls to the same key into exactly two work executions: the first call runs
// work 1, and the remaining N-1 calls (which arrive while work 1 is running)
// are batched into work 2.
//
// The previous version released all N goroutines behind a single gate and then
// spun on nextItem.count as a "barrier". That barrier was a timing race, not a
// synchronized counter: with ExclusiveWait=0, the first caller's worker
// installs nextItem BEFORE running work 1 (exclusive.go:265-272, then :300).
// Any caller that won e.mutex during the tiny window between the original-item
// install and the nextItem install joined the *original* item, not nextItem;
// those callers are not counted by nextItem.count, so the spin could wait
// forever (work 1 blocks on releaseWork, closed only after the barrier — a
// circular wait → permanent hang). The original "NOT a timing window" comment
// was incorrect: which batch a caller lands in is precisely a timing race.
//
// This version removes the race deterministically:
//  1. N=100 goroutines; the first is released alone behind gate1, the other 99
//     behind gate2.
//  2. The first call creates the original item; its worker sets
//     item.running=true, creates nextItem immediately (exclusive.go:265-272),
//     releases item.mutex, then calls the work function (exclusive.go:300).
//  3. The work function blocks on releaseWork and signals "started". Because
//     the started send happens AFTER the nextItem install (exclusive.go:271)
//     in worker program order, and the channel receive in this test is a
//     happens-before edge, observing <-started guarantees nextItem is already
//     installed in e.work[42].
//  4. Only now are the other 99 goroutines released (gate2). They all observe
//     nextItem in e.work[42] and therefore join nextItem deterministically
//     (k = 0): none can reach the original item, which is no longer in the map.
//  5. The barrier polls nextItem.count until it reaches N-1 — now a monotonic
//     counter that is guaranteed to arrive, with a bounded deadline so a
//     regression fails loudly instead of hanging until the go test panic.
//  6. close(releaseWork) completes work 1; exactly one joined caller starts
//     work 2; the rest share its result. Expected: workCalls == 2.
//
// Without any debouncing it would be N (100) work executions; a broken
// implementation batching only one pair (99 individual executions) gives
// workCalls == 99, which fails.
func TestExclusive_Call_concurrent(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const N = 100
	var (
		e           Exclusive
		workCalls   atomic.Int32
		releaseWork = make(chan struct{})
		started     = make(chan struct{}, 1)
		wg          sync.WaitGroup
	)
	wg.Add(N)

	// gate1 releases only the first caller; gate2 releases the remaining N-1.
	// Splitting the release is what guarantees none of the bulk callers can
	// join the original item (see the function doc above).
	gate1 := make(chan struct{})
	gate2 := make(chan struct{})
	for i := range N {
		go func(first bool) {
			defer wg.Done()
			if first {
				<-gate1
			} else {
				<-gate2
			}
			v, err := e.Call(42, func() (any, error) {
				workCalls.Add(1)
				select {
				case started <- struct{}{}:
				default:
				}
				<-releaseWork
				return "shared", nil
			})
			if err != nil {
				t.Error(err)
			}
			if v != "shared" {
				t.Errorf("expected 'shared', got %v", v)
			}
		}(i == 0)
	}

	// Release the first caller, which installs the original item and whose
	// worker installs nextItem, then starts work 1 (blocking on releaseWork).
	// The started signal is sent from the work function *after* nextItem is
	// installed, so observing <-started guarantees nextItem is in the map.
	close(gate1)

	// Wait for the first work invocation to start (it blocks on releaseWork)
	<-started

	// Now release the remaining callers. nextItem already occupies e.work[42],
	// so every one of them joins nextItem deterministically.
	close(gate2)

	// Barrier: poll e.work[42].count until all N-1 other calls have joined
	// nextItem. With the two-gate release above, this is now a monotonic
	// counter that is guaranteed to reach N-1 (all bulk callers join
	// nextItem). It is bounded by a deadline so a regression fails loudly
	// instead of hanging until the 10-minute go test panic. e.mutex is
	// released before acquiring item.mutex to avoid deadlock with the
	// production locking order (item.mutex → e.mutex).
	deadline := time.Now().Add(10 * time.Second)
	for {
		e.mutex.Lock()
		item := e.work[42]
		e.mutex.Unlock()
		if item != nil {
			item.mutex.Lock()
			count := item.count
			item.mutex.Unlock()
			if count >= N-1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("barrier timed out: expected %d joiners on nextItem, workCalls=%d", N-1, workCalls.Load())
		}
		runtime.Gosched()
	}

	// All N calls have joined. Release the work.
	close(releaseWork)
	wg.Wait()

	// With ExclusiveWait=0, nextItem is created before work runs, so calls
	// that arrive during work join nextItem. After work 1 completes, exactly
	// one more call starts work 2 (instant), and the remaining N-2 calls
	// share work 2's result. Total: 2 work executions for 100 calls.
	if workCalls := workCalls.Load(); workCalls != 2 {
		t.Errorf("expected exactly 2 work calls (N=%d debounced via Exclusive.Call), got %d", N, workCalls)
	}

	waitForNoWork(t, &e)
}

func TestExclusive_Call_nilReceiver(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		if recover() == nil {
			t.Error("expected panic")
		}
	}()
	var e *Exclusive
	e.Call(1, func() (any, error) {
		return nil, nil
	})
	t.Error("should not reach")
}

func TestExclusive_Call_nilValue(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		if recover() == nil {
			t.Error("expected panic")
		}
	}()
	var e Exclusive
	e.Call(1, nil)
	t.Error("should not reach")
}

// TestExclusive_CallAfter verifies that CallAfter's wait window batches
// concurrent calls into a single work execution.
//
// Mechanics with ExclusiveWait=1s:
//  1. N=100 goroutines are gated, then released simultaneously.
//  2. All call e.CallAfter(42, work, 1*time.Second).
//  3. The first call creates the item; its goroutine sets item.running=true,
//     then sleeps for 1s (exclusive.go:254-260) with item.mutex UNLOCKED.
//  4. During this sleep, all other calls find the original item in e.work[42]
//     (nextItem hasn't been created yet), join it, and their goroutines wait
//     for item.running=false.
//  5. After 1s, the goroutine creates nextItem, releases mutex, calls work.
//     The work function blocks on releaseWork and signals "started".
//  6. The test receives "started" and verifies e.work[42].count == 0 (nextItem
//     has zero joiners — all calls are on the original item). This is the
//     deterministic confirmation that no call arrived late.
//  7. The test closes releaseWork. Work completes; resolve sets
//     item.complete=true and item.running=false. All N goroutines see
//     item.complete=true and share the single result.
//
// The 1s wait is a generous scheduling assumption: all 100 gated
// goroutines are released together and arrive in milliseconds, so even
// with scheduling jitter they join the original item well within the 1s
// window. The nextItem.count==0 check in step 6 is the verification that
// no call arrived late (after nextItem was created) and joined nextItem;
// if that ever happened the count would be > 0 and the test would fail.
// workCalls==1 is the exact expected count.
func TestExclusive_CallAfter(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const N = 100
	var (
		e           Exclusive
		workCalls   atomic.Int32
		releaseWork = make(chan struct{})
		started     = make(chan struct{}, 1)
		wg          sync.WaitGroup
	)
	wg.Add(N)

	// Gate all goroutines so they call e.CallAfter simultaneously
	gate := make(chan struct{})
	for range N {
		go func() {
			defer wg.Done()
			<-gate
			v, err := e.CallAfter(42, func() (any, error) {
				workCalls.Add(1)
				select {
				case started <- struct{}{}:
				default:
				}
				<-releaseWork
				return "shared", nil
			}, time.Second)
			if err != nil {
				t.Error(err)
			}
			if v != "shared" {
				t.Errorf("expected 'shared', got %v", v)
			}
		}()
	}

	// Release all goroutines simultaneously
	close(gate)

	// Wait for the 1s wait to expire and the work to start (the work
	// function blocks on releaseWork and signals "started").
	<-started

	// Deterministic verification: nextItem should have ZERO joiners, proving
	// all calls joined the original item during the wait window. If any call
	// arrived late (after nextItem was created), it would join nextItem and
	// this count would be > 0, failing the test.
	e.mutex.Lock()
	nextItem := e.work[42]
	e.mutex.Unlock()
	if nextItem != nil {
		nextItem.mutex.Lock()
		nextCount := nextItem.count
		nextItem.mutex.Unlock()
		if nextCount != 0 {
			t.Errorf("expected 0 calls on nextItem (all should be on original item), got %d", nextCount)
		}
	}

	// Release the work — all N calls share this single execution.
	close(releaseWork)
	wg.Wait()

	if workCalls := workCalls.Load(); workCalls != 1 {
		t.Errorf("expected exactly 1 work call (N=%d calls batched via 1s wait), got %d", N, workCalls)
	}

	waitForNoWork(t, &e)
}

func TestExclusive_CallAfter_adjustedWait(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		e Exclusive
		c int
		d = make(chan struct{})
	)

	ts := time.Now()

	go func() {
		e.Call(1, func() (any, error) {
			c++
			close(d)
			time.Sleep(time.Millisecond * 500)
			return nil, nil
		})
	}()

	<-d

	e.CallAfter(1, func() (any, error) {
		c++
		return nil, nil
	}, time.Millisecond*800)

	diff := time.Now().Sub(ts)

	if diff > time.Millisecond*900 || diff < time.Millisecond*700 {
		t.Error(diff)
	}

	if c != 2 {
		t.Error(c)
	}
}

func TestExclusive_CallAfter_adjustedWaitSmaller(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		e Exclusive
		c int
		d = make(chan struct{})
	)

	ts := time.Now()

	go func() {
		e.Call(1, func() (any, error) {
			c++
			close(d)
			time.Sleep(time.Millisecond * 500)
			return nil, nil
		})
	}()

	<-d

	e.CallAfter(1, func() (any, error) {
		c++
		return nil, nil
	}, time.Millisecond*100)

	diff := time.Now().Sub(ts)

	if diff < time.Millisecond*400 || diff > time.Millisecond*600 {
		t.Error(diff)
	}

	if c != 2 {
		t.Error(c)
	}
}

func TestCallAsync(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	for range 10 {
		var (
			exclusive   Exclusive
			outcomeChan <-chan *ExclusiveOutcome
			mutex       sync.Mutex
			locked      = true
			calls       int
		)
		exclusive.CallAsync(1, func() (any, error) {
			time.Sleep(time.Millisecond * 200)
			mutex.Lock()
			locked = false
			mutex.Unlock()
			return nil, nil
		})
		for y := range 1000 {
			func(y int) {
				outcomeChan = exclusive.CallAsync(1, func() (any, error) {
					mutex.Lock()
					if locked {
						t.Error(locked)
					}
					calls++
					mutex.Unlock()
					time.Sleep(time.Millisecond * 250)
					return y, nil
				})
			}(y)
		}
		outcome := <-outcomeChan
		if outcome.Result != 1000-1 || outcome.Error != nil {
			t.Error(outcome)
		}
		if v := <-outcomeChan; v != nil {
			t.Fatal(v)
		}
		mutex.Lock()
		if calls != 1 {
			t.Error(calls)
		}
		mutex.Unlock()
	}
}

func TestExclusive_Start(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		mutex   sync.Mutex
		counter int
		final   int
		e       Exclusive
		wg      sync.WaitGroup
	)
	wg.Add(10)
	for range 10 {
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 5)
			for range 10 {
				e.Start(nil, func() (i any, e error) {
					time.Sleep(time.Millisecond * 200)
					mutex.Lock()
					counter++
					final = counter
					mutex.Unlock()
					return
				})
				time.Sleep(time.Millisecond * 100)
			}
		}()
	}
	wg.Wait()
	time.Sleep(time.Millisecond * 300)
	mutex.Lock()
	if counter != final {
		t.Fatal(counter, final)
	}
	if final != 6 {
		t.Fatal(final)
	}
}

func TestExclusive_StartAfter(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		mutex   sync.Mutex
		counter int
		final   int
		e       Exclusive
		value   = new(int)
		err     = errors.New(`err`)
	)
	e.StartAfter(
		nil,
		func() (any, error) {
			t.Error(`bad`)
			panic(`bad`)
		},
		time.Millisecond*100,
	)
	outcome := e.CallAsync(
		nil,
		func() (i any, e error) {
			t.Error(`bad`)
			panic(`bad`)
		},
	)
	e.Start(
		nil,
		func() (any, error) {
			mutex.Lock()
			counter++
			final = counter
			mutex.Unlock()
			return value, err
		},
	)
	if outcome := <-outcome; outcome.Result != value || outcome.Error != err {
		t.Error(outcome)
	}
	mutex.Lock()
	if counter != 1 || counter != final {
		t.Fatal(counter)
	}
	mutex.Unlock()
	time.Sleep(time.Millisecond * 200)
	mutex.Lock()
	if counter != 1 || counter != final {
		t.Fatal(counter)
	}
	mutex.Unlock()
}

var benchmarkOutput any

func BenchmarkExclusive_outcomeContention(b *testing.B) {
	for _, tc := range [...]struct {
		Name  string
		Count int
	}{
		{
			Name:  `count 32768`,
			Count: 1 << 15,
		},
		{
			Name:  `count 16384`,
			Count: 1 << 14,
		},
		{
			Name:  `count 8192`,
			Count: 1 << 13,
		},
		{
			Name:  `count 1024`,
			Count: 1 << 10,
		},
		{
			Name:  `count 512`,
			Count: 1 << 9,
		},
		{
			Name:  `count 256`,
			Count: 1 << 8,
		},
		{
			Name:  `count 16`,
			Count: 1 << 4,
		},
		{
			Name:  `count 1`,
			Count: 1,
		},
	} {
		b.Run(tc.Name, func(b *testing.B) {
			b.StopTimer()
			var r int
			for n := 0; n < b.N; n++ {
				func() {
					var exclusive Exclusive

					// initial set up
					initialIn := make(chan struct{})
					defer close(initialIn)
					initialOut := make(chan struct{})
					defer close(initialOut)
					initialOutcome := exclusive.CallWithOptions(ExclusiveValue(func() (any, error) {
						initialIn <- struct{}{}
						<-initialOut
						return 123, nil
					}))
					defer func() {
						v := <-initialOutcome
						r = v.Result.(int)
					}()
					<-initialIn

					item := exclusive.work[nil]

					in := make(chan struct{})
					defer close(in)

					var wg sync.WaitGroup
					wg.Add(tc.Count)

					for i := 0; i < tc.Count; i++ {
						go func() {
							v := <-exclusive.CallWithOptions(ExclusiveValue(func() (any, error) {
								in <- struct{}{}
								return 456, nil
							}))
							r = v.Result.(int)
							wg.Done()
						}()
					}

					item.mutex.Lock()
					for item.count != tc.Count {
						item.mutex.Unlock()
						time.Sleep(time.Microsecond)
						item.mutex.Lock()
					}
					item.mutex.Unlock()

					b.StartTimer()

					initialOut <- struct{}{}
					<-in
					wg.Wait()

					b.StopTimer()
				}()
			}
			benchmarkOutput = r
		})
	}
}

func TestExclusive_CallWithOptions_startInitialCase(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		x     Exclusive
		calls atomic.Int32
	)
	if v := x.CallWithOptions(ExclusiveStart(true), ExclusiveWork(func(resolve func(result any, err error)) { calls.Add(1) })); v != nil {
		t.Error()
	}
	for i := time.Duration(0); i < 1000 && calls.Load() == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	if v := calls.Load(); v != 1 {
		t.Error(v)
	}
}

func TestExclusive_CallWithOptions_resolveNotCalled(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	v := <-new(Exclusive).CallWithOptions(ExclusiveWork(func(resolve func(result any, err error)) {}))
	if v.Error != errResolveNotCalled || v.Result != nil {
		t.Error(v)
	}
}

func TestExclusiveRateLimit(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const (
		num  = 100
		wait = time.Millisecond * 500
		min  = time.Millisecond * 60
	)
	var (
		x       Exclusive
		called  atomic.Int64
		shorter atomic.Int64
		wg      sync.WaitGroup
	)
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	wg.Add(num)
	for range num {
		go func() {
			var last int64
			for {
				ts := time.Now()
				v := <-x.CallWithOptions(
					ExclusiveRateLimit(ctx, min),
					ExclusiveWork(func(resolve func(result any, err error)) { resolve(called.Add(1), nil) }),
				)
				if d := time.Since(ts); d <= min/2 {
					shorter.Add(1)
				}
				if v.Error != nil {
					if v.Error != context.DeadlineExceeded {
						t.Error(v)
					}
					break
				}
				this := v.Result.(int64)
				if this <= last {
					t.Error(this, last)
				}
				last = this
			}
			wg.Done()
		}()
	}
	<-ctx.Done()
	ts := time.Now()
	wg.Wait()
	if d := time.Since(ts); d > time.Millisecond*10 {
		t.Error(d)
	}
	if v := shorter.Load(); v <= num/2 {
		t.Error(v)
	}
	if v := math.Round(math.Abs(float64(called.Load()) - float64(wait)/float64(min))); v > 1 {
		t.Error(v)
	}
	//t.Log(atomic.LoadInt64(&called), atomic.LoadInt64(&shorter))
}
