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
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// This example demonstrates how two channels and the [ChanCaster]
// implementation may be used to wait for the next iteration that started after
// waiting was started, notably supporting cancellation, e.g. via [context].
//
// In this scenario, there is a single iterative process performing the
// broadcasting. Were it necessary to support multiple processes, the pattern
// would require adjusting, e.g. to associate start and end signals.
//
// See also [BenchmarkChanCaster_Send_waitForNextFullCycle].
func ExampleChanCaster_waitForNextFullCycle() {
	defer checkNumGoroutines(nil)

	started := NewChanCaster(make(chan struct{}))
	ended := NewChanCaster(make(chan struct{}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// wait for cycles until cancellation, send to fullCycleComplete each time,
	// if it is non-nil
	cycleWaiter := func(fullCycleComplete chan<- struct{}) {
	CycleLoop:
		for ctx.Err() == nil {
			// register receives for both channels (note: we need to have
			// registered with ended before we receive from started)
			started.Add(1)
			ended.Add(1)

			// receive from started, also handling cancellation, and
			// receiving then re-registering to receive from ended
			// (we might get ended before started)
		StartedLoop:
			for {
				select {
				case <-ctx.Done():
					started.Add(-1)
					ended.Add(-1)
					break CycleLoop
				case <-ended.C:
					ended.Add(1) // re-register
				case <-started.C:
					break StartedLoop
				}
			}

			// receive from ended, also handling cancellation
		EndedLoop:
			for {
				select {
				case <-ctx.Done():
					ended.Add(-1)
					break CycleLoop
				case <-ended.C:
					break EndedLoop
				}
			}

			// send to fullCycleComplete, if it is non-nil
			if fullCycleComplete != nil {
				select {
				case <-ctx.Done():
					break CycleLoop
				case fullCycleComplete <- struct{}{}:
				}
			}
		}
	}

	var wg sync.WaitGroup

	// start waiter to send to a channel each cycle (that it detects - might drop some, if they start prior to re-registering)
	fullCycleComplete := make(chan struct{})
	wg.Go(func() {
		cycleWaiter(fullCycleComplete)
	})

	// start some additional waiters (these don't do anything useful, just to demonstrate concurrency is ok)
	for range 100 {
		wg.Go(func() {
			cycleWaiter(nil)
		})
	}

	// start the single sender (what is broadcasting the cycles)
	wg.Go(func() {
		for ctx.Err() == nil {
			time.Sleep(time.Millisecond)
			started.Send(struct{}{})
			ended.Send(struct{}{})
		}
	})

	// wait for 10 full cycles, as handled by the first waiter
	for i := range 10 {
		<-fullCycleComplete
		fmt.Printf("handled %d cycles so far...\n", i+1)
	}

	// cleanup
	cancel()
	wg.Wait()

	fmt.Println(`OK!`)

	// Output:
	//handled 1 cycles so far...
	//handled 2 cycles so far...
	//handled 3 cycles so far...
	//handled 4 cycles so far...
	//handled 5 cycles so far...
	//handled 6 cycles so far...
	//handled 7 cycles so far...
	//handled 8 cycles so far...
	//handled 9 cycles so far...
	//handled 10 cycles so far...
	//OK!
}

func ExampleChanCaster_decrementReceiversDuringSend() {
	defer checkNumGoroutines(nil)

	b := NewChanCaster(make(chan int))
	var wg sync.WaitGroup

	stateStr := func() string {
		state := b.state.Load()
		receivers := int32(state >> 32)
		remaining := int64(uint32(state)) - math.MaxInt32
		s := strconv.FormatUint(state, 2)
		if len(s) < 64 {
			s = fmt.Sprintf("%0[2]*[1]s", s, 64)
		}
		return fmt.Sprintf(`receivers: %d, remaining: %d, state: %s|%s`, receivers, remaining, s[:32], s[32:])
	}

	send := func(v int) {
		fmt.Printf("sent value %d was received by non-decrement %d times\n", v, b.Send(v))
	}

	// without cancels, simple usage (not real usage pattern)

	for range 3 {
		b.Add(1)
	}
	fmt.Println(`state before send:`, stateStr())
	wg.Go(func() {
		for range 3 {
			fmt.Println(`received:`, <-b.C)
		}
	})
	send(1)
	wg.Wait()
	fmt.Println(`state after send:`, stateStr())

	// note: this does nothing
	send(213)

	wg.Go(func() {
		for range 3 {
			fmt.Println(`received:`, <-b.C)
		}
	})
	for range 3 {
		b.Add(1)
	}
	send(2)
	wg.Wait()

	wg.Wait()

	// simulating cancel mid send (not real usage pattern)

	for range 3 {
		b.Add(1)
	}

	wg.Go(func() {
		fmt.Println(`received:`, <-b.C)
		b.Add(-1)
		fmt.Println(`state after canceling mid send:`, stateStr())
		fmt.Println(`received:`, <-b.C)
	})

	send(3)

	wg.Wait()

	fmt.Println(`final num receivers:`, stateStr())

	// Unordered output:
	//state before send: receivers: 3, remaining: -2147483644, state: 00000000000000000000000000000011|00000000000000000000000000000011
	//received: 1
	//received: 1
	//received: 1
	//sent value 1 was received by non-decrement 3 times
	//state after send: receivers: 0, remaining: -2147483647, state: 00000000000000000000000000000000|00000000000000000000000000000000
	//sent value 213 was received by non-decrement 0 times
	//received: 2
	//received: 2
	//received: 2
	//sent value 2 was received by non-decrement 3 times
	//received: 3
	//state after canceling mid send: receivers: 2, remaining: 2, state: 00000000000000000000000000000010|10000000000000000000000000000001
	//received: 3
	//sent value 3 was received by non-decrement 2 times
	//final num receivers: receivers: 0, remaining: -2147483647, state: 00000000000000000000000000000000|00000000000000000000000000000000
}

// TestChanCaster_Send_waitForNextFullCycle_stress is a high-contention stress
// test that verifies no deadlocks, panics, or lost values under load. It uses
// intentionally lenient bounds because the exact cycle count depends on
// scheduling. The deterministic variant is
// TestChanCaster_Send_waitForNextFullCycle_deterministic.
func TestChanCaster_Send_waitForNextFullCycle_stress(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const (
		numReceivers     = 100
		actualCycleCount = 50_000
		// minCycleCount is intentionally lenient (1) rather than a tight bound
		// such as actualCycleCount/350 (~142). Empirically, adjustedCycleCount
		// (cycleCount/numReceivers) varies widely with scheduling: it was
		// observed at 157, 69 and 55 across three consecutive runs on this
		// machine, because the 100 receivers cannot keep up with 50_000 sends
		// within the test window before cancellation. A tight bound would
		// therefore flake under load. The meaningful invariants are still
		// strongly enforced: startedCount must equal cycleCount exactly, and
		// endedCount must be >= cycleCount.
		minCycleCount = 1
	)
	startedCount, endedCount, cycleCount := testChanCasterSendWaitForNextFullCycle(numReceivers, func(started, ended *ChanCaster[chan struct{}, struct{}]) {
		for range actualCycleCount {
			started.Send(struct{}{})
			ended.Send(struct{}{})
		}
	})
	adjustedCycleCount := cycleCount / numReceivers
	t.Logf(`startedCount=%d, endedCount=%d, cycleCount=%d, adjustedCycleCount=%d`, startedCount, endedCount, cycleCount, adjustedCycleCount)
	if startedCount != cycleCount {
		t.Errorf(`expected startedCount=%d, got %d`, cycleCount, startedCount)
	}
	// endedCount counts ALL receives from ended.C, both in StartedLoop (where
	// a receiver may pick up an ended value from a previous cycle before
	// receiving the current started value) and in EndedLoop (where it
	// receives the ended value that completes the current cycle). cycleCount
	// counts only complete cycles (started received in StartedLoop + ended
	// received in EndedLoop). Equality (endedCount == cycleCount) is possible
	// when no receiver picks up extra ended values in StartedLoop, which
	// occurs when the scheduler delivers started values before ended values.
	// endedCount > cycleCount occurs when receivers pick up extra ended values
	// in StartedLoop. Therefore endedCount >= cycleCount is the correct
	// invariant — it is a lower bound, not a strict inequality.
	if endedCount < cycleCount {
		t.Errorf(`expected endedCount>=%d, got %d`, cycleCount, endedCount)
	}
	if adjustedCycleCount < minCycleCount {
		t.Errorf(`expected adjustedCycleCount>=%d, got %d`, minCycleCount, adjustedCycleCount)
	}
}

// TestChanCaster_Send_waitForNextFullCycle_deterministic verifies exact cycle
// counting using barrier synchronization. Unlike the stress test, this test
// uses a small number of receivers (2) and two barrier channels to ensure
// deterministic ordering:
//
//   - readyForCycle: each receiver signals after registering (Add(1) for both
//     started and ended) and before entering StartedLoop. The test waits for
//     all signals before calling started.Send, guaranteeing that all receivers
//     are registered and ready to receive.
//
//   - enteredEnded: each receiver signals after receiving started (breaking
//     out of StartedLoop) and before entering EndedLoop. The test waits for
//     all signals before calling ended.Send, guaranteeing that all receives
//     from ended.C happen in EndedLoop, not StartedLoop.
//
// This eliminates the timing-dependent extra ended receives that occur in the
// stress test, making all counts exact.
func TestChanCaster_Send_waitForNextFullCycle_deterministic(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const (
		numReceivers = 2
		numCycles    = 10
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		started                              = NewChanCaster(make(chan struct{}))
		ended                                = NewChanCaster(make(chan struct{}))
		startedCount, endedCount, cycleCount uint64
		readyWG                              sync.WaitGroup
		doneWG                               sync.WaitGroup
		// Barriers (buffered so sends don't block if the test is between cycles)
		readyForCycle = make(chan struct{}, numReceivers)
		enteredEnded  = make(chan struct{}, numReceivers)
	)

	readyWG.Add(numReceivers)
	doneWG.Add(numReceivers)

	for range numReceivers {
		go func() {
			defer doneWG.Done()
			var ready bool
		CycleLoop:
			for ctx.Err() == nil {
				started.Add(1)
				ended.Add(1)

				if !ready {
					ready = true
					readyWG.Done()
				}

				// Signal that we've registered and are entering StartedLoop
				readyForCycle <- struct{}{}

			StartedLoop:
				for {
					select {
					case <-ctx.Done():
						started.Add(-1)
						ended.Add(-1)
						break CycleLoop
					case <-ended.C:
						atomic.AddUint64(&endedCount, 1)
						ended.Add(1)
					case <-started.C:
						atomic.AddUint64(&startedCount, 1)
						break StartedLoop
					}
				}

				// Signal that we've received started and are entering EndedLoop
				enteredEnded <- struct{}{}

			EndedLoop:
				for {
					select {
					case <-ctx.Done():
						ended.Add(-1)
						break CycleLoop
					case <-ended.C:
						atomic.AddUint64(&endedCount, 1)
						break EndedLoop
					}
				}

				atomic.AddUint64(&cycleCount, 1)
			}
		}()
	}

	readyWG.Wait()

	for range numCycles {
		// Wait for all receivers to register and enter StartedLoop
		for range numReceivers {
			<-readyForCycle
		}

		// Send started — blocks until all receivers receive in StartedLoop
		started.Send(struct{}{})

		// Wait for all receivers to enter EndedLoop
		for range numReceivers {
			<-enteredEnded
		}

		// Send ended — all receives happen in EndedLoop, not StartedLoop
		ended.Send(struct{}{})
	}

	cancel()
	doneWG.Wait()

	expected := uint64(numCycles * numReceivers)
	if startedCount != expected {
		t.Errorf(`expected startedCount=%d, got %d`, expected, startedCount)
	}
	if endedCount != expected {
		t.Errorf(`expected endedCount=%d, got %d`, expected, endedCount)
	}
	if cycleCount != expected {
		t.Errorf(`expected cycleCount=%d, got %d`, expected, cycleCount)
	}
}

func testChanCasterSendWaitForNextFullCycle(numReceivers int, send func(started, ended *ChanCaster[chan struct{}, struct{}])) (startedCount, endedCount, cycleCount uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		started = NewChanCaster(make(chan struct{}))
		ended   = NewChanCaster(make(chan struct{}))
		//nextCycle atomic.Int32
		readyWG sync.WaitGroup
		doneWG  sync.WaitGroup
	)

	readyWG.Add(numReceivers)
	doneWG.Add(numReceivers)
	for range numReceivers {
		go func() {
			var ready bool
		CycleLoop:
			for ctx.Err() == nil {
				// register receives for both channels (note: we need to have
				// registered with ended before we receive from started)
				started.Add(1)
				ended.Add(1)

				// ready up after we register our first receives
				if !ready {
					ready = true
					readyWG.Done()
				}

				// receive from started, also handling cancellation, and
				// receiving then re-registering to receive from ended
				// (we might get ended before started)
			StartedLoop:
				for {
					select {
					case <-ctx.Done():
						started.Add(-1)
						ended.Add(-1)
						break CycleLoop
					case <-ended.C:
						atomic.AddUint64(&endedCount, 1)
						ended.Add(1) // re-register
					case <-started.C:
						atomic.AddUint64(&startedCount, 1)
						break StartedLoop
					}
				}

				// receive from ended, also handling cancellation
			EndedLoop:
				for {
					select {
					case <-ctx.Done():
						ended.Add(-1)
						break CycleLoop
					case <-ended.C:
						atomic.AddUint64(&endedCount, 1)
						break EndedLoop
					}
				}

				atomic.AddUint64(&cycleCount, 1)
			}
			doneWG.Done()
		}()
	}

	readyWG.Wait()

	send(started, ended)

	cancel()
	doneWG.Wait()

	return
}

func TestChanCaster_Add_success(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanCaster(make(chan int))

	assertState := func(hi, lo uint32) {
		t.Helper()
		expected := strconv.FormatUint(uint64(lo), 2)
		if len(expected) < 32 {
			expected = fmt.Sprintf("%0[2]*[1]s", expected, 32)
		}
		expected = strconv.FormatUint(uint64(hi), 2) + expected
		if len(expected) < 64 {
			expected = fmt.Sprintf("%0[2]*[1]s", expected, 64)
		}
		actual := strconv.FormatUint(c.state.Load(), 2)
		if len(actual) < 64 {
			actual = fmt.Sprintf("%0[2]*[1]s", actual, 64)
		}
		if expected != actual {
			t.Errorf(`expected state: %s, got: %s`, expected, actual)
		}
	}

	assertAdd := func(delta int, expected int) {
		t.Helper()
		if actual := c.Add(delta); actual != expected {
			t.Errorf(`expected %d, got %d`, expected, actual)
		}
	}

	assertState(0, 0)

	assertAdd(0, 0)
	assertState(0, 0)

	assertAdd(math.MaxInt32, math.MaxInt32)
	assertState(math.MaxInt32, math.MaxInt32)

	assertAdd(-math.MaxInt32, 0)
	assertState(0, 0)

	assertAdd(math.MaxInt32-1, math.MaxInt32-1)
	assertState(math.MaxInt32-1, math.MaxInt32-1)

	assertAdd(1, math.MaxInt32)
	assertState(math.MaxInt32, math.MaxInt32)

	assertAdd(1-math.MaxInt32, 1)
	assertState(1, 1)

	assertAdd(2, 3)
	assertState(3, 3)

	assertAdd(-1, 2)
	assertState(2, 2)

	assertAdd(-1, 1)
	assertState(1, 1)

	assertAdd(-1, 0)
	assertState(0, 0)

	c.state.Store(5<<32 | (math.MaxInt32 + 5))
	assertState(5, math.MaxInt32+5)
	assertAdd(0, 5)
	assertState(5, math.MaxInt32+5)

	var wg sync.WaitGroup
	wg.Go(func() {
		assertAdd(-2, 3)
	})
	c.C <- 0
	c.C <- 0
	wg.Wait()
	assertState(3, math.MaxInt32+3)

	wg.Go(func() {
		assertAdd(-3, 0)
	})
	c.C <- 0
	c.C <- 0
	c.C <- 0
	wg.Wait()
	assertState(0, math.MaxInt32+0)

	c.state.Store(math.MaxInt32<<32 | (math.MaxInt32 * 2))
	assertState(math.MaxInt32, math.MaxInt32*2)

	assertAdd(0, math.MaxInt32)
	assertState(math.MaxInt32, math.MaxInt32*2)

	wg.Go(func() {
		assertAdd(-1, math.MaxInt32-1)
	})
	c.C <- 0
	wg.Wait()
	assertState(math.MaxInt32-1, math.MaxInt32*2-1)

	wg.Go(func() {
		assertAdd(-299, math.MaxInt32-300)
	})
	for range 299 {
		c.C <- 0
	}
	wg.Wait()
	assertState(math.MaxInt32-300, math.MaxInt32*2-300)
}

func FuzzChanCaster_Add(f *testing.F) {
	// Note: checkNumGoroutines is intentionally omitted from fuzz tests.
	// The fuzz worker pool creates goroutines not controlled by the test
	// code, which may not have fully terminated when a cleanup runs,
	// producing false-positive leak reports. Fuzz tests detect bugs via
	// panics and timeouts, not via goroutine count heuristics.

	add := func(hi, lo uint32, delta int) {
		f.Add(hi, lo, delta)
	}

	add(0, 0, 0)
	add(0, 0, 1)
	add(0, 0, -1)
	add(0, 0, math.MaxInt32)
	add(0, 0, math.MaxInt32-1)
	add(0, 0, math.MaxInt32+1)
	add(math.MaxInt32, math.MaxInt32, 0)
	add(math.MaxInt32, math.MaxInt32, 0)
	add(math.MaxInt32, math.MaxInt32*2, 0)
	add(0, math.MaxInt32, 0)
	add(1, 0, 0)
	add(1, math.MaxInt32, 0)
	add(1, 1, -1)
	add(1, 1, 1)
	add(5, 5, 23023)
	add(1, math.MaxInt32+1, -1)
	add(1, math.MaxInt32+1, -2)
	add(math.MaxInt32, math.MaxInt32*2, -10_000)
	add(99, math.MaxInt32+99, -99)
	add(99, math.MaxInt32+99, -100)
	add(math.MaxInt32, math.MaxInt32, -math.MaxInt32)
	add(math.MaxInt32, math.MaxInt32, -math.MaxInt32-1)
	add(math.MaxUint32, math.MaxUint32, 0)
	add(math.MaxUint32-129239, math.MaxUint32-129239, 1)
	add(math.MaxUint32-129239, math.MaxUint32-129239, -1)
	add(math.MaxInt32+1, math.MaxInt32*2+1, -1)

	f.Fuzz(func(t *testing.T, hi, lo uint32, delta int) {
		caster := NewChanCaster(make(chan struct{}))

		storeChanCasterState(t, caster, hi, lo)

		// ensure we don't try and acquire the write lock, just because
		caster.mutex.RLock()
		defer caster.mutex.RUnlock()

		// high-level delta validity rules (also dependent on state validity):
		deltaValid := delta >= -math.MaxInt32 && delta <= math.MaxInt32 // one more check below

		var expected int64
		{
			v1 := new(big.Int).SetUint64(uint64(hi))
			v2 := new(big.Int).SetInt64(int64(delta))
			v1.Add(v1, v2)
			if deltaValid && (v1.Sign() < 0 || v1.Cmp(v2.SetUint64(math.MaxInt32)) > 0) {
				deltaValid = false
			}
			expected = v1.Int64()
		}

		// the rules for determining validity of the state (inclusive of no write lock) are simply:
		stateValid := hi <= math.MaxInt32 && (lo == hi || (delta <= 0 && lo == hi+math.MaxInt32))

		var success bool
		if !deltaValid || !stateValid { // expect panic
			defer func() {
				if success {
					t.Error(`expected panic, got success`)
				} else if v := fmt.Sprint(recover()); !strings.HasPrefix(v, `bigbuff: chancaster: add: `) {
					t.Errorf(`unexpected panic string prefix: %s`, v)
				}
			}()
		} else if delta < -100_000 && hi != lo {
			t.Skip(`skipping large negative delta fuzz case during send:`, hi, lo, delta)
		} else {
			var count int
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					case caster.C <- struct{}{}:
						count--
					}
				}
			})
			defer func() {
				close(stop)
				wg.Wait()
				if hi == lo {
					if count != 0 {
						t.Fatalf(`expected 0 receives, got %d`, count)
					}
				} else if count != delta {
					t.Fatalf(`expected %d receives, got %d`, delta, count)
				}
			}()
		}

		receivers := caster.Add(delta)
		success = true

		if receivers < 0 || receivers > math.MaxInt32 || expected != int64(receivers) {
			t.Fatalf(`expected valid, and equal to %d, got %d`, expected, receivers)
		}

		stateBytes := new(big.Int).SetUint64(caster.state.Load()).FillBytes(make([]byte, 8))
		expectedBytes := make([]byte, 8)
		new(big.Int).SetInt64(expected).FillBytes(expectedBytes[:4])
		{
			v := expected
			if lo != hi {
				v += math.MaxInt32
			}
			new(big.Int).SetInt64(v).FillBytes(expectedBytes[4:])
		}
		if !bytes.Equal(stateBytes, expectedBytes) {
			t.Fatalf(`expected state: %v, got: %v`, expectedBytes, stateBytes)
		}
	})
}

func FuzzChanCaster_Send(f *testing.F) {
	// Note: checkNumGoroutines is intentionally omitted from fuzz tests.
	// See FuzzChanCaster_Add for the rationale.

	add := func(hi, lo uint32, dec uint8) {
		f.Add(hi, lo, dec)
	}

	add(0, 0, 0)
	add(0, 0, 1)
	add(372, 372, 0)
	add(math.MaxInt32, math.MaxInt32*2, 0)
	add(0, math.MaxInt32, 0)
	add(1, 0, 0)
	add(1, math.MaxInt32, 0)
	add(1, 1, 1)
	add(5, 5, 222)
	add(1, math.MaxInt32+1, 1)
	add(1, math.MaxInt32+1, 2)
	add(math.MaxInt32, math.MaxInt32*2, 200)
	add(99, math.MaxInt32+99, 99)
	add(99, math.MaxInt32+99, 100)
	add(201, 201, 200)
	add(99, 99, 99)
	add(99, 99, 100)
	add(math.MaxInt32, math.MaxInt32*2, math.MaxUint8)
	add(1243, 923, 0)

	f.Fuzz(func(t *testing.T, hi, lo uint32, dec uint8) {
		timeout := time.NewTimer(time.Millisecond * 1000)
		defer timeout.Stop()

		expectedInt := rand.Int()
		expectedValue := &expectedInt

		caster := NewChanCaster(make(chan *int))
		defer close(caster.C)

		storeChanCasterState(t, caster, hi, lo)

		validState := hi <= math.MaxInt32 && lo == hi
		validDec := uint32(dec) <= hi
		expected := int64(hi) - int64(dec)
		if hi == 0 {
			expected = 0
		}

		if validState && !validDec && hi == 1 {
			t.Skip(`flappy test scenario`)
		}
		if validState && hi > 10_000 {
			t.Skip(`skipping too large hi fuzz case`)
		}

		var success bool
		if !validState || (!validDec && hi != 0) { // expect panic
			defer func() {
				if success {
					t.Error(`expected panic, got success`)
				} else if v := fmt.Sprint(recover()); !strings.HasPrefix(v, `bigbuff: chancaster: send: `) {
					t.Errorf(`unexpected panic string prefix: %s`, v)
				}
			}()
		}

		var ready, wg sync.WaitGroup
		wg.Add(1)
		ready.Add(1)

		firstPanicCh := make(chan struct{})
		var firstPanicOnce sync.Once

		if !validState || uint32(dec) == hi || hi == 0 {
			ready.Done()
		} else {
			wg.Go(func() {
				var readied bool
				n := expected
				if !validDec {
					n = int64(hi)
				}
				for range n {
					select {
					case v := <-caster.C:
						if v != expectedValue {
							t.Errorf(`expected %v, got %v`, expectedValue, v)
						}
					case <-timeout.C:
						t.Error(`timeout`)
						return
					}
					if !readied {
						readied = true
						ready.Done()
						time.Sleep(time.Millisecond * 30)
						if !validDec {
							// wait for panic
							select {
							case <-firstPanicCh:
							case <-timeout.C:
								t.Error(`timeout waiting for panic`)
								panic(`timeout waiting for panic`)
							}
						}
					}
				}
			})
		}

		if validState && hi != 0 {
			n := dec
			if !validDec {
				n = 1
			}

			for range n {
				wg.Go(func() {
					ready.Wait()

					var success bool
					if !validDec {
						// simulate the correct number of receives
						caster.state.Store(math.MaxInt32)

						defer func() {
							if success {
								t.Error(`dec: expected error, got success`)
								return
							}
							firstPanicOnce.Do(func() {
								close(firstPanicCh)
							})
							if v := fmt.Sprint(recover()); !strings.HasPrefix(v, `bigbuff: chancaster: add: `) {
								t.Errorf(`dec: unexpected panic string prefix: %s`, v)
							}
						}()
					}

					v := caster.Add(-1)
					if v < 0 || int64(v) >= int64(hi) || v > math.MaxInt32 || (validDec && int64(v) < int64(expected)) {
						t.Errorf(`expected ~%d, got %d`, hi-uint32(dec), v)
					}
					success = true
				})
			}
		}

		wg.Done()

		received := func() int {
			defer func() {
				ready.Wait()
				wg.Wait()
			}()
			return caster.Send(expectedValue)
		}()

		success = true

		if int64(received) != int64(expected) {
			t.Errorf(`expected %d, got %d`, expected, received)
		}

		time.Sleep(time.Millisecond * 3)
		select {
		case <-caster.C:
			t.Error(`expected no more values`)
		default:
		}
		if v := caster.state.Load(); v != 0 {
			t.Errorf(`expected state to be 0, got %d`, v)
		}
	})
}

func storeChanCasterState[C ~chan V, V any](t *testing.T, caster *ChanCaster[C, V], hi, lo uint32) {
	h := (*[4]byte)(unsafe.Pointer(&hi))
	l := (*[4]byte)(unsafe.Pointer(&lo))
	// note: want big endian, might need to flip each segment
	b := append(append(make([]byte, 0, 8), h[:]...), l[:]...)
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)
	switch buf {
	case [2]byte{0xCD, 0xAB}:
		// little endian
		slices.Reverse(b[:4])
		slices.Reverse(b[4:])
	case [2]byte{0xAB, 0xCD}:
		// big endian
	default:
		t.Fatal(`unexpected endianness`)
	}
	v := new(big.Int).SetBytes(b)
	state := v.Uint64()
	if v.Cmp(new(big.Int).SetUint64(state)) != 0 {
		t.Fatal(`unexpected state conversion`)
	}
	// note: we could've just done this:
	if state != uint64(hi)<<32|uint64(lo) {
		t.Fatal(`unexpected state conversion`)
	}
	caster.state.Store(state)
}

func TestChanCaster_Send_multipleRacingSenders(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const n = 500
	c := NewChanCaster(make(chan struct{}))
	c.Add(n)

	// Start n one-shot receivers and wait until they have all been started.
	// Each will promptly block on <-c.C; any transient lack of a receiver is
	// resolved as the goroutines schedule, because Send performs exactly one
	// send per registered receiver.
	var (
		received    atomic.Int32
		recvWg      sync.WaitGroup
		recvReadyWg sync.WaitGroup
	)
	recvWg.Add(n)
	recvReadyWg.Add(n)
	for range n {
		go func() {
			defer recvWg.Done()
			recvReadyWg.Done()
			<-c.C
			received.Add(1)
		}()
	}
	recvReadyWg.Wait()

	// Hold the ChanCaster mutex while spawning the senders, then release both
	// the goroutine gate and the mutex. The senders race for the mutex, but the
	// Go scheduler does not guarantee they all queue on it before the unlock
	// completes; the test below only relies on final invariants, not ordering.
	c.mutex.Lock()
	var (
		wg        sync.WaitGroup
		ready     sync.WaitGroup
		start     = make(chan struct{})
		totalSent atomic.Int32
	)
	wg.Add(n)
	ready.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			totalSent.Add(int32(c.Send(struct{}{})))
		}()
	}
	ready.Wait()
	close(start)
	c.mutex.Unlock()
	wg.Wait()

	// Wait for the one-shot receivers to drain the channel.
	recvWg.Wait()

	if c.state.Load() != 0 {
		t.Error(`expected state to be 0`)
	}
	if v := totalSent.Load(); v != int32(n) {
		t.Errorf(`expected %d total sends, got %d`, n, v)
	}
	if v := received.Load(); v != int32(n) {
		t.Errorf(`expected %d receives, got %d`, n, v)
	}
}

// TestChanCaster_panicWhenChannelBuffered verifies that constructing
// a ChanCaster with a buffered channel panics immediately.
func TestChanCaster_panicWhenChannelBuffered(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic when using a buffered channel, got none")
		} else if s := fmt.Sprint(r); s != `bigbuff: chancaster: must use an unbuffered channel` {
			t.Errorf("unexpected panic string: %s", s)
		}
	}()
	_ = NewChanCaster(make(chan int, 1)) // Should panic
}

// TestChanCaster_nilChannel verifies that a ChanCaster constructed with a nil
// channel is usable as normal — Send returns 0 (no receivers), Add manages
// state correctly, and the nil channel can be included in a select statement
// without ever being selected (standard Go nil channel semantics).
func TestChanCaster_nilChannel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	// Construction with a nil channel must NOT panic
	c := NewChanCaster((chan int)(nil))

	// C is nil
	if c.C != nil {
		t.Error("expected C to be nil")
	}

	// Send with no receivers returns 0 (fast path, no channel access)
	if n := c.Send(42); n != 0 {
		t.Errorf("expected 0 sends, got %d", n)
	}

	// Add(0) returns 0, no change
	if n := c.Add(0); n != 0 {
		t.Errorf("expected 0 receivers, got %d", n)
	}

	// Add(1) then Add(-1) works (state management, no channel access)
	c.Add(1)
	if n := c.Add(0); n != 1 {
		t.Errorf("expected 1 receiver, got %d", n)
	}
	c.Add(-1)
	if n := c.Add(0); n != 0 {
		t.Errorf("expected 0 receivers after add/remove, got %d", n)
	}

	// Send still returns 0 after add/remove cycle
	if n := c.Send(99); n != 0 {
		t.Errorf("expected 0 sends, got %d", n)
	}

	// Nil channel never fires in a select — other cases work normally
	other := make(chan int, 1)
	other <- 123
	select {
	case <-c.C:
		t.Error("expected nil channel case to never fire")
	case v := <-other:
		if v != 123 {
			t.Errorf("expected 123, got %d", v)
		}
	}

	// Nil channel with default also works
	select {
	case <-c.C:
		t.Error("expected nil channel case to never fire")
	default:
		// expected
	}
}

// TestChanCaster_newChanCaster_valid verifies that constructing a ChanCaster
// via NewChanCaster with a valid unbuffered channel produces a working caster.
func TestChanCaster_newChanCaster_valid(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanCaster(make(chan int))
	c.Add(1)

	var wg sync.WaitGroup
	wg.Go(func() {
		if v := <-c.C; v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	})

	if n := c.Send(42); n != 1 {
		t.Errorf("expected 1 send, got %d", n)
	}
	wg.Wait()

	if c.state.Load() != 0 {
		t.Error("expected state to be 0")
	}
}

// TestChanCaster_newChanCaster_namedChannelType verifies that the ~chan V
// constraint enables type inference with named bidirectional channel types.
func TestChanCaster_newChanCaster_namedChannelType(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	type MyChan chan int
	c := NewChanCaster(MyChan(make(chan int)))
	c.Add(1)

	var wg sync.WaitGroup
	wg.Go(func() {
		if v := <-c.C; v != 99 {
			t.Errorf("expected 99, got %d", v)
		}
	})

	if n := c.Send(99); n != 1 {
		t.Errorf("expected 1 send, got %d", n)
	}
	wg.Wait()

	if c.state.Load() != 0 {
		t.Error("expected state to be 0")
	}
}
