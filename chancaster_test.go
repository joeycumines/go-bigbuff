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
	wg.Add(1)
	go func() {
		defer wg.Done()
		cycleWaiter(fullCycleComplete)
	}()

	// start some additional waiters (these don't do anything useful, just to demonstrate concurrency is ok)
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cycleWaiter(nil)
		}()
	}

	// start the single sender (what is broadcasting the cycles)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			time.Sleep(time.Millisecond)
			started.Send(struct{}{})
			ended.Send(struct{}{})
		}
	}()

	// wait for 10 full cycles, as handled by the first waiter
	for i := range 10 {
		fmt.Printf("handled %d cycles so far...\n", i+1)
	}

	// cleanup
	cancel()
	wg.Wait()

	fmt.Println(`OK!`)

	//output:
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 3 {
			fmt.Println(`received:`, <-b.C)
		}
	}()
	send(1)
	wg.Wait()
	fmt.Println(`state after send:`, stateStr())

	// note: this does nothing
	send(213)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 3 {
			fmt.Println(`received:`, <-b.C)
		}
	}()
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println(`received:`, <-b.C)
		b.Add(-1)
		fmt.Println(`state after canceling mid send:`, stateStr())
		fmt.Println(`received:`, <-b.C)
	}()

	send(3)

	wg.Wait()

	fmt.Println(`final num receivers:`, stateStr())

	//output:
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

var dontElideMe any

func BenchmarkChanCaster_Send_waitForNextFullCycle(b *testing.B) {
	b.ReportAllocs()

	startedCount, endedCount, cycleCount := testChanCasterSendWaitForNextFullCycle(1_000, func(started, ended *ChanCaster[chan struct{}, struct{}]) {
		b.ResetTimer()

		for range b.N {
			started.Send(struct{}{})
			ended.Send(struct{}{})
		}

		b.StopTimer()
	})

	dontElideMe = fmt.Sprintf(`startedCount=%d, endedCount=%d, cycleCount=%d`, startedCount, endedCount, cycleCount)
	b.Log(dontElideMe)
}

func TestChanCaster_Send_waitForNextFullCycle(t *testing.T) {
	const (
		numReceivers     = 100
		actualCycleCount = 50_000
		minCycleCount    = actualCycleCount / 350
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
	if endedCount <= cycleCount {
		t.Errorf(`expected endedCount>%d, got %d`, cycleCount, endedCount)
	}
	if adjustedCycleCount < minCycleCount {
		t.Errorf(`expected adjustedCycleCount>=%d, got %d`, minCycleCount, adjustedCycleCount)
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
	wg.Add(1)
	go func() {
		assertAdd(-2, 3)
		wg.Done()
	}()
	c.C <- 0
	c.C <- 0
	wg.Wait()
	assertState(3, math.MaxInt32+3)

	wg.Add(1)
	go func() {
		assertAdd(-3, 0)
		wg.Done()
	}()
	c.C <- 0
	c.C <- 0
	c.C <- 0
	wg.Wait()
	assertState(0, math.MaxInt32+0)

	c.state.Store(math.MaxInt32<<32 | (math.MaxInt32 * 2))
	assertState(math.MaxInt32, math.MaxInt32*2)

	assertAdd(0, math.MaxInt32)
	assertState(math.MaxInt32, math.MaxInt32*2)

	wg.Add(1)
	go func() {
		assertAdd(-1, math.MaxInt32-1)
		wg.Done()
	}()
	c.C <- 0
	wg.Wait()
	assertState(math.MaxInt32-1, math.MaxInt32*2-1)

	wg.Add(1)
	go func() {
		assertAdd(-299, math.MaxInt32-300)
		wg.Done()
	}()
	for range 299 {
		c.C <- 0
	}
	wg.Wait()
	assertState(math.MaxInt32-300, math.MaxInt32*2-300)
}

func FuzzChanCaster_Add(f *testing.F) {
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					case caster.C <- struct{}{}:
						count--
					}
				}
			}()
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
			wg.Add(1)
			go func() {
				defer wg.Done()
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
			}()
		}

		if validState && hi != 0 {
			n := dec
			if !validDec {
				n = 1
			}

			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
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
				}()
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

func storeChanCasterState[C chan V, V any](t *testing.T, caster *ChanCaster[C, V], hi, lo uint32) {
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
	const n = 500
	c := NewChanCaster(make(chan struct{}, n*2))
	c.Add(1)
	c.mutex.Lock()
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			c.Send(struct{}{})
		}()
	}
	time.Sleep(time.Millisecond * 10)
	if len(c.C) != 0 {
		t.Error(`expected no values prior to unlock`)
	}
	c.mutex.Unlock()
	wg.Wait()
	if len(c.C) != 1 {
		t.Error(`expected 1 value after unlock`)
	}
	if c.state.Load() != 0 {
		t.Error(`expected state to be 0`)
	}
}

var bmrInt int

func BenchmarkChanCaster_highContention(b *testing.B) {
	const numReceivers = 100_000

	c := NewChanCaster(make(chan struct{}))

	stop := make(chan struct{})

	var allStopped sync.WaitGroup
	allStopped.Add(numReceivers)

	var ready sync.WaitGroup

	for i := 0; i < numReceivers; i++ {
		go func() {
			defer allStopped.Done()
			for {
				select {
				case <-stop:
					return
				case <-c.C:
				}
				ready.Done()
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Add(numReceivers)
		ready.Add(numReceivers)
		bmrInt = c.Send(struct{}{})
		if bmrInt != numReceivers {
			b.Fatalf(`expected %d, got %d`, numReceivers, bmrInt)
		}
		ready.Wait()
	}
	b.StopTimer()

	close(stop)
	allStopped.Wait()
}
