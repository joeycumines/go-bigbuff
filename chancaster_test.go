package bigbuff

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	b.Send(1)
	wg.Wait()
	fmt.Println(`state after send:`, stateStr())

	// note: this does nothing
	b.Send(213)

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
	b.Send(2)
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

	b.Send(3)

	wg.Wait()

	fmt.Println(`final num receivers:`, stateStr())

	//output:
	//state before send: receivers: 3, remaining: -2147483644, state: 00000000000000000000000000000011|00000000000000000000000000000011
	//received: 1
	//received: 1
	//received: 1
	//state after send: receivers: 0, remaining: -2147483647, state: 00000000000000000000000000000000|00000000000000000000000000000000
	//received: 2
	//received: 2
	//received: 2
	//received: 3
	//state after canceling mid send: receivers: 2, remaining: 2, state: 00000000000000000000000000000010|10000000000000000000000000000001
	//received: 3
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
