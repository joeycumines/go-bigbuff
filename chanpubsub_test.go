package bigbuff

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func ExampleChanPubSub_SubscribeContext() {
	defer checkNumGoroutines(nil)

	const (
		subscribers = 3
		messages    = 4
	)

	// using the factory function is mandatory, unlike ChanCaster
	c := NewChanPubSub(make(chan any))

	// the return value of send indicates how many subscribers received the message
	fmt.Println(`send result without subscribers:`, c.Send(`into the void`))

	var wg sync.WaitGroup
	wg.Add(subscribers)

	// start our subscribers
	counts := make([]int, subscribers)
	for i := range counts {
		// Subscriptions are registered immediately, meaning we don't drop any
		// messages. For this to be safe, we must (also) start iterating,
		// which, in this case, is handled in another goroutine.
		iter := c.SubscribeContext(nil)
		go func() {
			defer wg.Done()
			for v := range iter {
				counts[i]++
				fmt.Println(`subscriber received:`, v)
			}
		}()
	}

	// send some messages - they will all be received, by all subscribers
	for i := range messages {
		if n := c.Send(i); n != subscribers {
			panic(fmt.Sprintf("expected %d, got %d", subscribers, n))
		}
	}

	// closing the channel is allowed (optional, behaves as one might expect)
	close(c.C())

	wg.Wait() // wait for our subscribers to finish

	for i, n := range counts {
		fmt.Printf("subscriber %d received %d messages\n", i, n)
	}

	//output:
	//send result without subscribers: 0
	//subscriber received: 0
	//subscriber received: 0
	//subscriber received: 0
	//subscriber received: 1
	//subscriber received: 1
	//subscriber received: 1
	//subscriber received: 2
	//subscriber received: 2
	//subscriber received: 2
	//subscriber received: 3
	//subscriber received: 3
	//subscriber received: 3
	//subscriber 0 received 4 messages
	//subscriber 1 received 4 messages
	//subscriber 2 received 4 messages
}

func BenchmarkChanPubSub_highContention(b *testing.B) {
	const numReceivers = 100_000

	c := NewChanPubSub(make(chan struct{}))
	c.Add(numReceivers)

	stop := make(chan struct{})

	var allStopped sync.WaitGroup
	allStopped.Add(numReceivers)

	var count int32

	for i := 0; i < numReceivers; i++ {
		go func() {
			defer allStopped.Done()
			for {
				select {
				case <-stop:
					return
				case <-c.C():
				}
				atomic.AddInt32(&count, 1)
				c.Wait()
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v := atomic.LoadInt32(&count); v != 0 {
			b.Fatalf(`expected 0, got %d`, v)
		}
		bmrInt = c.Send(struct{}{})
		if bmrInt != numReceivers {
			b.Fatalf(`expected %d, got %d`, numReceivers, bmrInt)
		}
		if !atomic.CompareAndSwapInt32(&count, numReceivers, 0) {
			b.Fatalf(`expected %d, got %d`, numReceivers, atomic.LoadInt32(&count))
		}
	}
	b.StopTimer()

	close(stop)
	allStopped.Wait()
	c.Add(-numReceivers)
	if v := c.Send(struct{}{}); v != 0 {
		b.Error(`expected 0, got`, v)
	}
}

func BenchmarkChanPubSub_SubscribeContext_highContention(b *testing.B) {
	const numReceivers = 100_000

	c := NewChanPubSub(make(chan struct{}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var allStopped sync.WaitGroup
	allStopped.Add(numReceivers)

	var count int32

	for i := 0; i < numReceivers; i++ {
		iter := c.SubscribeContext(ctx)
		go func() {
			defer allStopped.Done()
			for range iter {
				atomic.AddInt32(&count, 1)
			}
		}()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bmrInt = c.Send(struct{}{})
		if bmrInt != numReceivers {
			b.Fatalf(`expected %d, got %d`, numReceivers, bmrInt)
		}
	}
	cancel()
	allStopped.Wait()
	b.StopTimer()

	if v := int(atomic.LoadInt32(&count)); v != b.N*numReceivers {
		b.Errorf(`expected %d, got %d`, b.N*numReceivers, v)
	}
}

// TestChanPubSub_basicSendReceive tests a single subscriber receiving all
// values sent, in order.
func TestChanPubSub_basicSendReceive(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int)) // unbuffered channel
	defer func() {
		// Ensure it hasn't been broken.
		select {
		case <-c.broken:
			t.Error("ChanPubSub is broken unexpectedly")
		default:
		}
	}()

	// Create a subscriber via SubscribeContext
	iter := c.SubscribeContext(context.Background())

	// We send values in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			sent := c.Send(i)
			if sent != 1 {
				t.Errorf("expected 1 subscriber, got %d", sent)
			}
		}
		// Closing the underlying channel to signal iteration is done
		close(c.C())
	}()

	// We'll collect results
	var received []int
	iter(func(v int) bool {
		received = append(received, v)
		return true // keep consuming
	})

	wg.Wait()

	// Check we got all 5 in correct order
	expected := []int{0, 1, 2, 3, 4}
	if !reflect.DeepEqual(received, expected) {
		t.Errorf("expected %v, got %v", expected, received)
	}
}

// TestChanPubSub_noSubscribers confirms that Send returns 0 when
// there are no subscribers.
func TestChanPubSub_noSubscribers(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan string))

	sent := c.Send("hello, nobody")
	if sent != 0 {
		t.Errorf("expected 0 sends, got %d", sent)
	}

	// Add and remove a subscriber, then send again
	c.Add(1)
	c.Add(-1)
	sent = c.Send("bye again, nobody")
	if sent != 0 {
		t.Errorf("expected 0 sends after unsubscribing, got %d", sent)
	}
}

// TestChanPubSub_multipleSubscribers tests multiple concurrent subscribers,
// each receiving all messages (the usage pattern is synchronous in the
// subscribe goroutine).
func TestChanPubSub_multipleSubscribers(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int))
	const numSubscribers = 5
	const totalSends = 10

	var wg sync.WaitGroup
	results := make([][]int, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		wg.Add(1)

		seq := c.SubscribeContext(context.Background())
		subscriberIndex := i

		go func() {
			defer wg.Done()
			// Capture all values
			seq(func(v int) bool {
				results[subscriberIndex] = append(results[subscriberIndex], v)
				return true
			})
		}()
	}

	// Send some values in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < totalSends; i++ {
			sent := c.Send(i)
			if sent != numSubscribers {
				t.Errorf("expected %d subscribers, got %d", numSubscribers, sent)
			}
		}
		// Close so that iteration finishes
		close(c.C())
	}()

	wg.Wait()

	// All subscribers should have the same sequence
	for i := 0; i < numSubscribers; i++ {
		if len(results[i]) != totalSends {
			t.Errorf("subscriber %d got %d items, expected %d", i, len(results[i]), totalSends)
		}
		if !reflect.DeepEqual(results[i], results[0]) {
			t.Errorf("subscriber %d got %v, subscriber 0 got %v", i, results[i], results[0])
		}
	}
}

// TestChanPubSub_unsubscribeEarly ensures that unsubscribing before receiving
// all items means that Send still returns the correct number of subscribers
// who got each subsequent item.
func TestChanPubSub_unsubscribeEarly(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int))

	// We'll have 2 subscribers, but 1 unsubscribes early.
	var wg sync.WaitGroup
	subscriber1Received := 0
	subscriber2Received := 0

	sub1 := c.SubscribeContext(context.Background())
	sub2Ctx, cancel := context.WithCancel(context.Background())
	sub2 := c.SubscribeContext(sub2Ctx)

	wg.Add(1)
	go func() {
		defer wg.Done()
		sub1(func(v int) bool {
			subscriber1Received++
			return true
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		sub2(func(v int) bool {
			subscriber2Received++
			// We'll stop receiving after the 3rd item
			if subscriber2Received == 3 {
				cancel()
			}
			return true
		})
	}()

	// Send 5 items
	for i := 0; i < 5; i++ {
		sent := c.Send(i)
		// For the first 3 sends, we expect 2 subscribers.
		// After subscriber2 unsubscribes, we expect just 1.
		if i < 3 {
			if sent != 2 {
				t.Errorf("expected 2 sends for item %d, got %d", i, sent)
			}
		} else {
			if sent != 1 {
				t.Errorf("expected 1 send for item %d after unsub, got %d", i, sent)
			}
		}
	}

	// Close so sub1 stops receiving
	close(c.C())

	wg.Wait()

	if subscriber1Received != 5 {
		t.Errorf("subscriber1 should have received all 5 messages, got %d", subscriber1Received)
	}
	if subscriber2Received != 3 {
		t.Errorf("subscriber2 should have received only 3 before unsubscribing, got %d", subscriber2Received)
	}
}

// TestChanPubSub_concurrentSubscribeUnsubscribe ensures that sending
// concurrently with multiple dynamic subscribers works correctly. This
// is a stress test, mostly verifying no panics and correct final counts.
func TestChanPubSub_concurrentSubscribeUnsubscribe(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const totalGoroutines = 50
	const sendsPerGoroutine = 20
	const expectedMessages = totalGoroutines * sendsPerGoroutine

	c := NewChanPubSub(make(chan int))
	var wg sync.WaitGroup
	wg.Add(1)

	// We'll track how many times total messages are received
	var totalReceived, totalSent int64
	var mu sync.Mutex

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start some subscribers that randomly unsubscribe
	for i := 0; i < totalGoroutines; i++ {
		seq := c.SubscribeContext(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := 0
			seq(func(v int) bool {
				mu.Lock()
				totalReceived++
				mu.Unlock()
				count++
				// random unsubscribe
				if count > 2 && count%5 == 0 && rand.IntN(4) == 0 {
					// add the remaining expected, so we can validate the total
					mu.Lock()
					totalReceived += int64(sendsPerGoroutine - count)
					mu.Unlock()
					return false // stop early
				}
				return true
			})
		}()
	}

	// Start some senders
	for i := 0; i < totalGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < sendsPerGoroutine; j++ {
				c.Send(j)
				time.Sleep(time.Millisecond * 1)
				mu.Lock()
				totalSent++
				mu.Unlock()
			}
		}()
	}

	wg.Done()
	wg.Wait()

	mu.Lock() // leave it locked, just because (not actually necessary bc wg)

	if totalReceived != expectedMessages {
		t.Errorf("expected to receive %d messages, got %d", expectedMessages, totalReceived)
	}

	if totalSent != expectedMessages {
		t.Errorf("expected to send %d messages, got %d", expectedMessages, totalSent)
	}

	select {
	case <-c.broken:
		t.Error("ChanPubSub ended in a broken state (usage violation) unexpectedly")
	default:
	}
}

// TestChanPubSub_panicWhenChannelBuffered verifies that constructing
// a ChanPubSub with a buffered channel panics immediately.
func TestChanPubSub_panicWhenChannelBuffered(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		r := recover()
		if r == nil {
			t.Error("expected panic when using a buffered channel, got none")
		} else {
			s := r.(string)
			if s != `bigbuff: chanpubsub: must use a non-nil channel without buffer` {
				t.Errorf("unexpected panic string: %s", s)
			}
		}
	}()
	_ = NewChanPubSub(make(chan int, 1)) // Should panic
}

func mustPanic(t *testing.T, fn func()) (panicValue interface{}) {
	t.Helper()
	defer func() {
		panicValue = recover()
	}()
	fn()
	t.Fatal("expected panic but got none")
	return
}

func TestChanPubSub_sanityCheckSubscribersDelta(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	isBroken := func(x *ChanPubSub[chan struct{}, struct{}]) bool {
		select {
		case <-x.broken:
			return true
		default:
			return false
		}
	}

	t.Run("negative new subscribers panics + breaks", func(t *testing.T) {
		x := NewChanPubSub(make(chan struct{}))

		if isBroken(x) {
			t.Errorf("expected not broken initially")
		}

		// We expect a panic, so use mustPanic to capture it.
		panicVal := mustPanic(t, func() {
			x.sanityCheckSubscribersDelta(-1, 0)
		})
		want := "bigbuff: chanpubsub: negative subscribers"
		if got, ok := panicVal.(string); !ok {
			t.Errorf("panic value is not a string: %v", panicVal)
		} else if got != want {
			t.Errorf("got panic message %q, want %q", got, want)
		}

		if !isBroken(x) {
			t.Errorf("expected broken, but IsBroken() is false")
		}
	})

	t.Run("negative old subscribers panics + breaks", func(t *testing.T) {
		x := NewChanPubSub(make(chan struct{}))

		if isBroken(x) {
			t.Errorf("expected not broken initially")
		}

		// subscribers=5, delta=10 => oldSubscribers = 5 - 10 => -5 => panic
		panicVal := mustPanic(t, func() {
			x.sanityCheckSubscribersDelta(5, 10)
		})
		want := "bigbuff: chanpubsub: negative old subscribers"
		if got, ok := panicVal.(string); !ok {
			t.Errorf("panic value is not a string: %v", panicVal)
		} else if got != want {
			t.Errorf("got panic message %q, want %q", got, want)
		}

		if !isBroken(x) {
			t.Errorf("expected broken, but IsBroken() is false")
		}
	})

	t.Run("overflow detection panics + breaks (delta>0 => old >= new)", func(t *testing.T) {
		x := NewChanPubSub(make(chan struct{}))

		if isBroken(x) {
			t.Errorf("expected not broken initially")
		}

		// We need a scenario that triggers (delta>0 && oldSubscribers >= subscribers).
		// In normal arithmetic, old = new - delta < new if delta>0.
		// So let's contrive a case that fails the check forcibly.
		panicVal := mustPanic(t, func() {
			const old int32 = math.MaxInt32
			const delta int32 = 1
			val := old
			val += delta
			x.sanityCheckSubscribersDelta(int(val), int(delta))
		})
		want := "bigbuff: chanpubsub: addition overflowed or underflowed"
		if got, ok := panicVal.(string); !ok {
			t.Errorf("panic value is not a string: %v", panicVal)
		} else if got != want {
			t.Errorf("got panic message %q, want %q", got, want)
		}

		if !isBroken(x) {
			t.Errorf("expected broken, but IsBroken() is false")
		}
	})

	t.Run("underflow detection panics + breaks (delta<0 => old <= new)", func(t *testing.T) {
		x := NewChanPubSub(make(chan struct{}))

		if isBroken(x) {
			t.Errorf("expected not broken initially")
		}

		// We need to trigger (delta<0 && oldSubscribers <= subscribers)
		// Contrive a case: new=50, delta=-something that yields old<=50.
		panicVal := mustPanic(t, func() {
			x.sanityCheckSubscribersDelta(math.MaxInt32, math.MinInt32)
		})
		want := "bigbuff: chanpubsub: addition overflowed or underflowed"
		if got, ok := panicVal.(string); !ok {
			t.Errorf("panic value is not a string: %v", panicVal)
		} else if got != want {
			t.Errorf("got panic message %q, want %q", got, want)
		}

		if !isBroken(x) {
			t.Errorf("expected broken, but IsBroken() is false")
		}
	})

	t.Run("valid scenario does NOT panic", func(t *testing.T) {
		x := NewChanPubSub(make(chan struct{}))

		if isBroken(x) {
			t.Errorf("expected not broken initially")
		}

		// Try a normal scenario: new=10, delta=2 => old=8 => no panic
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()
			x.sanityCheckSubscribersDelta(10, 2)
		}()

		if isBroken(x) {
			t.Errorf("expected not broken after valid scenario")
		}
	})
}

func TestChanPubSub_Wait_brokenDuring(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan struct{}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if v := recover(); v != `bigbuff: chanpubsub: state invariant violation` {
				t.Errorf(`unexpected recover: %v`, v)
			}
		}()
		c.Wait()
	}()
	time.Sleep(time.Millisecond * 15)
	select {
	case <-done:
		t.Fatal(`expected Wait to block`)
	default:
	}
	c.markBroken()
	select {
	case <-time.After(time.Second * 3):
		t.Fatal(`expected Wait to unblock`)
	case <-done:
	}
}

func TestChanPubSub_check_brokenPanicMessage(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan struct{}))
	for range 5 {
		c.checkBroken()
	}
	c.markBroken()
	func() {
		defer func() {
			if v := recover(); v != `bigbuff: chanpubsub: state invariant violation` {
				t.Errorf(`unexpected recover: %v`, v)
			}
		}()
		c.checkBroken()
	}()
}

func TestChanPubSub_checkUsedFactoryFunction_failInitMessage(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := &ChanPubSub[chan struct{}, struct{}]{}
	for i, f := range []func(){
		c.checkUsedFactoryFunction,
		func() { c.C() },
		func() { c.Send(struct{}{}) },
		func() { c.Add(0) },
		func() { c.Add(1) },
		func() { c.Add(-1) },
		func() { c.SubscribeContext(nil) },
		c.Wait,
	} {
		func() {
			defer func() {
				if v := recover(); v != `bigbuff: chanpubsub: must use factory function` {
					t.Errorf(`%d: unexpected recover: %v`, i, v)
				}
			}()
			f()
		}()
	}
}

func TestChanPubSub_Add_maxInt32(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan struct{}))
	if v := c.Add(0); v != 0 {
		t.Errorf(`expected 0, got %d`, v)
	}
	if v := c.Add(math.MaxInt32); v != math.MaxInt32 {
		t.Errorf(`expected %d, got %d`, math.MaxInt32, v)
	}
	if v := c.Add(-math.MaxInt32); v != 0 {
		t.Errorf(`expected 0, got %d`, v)
	}
	if v := c.Add(0); v != 0 {
		t.Errorf(`expected 0, got %d`, v)
	}
}

func TestChanPubSub_Add_minInt32(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan struct{}))
	defer func() {
		if v := recover(); v != `bigbuff: chanpubsub: delta out of bounds` {
			t.Errorf(`unexpected recover: %v`, v)
		}
	}()
	c.Add(math.MinInt32)
}

func TestChanPubSub_Send_pingCasterAddedBorked(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan struct{}))
	c.ping.Add(1)
	c.Add(1)
	defer func() {
		if v := recover(); v != `bigbuff: chanpubsub: state invariant violation` {
			t.Errorf(`unexpected recover: %v`, v)
		}
		select {
		case <-c.broken:
		default:
			t.Error(`expected broken`)
		}
		if !c.sendMu.TryLock() {
			t.Error(`expected sendMu to be unlocked`)
		}
		if !c.sendingMu.TryLock() {
			t.Error(`expected sendingMu to be unlocked`)
		}
	}()
	c.Send(struct{}{})
}

func TestChanPubSub_highContention(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	startedAt := time.Now()

	const (
		numReceivers                          = 10_000
		numWithInterruption                   = numReceivers - (numReceivers / 4)
		numWithoutInterruption                = numReceivers - numWithInterruption
		timeout                               = time.Second * 8
		initialWaitBase                       = time.Millisecond * 40
		minWorkerIterDur                      = time.Millisecond * 30
		randDurFactor                         = timeout/2 - minWorkerIterDur
		timeoutOverride                       = time.Minute * 0 // for debug
		ratePerChunkRequireAtLeastOneChunk    = 1_000
		ratePerChunkRequireAtLeastOneInterval = time.Millisecond * 40
		ratePerChunkRequireAtLeastOne         = ratePerChunkRequireAtLeastOneInterval * (numReceivers / ratePerChunkRequireAtLeastOneChunk)
		minPerWorkerChunk                     = 1000
		minPerWorkerValue                     = 35
		minPerWorker                          = max(1, minPerWorkerValue/(numReceivers/minPerWorkerChunk))
	)

	c := NewChanPubSub(make(chan uint32))

	var ord atomic.Uint32

	ctx, cancel := context.WithTimeout(context.Background(), timeout+timeoutOverride)
	defer cancel()

	var crashHook atomic.Pointer[func()]

	die := func(s string, v ...interface{}) {
		t.Helper()
		msg := fmt.Sprintf(s, v...)
		if !t.Failed() {
			t.Error(msg)
		}
		if hook := crashHook.Load(); hook != nil {
			(*hook)()
		}
		//panic(msg)
		cancel()
		runtime.Goexit()
	}

	var (
		subLoops          atomic.Int64
		totalIter         atomic.Int64
		expectedNext      atomic.Uint32
		firstExpectedNext atomic.Uint32
	)

	runAndVerify := func(prefix string, d time.Duration, f func()) (n int) {
		pctx := ctx
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		before := ord.Load()
		iter := c.SubscribeContext(ctx)
		if f != nil {
			f()
		}
		after := ord.Load()
		var last uint32
		var recent []uint32
		const keep = 8
		getRecent := func() []uint32 {
			return recent[max(0, len(recent)-keep):]
		}
		for v := range iter {
			n++
			recent = append(recent, v)
			if len(recent) == keep*2*2 {
				copy(recent, recent[len(recent)-keep:])
				recent = recent[:keep]
			}

			if expectedNext.CompareAndSwap(0, v) {
				firstExpectedNext.Store(v)
			}
			expectedNext.CompareAndSwap(v, v+1)

			if v < before {
				die(prefix+`expected %d to be >= %d: %d`, v, before, getRecent())
			}
			if v == 0 || (last != 0 && v != last+1) {
				die(prefix+`expected %d to be %d: %d: ctx1Err=%t ctx2Err=%t`, v, last+1, getRecent(), pctx.Err() != nil, ctx.Err() != nil)
			}
			last = v
		}
		//t.Logf(prefix+`received %d`, n)
		if ctx.Err() == nil {
			die(prefix + `expected context cancel to stop`)
		}
		if pctx.Err() == nil && ((last == 0 && d >= ratePerChunkRequireAtLeastOne) || (last != 0 && last < after)) {
			die(prefix+`expected %d to be >= %d: %d (bound was %s)`, last, after, getRecent(), ratePerChunkRequireAtLeastOne)
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(numReceivers)
	var withoutInterruptListening sync.WaitGroup
	var readyNoInterAt atomic.Pointer[time.Time]
	withoutInterruptListening.Add(numWithoutInterruption)
	go func() {
		withoutInterruptListening.Wait()
		now := time.Now()
		readyNoInterAt.Store(&now)
	}()
	for i := range numReceivers {
		hasInterruption := i < numWithInterruption
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Float64()*float64(initialWaitBase)) + initialWaitBase)
			var total int
			var hasInitDone bool
			for ctx.Err() == nil {
				var d time.Duration
				var f func()
				if hasInterruption {
					d = time.Duration(rand.Float64()*float64(randDurFactor)) + minWorkerIterDur
				} else {
					d = timeout * 2
					if !hasInitDone {
						hasInitDone = true
						f = withoutInterruptListening.Done
					}
				}
				total += runAndVerify(fmt.Sprintf(`[i=%d hasInterruption=%t d=%s] `, i, hasInterruption, d), d, f)
				subLoops.Add(1)
			}
			totalIter.Add(int64(total))
			if !hasInterruption && !hasInitDone && !t.Failed() {
				t.Error(`expected init done for no interruption by now...`)
			}
			if total < minPerWorker && !t.Failed() {
				t.Errorf(`[i=%d hasInterruption=%t] expected at least %d, got %d`, i, hasInterruption, minPerWorker, total)
			}
			if t.Failed() {
				cancel()
			} else {
				//t.Logf(`[i=%d hasInterruption=%t] total: %d`, i, hasInterruption, total)
			}
		}()
	}

	var maxValPriorToCancel atomic.Uint32
	var totalReceives atomic.Int64

	logValues := func() {
		t.Helper()
		readyNoInterAtV := `<nil>`
		if p := readyNoInterAt.Load(); p != nil {
			readyNoInterAtV = fmt.Sprintf(`%s (%s)`, p.Format(time.RFC3339Nano), p.Sub(startedAt))
		}
		t.Logf(
			"\n---\n\ntotal iterations: %d\ntotal subscribe loops: %d\ntotal sends: %d\ntotal receives: %d\n"+
				"max value prior to cancel: %d\nexpected next value: %d\nready no interruption at: %s\n"+
				"first expected next value: %d\n"+
				"\n---",
			totalIter.Load(),
			subLoops.Load(),
			ord.Load(),
			totalReceives.Load(),
			maxValPriorToCancel.Load(),
			expectedNext.Load(),
			readyNoInterAtV,
			firstExpectedNext.Load(),
		)
	}
	t.Cleanup(logValues)
	crashHook.Store(&logValues)

	for {
		readyNoInterAtNotNil := readyNoInterAt.Load() != nil
		val := ord.Add(1)
		sent := c.Send(val)
		totalReceives.Add(int64(sent))
		if ctx.Err() != nil {
			break
		}
		if readyNoInterAtNotNil && (sent < 0 || sent < numWithoutInterruption) {
			t.Fatalf(`unexpected num sends: %d`, sent)
		}
		maxValPriorToCancel.Store(val)
	}

	wg.Wait()
}

func TestChanPubSub_withSpammingSubscribeUnsubscribe(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const (
		numWorkersSpammingSubscribeUnsubscribe = 500
		numWorkersReceiving                    = 3
		valuesToSend                           = 100
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	c := NewChanPubSub(make(chan int))

	var wg sync.WaitGroup
	wg.Add(1)

	for range numWorkersSpammingSubscribeUnsubscribe {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cancel()
			for ctx.Err() == nil {
				c.Add(1)
				time.Sleep(time.Duration(float64(time.Millisecond) * rand.Float64()))
				c.Add(-1)
				time.Sleep(time.Duration(float64(time.Microsecond) * rand.Float64()))
			}
		}()
	}

	for range numWorkersReceiving {
		iter := c.SubscribeContext(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cancel()
			var i int
			defer func() {
				if i != valuesToSend {
					t.Errorf(`expected %d, got %d`, valuesToSend, i)
				}
			}()
			for j := range iter {
				if j != i {
					t.Errorf(`expected %d, got %d`, i, j)
					break
				}
				i++
			}
		}()
	}

	for i := range valuesToSend {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		if v := c.Send(i); v != 3 {
			t.Fatal(v, ctx.Err())
		}
		time.Sleep(time.Duration(float64(time.Millisecond) * rand.Float64()))
	}

	cancel()

	wg.Done()
	wg.Wait()
}

func TestChanPubSub_SubscribeContext_nilYield(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	time.Sleep(time.Millisecond * 30)
	startGoroutines := runtime.NumGoroutine()
	checkGoroutinesBackToBaseline := func() {
		t.Helper()
		if n := waitNumGoroutines(time.Second, func(n int) bool { return n <= startGoroutines }); n > startGoroutines {
			t.Errorf(`unexpected num goroutines (expected less than or equal to start): %d, %d`, startGoroutines, n)
		}
	}
	checkGoroutinesElevatedFromBaseline := func() {
		t.Helper()
		n := runtime.NumGoroutine()
		if n <= startGoroutines {
			t.Errorf(`unexpected num goroutines (expected more than start, case 1): %d, %d`, startGoroutines, n)
			return
		}
		time.Sleep(time.Millisecond * 10)
		n = runtime.NumGoroutine()
		if n <= startGoroutines {
			t.Errorf(`unexpected num goroutines (expected more than start, 2): %d, %d`, startGoroutines, n)
		}
	}
	checkGoroutinesBackToBaseline()
	c := NewChanPubSub(make(chan int))
	checkGoroutinesBackToBaseline()
	// WARNING: need cancelable context to trigger the background goroutine
	// (if ctx.Done returns nil, it will skip spawning the goroutine)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// it is really quite difficult to trigger the background goroutine...
	ctx = &valuelessContext{Context: ctx}
	iter := c.SubscribeContext(ctx)
	checkGoroutinesElevatedFromBaseline()
	if iter == nil {
		t.Fatal(`expected non-nil iter`)
	}
	if v := c.Add(0); v != 1 {
		t.Fatalf(`expected 1, got %d`, v)
	}
	checkGoroutinesElevatedFromBaseline()
	func() {
		defer func() {
			if v := recover(); v != `bigbuff: chanpubsub: iterator yield func is nil` {
				t.Fatalf(`unexpected recover: %v`, v)
			}
		}()
		iter(nil)
	}()
	checkGoroutinesBackToBaseline()
	if v := c.Add(0); v != 0 {
		t.Fatalf(`expected 0, got %d`, v)
	}
	checkGoroutinesBackToBaseline()
}

func TestChanPubSub_SubscribeContext_stopsContextOnCallingIterator(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	get := make(chan *ChanPubSub[chan int, int])
	go func() {
		c := <-get
		c.Send(1)
		c.Send(2)
		close(c.C())
	}()
	startGoroutines := runtime.NumGoroutine()
	checkGoroutinesBackToBaseline := func() {
		t.Helper()
		if n := waitNumGoroutines(time.Second, func(n int) bool { return n <= startGoroutines }); n > startGoroutines {
			t.Errorf(`unexpected num goroutines (expected less than or equal to start): %d, %d`, startGoroutines, n)
		}
	}
	checkGoroutinesElevatedFromBaseline := func() {
		t.Helper()
		n := runtime.NumGoroutine()
		if n <= startGoroutines {
			t.Errorf(`unexpected num goroutines (expected more than start, case 1): %d, %d`, startGoroutines, n)
			return
		}
		time.Sleep(time.Millisecond * 10)
		n = runtime.NumGoroutine()
		if n <= startGoroutines {
			t.Errorf(`unexpected num goroutines (expected more than start, 2): %d, %d`, startGoroutines, n)
		}
	}
	checkGoroutinesBackToBaseline()
	c := NewChanPubSub(make(chan int))
	checkGoroutinesBackToBaseline()
	// WARNING: need cancelable context to trigger the background goroutine
	// (if ctx.Done returns nil, it will skip spawning the goroutine)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// it is really quite difficult to trigger the background goroutine...
	ctx = &valuelessContext{Context: ctx}
	iter := c.SubscribeContext(ctx)
	get <- c // WARNING: only after subscribing
	checkGoroutinesElevatedFromBaseline()
	if iter == nil {
		t.Fatal(`expected non-nil iter`)
	}
	if v := c.Add(0); v != 1 {
		t.Fatalf(`expected 1, got %d`, v)
	}
	checkGoroutinesElevatedFromBaseline()
	var i int
	for v := range iter {
		checkGoroutinesBackToBaseline() // should've stopped on calling iter
		i++
		if v != i {
			t.Errorf(`expected value %d, got %d`, i, v)
		}
		if i == 2 {
			if n := waitNumGoroutines(time.Second, func(n int) bool { return n < startGoroutines }); n >= startGoroutines {
				t.Errorf(`unexpected num goroutines (expected less than start): %d, %d`, startGoroutines, n)
			}
			break
		}
	}
	if i != 2 {
		t.Errorf(`expected 2, got %d`, i)
	}
	checkGoroutinesBackToBaseline()
	if v := c.Add(0); v != 0 {
		t.Fatalf(`expected 0, got %d`, v)
	}
	checkGoroutinesBackToBaseline()
}

func TestChanPubSub_SubscribeContext_multipleIterCalls(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int))
	iter := c.SubscribeContext(context.Background())
	go func() {
		c.Send(1)
		c.Send(2)
		c.Send(3)
		close(c.C())
	}()
	values := make([][]int, 5)
	var wg sync.WaitGroup
	wg.Add(len(values))
	for i := range values {
		go func() {
			defer wg.Done()
			for v := range iter {
				values[i] = append(values[i], v)
			}
		}()
	}
	wg.Wait()
	var received []int
	for _, v := range values {
		if len(v) == 0 {
			continue
		}
		if len(received) != 0 {
			t.Fatalf(`expected only one worker got values`)
		}
		received = v
	}
	if !slices.Equal(received, []int{1, 2, 3}) {
		t.Errorf(`expected [1, 2, 3], got %v`, received)
	}
	if v := c.Add(0); v != 0 {
		t.Errorf(`expected 0, got %d`, v)
	}
}

func TestChanPubSub_SubscribeContext_brokenDuring(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int))

	c.Subscribe() // manual subscriber, for getting it to the desired state

	iter1 := c.SubscribeContext(context.Background())

	if v := c.ping.Add(0); v != 0 {
		t.Fatalf(`expected 0, got %d`, v)
	}

	expectPanic := func() {
		if v := recover(); v != `bigbuff: chanpubsub: state invariant violation` {
			t.Errorf(`unexpected recover: %v`, v)
		}
	}

	sending := make(chan struct{})
	go func() {
		defer close(sending)
		defer expectPanic()
		c.Send(1)
	}()

	iter1Ch := make(chan int)
	go func() {
		defer close(iter1Ch)
		defer expectPanic()
		for v := range iter1 {
			iter1Ch <- v
		}
	}()

	// starts at 2, because it only decrements after all sent
	// (but iter1 should be waiting, rn)
	for i := 0; c.ping.Add(0) != 2; {
		time.Sleep(time.Millisecond * 10)
		i++
		if i >= 10 {
			t.Fatal(`expected ping to be set:`, c.ping.Add(0))
		}
	}

	// unblocks the ability to enqueue more values
	if v := <-c.C(); v != 1 {
		t.Fatalf(`expected 1, got %d`, v)
	}

	// confirm we are good
	for i := 0; c.ping.Add(0) != 0; {
		time.Sleep(time.Millisecond * 10)
		i++
		if i >= 10 {
			t.Fatal(`expected ping to be back to 0`)
		}
	}
	for i := 0; ; {
		if c.sendingMu.TryRLock() {
			c.sendingMu.RUnlock()
			break
		}
		time.Sleep(time.Millisecond * 10)
		i++
		if i >= 10 {
			t.Fatal(`expected sendingMu to be unlocked for reading`)
		}
	}
	// at which point, we should be able to receive our iter1 value
	if v := <-iter1Ch; v != 1 {
		t.Fatalf(`expected 1, got %d`, v)
	}

	// ... but send is still blocking
	{
		c.pongC.L.Lock()
		n := c.pongN
		c.pongC.L.Unlock()
		if n != 1 {
			t.Fatalf(`expected 1, got %d`, n)
		}
	}

	iter2Ch := make(chan int)

	nothingDoneOrSent := func() {
		var s string
		select {
		case <-sending:
			s = `sending`
		case <-iter1Ch:
			s = `iter1`
		case <-iter2Ch:
			s = `iter2`
		case <-c.C():
			s = `c.C()`
		default:
			return
		}
		t.Fatal(`expected nothing done or sent:`, s)
	}

	nothingDoneOrSent()
	time.Sleep(time.Millisecond * 10)
	nothingDoneOrSent()
	iter2 := c.SubscribeContext(context.Background())
	go func() {
		defer close(iter2Ch)
		defer expectPanic()
		for v := range iter2 {
			iter2Ch <- v
		}
	}()
	nothingDoneOrSent()
	time.Sleep(time.Millisecond * 10)
	nothingDoneOrSent()

	c.markBroken()

	select {
	case _, ok := <-sending:
		if ok {
			t.Fatal(`expected sending to be closed`)
		}
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for sending to close`)
	}

	select {
	case _, ok := <-iter1Ch:
		if ok {
			t.Fatal(`expected iter1 to be closed`)
		}
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for iter1 to close`)
	}

	select {
	case _, ok := <-iter2Ch:
		if ok {
			t.Fatal(`expected iter2 to be closed`)
		}
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for iter2 to close`)
	}
}

func TestChanPubSub_SubscribeContext_brokenBefore(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int))

	c.Subscribe() // manual subscriber, for getting it to the desired state

	iter1 := c.SubscribeContext(context.Background())

	if v := c.ping.Add(0); v != 0 {
		t.Fatalf(`expected 0, got %d`, v)
	}

	expectPanic := func() {
		if v := recover(); v != `bigbuff: chanpubsub: state invariant violation` {
			t.Errorf(`unexpected recover: %v`, v)
		}
	}

	sending := make(chan struct{})
	go func() {
		defer close(sending)
		defer expectPanic()
		c.Send(1)
	}()

	iter1Ch := make(chan int)
	go func() {
		defer close(iter1Ch)
		defer expectPanic()
		for v := range iter1 {
			iter1Ch <- v
		}
	}()

	// starts at 2, because it only decrements after all sent
	// (but iter1 should be waiting, rn)
	for i := 0; c.ping.Add(0) != 2; {
		time.Sleep(time.Millisecond * 10)
		i++
		if i >= 10 {
			t.Fatal(`expected ping to be set:`, c.ping.Add(0))
		}
	}

	// unblocks the ability to enqueue more values
	if v := <-c.C(); v != 1 {
		t.Fatalf(`expected 1, got %d`, v)
	}

	// confirm we are good
	for i := 0; c.ping.Add(0) != 0; {
		time.Sleep(time.Millisecond * 10)
		i++
		if i >= 10 {
			t.Fatal(`expected ping to be back to 0`)
		}
	}
	for i := 0; ; {
		if c.sendingMu.TryRLock() {
			c.sendingMu.RUnlock()
			break
		}
		time.Sleep(time.Millisecond * 10)
		i++
		if i >= 10 {
			t.Fatal(`expected sendingMu to be unlocked for reading`)
		}
	}

	// ... but send is still blocking
	{
		c.pongC.L.Lock()
		n := c.pongN
		c.pongC.L.Unlock()
		if n != 1 {
			t.Fatalf(`expected 1, got %d`, n)
		}
	}

	iter2Ch := make(chan int)

	nothingDoneOrSent := func() {
		var s string
		select {
		case <-sending:
			s = `sending`
		case <-iter2Ch:
			s = `iter2`
		case <-c.C():
			s = `c.C()`
		default:
			return
		}
		t.Fatal(`expected nothing done or sent:`, s)
	}

	nothingDoneOrSent()
	time.Sleep(time.Millisecond * 10)
	nothingDoneOrSent()
	iter2 := c.SubscribeContext(context.Background())
	go func() {
		defer close(iter2Ch)
		defer expectPanic()
		for v := range iter2 {
			iter2Ch <- v
		}
	}()
	nothingDoneOrSent()
	time.Sleep(time.Millisecond * 10)
	nothingDoneOrSent()

	c.markBroken()

	select {
	case _, ok := <-sending:
		if ok {
			t.Fatal(`expected sending to be closed`)
		}
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for sending to close`)
	}

	select {
	case _, ok := <-iter2Ch:
		if ok {
			t.Fatal(`expected iter2 to be closed`)
		}
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for iter2 to close`)
	}

	// we blocked iter1 so we could test the guard code path
	if v := <-iter1Ch; v != 1 {
		t.Fatalf(`expected 1, got %d`, v)
	}
	select {
	case _, ok := <-iter1Ch:
		if ok {
			t.Fatal(`expected iter1 to be closed`)
		}
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for iter1 to close`)
	}
}

func TestChanPubSub_Add_brokenDuringUnsubscribe(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int))

	const messages = 5
	c.Add(messages)

	if v := c.ping.Add(0); v != 0 {
		t.Fatalf(`expected 0, got %d`, v)
	}

	sending := make(chan struct{})
	go func() {
		defer close(sending)
		defer func() {
			if v := recover(); v != `bigbuff: chancaster: send: state invariant violation` {
				t.Errorf(`unexpected recover: %v`, v)
			}
		}()
		c.Send(1)
	}()

	// wait until we block on sending to all (ping)
	for i := 0; c.ping.Add(0) != messages; {
		time.Sleep(time.Millisecond * 10)
		i++
		if i >= 10 {
			t.Fatal(`expected ping to be set:`, c.ping.Add(0))
		}
	}

	time.Sleep(time.Millisecond * 10)
	select {
	case <-sending:
		t.Fatal(`expected nothing closed, sending closed?`)
	default:
	}

	// decrement it past what is valid
	c.subscribers.Add(1)
	func() {
		defer func() {
			if v := recover(); v != `bigbuff: chancaster: add: state invariant violation` {
				t.Error(`unexpected panic:`, v)
			}
		}()
		c.Add(-messages - 1)
	}()

	// so it can unblock
	for range messages {
		if v := <-c.ping.C; v != 1 {
			t.Fatalf(`expected 1, got %d`, v)
		}
	}

	// all should unblock
	select {
	case _, ok := <-sending:
		if ok {
			t.Fatal(`expected sending to be closed`)
		}
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for sending to close`)
	}
}

func TestChanPubSub_Send_guardSubscribersInner(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := NewChanPubSub(make(chan int))
	c.sendMu.Lock()
	c.ping.Add(99)
	c.subscribers.Store(1)
	done := make(chan struct{})
	go func() {
		if v := c.Send(1); v != 0 {
			t.Errorf(`expected 0, got %d`, v)
		}
		close(done)
	}()
	time.Sleep(time.Millisecond * 50)
	select {
	case <-done:
		t.Fatal(`expected send to block`)
	default:
	}
	c.subscribers.Store(0)
	c.sendMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for send`)
	}
}
