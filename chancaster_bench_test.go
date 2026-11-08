package bigbuff

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

var (
	dontElideMe any
	bmrInt      int
)

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

func BenchmarkChanCaster_highContention(b *testing.B) {
	const numReceivers = 100_000

	c := NewChanCaster(make(chan struct{}))

	stop := make(chan struct{})

	var allStopped sync.WaitGroup
	allStopped.Add(numReceivers)

	var ready sync.WaitGroup

	for range numReceivers {
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

// BenchmarkChanCaster_AddCancel_Idle measures the common select-cancellation
// path where a caller registers intent to receive via Add(1), another select
// branch (e.g. context cancellation, timeout, or an already-ready channel)
// wins before Send is called, and the caller unregisters via Add(-1).
func BenchmarkChanCaster_AddCancel_Idle(b *testing.B) {
	b.Run("Serial", func(b *testing.B) {
		c := NewChanCaster(make(chan struct{}))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.Add(1)
			c.Add(-1)
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		c := NewChanCaster(make(chan struct{}))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Add(1)
				c.Add(-1)
			}
		})
	})
}

// BenchmarkChanCaster_Send_NoReceivers measures the fast-path cost of calling
// Send when no receivers are registered.
func BenchmarkChanCaster_Send_NoReceivers(b *testing.B) {
	b.Run("Serial", func(b *testing.B) {
		c := NewChanCaster(make(chan struct{}))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			bmrInt = c.Send(struct{}{})
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		c := NewChanCaster(make(chan struct{}))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = c.Send(struct{}{})
			}
		})
	})
}

// BenchmarkChanCaster_Broadcast_QuiescentAdd measures full broadcast cycles
// where each of N receivers calls Add(1) individually while Send is idle
// (exercising the lock-free CAS fast path in the new implementation), followed
// by Send broadcasting to all N receivers.
func BenchmarkChanCaster_Broadcast_QuiescentAdd(b *testing.B) {
	for _, numReceivers := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("receivers=%d", numReceivers), func(b *testing.B) {
			c := NewChanCaster(make(chan struct{}))

			var (
				sendEpoch atomic.Uint64
				ready     atomic.Uint64
				stopping  atomic.Bool
				wg        sync.WaitGroup
			)

			wg.Add(numReceivers)
			for range numReceivers {
				go func() {
					defer wg.Done()
					myEpoch := uint64(1)
					for {
						for sendEpoch.Load() < myEpoch {
							runtime.Gosched()
						}
						if stopping.Load() {
							return
						}
						c.Add(1)
						ready.Add(1)
						<-c.C
						myEpoch++
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				// Release workers only after the previous Send has returned
				// (guaranteeing chanCasterSendingBit == 0).
				sendEpoch.Add(1)
				target := uint64(i+1) * uint64(numReceivers)
				for ready.Load() < target {
					runtime.Gosched()
				}
				if n := c.Send(struct{}{}); n != numReceivers {
					b.Fatalf("expected %d receivers, got %d", numReceivers, n)
				}
			}

			b.StopTimer()
			stopping.Store(true)
			sendEpoch.Add(1)
			wg.Wait()
		})
	}
}

// BenchmarkChanCaster_Broadcast_MidSendReAdd measures full broadcast cycles
// where each of N receivers immediately re-registers via Add(1) upon receiving
// from <-c.C. Because Send delivers to receivers sequentially on an unbuffered
// channel, the first N-1 receivers call Add(1) while Send is still mid-flight
// (chanCasterSendingBit == 1), exercising the mutex slow path.
func BenchmarkChanCaster_Broadcast_MidSendReAdd(b *testing.B) {
	for _, numReceivers := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("receivers=%d", numReceivers), func(b *testing.B) {
			c := NewChanCaster(make(chan struct{}))

			var (
				ready    atomic.Uint64
				stopping atomic.Bool
				wg       sync.WaitGroup
			)

			wg.Add(numReceivers)
			for range numReceivers {
				go func() {
					defer wg.Done()
					c.Add(1)
					ready.Add(1)
					for {
						<-c.C
						if stopping.Load() {
							return
						}
						// Re-register immediately while Send is still delivering
						// to the remaining receivers in the current batch.
						c.Add(1)
						ready.Add(1)
					}
				}()
			}

			// Wait for initial registration before starting the timer.
			for ready.Load() < uint64(numReceivers) {
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				if n := c.Send(struct{}{}); n != numReceivers {
					b.Fatalf("expected %d receivers, got %d", numReceivers, n)
				}
				target := uint64(i+2) * uint64(numReceivers)
				for ready.Load() < target {
				}
			}

			b.StopTimer()
			stopping.Store(true)
			c.Send(struct{}{}) // Drain the final N registrations cleanly.
			wg.Wait()
		})
	}
}
