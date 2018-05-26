package bigbuff

import (
	"testing"
	"context"
	"sync"
	"time"
	"runtime"
)

func TestWaitCond_canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cond := sync.NewCond(new(sync.Mutex))
	err := WaitCond(ctx, cond, func() bool {
		panic("some_error")
	})
	if err == nil || err.Error() != "bigbuff.WaitCond context error: context canceled" {
		t.Fatal("unexpected error", err)
	}
	cond.L.Lock()
	cond.L.Unlock()
}

func TestWaitCond_nilCond(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := WaitCond(ctx, nil, func() bool {
		panic("some_error")
	})
	if err == nil || err.Error() != "bigbuff.WaitCond requires a non-nil cond" {
		t.Fatal("unexpected error", err)
	}
}

func TestWaitCond_nilMutex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cond := &sync.Cond{}
	err := WaitCond(ctx, cond, func() bool {
		panic("some_error")
	})
	if err == nil || err.Error() != "bigbuff.WaitCond requires a cond with a non-nil locker" {
		t.Fatal("unexpected error", err)
	}
}

func TestWaitCond_nilFn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cond := sync.NewCond(new(sync.Mutex))
	err := WaitCond(ctx, cond, nil)
	if err == nil || err.Error() != "bigbuff.WaitCond requires a non-nil fn" {
		t.Fatal("unexpected error", err)
	}
}

func TestWaitCond_nilContext(t *testing.T) {
	cond := sync.NewCond(new(sync.Mutex))
	err := WaitCond(nil, cond, func() bool {
		return true
	})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
}

func TestWaitCond_contextCancel(t *testing.T) {
	initialGoroutineCount := runtime.NumGoroutine()

	out := make(chan error)

	ctx, cancel := context.WithCancel(context.Background())
	count := 0
	mutex := new(sync.Mutex)
	cond := sync.NewCond(new(sync.Mutex))

	go func() {
		cond.L.Lock()
		defer cond.L.Unlock()
		out <- WaitCond(ctx, cond, func() bool {
			mutex.Lock()
			defer mutex.Unlock()
			count++
			return false
		})
	}()

	time.Sleep(time.Millisecond * 20)
	mutex.Lock()
	if 1 != count {
		t.Error("unexpected count", count)
	}
	mutex.Unlock()

	cond.Broadcast()

	time.Sleep(time.Millisecond * 20)
	mutex.Lock()
	if 2 != count {
		t.Error("unexpected count", count)
	}
	mutex.Unlock()

	cond.L.Lock()
	cancel()
	time.Sleep(time.Millisecond * 20)
	select {
	case <-out:
		t.Fatal("should have waited")
	default:
	}
	cond.L.Unlock()

	err := <-out
	if err == nil || err.Error() != "bigbuff.WaitCond context error: context canceled" {
		t.Fatal("unexpected error", err)
	}

	time.Sleep(time.Millisecond * 20)

	mutex.Lock()
	if 2 != count {
		t.Error("unexpected count", count)
	}
	mutex.Unlock()

	finalGoroutineCount := runtime.NumGoroutine()

	if finalGoroutineCount > initialGoroutineCount {
		t.Fatalf("spawned too many goroutines... leaky? initial=%d;final=%d", initialGoroutineCount, finalGoroutineCount)
	}
}

func TestWaitCond_success(t *testing.T) {
	initialGoroutineCount := runtime.NumGoroutine()

	out := make(chan error)

	result := false
	mutex := new(sync.Mutex)
	count := 0
	cond := sync.NewCond(new(sync.Mutex))

	go func() {
		cond.L.Lock()
		defer cond.L.Unlock()
		out <- WaitCond(context.Background(), cond, func() bool {
			mutex.Lock()
			defer mutex.Unlock()
			count++
			return result
		})
	}()

	for expected := 1; expected <= 10; expected++ {
		if expected > 1 {
			cond.Broadcast()
		}
		time.Sleep(time.Millisecond * 20)
		mutex.Lock()
		if expected != count {
			t.Errorf("expected count %d != actual %d", expected, count)
		}
		mutex.Unlock()
	}

	mutex.Lock()
	result = true
	cond.Broadcast()
	mutex.Unlock()

	err := <-out
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	time.Sleep(time.Millisecond * 20)
	mutex.Lock()
	if 11 != count {
		t.Errorf("expected count %d != actual %d", 11, count)
	}
	mutex.Unlock()

	finalGoroutineCount := runtime.NumGoroutine()

	if finalGoroutineCount > initialGoroutineCount {
		t.Fatalf("spawned too many goroutines... leaky? initial=%d;final=%d", initialGoroutineCount, finalGoroutineCount)
	}
}
