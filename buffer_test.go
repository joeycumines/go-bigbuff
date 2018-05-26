package bigbuff

import (
	"testing"
	"sync"
	"time"
	"runtime"
	"github.com/go-test/deep"
	"reflect"
	"context"
)

func TestBuffer_Close(t *testing.T) {
	calledCancel := make(chan struct{})
	out := make(chan error)

	c := &Buffer{
		done: make(chan struct{}),
		cancel: func() {
			calledCancel <- struct{}{}
		},
		consumers: map[*consumer]int{
			nil: 12,
		},
	}
	c.cond = sync.NewCond(&c.mutex)

	c.mutex.Lock()

	go func() {
		out <- c.Close()
	}()

	time.Sleep(time.Millisecond * 10)

	select {
	case <-calledCancel:
		t.Fatal("expected to be locked")
	default:
	}

	c.mutex.Unlock()

	<-calledCancel

	time.Sleep(time.Millisecond * 10)

	select {
	case <-c.done:
		t.Fatal("expected to be open")
	default:
	}

	c.mutex.Lock()
	delete(c.consumers, nil)
	c.cond.Broadcast()
	c.mutex.Unlock()

	<-c.done

	if err := <-out; err != nil {
		t.Fatal("unexpected error", err)
	}

	c.mutex.Lock()

	if err := c.Close(); err == nil {
		t.Fatal("unexpected error", err)
	}

	c.mutex.Unlock()
}

func TestBuffer_Done(t *testing.T) {
	d := make(chan struct{})
	b := &Buffer{
		done: d,
	}
	if b.Done() != d {
		t.Errorf("unexpected done")
	}
	out := make(chan (<-chan struct{}))
	b.mutex.Lock()
	go func() {
		out <- b.Done()
	}()
	time.Sleep(time.Millisecond * 10)
	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}
	b.mutex.Unlock()
	if <-out != d {
		t.Fatal("unexpected done")
	}
	b.mutex.RLock()
	b.Done()
	b.mutex.RUnlock()
	if b.cond == nil {
		t.Fatal("expected ensure to be called")
	}
}

func TestBuffer_ensure(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	b := new(Buffer)

	b.mutex.RLock()

	out := make(chan struct{})

	go func() {
		b.ensure()
		out <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 20)

	if b.ctx != nil || b.consumers != nil || b.done != nil || b.cancel != nil || b.buffer != nil {
		t.Fatalf("unexpected buffer: %+v", b)
	}

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	b.mutex.RUnlock()

	<-out

	if b.ctx == nil || b.consumers == nil || b.done == nil || b.cancel == nil || b.buffer != nil || b.ctx.Err() != nil {
		t.Fatalf("unexpected buffer: %+v", b)
	}

	a := *b

	time.Sleep(time.Millisecond * 20)

	b.ensure()

	if diff := deep.Equal(a, *b); diff != nil {
		t.Fatal("unexpected diff:", diff)
	}

	b.cancel()

	if b.ctx.Err() == nil {
		t.Fatal("expected err")
	}

	time.Sleep(time.Millisecond * 20)

	finalGoroutines := runtime.NumGoroutine()

	if finalGoroutines > initialGoroutines {
		t.Fatal("more goroutines than expected:", finalGoroutines-initialGoroutines)
	}
}

func TestBuffer_ensure_nil(t *testing.T) {
	var b *Buffer
	func() {
		defer func() {
			r := recover()
			err, ok := r.(error)
			if !ok || err == nil || err.Error() != "bigbuff.Buffer encountered a nil receiver" {
				t.Fatal("unexpected recovered value:", r)
			}
		}()
		b.ensure()
	}()
}

func TestBuffer_Slice_nil(t *testing.T) {
	out := make(chan struct{})
	b := new(Buffer)
	b.mutex.Lock()
	go func() {
		if s := b.Slice(); s != nil {
			t.Error("expected nil")
		}
		out <- struct{}{}
	}()
	time.Sleep(time.Millisecond * 10)
	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}
	b.mutex.Unlock()
	<-out
}

func TestBuffer_Slice_copy(t *testing.T) {
	out := make(chan struct{})
	b := new(Buffer)
	c := []interface{}{
		1,
		2,
		new(struct{}),
		"three",
	}
	b.buffer = c
	b.mutex.Lock()
	go func() {
		s := b.Slice()
		if s == nil {
			t.Error("nil slice")
		}
		if reflect.ValueOf(c).Pointer() == reflect.ValueOf(s).Pointer() {
			t.Error("expected a copy")
		}
		if diff := deep.Equal(c, s); diff != nil {
			t.Error("unexpected diff", diff)
		}
		out <- struct{}{}
	}()
	time.Sleep(time.Millisecond * 10)
	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}
	b.mutex.Unlock()
	<-out
	b.mutex.RLock()
	if diff := deep.Equal(b.Slice(), c); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	b.mutex.RUnlock()
	if b.cond == nil {
		t.Fatal("expected ensure to be called")
	}
}

func TestBuffer_Put_canceledInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := new(Buffer)
	err := b.Put(ctx)
	if err == nil || err.Error() != "bigbuff.Buffer.Put input context error: context canceled" {
		t.Fatal("unexpected error", err)
	}
	if b.cond == nil {
		t.Fatal("expected ensure to be called")
	}
}

func TestBuffer_Put_canceledInternal(t *testing.T) {
	out := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := new(Buffer)
	b.ctx = ctx

	b.mutex.RLock()
	go func() {
		err := b.Put(nil)
		if err == nil || err.Error() != "bigbuff.Buffer.Put internal context error: context canceled" {
			t.Error("unexpected error", err)
		}
		out <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	b.mutex.RUnlock()

	<-out

	if b.cond == nil {
		t.Fatal("expected ensure to be called")
	}
}

func TestBuffer_Put_success(t *testing.T) {
	out := make(chan struct{})
	b := new(Buffer)
	b.ctx = context.Background()
	z := []interface{}{1, 2, 3}
	b.buffer = z
	b.ensure()

	broad := make(chan struct{})
	b.mutex.Lock()
	go func() {
		b.cond.Wait()
		b.mutex.Unlock()
		broad <- struct{}{}
	}()

	b.mutex.RLock()

	go func() {
		err := b.Put(nil, 4, 5)

		if err != nil {
			t.Error("unexpected error", err)
		}

		out <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	b.mutex.RUnlock()

	<-out

	if diff := deep.Equal([]interface{}{1, 2, 3, 4, 5}, b.buffer); diff != nil {
		t.Fatal("unexpected diff", diff)
	}

	<-broad

	b.mutex.Lock()
	b.mutex.Unlock()
}

func TestBuffer_Put_successNil(t *testing.T) {
	out := make(chan struct{})
	b := new(Buffer)
	b.ctx = context.Background()

	b.mutex.RLock()

	go func() {
		err := b.Put(nil, 4, 5)

		if err != nil {
			t.Error("unexpected error", err)
		}

		out <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	b.mutex.RUnlock()

	<-out

	if diff := deep.Equal([]interface{}{4, 5}, b.buffer); diff != nil {
		t.Fatal("unexpected diff", diff)
	}

	b.mutex.Lock()
	b.mutex.Unlock()

	if b.cond == nil {
		t.Fatal("expected ensure to be called")
	}
}

func TestBuffer_NewConsumer_canceledInternal(t *testing.T) {
	out := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := new(Buffer)
	b.ctx = ctx

	b.mutex.RLock()
	go func() {
		c, err := b.NewConsumer()
		if c != nil {
			t.Error("expected nil consumer")
		}
		if err == nil || err.Error() != "bigbuff.Buffer.NewConsumer context error: context canceled" {
			t.Error("unexpected error", err)
		}
		out <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	b.mutex.RUnlock()

	<-out

	if b.cond == nil {
		t.Fatal("expected ensure to be called")
	}
}

func TestBuffer_NewConsumer_success(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	out := make(chan struct{})
	b := new(Buffer)
	b.offset = 99
	b.ctx = context.Background()
	b.ensure()
	originalConsumerMapPtr := reflect.ValueOf(b.consumers).Pointer()

	broad := make(chan struct{})
	b.mutex.Lock()
	go func() {
		b.cond.Wait()
		b.mutex.Unlock()
		broad <- struct{}{}
	}()

	b.mutex.RLock()

	go func() {
		cd, err := b.NewConsumer()

		if cd == nil {
			t.Error("expected non-nil consumer")
		} else {
			c, ok := cd.(*consumer)
			if !ok || c == nil {
				t.Errorf("expected *consumer, got %T: %+v", cd, c)
			} else {
				if c.done == nil {
					t.Error("expected a done")
				} else {
					select {
					case <-c.done:
						t.Error("expected not done")
					default:
					}
				}
				if c.cond == nil || c.cond.L != &c.mutex {
					t.Errorf("unexpected cond: %+v", c.cond)
				}
				if c.producer != b {
					t.Error("unexpected producer", c.producer)
				}

				// check the state of the map before canceling
				if o, ok := b.consumers[c]; len(b.consumers) != 1 ||
					!ok ||
					o != 99 ||
					reflect.ValueOf(b.consumers).Pointer() != originalConsumerMapPtr {
					t.Errorf("unexpected consumer map: %+v", b.consumers)
				}

				if c.ctx == nil || c.cancel == nil {
					t.Error("nil context")
				} else {
					// cancel the consumer's context, and wait for done
					c.cancel()
					<-c.done
					if err := c.Close(); err == nil {
						t.Error("expected close to give an error")
					}
					// check the state of the map after canceling
					if len(b.consumers) != 0 || reflect.ValueOf(b.consumers).Pointer() != originalConsumerMapPtr {
						t.Errorf("unexpected consumer map: %+v", b.consumers)
					}
				}
			}
		}

		if err != nil {
			t.Error("unexpected error", err)
		}

		out <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	b.mutex.RUnlock()

	<-broad
	<-out

	b.mutex.Lock()
	b.mutex.Unlock()

	b.cancel()
	time.Sleep(time.Millisecond * 30)

	finalGoroutines := runtime.NumGoroutine()

	if finalGoroutines > initialGoroutines {
		t.Fatal("goroutine diff:", finalGoroutines-initialGoroutines)
	}
}
