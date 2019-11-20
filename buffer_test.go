package bigbuff

import (
	"context"
	"github.com/go-test/deep"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
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
	if err != context.Canceled {
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
		if err != context.Canceled {
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
		if err != context.Canceled {
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

func TestBuffer_Diff_invalid(t *testing.T) {
	buffer := new(Buffer)

	defer buffer.Close()

	if d, ok := buffer.Diff(nil); ok || d != 0 {
		t.Fatal("unexpected diff", d, ok)
	}

	if d, ok := buffer.Diff((*consumer)(nil)); ok || d != 0 {
		t.Fatal("unexpected diff", d, ok)
	}

	if d, ok := buffer.Diff(new(consumer)); ok || d != 0 {
		t.Fatal("unexpected diff", d, ok)
	}

	if d, ok := buffer.Diff(&consumer{producer: buffer}); ok || d != 0 {
		t.Fatal("unexpected diff", d, ok)
	}
}

func TestBuffer_Diff_range(t *testing.T) {
	buffer := new(Buffer)

	defer buffer.Close()

	buffer.Put(nil, 1, 2, 3, 4)

	c, err := buffer.NewConsumer()

	defer c.Close()

	if d, ok := buffer.Diff(c); !ok || d != 4 {
		t.Fatal("unexpected diff", d, ok)
	}

	if err != nil {
		t.Fatal(err)
	}

	var values []interface{}

	err = Range(
		nil,
		c,
		func(index int, value interface{}) bool {
			values = append(values, value)

			diff, ok := buffer.Diff(c)

			if !ok {
				panic("should always be ok")
			}

			return diff > 0
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(values, []interface{}{1, 2, 3, 4}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
}

func TestBuffer_Range_simple(t *testing.T) {
	buffer := new(Buffer)

	defer buffer.Close()

	buffer.Put(nil, 1, 2, 3, 4)

	c, err := buffer.NewConsumer()

	defer c.Close()

	if err != nil {
		t.Fatal(err)
	}

	var values []interface{}

	err = buffer.Range(
		nil,
		c,
		func(index int, value interface{}) bool {
			values = append(values, value)
			return true
		},
	)

	if err != nil {
		t.Fatal(err)
	}
}

func TestBuffer_Range_added(t *testing.T) {
	buffer := new(Buffer)

	defer buffer.Close()

	buffer.Put(nil, 1, 2, 3, 4)

	c, err := buffer.NewConsumer()

	defer c.Close()

	if err != nil {
		t.Fatal(err)
	}

	var values []interface{}

	go func() {
		time.Sleep(time.Millisecond * 50)
		buffer.Put(nil, 5, 6)
	}()

	err = buffer.Range(
		nil,
		c,
		func(index int, value interface{}) bool {
			values = append(values, value)
			time.Sleep(time.Millisecond * 30)
			return true
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(values, []interface{}{1, 2, 3, 4, 5, 6}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
}

func TestBuffer_Range_bailOut(t *testing.T) {
	buffer := new(Buffer)

	defer buffer.Close()

	buffer.Put(nil, 1, 2, 3, 4)

	c, err := buffer.NewConsumer()

	defer c.Close()

	if err != nil {
		t.Fatal(err)
	}

	var values []interface{}

	err = buffer.Range(
		nil,
		c,
		func(index int, value interface{}) bool {
			values = append(values, value)
			return value != 2
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(values, []interface{}{1, 2}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
}

func TestBuffer_Range_in(t *testing.T) {
	buffer := new(Buffer)
	defer buffer.Close()

	other := new(Buffer)
	defer other.Close()

	consumers := []Consumer{
		nil,
		(*consumer)(nil),
		new(consumer),
		&consumer{producer: other},
	}

	for _, c := range consumers {
		err := buffer.Range(context.Background(), c, func(index int, value interface{}) bool {
			return false
		})

		if err == nil || err.Error() != "bigbuff.Buffer.Range consumer must be one constructed via bigbuff.Buffer.NewConsumer" {
			t.Fatal("unexpected error", err)
		}
	}
}

func TestBuffer_Range_nilFunc(t *testing.T) {
	buffer := new(Buffer)
	defer buffer.Close()

	c, err := buffer.NewConsumer()

	if err != nil {
		t.Fatal(err)
	}

	err = buffer.Range(nil, c, nil)

	if err == nil || err.Error() != "bigbuff.Buffer.Range nil fn" {
		t.Fatal("unexpected error", err)
	}
}
