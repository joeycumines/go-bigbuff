/*
   Copyright 2021 Joseph Cumines

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
	"github.com/go-test/deep"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestNewChannel_ctxDefaults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c, err := NewChannel(nil, time.Second, make(chan struct{}))
	if c != nil {
		t.Cleanup(func() {
			_ = c.Close()
		})
	}
	if err != nil {
		t.Error("unexpected error", err)
	}

	if c == nil {
		t.Error("nil *Channel")
	} else if c.ctx == nil {
		t.Error("nil ctx")
	}
}

func TestNewChannel_pollRateDefaults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c, err := NewChannel(context.Background(), 0, make(chan struct{}))
	if c != nil {
		t.Cleanup(func() {
			_ = c.Close()
		})
	}
	if err != nil {
		t.Error("unexpected error", err)
	}

	if c == nil {
		t.Error("nil *Channel")
	} else if c.rate != time.Millisecond {
		t.Error("unexpected poll rate", c.rate)
	}
}

func TestNewChannel_pollRateNegative(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c, err := NewChannel(context.Background(), -1, make(chan struct{}))

	if err == nil || err.Error() != "bigbuff.NewChannel poll rate must be > 0, or 0 to use the default" {
		t.Error("unexpected error", err)
	}

	if c != nil {
		t.Error("expected nil")
	}
}

func TestNewChannel_nilSource(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c, err := NewChannel(context.Background(), time.Second, nil)

	if err == nil || err.Error() != "bigbuff.NewChannel nil source" {
		t.Error("unexpected error", err)
	}

	if c != nil {
		t.Error("expected nil")
	}
}

func TestNewChannel_nonChannel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c, err := NewChannel(context.Background(), time.Second, 4)

	if err == nil || err.Error() != "bigbuff.NewChannel source (int) must be a channel" {
		t.Error("unexpected error", err)
	}

	if c != nil {
		t.Error("expected nil")
	}
}

func TestNewChannel_readChannel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan struct{})
	var read <-chan struct{} = in

	c, err := NewChannel(context.Background(), time.Second, read)
	if c != nil {
		t.Cleanup(func() {
			_ = c.Close()
		})
	}
	if err != nil {
		t.Error("unexpected error", err)
	}

	if c == nil {
		t.Error("nil *Channel")
	} else if source := c.source.Interface(); source != read {
		t.Errorf("unexpected source (%T): %+v", source, source)
	}
}

func TestNewChannel_rwChannel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan struct{})

	c, err := NewChannel(context.Background(), time.Second, in)
	if c != nil {
		t.Cleanup(func() {
			_ = c.Close()
		})
	}
	if err != nil {
		t.Error("unexpected error", err)
	}

	if c == nil {
		t.Error("nil *Channel")
	} else if source := c.source.Interface(); source != in {
		t.Errorf("unexpected source (%T): %+v", source, source)
	}
}

func TestNewChannel_rwChannelClosed(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan struct{})
	close(in)

	c, err := NewChannel(context.Background(), time.Second, in)
	if c != nil {
		t.Cleanup(func() {
			_ = c.Close()
		})
	}
	if err != nil {
		t.Error("unexpected error", err)
	}

	if c == nil {
		t.Error("nil *Channel")
	} else if source := c.source.Interface(); source != in {
		t.Errorf("unexpected source (%T): %+v", source, source)
	}
}

func TestNewChannel_writeChannel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan struct{})
	var write chan<- struct{} = in

	c, err := NewChannel(context.Background(), time.Second, write)

	if err == nil || err.Error() != "bigbuff.NewChannel source (chan<- struct {}) must have a receivable direction" {
		t.Error("unexpected error", err)
	}

	if c != nil {
		t.Error("expected nil")
	}
}

func TestNewChannel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan struct{}, 1)

	c, err := NewChannel(ctx, time.Second+time.Millisecond, in)

	if err != nil {
		t.Error("unexpected error", err)
	}

	if c == nil {
		t.Error("nil *Channel")
	} else {
		if !c.valid {
			t.Error("expected valid")
		}
		if c.source.Interface() != in {
			t.Error("unexpected source")
		}
		if c.buffer != nil {
			t.Error("unexpected buffer")
		}
		if c.done == nil {
			t.Error("nil done")
		} else {
			select {
			case <-c.done:
				t.Error("expected not done")
			default:
			}
		}
		if c.ctx == nil || c.ctx == ctx || c.ctx.Err() != nil {
			t.Error("unexpected ctx")
		} else {
			cancel()
			if c.ctx.Err() == nil {
				t.Error("expected context to cancel")
			} else {
				time.Sleep(time.Millisecond * 20)
				if err := c.Close(); err == nil || err.Error() != "bigbuff.Channel.Close may be closed at most once" {
					t.Error("unexpected close error", err)
				} else {
					// in should still be open too
					in <- struct{}{}
					<-in
				}
			}
		}
		if c.rate != time.Second+time.Millisecond {
			t.Error("unexpected rate", c.rate)
		}
		if c.rollback != 0 {
			t.Error("unexpected rollback", c.rollback)
		}
	}
}

func TestChannel_Buffer_nil(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := &Channel{
		valid: true,
	}

	b := c.Buffer()

	if b != nil {
		t.Error("unexpected buffer", b)
	}
}

func TestChannel_Buffer(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	o := make([]interface{}, 3, 10)
	o[0] = 0
	o[1] = 1
	o[2] = 2

	c := &Channel{
		valid:  true,
		buffer: o,
	}

	b := c.Buffer()

	if cap(b) != len(b) {
		t.Error("unexpected capacity")
	}

	if diff := deep.Equal(b, []interface{}{0, 1, 2}); diff != nil {
		t.Error("unexpected diff", diff)
	} else {
		b[1] = 66
		if diff := deep.Equal(o, []interface{}{0, 1, 2}); diff != nil {
			t.Error("unexpected diff", diff)
		}
	}
}

func TestChannel_Close(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	cancel := make(chan struct{})

	done := make(chan struct{})

	c := &Channel{
		valid: true,
		cancel: func() {
			cancel <- struct{}{}
		},
		done: done,
	}

	if c.Done() != done {
		t.Fatal("unexpected done")
	}

	out := make(chan error)

	c.mutex.Lock()

	go func() {
		out <- c.Close()
	}()

	time.Sleep(time.Millisecond * 15)

	select {
	case <-cancel:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-done:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-out:
		t.Fatal("unexpected value")
	default:
	}

	secondOut := make(chan error)

	go func() {
		secondOut <- c.Close()
	}()

	time.Sleep(time.Millisecond * 15)

	select {
	case <-cancel:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-done:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-out:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-secondOut:
		t.Fatal("unexpected value")
	default:
	}

	c.mutex.Unlock()

	// first, the cancel happens
	time.Sleep(time.Millisecond * 10)
	select {
	case <-done:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-out:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-secondOut:
		t.Fatal("unexpected value")
	default:
	}
	select {
	case <-cancel:
	default:
		t.Fatal("unexpected value")
	}

	// then the close of done
	<-done

	// then the nil return of out, and the error return of second out
	if err := <-out; err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := <-secondOut; err == nil || err.Error() != "bigbuff.Channel.Close may be closed at most once" {
		t.Fatal("unexpected error", err)
	}

	if c.Done() != done {
		t.Fatal("unexpected done")
	}
}

func TestChannel_Get(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	z := new(struct{})

	input := []interface{}{
		"A",
		12,
		2.12314,
		nil,
		z,
	}

	in := make(chan interface{}, 2)

	in <- 0

	c, err := NewChannel(nil, time.Millisecond, in)
	if err != nil || c == nil {
		t.Fatal("unexpected", c, err)
	}

	startTS := time.Now()
	go func() {
		time.Sleep(time.Millisecond * 40)
		defer close(in)
		for _, v := range input {
			in <- v
		}
	}()

	// read the first value
	v, err := c.Get(nil)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != 0 {
		t.Fatal("unexpected value", v)
	}

	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{0}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 1 {
		t.Fatal("unexpected pending", i)
	}

	// read the second value
	v, err = c.Get(nil)
	spent := time.Now().Sub(startTS)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != "A" {
		t.Fatal("unexpected value", v)
	}

	// verify time spent on those first two fetches
	if spent < 40*time.Millisecond || spent > 44*time.Millisecond {
		t.Fatal("unexpected spent", spent)
	}

	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{0, "A"}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 2 {
		t.Fatal("unexpected pending", i)
	}

	// rollback
	err = c.Rollback()
	if err != nil {
		t.Fatal("unexpected err", err)
	}

	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{0, "A"}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 2 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 0 {
		t.Fatal("unexpected pending", i)
	}

	// get the 0 again
	v, err = c.Get(nil)
	spent = time.Now().Sub(startTS)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != 0 {
		t.Fatal("unexpected value", v)
	}

	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{0, "A"}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 1 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 1 {
		t.Fatal("unexpected pending", i)
	}

	// verify time spent
	if spent < 40*time.Millisecond || spent > 44*time.Millisecond {
		t.Fatal("unexpected spent", spent)
	}

	// commit the 0
	b := c.buffer
	err = c.Commit()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	// verify the buffer ref was cleared
	if diff := deep.Equal(b, []interface{}{nil, "A"}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{"A"}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 1 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 0 {
		t.Fatal("unexpected pending", i)
	}

	// get the "A" again
	v, err = c.Get(nil)
	spent = time.Now().Sub(startTS)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != "A" {
		t.Fatal("unexpected value", v)
	}

	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{"A"}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 1 {
		t.Fatal("unexpected pending", i)
	}

	// 12
	v, err = c.Get(nil)
	spent = time.Now().Sub(startTS)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != 12 {
		t.Fatal("unexpected value", v)
	}
	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{"A", 12}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 2 {
		t.Fatal("unexpected pending", i)
	}

	// 2.12314
	v, err = c.Get(nil)
	spent = time.Now().Sub(startTS)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != 2.12314 {
		t.Fatal("unexpected value", v)
	}
	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{"A", 12, 2.12314}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 3 {
		t.Fatal("unexpected pending", i)
	}

	// commit
	b = c.buffer
	err = c.Commit()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	// verify the buffer refs were cleared
	if diff := deep.Equal(b, []interface{}{nil, nil, nil}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 0 {
		t.Fatal("unexpected pending", i)
	}

	// nil
	v, err = c.Get(nil)
	spent = time.Now().Sub(startTS)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != nil {
		t.Fatal("unexpected value", v)
	}
	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{nil}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 1 {
		t.Fatal("unexpected pending", i)
	}

	// verify time spent
	if spent < 40*time.Millisecond || spent > 45*time.Millisecond {
		t.Fatal("unexpected spent", spent)
	}

	// sleep a bit before the last one just to make sure it obeys get rate
	time.Sleep(time.Millisecond * 20)
	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{nil}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 1 {
		t.Fatal("unexpected pending", i)
	}

	// z
	v, err = c.Get(nil)
	if err != nil {
		t.Fatal("unexpected err", err)
	}
	if v != z {
		t.Fatal("unexpected value", v)
	}
	// verify buffer
	if diff := deep.Equal(c.buffer, []interface{}{nil, z}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
	// verify rollback
	if i := c.rollback; i != 0 {
		t.Fatal("unexpected rollback", i)
	}
	// verify pending
	if i := c.pending(); i != 2 {
		t.Fatal("unexpected pending", i)
	}

	// in will be closed shortly, sleep a bit to ensure that, then verify no more
	blockedGet := make(chan struct {
		Value interface{}
		Error error
	})
	go func() {
		var v struct {
			Value interface{}
			Error error
		}
		v.Value, v.Error = c.Get(nil)
		blockedGet <- v
	}()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-blockedGet:
		t.Fatal("expected to be blocked")
	default:
	}

	// start another get as well, with a different tick rate, to verify bail out
	finalSpent := make(chan time.Duration, 1)
	c.mutex.Lock()
	go func() {
		startTS := time.Now()
		defer func() {
			finalSpent <- time.Now().Sub(startTS)
		}()
		// set the rate for the new get
		c.rate = time.Millisecond * 50 // this WONT be how long it takes - it has a bail out
		startTS = time.Now()
		c.mutex.Unlock()
		v, err := c.Get(nil)
		if v != nil || err == nil {
			t.Error("unexpected get", v, err)
		}
	}()

	// ensure we start the get around the right time
	c.mutex.Lock()
	c.mutex.Unlock()

	// sleep for a bit to let the second fail then block
	time.Sleep(time.Millisecond * 2)

	// close will unblock the bastards
	if err := c.Close(); err != nil {
		t.Fatal("unexpected error", err)
	}

	vb := <-blockedGet

	if vb.Value != nil || vb.Error != context.Canceled {
		t.Error("unexpected final value", vb)
	}

	if spent := <-finalSpent; spent > time.Millisecond*4 {
		t.Fatal("unexpected final", spent)
	}

	// check a new get
	vb.Value, vb.Error = c.Get(nil)
	if vb.Value != nil || vb.Error != context.Canceled {
		t.Error("unexpected final value", vb)
	}

	// verify buffer
	if diff := deep.Equal(c.Buffer(), []interface{}{nil, z}); diff != nil {
		t.Fatal("unexpected diff", diff)
	}
}

func TestConsumer_Get_inputCanceled(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan interface{}, 2)

	in <- 14

	c, err := NewChannel(nil, time.Millisecond, in)
	if c != nil {
		t.Cleanup(func() {
			_ = c.Close()
		})
	}
	if err != nil || c == nil {
		t.Fatal("unexpected", c, err)
	}

	spent := make(chan time.Duration)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		startTS := time.Now()
		defer func() {
			spent <- time.Now().Sub(startTS)
		}()

		v, err := c.Get(ctx)
		if err != nil {
			t.Error("unexpected err", err)
		}
		if v != 14 {
			t.Error("unexpected value", v)
		}

		v, err = c.Get(ctx)
		if v != nil {
			t.Error("unexpected value", v)
		}
		if err != context.Canceled {
			t.Error("unexpected err", err)
		}
	}()

	time.Sleep(time.Millisecond * 10)
	cancel()

	if s := <-spent; s < time.Millisecond*10 || s > time.Millisecond*15 {
		t.Fatal("unexpected spent", s)
	}

	v, err := c.Get(ctx)
	if v != nil {
		t.Error("unexpected value", v)
	}
	if err != context.Canceled {
		t.Error("unexpected err", err)
	}
}

func TestChannel_Rollback_errs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan interface{}, 2)
	ctx, cancel := context.WithCancel(context.Background())

	in <- 14

	c, err := NewChannel(ctx, time.Millisecond, in)
	if err != nil || c == nil {
		t.Fatal("unexpected", c, err)
	}

	v, err := c.Get(nil)
	if err != nil {
		t.Error("unexpected err", err)
	}
	if v != 14 {
		t.Error("unexpected value", v)
	}

	cancel()

	time.Sleep(time.Millisecond * 2)

	err = c.Rollback()
	if err != nil {
		t.Fatal("unexpected err", err)
	}

	err = c.Rollback()
	if err == nil || err.Error() != "bigbuff.Channel.Rollback nothing to rollback" {
		t.Fatal("unexpected err", err)
	}
}

func TestChannel_Commit_errs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan interface{}, 2)
	ctx, cancel := context.WithCancel(context.Background())

	in <- 14

	c, err := NewChannel(ctx, time.Millisecond, in)
	if err != nil || c == nil {
		t.Fatal("unexpected", c, err)
	}

	time.Sleep(time.Millisecond * 2)

	err = c.Commit()
	if err == nil || err.Error() != "bigbuff.Channel.Commit nothing to commit" {
		t.Fatal("unexpected err", err)
	}

	v, err := c.Get(nil)
	if err != nil {
		t.Error("unexpected err", err)
	}
	if v != 14 {
		t.Error("unexpected value", v)
	}

	cancel()

	err = c.Commit()
	if err != context.Canceled {
		t.Fatal("unexpected err", err)
	}

	err = c.Rollback()
	if err != nil {
		t.Fatal("unexpected err", err)
	}
}

func TestChannel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	in := make(chan int, 50)

	var (
		aSaw  = make(map[int]int)
		bSaw  = make(map[int]int)
		cSaw  = make(map[int]int)
		aLast = -1
		bLast = -1
		cLast = -1
		wg    sync.WaitGroup
	)

	wg.Add(5)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	out := make(chan int, 100)
	output := make([]int, 0, 10000)

	go func() {
		defer wg.Done()
		defer cancel()
		for v := range out {
			output = append(output, v)
			if len(output) == 10000 {
				break
			}
		}
	}()

	// a
	go func() {
		defer wg.Done()

		c, err := NewChannel(nil, 0, in)
		if c != nil {
			t.Cleanup(func() {
				_ = c.Close()
			})
		}
		if err != nil || c == nil {
			t.Error("unexpected", c, err)
		}

		defer func() {
			if l := len(c.Buffer()); l != 0 {
				t.Error("unexpected final buffer", l)
			}
		}()

		err = Range(ctx, c, func(index int, value interface{}) bool {
			if index != len(aSaw) {
				t.Error("bad index vs map", index, len(aSaw))
			}
			v := value.(int)
			if v <= aLast {
				t.Errorf("saw %d last and just got %d", aLast, v)
			}
			aLast = v
			count, _ := aSaw[v]
			count++
			aSaw[v] = count
			out <- v
			return true
		})

		if err != nil && ctx.Err() == nil {
			t.Error(err)
		}
	}()

	// b
	go func() {
		defer wg.Done()

		c, err := NewChannel(nil, 0, in)
		if c != nil {
			t.Cleanup(func() {
				_ = c.Close()
			})
		}
		if err != nil || c == nil {
			t.Error("unexpected", c, err)
		}

		defer func() {
			if l := len(c.Buffer()); l != 0 {
				t.Error("unexpected final buffer", l)
			}
		}()

		err = Range(ctx, c, func(index int, value interface{}) bool {
			if index != len(bSaw) {
				t.Error("bad index vs map", index, len(bSaw))
			}
			v := value.(int)
			if v <= bLast {
				t.Errorf("saw %d last and just got %d", bLast, v)
			}
			bLast = v
			count, _ := bSaw[v]
			count++
			bSaw[v] = count
			out <- v
			return true
		})

		if err != nil && ctx.Err() == nil {
			t.Error(err)
		}
	}()

	// c
	go func() {
		defer wg.Done()

		c, err := NewChannel(nil, 0, in)
		if c != nil {
			t.Cleanup(func() {
				_ = c.Close()
			})
		}
		if err != nil || c == nil {
			t.Error("unexpected", c, err)
		}

		defer func() {
			if l := len(c.Buffer()); l != 0 {
				t.Error("unexpected final buffer", l)
			}
		}()

		err = Range(ctx, c, func(index int, value interface{}) bool {
			if index != len(cSaw) {
				t.Error("bad index vs map", index, len(cSaw))
			}
			v := value.(int)
			if v <= cLast {
				t.Errorf("saw %d last and just got %d", cLast, v)
			}
			cLast = v
			count, _ := cSaw[v]
			count++
			cSaw[v] = count
			out <- v
			return true
		})

		if err != nil && ctx.Err() == nil {
			t.Error(err)
		}
	}()

	go func() {
		defer wg.Done()

		// send 0 to 9999 in order
		for x := 0; x < 10000; x++ {
			in <- x
		}
	}()

	wg.Wait()

	aLen, bLen, cLen := len(aSaw), len(bSaw), len(cSaw)

	if l := aLen + bLen + cLen; l != 10000 {
		t.Fatal("unexpected output len", l)
	}

	if aLen < 2000 || bLen < 2000 || cLen < 2000 {
		t.Error("unexpected balance", aLen, bLen, cLen)
	}

	// verify output
	sort.Sort(sort.IntSlice(output))

	if len(output) != 10000 {
		t.Fatal("bad output")
	}

	for x := 0; x < 10000; x++ {
		if x != output[x] {
			t.Fatal("bad output")
		}
		a, _ := aSaw[x]
		b, _ := bSaw[x]
		c, _ := cSaw[x]
		delete(aSaw, x)
		delete(bSaw, x)
		delete(cSaw, x)
		if a+b+c != 1 {
			t.Fatalf("value %d was not seen exactly once (a %d, b %d, c %d)", x, a, b, c)
		}
	}

	aLen, bLen, cLen = len(aSaw), len(bSaw), len(cSaw)

	if l := aLen + bLen + cLen; l != 0 {
		t.Fatal("unexpected output len", l)
	}
}
