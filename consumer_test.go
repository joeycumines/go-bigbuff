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
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type mockProducer struct {
	_delete   func(c *consumer)
	_getAsync func(ctx context.Context, c *consumer, offset int, cancels ...context.Context) (<-chan struct {
		Value interface{}
		Error error
	}, interface{}, error)
	_commit func(c *consumer, offset int) error
}

func (p *mockProducer) delete(c *consumer) {
	if p._delete != nil {
		p._delete(c)
		return
	}
	panic("implement me")
}

func (p *mockProducer) getAsync(ctx context.Context, c *consumer, offset int, cancels ...context.Context) (<-chan struct {
	Value interface{}
	Error error
}, interface{}, error) {
	if p._getAsync != nil {
		return p._getAsync(ctx, c, offset, cancels...)
	}
	panic("implement me")
}

func (p *mockProducer) commit(c *consumer, offset int) error {
	if p._commit != nil {
		return p._commit(c, offset)
	}
	panic("implement me")
}

func TestConsumer_Close(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	calledCancel := make(chan struct{})
	calledDelete := make(chan *consumer)
	out := make(chan error)

	c := &consumer{
		done: make(chan struct{}),
		cancel: func() {
			calledCancel <- struct{}{}
		},
		producer: &mockProducer{
			_delete: func(a *consumer) {
				calledDelete <- a
			},
		},
		offset: 1,
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

	select {
	case <-calledDelete:
		t.Fatal("expected to be blocking still")
	default:
	}

	c.mutex.Lock()
	c.offset = 0
	c.cond.Broadcast()
	c.mutex.Unlock()

	if d := <-calledDelete; d != c {
		t.Fatal("unexpected delete", d)
	}

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

func TestConsumer_Done(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	d := make(chan struct{})
	c := &consumer{
		done: d,
	}
	if c.Done() != d {
		t.Errorf("unexpected done")
	}
	out := make(chan (<-chan struct{}))
	c.mutex.Lock()
	go func() {
		out <- c.Done()
	}()
	time.Sleep(time.Millisecond * 10)
	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}
	c.mutex.Unlock()
	if <-out != d {
		t.Fatal("unexpected done")
	}
}

func TestConsumer_Get_canceledInput(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	v, err := (&consumer{}).Get(ctx)

	if v != nil {
		t.Error("unexpected value", v)
	}

	if err != context.Canceled {
		t.Error("unexpected err", err)
	}
}

func TestConsumer_Get_canceledInternal(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &consumer{ctx: ctx}

	v, err := c.Get(nil)

	if v != nil {
		t.Error("unexpected value", v)
	}

	if err != context.Canceled {
		t.Error("unexpected err", err)
	}

	if c.offset != 0 {
		t.Error("unexpected offset", c.offset)
	}
}

func TestConsumer_Get_syncError(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := &consumer{
		ctx: context.Background(),
		producer: &mockProducer{
			_getAsync: func(ctx context.Context, c *consumer, offset int, cancels ...context.Context) (<-chan struct {
				Value interface{}
				Error error
			}, interface{}, error) {
				return nil, 123, errors.New("some_error")
			},
		},
	}

	v, err := c.Get(context.Background())

	if v != nil {
		t.Error("unexpected value", v)
	}

	if err == nil || err.Error() != "some_error" {
		t.Error("unexpected err", err)
	}

	if c.offset != 0 {
		t.Error("unexpected offset", c.offset)
	}
}

func TestConsumer_Get_syncSuccess(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := &consumer{
		ctx: context.Background(),
		producer: &mockProducer{
			_getAsync: func(ctx context.Context, c *consumer, offset int, cancels ...context.Context) (<-chan struct {
				Value interface{}
				Error error
			}, interface{}, error) {
				return nil, 123, nil
			},
		},
	}

	c.cond = sync.NewCond(&c.mutex)

	out := make(chan struct{})

	c.mutex.Lock()

	go func() {
		defer c.mutex.Unlock()
		c.cond.Wait()
		out <- struct{}{}
	}()

	v, err := c.Get(context.Background())

	if v != 123 {
		t.Error("unexpected value", v)
	}

	if err != nil {
		t.Error("unexpected err", err)
	}

	<-out

	if c.offset != 1 {
		t.Error("unexpected offset", c.offset)
	}
}

func TestConsumer_Get_asyncSuccess(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	z := make(chan struct {
		Value interface{}
		Error error
	})

	c := &consumer{
		ctx: context.Background(),
		producer: &mockProducer{
			_getAsync: func(ctx context.Context, c *consumer, offset int, cancels ...context.Context) (<-chan struct {
				Value interface{}
				Error error
			}, interface{}, error) {
				return z, -1, nil
			},
		},
	}

	c.cond = sync.NewCond(&c.mutex)

	out := make(chan struct{})

	c.mutex.Lock()

	go func() {
		defer c.mutex.Unlock()
		c.cond.Wait()
		out <- struct{}{}
	}()

	go func() {
		time.Sleep(time.Millisecond * 20)
		z <- struct {
			Value interface{}
			Error error
		}{
			Value: 123,
		}
	}()

	v, err := c.Get(context.Background())

	if v != 123 {
		t.Error("unexpected value", v)
	}

	if err != nil {
		t.Error("unexpected err", err)
	}

	<-out

	if c.offset != 1 {
		t.Error("unexpected offset", c.offset)
	}
}

func TestConsumer_Get_asyncError(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	z := make(chan struct {
		Value interface{}
		Error error
	})

	c := &consumer{
		ctx: context.Background(),
		producer: &mockProducer{
			_getAsync: func(ctx context.Context, c *consumer, offset int, cancels ...context.Context) (<-chan struct {
				Value interface{}
				Error error
			}, interface{}, error) {
				return z, -1, nil
			},
		},
	}

	go func() {
		time.Sleep(time.Millisecond * 20)
		z <- struct {
			Value interface{}
			Error error
		}{
			Error: errors.New("some_error"),
		}
	}()

	v, err := c.Get(context.Background())
	if v != nil {
		t.Error("unexpected value", v)
	}

	if err == nil || err.Error() != "some_error" {
		t.Error("unexpected err", err)
	}

	if c.offset != 0 {
		t.Error("unexpected offset", c.offset)
	}
}

func TestConsumer_Commit_nothing(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	out := make(chan error)

	c := &consumer{}

	c.mutex.Lock()

	go func() {
		out <- c.Commit()
	}()

	time.Sleep(time.Millisecond * 10)

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	c.mutex.Unlock()

	if err := <-out; err == nil || err.Error() != "bigbuff.consumer.Commit nothing to commit" {
		t.Fatal("unexpected error", err)
	}
}

func TestConsumer_Rollback_nothing(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	out := make(chan error)

	c := &consumer{}

	c.mutex.Lock()

	go func() {
		out <- c.Rollback()
	}()

	time.Sleep(time.Millisecond * 10)

	select {
	case <-out:
		t.Fatal("expected to be blocked")
	default:
	}

	c.mutex.Unlock()

	if err := <-out; err == nil || err.Error() != "bigbuff.consumer.Rollback nothing to rollback" {
		t.Fatal("unexpected error", err)
	}
}

func TestConsumer_Rollback(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := &consumer{
		offset: 1,
	}
	c.cond = sync.NewCond(&c.mutex)

	out := make(chan struct{})

	c.mutex.Lock()
	go func() {
		defer c.mutex.Unlock()
		c.cond.Wait()
		out <- struct{}{}
	}()

	if err := c.Rollback(); err != nil {
		t.Error("unexpected error", err)
	}

	<-out

	if c.offset != 0 {
		t.Error("unexpected offset", c.offset)
	}
}

func TestConsumer_Commit_success(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := &consumer{
		offset: 159,
	}
	var (
		pC *consumer
		pO int
	)
	c.cond = sync.NewCond(&c.mutex)
	c.producer = &mockProducer{
		_commit: func(c *consumer, offset int) error {
			pC = c
			pO = offset
			return nil
		},
	}

	out := make(chan struct{})

	c.mutex.Lock()
	go func() {
		defer c.mutex.Unlock()
		c.cond.Wait()
		out <- struct{}{}
	}()

	if err := c.Commit(); err != nil {
		t.Error("unexpected error", err)
	}

	<-out

	if c.offset != 0 {
		t.Error("unexpected offset", c.offset)
	}

	if pC != c {
		t.Error("unexpected consumer", pC)
	}

	if pO != 159 {
		t.Error("unexpected commit offset", pO)
	}
}

func TestConsumer_Commit_error(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	c := &consumer{
		offset: 159,
	}
	var (
		pC *consumer
		pO int
	)
	c.producer = &mockProducer{
		_commit: func(c *consumer, offset int) error {
			pC = c
			pO = offset
			return errors.New("some_error")
		},
	}

	if err := c.Commit(); err == nil || err.Error() != "some_error" {
		t.Error("unexpected error", err)
	}

	if c.offset != 159 {
		t.Error("unexpected offset", c.offset)
	}

	if pC != c {
		t.Error("unexpected consumer", pC)
	}

	if pO != 159 {
		t.Error("unexpected commit offset", pO)
	}
}

// TestConsumer_Get_leaky is a regression test for a bug where async wait conditions would block forever
func TestConsumer_Get_leaky(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	startGoroutines := runtime.NumGoroutine()
	defer func() {
		time.Sleep(time.Millisecond * 200)
		endGoroutines := runtime.NumGoroutine()
		if endGoroutines > startGoroutines {
			t.Error(startGoroutines, endGoroutines)
		}
	}()
	var buffer Buffer
	defer buffer.Close()
	consumer, err := buffer.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		var x int
		for {
			x++
			select {
			case <-ticker.C:
				if err := buffer.Put(ctx, x); err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	for x := 1; x <= 1000; x++ {
		v, err := consumer.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		y := v.(int)
		if x != y {
			t.Fatal(x, y)
		}
		if err := consumer.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	endGoroutines := runtime.NumGoroutine()
	if endGoroutines > startGoroutines+20 {
		t.Error(startGoroutines, endGoroutines)
	}
}
