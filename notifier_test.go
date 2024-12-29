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
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func ExampleNotifier_pubSubKeys() {
	defer checkNumGoroutines(nil)

	var (
		k1 = `some-key`
		k2 = 100
		c1 = make(chan string)
		c2 = make(chan string)
		c3 = make(chan string)
		wg sync.WaitGroup
		nf Notifier
	)
	wg.Add(3)
	go func() {
		defer wg.Done()
		for v := range c1 {
			fmt.Println("c1 recv:", v)
		}
	}()
	go func() {
		defer wg.Done()
		for v := range c2 {
			fmt.Println("c2 recv:", v)
		}
	}()
	go func() {
		defer wg.Done()
		for v := range c3 {
			time.Sleep(time.Millisecond * 100)
			fmt.Println("c3 recv:", v)
		}
	}()
	nf.Subscribe(k1, c1)
	nf.Subscribe(k2, c2)
	nf.Subscribe(k1, c3)
	nf.Subscribe(k2, c3)

	nf.Publish(k1, `one`)
	time.Sleep(time.Millisecond * 200)
	nf.Publish(k2, `two`)

	close(c1)
	close(c2)
	close(c3)
	wg.Wait()

	//Output:
	//c1 recv: one
	//c3 recv: one
	//c2 recv: two
	//c3 recv: two
}

func ExampleNotifier_contextCancelSubscribe() {
	defer checkNumGoroutines(nil)

	var (
		nf          Notifier
		k           = 0
		c           = make(chan string)
		d           = make(chan struct{})
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()

	nf.SubscribeContext(ctx, k, c)

	fmt.Println(`starting blocking publish then waiting a bit...`)
	go func() {
		defer close(d)
		fmt.Println(`publish start`)
		nf.Publish(k, `one`)
		fmt.Println(`publish finish`)
	}()
	time.Sleep(time.Millisecond * 100)

	fmt.Println(`canceling context then blocking for publish exit...`)
	cancel()
	<-d

	fmt.Println(`closing publish channel...`)
	close(c)
	time.Sleep(time.Millisecond * 50)

	fmt.Println(`success!`)

	//Output:
	//starting blocking publish then waiting a bit...
	//publish start
	//canceling context then blocking for publish exit...
	//publish finish
	//closing publish channel...
	//success!
}

func TestNotifier_PublishContext_cancel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		nf          = new(Notifier)
		out         = make(chan int, 10)
		key         = 1
		target      = make(chan interface{})
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()

	nf.Subscribe(key, target)

	go func() {
		time.Sleep(time.Millisecond * 100)
		cancel()
		out <- 1
	}()
	go func() {
		nf.PublishContext(ctx, key, `val`)
		out <- 2
	}()
	go func() {
		nf.Publish(key, true)
		out <- 3
	}()

	if v := <-out; v != 1 {
		t.Fatal(v)
	}
	if v := <-out; v != 2 {
		t.Fatal(v)
	}
	select {
	case v := <-out:
		t.Fatal(v)
	default:
	}

	if v := <-target; v != true {
		t.Fatal(v)
	}
	if v := <-out; v != 3 {
		t.Fatal(v)
	}
}

func TestNotifier_PublishContext_cancelGuarded(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		nf          = new(Notifier)
		key         = 1
		target      = make(chan interface{})
		ctx, cancel = context.WithCancel(context.Background())
	)
	cancel()

	nf.Subscribe(key, target)
	if len(nf.subscribers) != 1 || len(nf.subscribers[key]) != 1 || nf.subscribers[key][reflect.ValueOf(target).Pointer()].ctx != nil {
		t.Fatal(nf.subscribers)
	}

	nf.PublishContext(ctx, key, errors.New(`some_error`))
}

func TestNotifier_SubscribeContext_premptivelyCanceled(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		nf          = new(Notifier)
		key         = 1
		target      = make(chan interface{})
		ctx, cancel = context.WithCancel(context.Background())
	)
	cancel()

	nf.SubscribeContext(ctx, key, target)
	if len(nf.subscribers) != 1 {
		t.Fatal(nf.subscribers)
	}
	if len(nf.subscribers[key]) != 1 {
		t.Fatal(nf.subscribers[key])
	}

	nf.Publish(key, errors.New(`some_error`))

	nf.Unsubscribe(key, target)

	if len(nf.subscribers) != 0 {
		t.Fatal(nf.subscribers)
	}
}

func TestNotifier_SubscribeContext_exists(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	target := make(chan interface{})
	nf := Notifier{
		subscribers: map[interface{}]map[uintptr]notifierSubscriber{
			123: {
				reflect.ValueOf(target).Pointer(): {},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	defer func() {
		if r := recover(); r == nil || !strings.HasPrefix(fmt.Sprint(r), `bigbuff.Notifier subscription already exists: key=123 `) {
			t.Error(r)
		}
	}()
	nf.SubscribeContext(ctx, 123, target)
	t.Error(`expected fatal`)
}

func TestNotifier_Unsubscribe_onceOnly(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	target := make(chan interface{})
	nf := Notifier{
		subscribers: map[interface{}]map[uintptr]notifierSubscriber{
			123: {
				reflect.ValueOf(target).Pointer(): {},
			},
		},
	}
	nf.Unsubscribe(123, target)
	if nf.subscribers != nil {
		t.Error(nf.subscribers)
	}
	defer func() {
		if r := recover(); r == nil || !strings.HasPrefix(fmt.Sprint(r), `bigbuff.Notifier subscription not found: key=123 `) {
			t.Error(r)
		}
	}()
	nf.Unsubscribe(123, target)
	t.Error(`expected fatal`)
}

func TestValueOfNotifierTarget_nil(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		if r := recover(); r == nil || !strings.HasPrefix(fmt.Sprint(r), `bigbuff.Notifier invalid target kind: invalid`) {
			t.Error(r)
		}
	}()
	valueOfNotifierTarget(nil)
	t.Error(`expected fatal`)
}

func TestValueOfNotifierTarget_readOnly(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		a = make(chan struct{})
		b = (chan<- struct{})(a)
		c = (<-chan struct{})(a)
	)
	valueOfNotifierTarget(a)
	valueOfNotifierTarget(b)
	defer func() {
		if r := recover(); r == nil || !strings.HasPrefix(fmt.Sprint(r), `bigbuff.Notifier invalid target channel direction: <-chan`) {
			t.Error(r)
		}
	}()
	valueOfNotifierTarget(c)
	t.Error(`expected fatal`)
}

func TestNotifier_Publish_none(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	nf := &Notifier{
		subscribers: map[interface{}]map[uintptr]notifierSubscriber{},
	}
	nf.Publish(nil, nil)
}

func TestNotifier_highVolumeIntegrityCheck(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		nf          Notifier
		key1        = `k1`
		wg1         sync.WaitGroup
		count1      = 100
		increments1 = 5000
		c1          = make(chan int, increments1*2)
		c2          = make(chan int, increments1*2)
		c3          = make(chan int, increments1*2)
		c4          = make(chan string)
		c5          = make(chan Producer)
		c6          = make(chan int)
		key2        = `k2`
	)

	// process 0 to increments1 for count1 subscribers
	wg1.Add(count1)
	for x := 0; x < count1; x++ {
		func() {
			c := make(chan int)
			ctx, cancel := context.WithCancel(context.Background())
			nf.SubscribeContext(ctx, key1, c)
			go func() {
				defer wg1.Done()
				defer nf.Unsubscribe(key1, c)
				defer cancel()
				for x := 0; x < increments1; x++ {
					v := <-c
					if v != x {
						t.Fatal(v, x)
					}
				}
			}()
		}()
	}

	// subscribe extra
	for _, c := range []interface{}{c1, c2, c3, c4, c5} {
		nf.Subscribe(key1, c)
	}
	nf.Subscribe(key2, c6)

	// publish twice the necessary amount of values (tests unsubscribe deadlock)
	for x := 0; x < increments1*2; x++ {
		nf.Publish(key1, x)
	}

	wg1.Wait()

	// unsubscribe extra
	for _, c := range []interface{}{c1, c2, c3, c4, c5} {
		nf.Unsubscribe(key1, c)
	}
	nf.Unsubscribe(key2, c6)

	if len(nf.subscribers) != 0 {
		t.Error(nf.subscribers)
	}

	// validate buffered channels
	for _, c := range []chan int{c1, c2, c3} {
		close(c)
		for x := 0; x < increments1*2; x++ {
			v := <-c
			if v != x {
				t.Fatal(v, x)
			}
		}
		for v := range c {
			t.Fatal(v)
		}
	}
}

func ExampleNotifier_SubscribeCancel() {
	defer checkNumGoroutines(nil)

	defer func() func() {
		startGoroutines := runtime.NumGoroutine()
		return func() {
			time.Sleep(time.Millisecond * 200)
			endGoroutines := runtime.NumGoroutine()
			if endGoroutines <= startGoroutines {
				fmt.Println(`our resources were freed`)
			}
		}
	}()()
	var (
		nf          Notifier
		ping        = make(chan float64)
		pong        = make(chan float64)
		ctx, cancel = context.WithCancel(context.Background())
	)
	defer cancel()
	defer nf.SubscribeCancel(nil, `ping`, ping)()
	defer nf.SubscribeCancel(nil, `pong`, pong)()
	go func() {
		// worker which will receive all values and respond with that value x2
		// but first... sleep, to demonstrate it's not racey
		time.Sleep(time.Millisecond * 100)
		for {
			select {
			case <-ctx.Done():
				fmt.Println(`worker exiting`)
				return
			case value := <-ping:
				nf.PublishContext(ctx, `pong`, value*2)
			}
		}
	}()
	fmt.Println(`PING 5 x 2 = ...`)
	nf.PublishContext(ctx, `ping`, 5.0)
	fmt.Println(`PONG`, <-pong)
	fmt.Println(`PING -23 x 2 = ...`)
	nf.PublishContext(ctx, `ping`, -23.0)
	fmt.Println(`PONG`, <-pong)
	//Output:
	//PING 5 x 2 = ...
	//PONG 10
	//PING -23 x 2 = ...
	//PONG -46
	//worker exiting
	//our resources were freed
}

func TestNotifier_SubscribeCancel_panic(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() func() {
		startGoroutines := runtime.NumGoroutine()
		return func() {
			time.Sleep(time.Millisecond * 200)
			endGoroutines := runtime.NumGoroutine()
			if endGoroutines > startGoroutines {
				t.Error(endGoroutines, startGoroutines)
			}
		}
	}()()
	target := make(chan interface{})
	nf := Notifier{
		subscribers: map[interface{}]map[uintptr]notifierSubscriber{
			123: {
				reflect.ValueOf(target).Pointer(): {},
			},
		},
	}
	defer func() {
		if r := recover(); r == nil || !strings.HasPrefix(fmt.Sprint(r), `bigbuff.Notifier subscription already exists: key=123 `) {
			t.Error(r)
		}
	}()
	nf.SubscribeCancel(nil, 123, target)
	t.Error(`expected fatal`)
}

func TestNotifier_SubscribeCancel_parentCancel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() func() {
		startGoroutines := runtime.NumGoroutine()
		return func() {
			time.Sleep(time.Millisecond * 200)
			endGoroutines := runtime.NumGoroutine()
			if endGoroutines > startGoroutines {
				t.Error(endGoroutines, startGoroutines)
			}
		}
	}()()
	var (
		nf          Notifier
		target      = make(chan interface{})
		ctx, cancel = context.WithCancel(context.Background())
	)
	nf.SubscribeContext(ctx, nil, target)
	go func() {
		time.Sleep(time.Millisecond * 100)
		cancel()
	}()
	nf.Publish(nil, 123)
}
