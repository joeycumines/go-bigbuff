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
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestLinearAttempt_panicCount(t *testing.T) {
	defer func() {
		if v := fmt.Sprint(recover()); v != "bigbuff.LinearAttempt invalid input" {
			t.Error(v)
		}
	}()
	LinearAttempt(context.Background(), time.Second, 0)
}

func TestLinearAttempt_single(t *testing.T) {
	defer func() func() {
		start := runtime.NumGoroutine()
		return func() {
			finish := runtime.NumGoroutine()
			if start < finish {
				t.Error(`started with`, start, `goroutines but finished with`, finish)
			}
		}
	}()()
	c := LinearAttempt(context.Background(), time.Hour, 1)
	r := reflect.ValueOf(c)
	if v, ok := r.TryRecv(); !ok {
		t.Fatal(v, ok)
	}
	if v, ok := r.TryRecv(); ok || v.Type() != reflect.TypeOf(time.Time{}) {
		t.Fatal(v, ok)
	}
}

func TestLinearAttempt_closed(t *testing.T) {
	defer func() func() {
		start := runtime.NumGoroutine()
		return func() {
			finish := runtime.NumGoroutine()
			if start < finish {
				t.Error(`started with`, start, `goroutines but finished with`, finish)
			}
		}
	}()()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := LinearAttempt(ctx, 1, 999)
	r := reflect.ValueOf(c)
	if v, ok := r.TryRecv(); ok || v.Type() != reflect.TypeOf(time.Time{}) {
		t.Fatal(v, ok)
	}
}

func TestLinearAttempt_immediateReceiveCloseAfter(t *testing.T) {
	defer func() func() {
		start := runtime.NumGoroutine()
		return func() {
			finish := runtime.NumGoroutine()
			if start < finish {
				t.Error(`started with`, start, `goroutines but finished with`, finish)
			}
		}
	}()()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := LinearAttempt(ctx, time.Millisecond*200, 999)
	r := reflect.ValueOf(c)
	if v, ok := r.TryRecv(); !ok {
		t.Fatal(v, ok)
	}
	time.Sleep(time.Millisecond * 100)
	if v, ok := r.TryRecv(); ok || v != (reflect.Value{}) {
		t.Fatal(v, ok)
	}
	time.Sleep(time.Millisecond * 200)
	if v, ok := r.TryRecv(); !ok {
		t.Fatal(v, ok)
	}
	cancel()
	time.Sleep(time.Millisecond * 100)
	if v, ok := r.TryRecv(); ok || v.Type() != reflect.TypeOf(time.Time{}) {
		t.Fatal(v, ok)
	}
	time.Sleep(time.Millisecond * 100)
	if v, ok := r.TryRecv(); ok || v.Type() != reflect.TypeOf(time.Time{}) {
		t.Fatal(v, ok)
	}
}

func ExampleLinearAttempt_full() {
	defer func() func() {
		start := runtime.NumGoroutine()
		return func() {
			finish := runtime.NumGoroutine()
			if start < finish {
				panic(fmt.Sprint(`started with`, start, `goroutines but finished with`, finish))
			}
		}
	}()()
	start := time.Now()
	for range LinearAttempt(context.Background(), time.Millisecond*200, 5) {
		fmt.Println(int64((time.Now().Sub(start) + (time.Millisecond * 100)) / (time.Millisecond * 200)))
	}
	time.Sleep(time.Millisecond * 300)
	//output:
	//0
	//1
	//2
	//3
	//4
}

func ExampleLinearAttempt_slowConsumer() {
	defer func() func() {
		start := runtime.NumGoroutine()
		return func() {
			finish := runtime.NumGoroutine()
			if start < finish {
				panic(fmt.Sprint(`started with`, start, `goroutines but finished with`, finish))
			}
		}
	}()()
	start := time.Now()
	r, c := func() (func(), func()) {
		c := LinearAttempt(context.Background(), time.Millisecond*200, 7)
		return func() {
				ts := <-c
				fmt.Println(int64((time.Now().Sub(start)+(time.Millisecond*100))/(time.Millisecond*200)), int64((ts.Sub(start)+(time.Millisecond*100))/(time.Millisecond*200)))
			}, func() {
				r := reflect.ValueOf(c)
				if v, ok := r.TryRecv(); ok || v.Type() != reflect.TypeOf(time.Time{}) {
					panic(c)
				}
			}
	}()
	defer c()
	defer time.Sleep(time.Millisecond * 300)
	time.Sleep(time.Millisecond * 325)
	r()
	r()
	r()
	r()
	time.Sleep(time.Millisecond * 525)
	r()
	r()
	r()
	//output:
	//2 0
	//2 2
	//3 3
	//4 4
	//7 5
	//7 7
	//8 8
}

type contextNeverDone struct {
	context.Context
}

var notDone = make(chan struct{})

func (c contextNeverDone) Done() <-chan struct{} {
	return notDone
}

func ExampleLinearAttempt_atMostOneTickAfterCancel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	i := 0
	for range LinearAttempt(contextNeverDone{ctx}, time.Millisecond*50, 10) {
		i++
		fmt.Printf("iteration #%d\n", i)
		if i < 5 {
			continue
		}
		cancel()
		time.Sleep(time.Millisecond * 100)
		fmt.Println(`canceled...`)
	}
	fmt.Printf("%d iterations\n", i)
	//output:
	//iteration #1
	//iteration #2
	//iteration #3
	//iteration #4
	//iteration #5
	//canceled...
	//5 iterations
}
