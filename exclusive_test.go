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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExclusive_Call_sequential(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	e := new(Exclusive)

	for x := 0; x < 5; x++ {
		v, err := e.Call(
			1,
			func() (interface{}, error) {
				return "one", errors.New("some_error")
			},
		)

		if v != "one" || err == nil || err.Error() != "some_error" {
			t.Fatal(v, err)
		}

		var failed bool
		var s string
		for range 5 {
			e.mutex.Lock()
			failed = len(e.work) != 0 || e.work == nil
			s = fmt.Sprint(e.work)
			e.mutex.Unlock()
			if !failed {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
		if failed {
			t.Fatal(s)
		}
	}
}

func TestExclusive_Call_blocking(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		e  = new(Exclusive)
		in = make(chan struct {
			V interface{}
			E error
		})
		f = func() (interface{}, error) {
			v := <-in
			return v.V, v.E
		}
		order      = make(chan int, 50)
		v1         = 7243
		e1         = errors.New("12451")
		v2         = 1471
		e2         = errors.New("6235")
		v3         = 781
		e3         = errors.New("623x")
		i1, i2, i3 *exclusiveItem
	)

	go func() {
		v, err := e.Call(22, f)
		if v != v1 || err != e1 {
			t.Error(v, err)
		}
		order <- 1
	}()

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work != nil || !i.running || i.count != 0 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		} else {
			i1 = i
		}
	}()

	v, err := e.Call(
		"a",
		func() (interface{}, error) {
			return 88, e1
		},
	)
	if v != 88 || err != e1 {
		t.Fatal(v, err)
	}

	go func() {
		v, err := e.Call(22, f)
		if v != v2 || err != e2 {
			t.Error(v, err)
		}
		order <- 2
	}()

	go func() {
		v, err := e.Call(22, f)
		if v != v2 || err != e2 {
			t.Error(v, err)
		}
		order <- 2
	}()

	go func() {
		v, err := e.Call(22, f)
		if v != v2 || err != e2 {
			t.Error(v, err)
		}
		order <- 2
	}()

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work == nil || !i.running || i.count != 3 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		}
	}()

	in <- struct {
		V interface{}
		E error
	}{
		V: v1,
		E: e1,
	}

	if v := <-order; v != 1 {
		t.Fatal(v)
	}

	go func() {
		v, err := e.Call(22, f)
		if v != v3 || err != e3 {
			t.Error(v, err)
		}
		order <- 3
	}()

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work == nil || !i.running || i.count != 1 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		} else {
			i2 = i
		}
	}()

	in <- struct {
		V interface{}
		E error
	}{
		V: v2,
		E: e2,
	}

	if v := <-order; v != 2 {
		t.Fatal(v)
	}
	if v := <-order; v != 2 {
		t.Fatal(v)
	}
	if v := <-order; v != 2 {
		t.Fatal(v)
	}

	time.Sleep(time.Millisecond * 500)

	select {
	case <-order:
		t.Fatal("expected none")
	default:
	}

	func() {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if len(e.work) != 1 {
			t.Fatal(e.work)
		}
		if i := e.work[22]; i == nil || i.work != nil || !i.running || i.count != 0 || i.complete || i.mutex == nil || i.cond == nil {
			t.Fatal(i)
		} else {
			i3 = i
		}
	}()

	in <- struct {
		V interface{}
		E error
	}{
		V: v3,
		E: e3,
	}

	if v := <-order; v != 3 {
		t.Fatal(v)
	}

	close(order)

	for range order {
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()
	if len(e.work) != 0 {
		t.Fatal(e)
	}

	if i1 == i2 || i1 == i3 || i2 == i3 || i1.mutex != i2.mutex || i3.mutex != i1.mutex || i1.cond != i2.cond || i3.cond != i1.cond {
		t.Fatal(i1, i2, i3)
	}
	if !i1.complete || i1.running {
		t.Fatal(i1)
	}
	if !i2.complete || i2.running {
		t.Fatal(i2)
	}
	if i3.complete || i3.running {
		// i3 is never started
		t.Fatal(i3)
	}
}

func TestExclusive_Call_concurrent(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	type Output struct {
		Key    int
		Start  int64
		Finish int64
		Actual bool
		Index  int
	}

	var (
		count     = 10000
		group     = 100
		wg        sync.WaitGroup
		mutex     sync.Mutex
		outputs   []Output
		exclusive Exclusive
	)

	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()

			output := Output{
				Index: i,
				Start: time.Now().UnixNano(),
				Key:   i / group,
			}

			v, err := exclusive.Call(
				output.Key,
				func() (interface{}, error) {
					ts := time.Now()
					output.Actual = true
					if output.Key >= count/group/2 {
						time.Sleep(time.Millisecond * 500)
					}
					return ts.UnixNano(), nil
				},
			)
			if err != nil {
				t.Error(err)
			}

			output.Finish = v.(int64)

			mutex.Lock()
			defer mutex.Unlock()
			outputs = append(outputs, output)
		}(i)
	}

	wg.Wait()

	if len(outputs) != count {
		t.Fatal(len(outputs), outputs)
	}
	exclusive.mutex.Lock()
	if len(exclusive.work) != 0 {
		t.Fatal(exclusive.work)
	}
	exclusive.mutex.Unlock()

	actualCounts := make(map[int]int)
	seenIndexes := make(map[int]int)

	for _, output := range outputs {
		key := output.Index / group
		if output.Actual {
			actualCounts[key]++
		}
		seenIndexes[output.Index]++
		if output.Start > output.Finish {
			t.Error(output)
		}
	}

	if l := len(seenIndexes); l != count {
		t.Fatal("indexes", l, "!= expected", count, seenIndexes)
	}
	for k := 0; k < count; k++ {
		v := seenIndexes[k]
		if v != 1 {
			t.Fatal("wat", k)
		}
	}

	if l := len(actualCounts); l != count/group {
		t.Fatal("counts", l, "!= expected", count/group, actualCounts)
	}
	for k := 0; k < count/group; k++ {
		v := actualCounts[k]
		if k < count/group/2 {
			if v <= 2 {
				t.Error("bad fast count", k, "=", v)
			}
		} else if v != 2 {
			t.Error("bad count", k, "=", v)
		}
	}
}

func TestExclusive_Call_nilReceiver(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		if recover() == nil {
			t.Error("expected panic")
		}
	}()
	var e *Exclusive
	e.Call(1, func() (interface{}, error) {
		return nil, nil
	})
	t.Error("should not reach")
}

func TestExclusive_Call_nilValue(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		if recover() == nil {
			t.Error("expected panic")
		}
	}()
	var e Exclusive
	e.Call(1, nil)
	t.Error("should not reach")
}

func TestExclusive_CallAfter(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	type Output struct {
		Key    int
		Start  int64
		Finish int64
		Actual bool
		Index  int
	}

	var (
		count     = 10000
		group     = 100
		wg        sync.WaitGroup
		mutex     sync.Mutex
		outputs   []Output
		exclusive Exclusive
	)

	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()

			output := Output{
				Index: i,
				Start: time.Now().UnixNano(),
				Key:   i / group,
			}

			v, err := exclusive.CallAfter(
				output.Key,
				func() (interface{}, error) {
					ts := time.Now()
					output.Actual = true
					return ts.UnixNano(), nil
				},
				time.Millisecond*500,
			)
			if err != nil {
				t.Error(err)
			}

			output.Finish = v.(int64)

			mutex.Lock()
			defer mutex.Unlock()
			outputs = append(outputs, output)
		}(i)
	}

	wg.Wait()

	if len(outputs) != count {
		t.Fatal(len(outputs), outputs)
	}
	exclusive.mutex.Lock()
	if len(exclusive.work) != 0 {
		t.Fatal(exclusive.work)
	}
	exclusive.mutex.Unlock()

	actualCounts := make(map[int]int)
	seenIndexes := make(map[int]int)

	for _, output := range outputs {
		key := output.Index / group
		if output.Actual {
			actualCounts[key]++
		}
		seenIndexes[output.Index]++
		if output.Start > output.Finish {
			t.Error(output)
		}
	}

	if l := len(seenIndexes); l != count {
		t.Fatal("indexes", l, "!= expected", count, seenIndexes)
	}
	for k := 0; k < count; k++ {
		v := seenIndexes[k]
		if v != 1 {
			t.Fatal("wat", k)
		}
	}

	if l := len(actualCounts); l != count/group {
		t.Fatal("counts", l, "!= expected", count/group, actualCounts)
	}
	for k := 0; k < count/group; k++ {
		v := actualCounts[k]
		if v != 1 {
			t.Error("bad count", k, "=", v)
		}
	}
}

func TestExclusive_CallAfter_adjustedWait(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		e Exclusive
		c int
		d = make(chan struct{})
	)

	ts := time.Now()

	go func() {
		e.Call(1, func() (interface{}, error) {
			c++
			close(d)
			time.Sleep(time.Millisecond * 500)
			return nil, nil
		})
	}()

	<-d

	e.CallAfter(1, func() (interface{}, error) {
		c++
		return nil, nil
	}, time.Millisecond*800)

	diff := time.Now().Sub(ts)

	if diff > time.Millisecond*900 || diff < time.Millisecond*700 {
		t.Error(diff)
	}

	if c != 2 {
		t.Error(c)
	}
}

func TestExclusive_CallAfter_adjustedWaitSmaller(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		e Exclusive
		c int
		d = make(chan struct{})
	)

	ts := time.Now()

	go func() {
		e.Call(1, func() (interface{}, error) {
			c++
			close(d)
			time.Sleep(time.Millisecond * 500)
			return nil, nil
		})
	}()

	<-d

	e.CallAfter(1, func() (interface{}, error) {
		c++
		return nil, nil
	}, time.Millisecond*100)

	diff := time.Now().Sub(ts)

	if diff < time.Millisecond*400 || diff > time.Millisecond*600 {
		t.Error(diff)
	}

	if c != 2 {
		t.Error(c)
	}
}

func TestCallAsync(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	for x := 0; x < 10; x++ {
		var (
			exclusive   Exclusive
			outcomeChan <-chan *ExclusiveOutcome
			mutex       sync.Mutex
			locked      = true
			calls       int
		)
		exclusive.CallAsync(1, func() (interface{}, error) {
			time.Sleep(time.Millisecond * 200)
			mutex.Lock()
			locked = false
			mutex.Unlock()
			return nil, nil
		})
		for y := 0; y < 1000; y++ {
			func(y int) {
				outcomeChan = exclusive.CallAsync(1, func() (interface{}, error) {
					mutex.Lock()
					if locked {
						t.Error(locked)
					}
					calls++
					mutex.Unlock()
					time.Sleep(time.Millisecond * 250)
					return y, nil
				})
			}(y)
		}
		outcome := <-outcomeChan
		if outcome.Result != 1000-1 || outcome.Error != nil {
			t.Error(outcome)
		}
		if v := <-outcomeChan; v != nil {
			t.Fatal(v)
		}
		mutex.Lock()
		if calls != 1 {
			t.Error(calls)
		}
		mutex.Unlock()
	}
}

func TestExclusive_Start(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		mutex   sync.Mutex
		counter int
		final   int
		e       Exclusive
		wg      sync.WaitGroup
	)
	wg.Add(10)
	for x := 0; x < 10; x++ {
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 5)
			for x := 0; x < 10; x++ {
				e.Start(nil, func() (i interface{}, e error) {
					time.Sleep(time.Millisecond * 200)
					mutex.Lock()
					counter++
					final = counter
					mutex.Unlock()
					return
				})
				time.Sleep(time.Millisecond * 100)
			}
		}()
	}
	wg.Wait()
	time.Sleep(time.Millisecond * 300)
	mutex.Lock()
	if counter != final {
		t.Fatal(counter, final)
	}
	if final != 6 {
		t.Fatal(final)
	}
}

func TestExclusive_StartAfter(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		mutex   sync.Mutex
		counter int
		final   int
		e       Exclusive
		value   = new(int)
		err     = errors.New(`err`)
	)
	e.StartAfter(
		nil,
		func() (interface{}, error) {
			t.Error(`bad`)
			panic(`bad`)
		},
		time.Millisecond*100,
	)
	outcome := e.CallAsync(
		nil,
		func() (i interface{}, e error) {
			t.Error(`bad`)
			panic(`bad`)
		},
	)
	e.Start(
		nil,
		func() (interface{}, error) {
			mutex.Lock()
			counter++
			final = counter
			mutex.Unlock()
			return value, err
		},
	)
	if outcome := <-outcome; outcome.Result != value || outcome.Error != err {
		t.Error(outcome)
	}
	mutex.Lock()
	if counter != 1 || counter != final {
		t.Fatal(counter)
	}
	mutex.Unlock()
	time.Sleep(time.Millisecond * 200)
	mutex.Lock()
	if counter != 1 || counter != final {
		t.Fatal(counter)
	}
	mutex.Unlock()
}

var benchmarkOutput interface{}

func BenchmarkExclusive_outcomeContention(b *testing.B) {
	for _, tc := range [...]struct {
		Name  string
		Count int
	}{
		{
			Name:  `count 32768`,
			Count: 1 << 15,
		},
		{
			Name:  `count 16384`,
			Count: 1 << 14,
		},
		{
			Name:  `count 8192`,
			Count: 1 << 13,
		},
		{
			Name:  `count 1024`,
			Count: 1 << 10,
		},
		{
			Name:  `count 512`,
			Count: 1 << 9,
		},
		{
			Name:  `count 256`,
			Count: 1 << 8,
		},
		{
			Name:  `count 16`,
			Count: 1 << 4,
		},
		{
			Name:  `count 1`,
			Count: 1,
		},
	} {
		b.Run(tc.Name, func(b *testing.B) {
			b.StopTimer()
			var r int
			for n := 0; n < b.N; n++ {
				func() {
					var exclusive Exclusive

					// initial set up
					initialIn := make(chan struct{})
					defer close(initialIn)
					initialOut := make(chan struct{})
					defer close(initialOut)
					initialOutcome := exclusive.CallWithOptions(ExclusiveValue(func() (interface{}, error) {
						initialIn <- struct{}{}
						<-initialOut
						return 123, nil
					}))
					defer func() {
						v := <-initialOutcome
						r = v.Result.(int)
					}()
					<-initialIn

					item := exclusive.work[nil]

					in := make(chan struct{})
					defer close(in)

					var wg sync.WaitGroup
					wg.Add(tc.Count)

					for i := 0; i < tc.Count; i++ {
						go func() {
							v := <-exclusive.CallWithOptions(ExclusiveValue(func() (interface{}, error) {
								in <- struct{}{}
								return 456, nil
							}))
							r = v.Result.(int)
							wg.Done()
						}()
					}

					item.mutex.Lock()
					for item.count != tc.Count {
						item.mutex.Unlock()
						time.Sleep(time.Microsecond)
						item.mutex.Lock()
					}
					item.mutex.Unlock()

					b.StartTimer()

					initialOut <- struct{}{}
					<-in
					wg.Wait()

					b.StopTimer()
				}()
			}
			benchmarkOutput = r
		})
	}
}

func TestExclusive_CallWithOptions_startInitialCase(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		x     Exclusive
		calls int32
	)
	if v := x.CallWithOptions(ExclusiveStart(true), ExclusiveWork(func(resolve func(result interface{}, err error)) { atomic.AddInt32(&calls, 1) })); v != nil {
		t.Error()
	}
	for i := time.Duration(0); i < 1000 && atomic.LoadInt32(&calls) == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	if v := atomic.LoadInt32(&calls); v != 1 {
		t.Error(v)
	}
}

func TestExclusive_CallWithOptions_resolveNotCalled(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	v := <-new(Exclusive).CallWithOptions(ExclusiveWork(func(resolve func(result interface{}, err error)) {}))
	if v.Error != errResolveNotCalled || v.Result != nil {
		t.Error(v)
	}
}

func TestExclusiveRateLimit(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	const (
		num  = 100
		wait = time.Millisecond * 500
		min  = time.Millisecond * 60
	)
	var (
		x       Exclusive
		called  int64
		shorter int64
		wg      sync.WaitGroup
	)
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func() {
			var last int64
			for {
				ts := time.Now()
				v := <-x.CallWithOptions(
					ExclusiveRateLimit(ctx, min),
					ExclusiveWork(func(resolve func(result interface{}, err error)) { resolve(atomic.AddInt64(&called, 1), nil) }),
				)
				if d := time.Since(ts); d <= min/2 {
					atomic.AddInt64(&shorter, 1)
				}
				if v.Error != nil {
					if v.Error != context.DeadlineExceeded {
						t.Error(v)
					}
					break
				}
				this := v.Result.(int64)
				if this <= last {
					t.Error(this, last)
				}
				last = this
			}
			wg.Done()
		}()
	}
	<-ctx.Done()
	ts := time.Now()
	wg.Wait()
	if d := time.Since(ts); d > time.Millisecond*10 {
		t.Error(d)
	}
	if v := atomic.LoadInt64(&shorter); v <= num/2 {
		t.Error(v)
	}
	if v := math.Round(math.Abs(float64(atomic.LoadInt64(&called)) - float64(wait)/float64(min))); v > 1 {
		t.Error(v)
	}
	//t.Log(atomic.LoadInt64(&called), atomic.LoadInt64(&shorter))
}
