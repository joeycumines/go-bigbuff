package bigbuff

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestExclusive_Call_sequential(t *testing.T) {
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

		if len(e.work) != 0 || e.work == nil {
			t.Fatal(e.work)
		}
	}
}

func TestExclusive_Call_blocking(t *testing.T) {
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

	if len(e.work) != 1 {
		t.Fatal(e.work)
	}
	if i := e.work[22]; i == nil || i.work != nil || !i.running || i.count != 0 || i.complete || i.mutex == nil || i.cond == nil {
		t.Fatal(i)
	} else {
		i1 = i
	}

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

	if len(e.work) != 1 {
		t.Fatal(e.work)
	}
	if i := e.work[22]; i == nil || i.work == nil || !i.running || i.count != 3 || i.complete || i.mutex == nil || i.cond == nil {
		t.Fatal(i)
	}

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

	if len(e.work) != 1 {
		t.Fatal(e.work)
	}
	if i := e.work[22]; i == nil || i.work == nil || !i.running || i.count != 1 || i.complete || i.mutex == nil || i.cond == nil {
		t.Fatal(i)
	} else {
		i2 = i
	}

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

	if len(e.work) != 1 {
		t.Fatal(e.work)
	}
	if i := e.work[22]; i == nil || i.work != nil || !i.running || i.count != 0 || i.complete || i.mutex == nil || i.cond == nil {
		t.Fatal(i)
	} else {
		i3 = i
	}

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

func TestExclusive_Call_valuePanic(t *testing.T) {
	startGoroutines := runtime.NumGoroutine()
	defer func() {
		time.Sleep(time.Millisecond * 200)
		endGoroutines := runtime.NumGoroutine()
		if startGoroutines < endGoroutines {
			t.Error(startGoroutines, endGoroutines)
		}
	}()

	var (
		e Exclusive
		d = make(chan struct{})
		f = func() (interface{}, error) {
			panic("some_panic")
		}
	)

	go func() {
		v, err := e.CallAfter("1", f, time.Millisecond*400)
		if v != nil || err == nil || err.Error() != "bigbuff.Exclusive.CallAfterAsync recovered from panic (string): some_panic" {
			t.Error(v, err)
		}
		close(d)
	}()

	time.Sleep(time.Millisecond * 200)

	v, err := e.Call("1", f)
	if v != nil || err == nil || err.Error() != "unknown error" {
		t.Fatal(v, err)
	}

	<-d
}

func TestCallAsync(t *testing.T) {
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
