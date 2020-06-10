/*
   Copyright 2020 Joseph Cumines

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
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestExponentialRetry_panic(t *testing.T) {
	defer func() {
		if v := fmt.Sprint(recover()); v != "bigbuff.ExponentialRetry nil value" {
			t.Error(v)
		}
	}()
	ExponentialRetry(context.Background(), time.Second, nil)
}

func TestExponentialRetry_success(t *testing.T) {
	defer func() func() {
		startGoroutines := runtime.NumGoroutine()
		return func() {
			endGoroutines := runtime.NumGoroutine()
			if startGoroutines < endGoroutines {
				t.Error(startGoroutines, endGoroutines)
			}
		}
	}()()

	type Event struct {
		Name     string
		Done     bool
		Count    int
		Duration time.Duration
	}

	eCtx, eCancel := context.WithCancel(context.Background())
	defer eCancel()
	rate := time.Second

	var (
		events   []Event
		done     bool
		count    int
		duration time.Duration
		calc     int
	)

	defer func() func() {
		old := calcExponentialRetry
		calcExponentialRetry = func(d time.Duration, c uint32) time.Duration {
			calc++
			if d != rate {
				t.Fatal(d, rate)
			}
			if c > maxShiftUint32 {
				t.Fatal(c, maxShiftUint32)
			}
			return time.Duration(c)
		}
		return func() {
			calcExponentialRetry = old
		}
	}()()
	defer func() func() {
		old := waitDuration
		waitDuration = func(ctx context.Context, d time.Duration) {
			if ctx != eCtx {
				t.Error(ctx, eCtx)
			}
			if d < 0 {
				t.Error(d)
			}
			done = ctx.Err() != nil
			duration = d
			events = append(events, Event{
				Name:     `wait`,
				Done:     done,
				Count:    count,
				Duration: duration,
			})
		}
		return func() {
			waitDuration = old
		}
	}()()

	result, err := ExponentialRetry(
		eCtx,
		rate,
		func() (interface{}, error) {
			count++
			events = append(events, Event{
				Name:     `call`,
				Done:     done,
				Count:    count,
				Duration: duration,
			})
			if count == 50 {
				return "success", nil
			}
			return 23123, errors.New("some_error")
		},
	)()
	if result != "success" || err != nil {
		t.Fatal(result, err)
	}
	if len(events) != 99 {
		t.Fatal(len(events))
	}
	if calc != 49 {
		t.Fatal(calc)
	}
	for i, event := range events {
		if i%2 == 0 {
			if event.Name != `call` {
				t.Fatal(i, event)
			}
		} else {
			if event.Name != `wait` {
				t.Fatal(i, event)
			}
		}
		if event.Count != (i+2)/2 {
			t.Fatal(i, event)
		}
		if event.Name == `wait` {
			if event.Count < 32 {
				if event.Duration != time.Duration(event.Count) {
					t.Fatal(i, event)
				}
			} else {
				if event.Duration != 31 {
					t.Fatal(i, event)
				}
			}
		}
		//t.Log(i, event)
	}
}

func TestExponentialRetry_cancelled(t *testing.T) {
	defer func() func() {
		startGoroutines := runtime.NumGoroutine()
		return func() {
			endGoroutines := runtime.NumGoroutine()
			if startGoroutines < endGoroutines {
				t.Error(startGoroutines, endGoroutines)
			}
		}
	}()()
	defer func() func() {
		old := calcExponentialRetry
		calcExponentialRetry = func(d time.Duration, c uint32) time.Duration {
			t.Error("bad")
			panic("unexpected")
		}
		return func() {
			calcExponentialRetry = old
		}
	}()()
	defer func() func() {
		old := waitDuration
		waitDuration = func(ctx context.Context, d time.Duration) {
			t.Error("bad")
			panic("unexpected")
		}
		return func() {
			waitDuration = old
		}
	}()()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := ExponentialRetry(
		ctx,
		0,
		func() (interface{}, error) {
			t.Error("bad")
			panic("unexpected")
		},
	)()
	if result != nil || err == nil || err.Error() != ctx.Err().Error() {
		t.Fatal(result, err)
	}
}

func TestExponentialRetry_fatal(t *testing.T) {
	defer func() func() {
		startGoroutines := runtime.NumGoroutine()
		return func() {
			endGoroutines := runtime.NumGoroutine()
			if startGoroutines < endGoroutines {
				t.Error(startGoroutines, endGoroutines)
			}
		}
	}()()
	defer func() func() {
		old := calcExponentialRetry
		calcExponentialRetry = func(d time.Duration, c uint32) time.Duration {
			t.Error("bad")
			panic("unexpected")
		}
		return func() {
			calcExponentialRetry = old
		}
	}()()
	defer func() func() {
		old := waitDuration
		waitDuration = func(ctx context.Context, d time.Duration) {
			t.Error("bad")
			panic("unexpected")
		}
		return func() {
			waitDuration = old
		}
	}()()
	e := errors.New("some_error")
	result, err := ExponentialRetry(
		nil,
		0,
		func() (interface{}, error) {
			return "val", FatalError(FatalError(FatalError(e)))
		},
	)()
	if result != "val" || err != e {
		t.Fatal(result, err)
	}
}

func TestWaitDuration_neg(t *testing.T) {
	defer func() func() {
		startGoroutines := runtime.NumGoroutine()
		return func() {
			endGoroutines := runtime.NumGoroutine()
			if startGoroutines < endGoroutines {
				t.Error(startGoroutines, endGoroutines)
			}
		}
	}()()
	waitDuration(nil, -123124)
}

func TestWaitDuration_timeout(t *testing.T) {
	defer func() func() {
		time.Sleep(time.Millisecond * 200)
		startGoroutines := runtime.NumGoroutine()
		return func() {
			endGoroutines := runtime.NumGoroutine()
			if startGoroutines < endGoroutines {
				t.Error(startGoroutines, endGoroutines)
			}
		}
	}()()
	s := time.Now()
	waitDuration(context.Background(), time.Millisecond*100)
	d := time.Now().Sub(s)
	if d < time.Millisecond*60 || d > time.Millisecond*140 {
		t.Fatal(d)
	}
}

func TestWaitDuration_cancel(t *testing.T) {
	defer func() func() {
		time.Sleep(time.Millisecond * 200)
		startGoroutines := runtime.NumGoroutine()
		return func() {
			endGoroutines := runtime.NumGoroutine()
			if startGoroutines < endGoroutines {
				t.Error(startGoroutines, endGoroutines)
			}
		}
	}()()
	s := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	waitDuration(ctx, time.Second*3)
	d := time.Now().Sub(s)
	if d < time.Millisecond*60 || d > time.Millisecond*140 {
		t.Fatal(d)
	}
}

func TestCalcExponentialRetry(t *testing.T) {
	type TestCase struct {
		Seed  int64
		Rate  time.Duration
		Count uint32
		Retry time.Duration
	}
	testCases := []TestCase{
		{
			Seed:  1,
			Rate:  time.Second,
			Count: 0,
			Retry: 0,
		},
		{
			Seed:  0,
			Rate:  time.Second,
			Count: 0,
			Retry: 0,
		},
		{
			Seed:  2,
			Rate:  time.Second,
			Count: 0,
			Retry: 0,
		},
		{
			Seed:  1,
			Rate:  time.Second,
			Count: 1,
			Retry: 0,
		},
		{
			Seed:  2,
			Rate:  time.Second,
			Count: 1,
			Retry: time.Second,
		},
		{
			Seed:  0,
			Rate:  time.Second,
			Count: 4,
			Retry: time.Second,
		},
		{
			Seed:  1,
			Rate:  time.Second,
			Count: 4,
			Retry: time.Second * 2,
		},
		{
			Seed:  2,
			Rate:  time.Second,
			Count: 4,
			Retry: time.Second * 15,
		},
		{
			Seed:  3,
			Rate:  time.Second,
			Count: 4,
			Retry: time.Second * 5,
		},
		{
			Seed:  1,
			Rate:  time.Second,
			Count: 10,
			Retry: time.Second * 338,
		},
		{
			Seed:  1,
			Rate:  time.Second,
			Count: 31,
			Retry: time.Second * 134020434,
		},
		{
			Seed:  1,
			Rate:  time.Second,
			Count: 1424,
			Retry: time.Second * 134020434,
		},
	}
	for i, testCase := range testCases {
		name := fmt.Sprintf("TestCalcExponentialRetry_#%d", i+1)
		rand.Seed(testCase.Seed)
		retry := calcExponentialRetry(testCase.Rate, testCase.Count)
		if retry != testCase.Retry {
			t.Error(name, retry, int64(retry/testCase.Rate), testCase.Retry, int64(testCase.Retry/testCase.Rate))
		}
	}
}
