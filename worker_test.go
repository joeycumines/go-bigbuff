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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorker_Do_panic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error(`expected panic`)
		}
	}()
	new(Worker).Do(nil)
}

func TestWorker_Do(t *testing.T) {
	func() func() {
		n1 := runtime.NumGoroutine()
		return func() {
			time.Sleep(time.Millisecond * 100)
			if n2 := runtime.NumGoroutine(); n2 > n1 {
				t.Error(n1, n2)
			}
		}
	}()()

	var (
		w     = new(Worker)
		count int32
		in    = make(chan []int)
		out   = make(chan int)
		sum   = func(v ...int) int {
			defer w.Do(func(done <-chan struct{}) {
				if done == nil {
					t.Fatal(`nil done`)
				}
				if v := atomic.AddInt32(&count, 1); v != 1 {
					t.Fatal(v)
				}
				for {
					select {
					case v := <-in:
						var sum int
						for _, v := range v {
							sum += v
						}
						out <- sum
						continue
					case <-done:
					}
					break
				}
				if v := atomic.AddInt32(&count, -1); v != 0 {
					t.Fatal(v)
				}
			})()
			in <- v
			return <-out
		}
	)

	for x := 0; x < 3; x++ {
		for x := 0; x < 5; x++ {
			var wg sync.WaitGroup
			for i, tc := range []struct {
				In  []int
				Out int
			}{
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
				{[]int{1, 5, -2}, 4},
				{[]int{0, 0, 4, 1}, 5},
				{[]int{}, 0},
			} {
				i, tc := i, tc
				wg.Add(1)
				go func() {
					defer wg.Done()
					if v := sum(tc.In...); v != tc.Out {
						t.Error(i, v)
					}
				}()
			}
			wg.Wait()
		}
		time.Sleep(time.Millisecond * 50)
		w.mu.Lock()
		if w.wg != nil || w.stop != nil || w.done != nil {
			t.Error(w.wg, w.stop, w.done)
		}
		w.mu.Unlock()
	}
}
