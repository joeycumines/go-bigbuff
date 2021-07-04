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
	"errors"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestWorkers_Call(t *testing.T) {
	startGoroutines := runtime.NumGoroutine()
	defer func() {
		time.Sleep(time.Millisecond * 200)
		endGoroutines := runtime.NumGoroutine()
		if endGoroutines > startGoroutines {
			t.Error(startGoroutines, endGoroutines)
		}
	}()

	var (
		w    = new(Workers)
		m    sync.Mutex
		c    int
		d    bool
		v    = 12314
		e    = errors.New("some_error")
		done = make(chan struct{})
	)

	w.Wait()
	w.mutex.Lock()
	if w.cond != nil || w.count != 0 || w.target != 0 || w.queue != nil {
		t.Error(w)
	}
	w.mutex.Unlock()

	for x := 0; x < 10000; x++ {
		go func() {
			value, err := w.Call(
				20,
				func() (interface{}, error) {
					s := time.Millisecond * 200
					m.Lock()
					c++
					if d {
						s = 0
					}
					m.Unlock()
					time.Sleep(s)
					return v, e
				},
			)
			if value != v || err != e {
				t.Error(value, err)
			}
			m.Lock()
			c--
			m.Unlock()
		}()
	}

	time.Sleep(time.Millisecond * 100)

	go func() {
		w.Wait()
		close(done)
	}()

	m.Lock()
	if diff := math.Abs(float64(c) - 20); diff > 2 {
		t.Error(c)
	}
	if count := w.Count(); count != c {
		t.Error(count, c)
	}
	m.Unlock()

	go func() {
		value, err := w.Call(
			12,
			func() (interface{}, error) {
				s := time.Millisecond * 200
				m.Lock()
				c++
				if d {
					s = 0
				}
				m.Unlock()
				time.Sleep(s)
				return v, e
			},
		)
		if value != v || err != e {
			t.Error(value, err)
		}
		m.Lock()
		c--
		m.Unlock()
	}()

	time.Sleep(time.Millisecond * 400)

	select {
	case <-done:
		t.Error("expected not done")
	default:
	}

	m.Lock()
	if diff := math.Abs(float64(c) - 12); diff > 2 {
		t.Error(c)
	}
	if count := w.Count(); count != c {
		t.Error(count, c)
	}
	m.Unlock()

	go func() {
		value, err := w.Wrap(
			100,
			func() (interface{}, error) {
				s := time.Millisecond * 200
				m.Lock()
				c++
				if d {
					s = 0
				}
				m.Unlock()
				time.Sleep(s)
				return v, e
			},
		)()
		if value != v || err != e {
			t.Error(value, err)
		}
		m.Lock()
		c--
		m.Unlock()
	}()

	time.Sleep(time.Millisecond * 100)

	select {
	case <-done:
		t.Error("expected not done")
	default:
	}

	m.Lock()
	if diff := math.Abs(float64(c) - 100); diff > 10 {
		t.Error(c)
	}
	if count := w.Count(); count != c {
		t.Error(count, c)
	}
	m.Unlock()

	m.Lock()
	d = true
	m.Unlock()

	time.Sleep(time.Millisecond * 200)

	m.Lock()
	if c != 0 {
		t.Error(c)
	}
	m.Unlock()

	select {
	case <-done:
	default:
		t.Error("expected done")
	}

	if count := w.Count(); count != 0 {
		t.Error(count)
	}

	w.mutex.Lock()
	if len(w.queue) != 0 || w.target != 100 || w.count != 0 {
		t.Errorf("%+v", w)
	}
	w.mutex.Unlock()
}
