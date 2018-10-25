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
		w = new(Workers)
		m sync.Mutex
		c int
		d bool
		v = 12314
		e = errors.New("some_error")
	)

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

	m.Lock()
	if diff := math.Abs(float64(c) - 20); diff > 2 {
		t.Error(c)
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

	m.Lock()
	if diff := math.Abs(float64(c) - 12); diff > 2 {
		t.Error(c)
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

	m.Lock()
	if diff := math.Abs(float64(c) - 100); diff > 10 {
		t.Error(c)
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

	w.mutex.Lock()
	if len(w.queue) != 0 || w.target != 100 || w.count != 0 {
		t.Errorf("%+v", w)
	}
	w.mutex.Unlock()
}
