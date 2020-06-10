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
	"errors"
	"sync"
)

// Call will call value synchronously, with up to count concurrency (with other concurrent calls), note that it will
// panic if the receiver is nil, the count is <= 0, or the value is nil.
func (w *Workers) Call(count int, value func() (interface{}, error)) (interface{}, error) {
	w.ensure()
	w.check(count, value)
	output := make(chan struct {
		result interface{}
		error  error
	}, 1)
	w.mutex.Lock()
	if w.cond == nil {
		w.cond = sync.NewCond(&w.mutex)
	}
	w.queue = append(
		w.queue,
		&struct {
			value  func() (interface{}, error)
			output chan<- struct {
				result interface{}
				error  error
			}
		}{
			value:  value,
			output: output,
		},
	)
	w.target = count
	for w.count < count {
		w.count++
		go w.worker()
	}
	w.mutex.Unlock()
	result := <-output
	return result.result, result.error
}

// Wrap encapsulates the provided value as a worker call, note that it will panic if the receiver is nil, the count
// is <= 0, or the value is nil.
func (w *Workers) Wrap(count int, value func() (interface{}, error)) func() (interface{}, error) {
	w.ensure()
	w.check(count, value)
	return func() (interface{}, error) {
		return w.Call(count, value)
	}
}

// Wait will unblock when all workers are complete
func (w *Workers) Wait() {
	w.ensure()
	w.mutex.Lock()
	defer w.mutex.Unlock()
	for w.count != 0 {
		w.cond.Wait()
	}
}

// Count will return the number of workers currently running
func (w *Workers) Count() int {
	w.ensure()
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.count
}

func (w *Workers) ensure() {
	if w == nil {
		panic(errors.New("bigbuff.Workers nil receiver"))
	}
}

func (w *Workers) check(count int, value func() (interface{}, error)) {
	if count <= 0 {
		panic(errors.New("bigbuff.Workers count <= 0"))
	}
	if value == nil {
		panic(errors.New("bigbuff.Workers nil value"))
	}
}

func (w *Workers) worker() {
	for {
		w.mutex.Lock()
		if len(w.queue) == 0 || w.count > w.target {
			w.count--
			if w.count == 0 {
				w.cond.Broadcast()
			}
			w.mutex.Unlock()
			return
		}
		item := w.queue[0]
		w.queue[0] = nil
		w.queue = w.queue[1:]
		w.mutex.Unlock()
		func() {
			defer close(item.output)
			var result struct {
				result interface{}
				error  error
			}
			result.result, result.error = item.value()
			item.output <- result
		}()
	}
}
